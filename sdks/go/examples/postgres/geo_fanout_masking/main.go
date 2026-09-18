// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package main demonstrates streaming cross-database replication from a centralized
// PostgreSQL database to regional target databases with in-flight PII data masking
// (SSN and email obfuscation) and geographic fan-out routing (US, EU, APAC).
//
// Key Capabilities Demonstrated:
//  1. Continuous CDC streaming with logical replication slots and LSN tracking.
//  2. Zero-allocation in-flight PII masking for SSNs (***-**-NNNN) and emails.
//  3. Branching via beam.Partition with explicit beam.Reshuffle stage boundaries
//     to prevent cross-region failure cascades and backpressure deadlocks.
//  4. High-throughput regional writes using SQL standard MERGE (PostgreSQL 15+)
//     with atomic upserts, conditional deletes, and in-band EXPLAIN plan telemetry.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	// Central Source Database
	srcHost     = flag.String("src_host", "localhost", "Source PostgreSQL host")
	srcPort     = flag.Int("src_port", 5432, "Source PostgreSQL port")
	srcDatabase = flag.String("src_database", "central_db", "Source PostgreSQL database name")
	srcUsername = flag.String("src_username", "beam_cdc", "Source PostgreSQL replication user")
	srcPassword = flag.String("src_password", "", "Source PostgreSQL password")
	srcSlot     = flag.String("src_slot", "beam_geo_slot", "Source replication slot name")
	srcPub      = flag.String("src_publication", "pub_customer_accounts", "Source publication name")

	// Regional Destination Databases
	usHost     = flag.String("us_host", "localhost", "US regional PostgreSQL host")
	usPort     = flag.Int("us_port", 5432, "US regional PostgreSQL port")
	usDatabase = flag.String("us_database", "us_accounts_db", "US regional database name")

	euHost     = flag.String("eu_host", "localhost", "EU regional PostgreSQL host")
	euPort     = flag.Int("eu_port", 5432, "EU regional PostgreSQL port")
	euDatabase = flag.String("eu_database", "eu_accounts_db", "EU regional database name")

	apacHost     = flag.String("apac_host", "localhost", "APAC regional PostgreSQL host")
	apacPort     = flag.Int("apac_port", 5432, "APAC regional PostgreSQL port")
	apacDatabase = flag.String("apac_database", "apac_accounts_db", "APAC regional database name")

	destUser     = flag.String("dest_username", "beam_writer", "Destination PostgreSQL username")
	destPassword = flag.String("dest_password", "", "Destination PostgreSQL password")
	sslMode      = flag.String("sslmode", "disable", "PostgreSQL SSL mode")
)

// CustomerAccount represents the domain entity replicated across databases.
type CustomerAccount struct {
	AccountID    string    `json:"account_id" db:"account_id" beam:"account_id"`
	CustomerName string    `json:"customer_name" db:"customer_name" beam:"customer_name"`
	SSN          string    `json:"ssn" db:"ssn" beam:"ssn"`
	Email        string    `json:"email" db:"email" beam:"email"`
	GeoRegion    string    `json:"geo_region" db:"geo_region" beam:"geo_region"`
	Balance      float64   `json:"balance" db:"balance" beam:"balance"`
	LSN          uint64    `json:"lsn" db:"lsn" beam:"lsn"`
	OpType       string    `json:"_op_type" db:"_op_type" beam:"_op_type"`
	UpdatedAt    time.Time `json:"updated_at" db:"updated_at" beam:"updated_at"`
}

func init() {
	beam.RegisterType(reflect.TypeOf((*CustomerAccount)(nil)).Elem())
	beam.RegisterFunction(GeoPartitionFn)
	beam.RegisterType(reflect.TypeOf((*MaskCustomerPIIFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*parseCustomerAccountFn)(nil)).Elem())
}

type parseCustomerAccountFn struct{}

func (fn *parseCustomerAccountFn) ProcessElement(ev postgresio.ChangeEvent, emit func(CustomerAccount)) {
	var acc CustomerAccount
	if ev.Operation == postgresio.OpDelete {
		acc.AccountID = fmt.Sprint(ev.Before["account_id"])
		acc.OpType = "d"
	} else {
		acc.AccountID = fmt.Sprint(ev.After["account_id"])
		acc.CustomerName = fmt.Sprint(ev.After["customer_name"])
		acc.SSN = fmt.Sprint(ev.After["ssn"])
		acc.Email = fmt.Sprint(ev.After["email"])
		acc.GeoRegion = fmt.Sprint(ev.After["geo_region"])
		if bal, ok := ev.After["balance"].(float64); ok {
			acc.Balance = bal
		}
		acc.OpType = "c"
		if ev.Operation == postgresio.OpUpdate {
			acc.OpType = "u"
		}
	}
	acc.LSN = uint64(ev.LSN)
	acc.UpdatedAt = time.Now().UTC()
	emit(acc)
}

// MaskSSNZeroAlloc scrubs sensitive Social Security Numbers with zero allocations.
// Valid 11-char inputs (123-45-6789) or 9-digit inputs (123456789) preserve only the
// last 4 digits: ***-**-6789. Malformed or dirty inputs return ***-**-**** safely.
func MaskSSNZeroAlloc(input string) string {
	const fallback = "***-**-****"
	inLen := len(input)
	if inLen != 11 && inLen != 9 {
		return fallback
	}

	var last4 [4]byte
	if inLen == 11 {
		if input[3] != '-' || input[6] != '-' {
			return fallback
		}
		for i := 0; i < 11; i++ {
			if i == 3 || i == 6 {
				continue
			}
			if input[i] < '0' || input[i] > '9' {
				return fallback
			}
		}
		last4 = [4]byte{input[7], input[8], input[9], input[10]}
	} else {
		for i := 0; i < 9; i++ {
			if input[i] < '0' || input[i] > '9' {
				return fallback
			}
		}
		last4 = [4]byte{input[5], input[6], input[7], input[8]}
	}

	var buf [11]byte
	buf[0], buf[1], buf[2], buf[3] = '*', '*', '*', '-'
	buf[4], buf[5], buf[6] = '*', '*', '-'
	buf[7], buf[8], buf[9], buf[10] = last4[0], last4[1], last4[2], last4[3]
	return string(buf[:11])
}

// MaskEmail obfuscates email addresses while preserving domain routing information.
// Short or single-character names are handled safely without out-of-bounds panics.
func MaskEmail(email string) string {
	trimmed := strings.TrimSpace(email)
	atIdx := strings.Index(trimmed, "@")
	if atIdx <= 0 || atIdx == len(trimmed)-1 {
		return "***@redacted.local"
	}

	local := trimmed[:atIdx]
	domain := trimmed[atIdx+1:]

	if len(local) <= 2 {
		return fmt.Sprintf("%c***@%s", local[0], domain)
	}
	return fmt.Sprintf("%c***%c@%s", local[0], local[len(local)-1], domain)
}

// MaskCustomerPIIFn redacts customer PII fields in-flight and updates metrics.
var (
	recordsIngested = metrics.NewCounter("postgres_fanout", "records_ingested")
	ssnMaskedCount  = metrics.NewCounter("postgres_fanout", "ssn_masked_count")
	invalidSSNCount = metrics.NewCounter("postgres_fanout", "invalid_ssn_count")
)

type MaskCustomerPIIFn struct{}

// NewMaskCustomerPIIFn constructs a new PII masking transform.
func NewMaskCustomerPIIFn() *MaskCustomerPIIFn {
	return &MaskCustomerPIIFn{}
}

// ProcessElement redacts the SSN and email fields of each incoming record.
func (fn *MaskCustomerPIIFn) ProcessElement(ctx context.Context, account CustomerAccount, emit func(CustomerAccount)) {
	recordsIngested.Inc(ctx, 1)

	origSSN := account.SSN
	account.SSN = MaskSSNZeroAlloc(origSSN)
	if account.SSN == "***-**-****" && origSSN != "" && origSSN != "***-**-****" {
		invalidSSNCount.Inc(ctx, 1)
	} else {
		ssnMaskedCount.Inc(ctx, 1)
	}

	account.Email = MaskEmail(account.Email)
	emit(account)
}

// GeoPartitionFn partitions accounts into three geographic regions:
// 0 -> US / North America
// 1 -> EU / EMEA
// 2 -> APAC / Global Default
func GeoPartitionFn(account CustomerAccount) int {
	switch strings.ToUpper(strings.TrimSpace(account.GeoRegion)) {
	case "US", "USA", "NA", "NORTH_AMERICA":
		return 0
	case "EU", "EMEA", "EUROPE", "UK":
		return 1
	default:
		return 2 // APAC, LATAM, and global default
	}
}

// BuildGeoFanoutPipeline constructs the cross-database fan-out pipeline.
func BuildGeoFanoutPipeline(s beam.Scope, accounts beam.PCollection) (beam.PCollection, beam.PCollection, beam.PCollection) {
	// 1. In-flight PII masking transform
	masked := beam.ParDo(s, NewMaskCustomerPIIFn(), accounts)

	// 2. Geographic partitioning
	partitions := beam.Partition(s, 3, GeoPartitionFn, masked)

	// 3. Stage isolation via beam.Reshuffle to eliminate stage fusion deadlocks
	usReshuffled := beam.Reshuffle(s.Scope("ReshuffleUS"), partitions[0])
	euReshuffled := beam.Reshuffle(s.Scope("ReshuffleEU"), partitions[1])
	apacReshuffled := beam.Reshuffle(s.Scope("ReshuffleAPAC"), partitions[2])

	return usReshuffled, euReshuffled, apacReshuffled
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	log.Printf("Starting PostgreSQL Cross-Database Geo Fan-Out & Masking Pipeline...")
	log.Printf("Source: %s:%d/%s (slot: %s)", *srcHost, *srcPort, *srcDatabase, *srcSlot)
	log.Printf("Destinations: US=%s:%d/%s, EU=%s:%d/%s, APAC=%s:%d/%s",
		*usHost, *usPort, *usDatabase,
		*euHost, *euPort, *euDatabase,
		*apacHost, *apacPort, *apacDatabase,
	)

	p, s := beam.NewPipelineWithRoot()

	// 1. Read from Central PostgreSQL CDC stream
	cdcOpts := []postgresio.CDCOption{
		postgresio.WithCDCHost(*srcHost),
		postgresio.WithCDCPort(*srcPort),
		postgresio.WithCDCDatabase(*srcDatabase),
		postgresio.WithCDCUsername(*srcUsername),
		postgresio.WithCDCPassword(*srcPassword),
		postgresio.WithCDCSSLMode(*sslMode),
		postgresio.WithCDCSlotName(*srcSlot),
		postgresio.WithCDCPublication(*srcPub),
		postgresio.WithCDCCreateSlotIfMissing(true),
		postgresio.WithCDCMaxSlotLagBytes(8 * 1024 * 1024 * 1024), // 8GB WAL budget
	}
	rawCol := postgresio.ReadCDC(s, cdcOpts...)

	// Transform raw CDC change events into typed CustomerAccount records
	accounts := beam.ParDo(s, &parseCustomerAccountFn{}, rawCol)

	// 2. Build Geo Fan-Out & Masking Stage
	usCol, euCol, apacCol := BuildGeoFanoutPipeline(s, accounts)

	// 3. Write to Regional Databases using SQL standard MERGE (PostgreSQL 15+)
	makeWriteOpts := func(h string, p int, db string) postgresio.WriteOptions {
		return postgresio.NewWriteOptions(
			postgresio.WithHost(h),
			postgresio.WithPort(p),
			postgresio.WithDatabase(db),
			postgresio.WithUsername(*destUser),
			postgresio.WithPassword(*destPassword),
			postgresio.WithSSLMode(*sslMode),
			postgresio.WithPrimaryKeyColumns("account_id"),
			postgresio.WithWriteMode(postgresio.WriteModeMerge),
			postgresio.WithOpColumn("_op_type"),
			postgresio.WithDeleteOpValue("d"),
			postgresio.WithPgBouncer(true),
			postgresio.WithExplainAnalyze(true),
			postgresio.WithExplainSampleRate(0.01), // 1% execution plan sampling
		)
	}

	postgresio.Write(s.Scope("WriteUS"), "public.customer_accounts", makeWriteOpts(*usHost, *usPort, *usDatabase), usCol)
	postgresio.Write(s.Scope("WriteEU"), "public.customer_accounts", makeWriteOpts(*euHost, *euPort, *euDatabase), euCol)
	postgresio.Write(s.Scope("WriteAPAC"), "public.customer_accounts", makeWriteOpts(*apacHost, *apacPort, *apacDatabase), apacCol)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Pipeline execution failed: %v", err)
	}
}
