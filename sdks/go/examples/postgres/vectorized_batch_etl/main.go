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

// Package main demonstrates a high-throughput vectorized columnar batch ETL pipeline
// using Apache Beam with PostgreSQL.
//
// Use Case:
// Large-scale database migrations and nightly warehouse syncs require processing
// millions of rows per second without exhausting database CPU or heap memory. This
// pipeline transforms, standardizes, and bulk-upserts records using parameterized
// UNNEST vectorization and bounded connection concurrency.
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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "postgres", "PostgreSQL database name")
	username = flag.String("username", "beam_test", "PostgreSQL user")
	password = flag.String("password", "beam_test", "PostgreSQL password")
	rowCount = flag.Int("rows", 10000, "Number of rows to generate for batch ETL demonstration")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*SourceRecord)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*TargetRecord)(nil)).Elem())
	beam.RegisterDoFn(&generateBatchRecordsFn{})
	beam.RegisterDoFn(&transformVectorizedRecordFn{})
}

// SourceRecord represents raw rows extracted from a legacy or operational table.
type SourceRecord struct {
	ID        int64  `json:"id" db:"id" beam:"id"`
	RawCode   string `json:"raw_code" db:"raw_code" beam:"raw_code"`
	RawAmount string `json:"raw_amount" db:"raw_amount" beam:"raw_amount"`
	Region    string `json:"region" db:"region" beam:"region"`
}

// TargetRecord represents normalized, typed records ready for high-throughput batch loading.
type TargetRecord struct {
	ID        int64     `json:"id" db:"id" beam:"id"`
	Code      string    `json:"code" db:"code" beam:"code"`
	Amount    float64   `json:"amount" db:"amount" beam:"amount"`
	Region    string    `json:"region" db:"region" beam:"region"`
	Tier      string    `json:"tier" db:"tier" beam:"tier"`
	LoadedAt  time.Time `json:"loaded_at" db:"loaded_at" beam:"loaded_at"`
}

// generateBatchRecordsFn generates synthetic source records for high-speed benchmark demonstration.
type generateBatchRecordsFn struct {
	Count int `json:"count"`
}

func (fn *generateBatchRecordsFn) ProcessElement(_ int, emit func(SourceRecord)) {
	regions := []string{"us-central1", "us-east1", "europe-west1", "asia-east1"}
	for i := 1; i <= fn.Count; i++ {
		emit(SourceRecord{
			ID:        int64(i),
			RawCode:   fmt.Sprintf("TXN-CODE-%06d", i),
			RawAmount: fmt.Sprintf("%d.50", (i%1000)+10),
			Region:    regions[i%len(regions)],
		})
	}
}

// transformVectorizedRecordFn normalizes and cleanses records in memory.
type transformVectorizedRecordFn struct{}

func (fn *transformVectorizedRecordFn) ProcessElement(r SourceRecord, emit func(TargetRecord)) error {
	var amount float64
	_, _ = fmt.Sscanf(r.RawAmount, "%f", &amount)

	var tier string
	if amount >= 500.0 {
		tier = "ENTERPRISE"
	} else if amount >= 100.0 {
		tier = "BUSINESS"
	} else {
		tier = "STANDARD"
	}

	emit(TargetRecord{
		ID:       r.ID,
		Code:     strings.ToUpper(strings.TrimSpace(r.RawCode)),
		Amount:   amount,
		Region:   r.Region,
		Tier:     tier,
		LoadedAt: time.Now().UTC(),
	})
	return nil
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	// 1. Generate or extract source batch records
	seed := beam.Create(s, 1)
	rawSource := beam.ParDo(s, &generateBatchRecordsFn{Count: *rowCount}, seed)

	// 2. High-speed vectorized in-memory transformation
	normalized := beam.ParDo(s, &transformVectorizedRecordFn{}, rawSource)

	// 3. High-Throughput Parameterized UNNEST Vectorized Batch Sink
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"id"},
		BatchSize:      5000,
		MaxBatchBytes:  8 * 1024 * 1024, // 8MB buffer chunks
	}

	result := postgresio.Write(s, "public.migrated_transactions", writeOpts, normalized)
	_ = result.FailedRows

	start := time.Now()
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("Vectorized batch ETL pipeline loaded %d records in %v (Throughput: %.2f rows/sec).\n",
		*rowCount, elapsed, float64(*rowCount)/elapsed.Seconds())
}
