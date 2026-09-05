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

// Package main demonstrates a Slowly Changing Dimension Type 2 (SCD2) historical
// audit pipeline using Apache Beam with PostgreSQL.
//
// Use Case:
// In analytics and financial reporting, tracking customer profile changes over time
// is mandatory. Instead of destructively overwriting dimension attributes in place,
// SCD Type 2 preserves the full history by generating version numbers, effective
// date ranges (valid_from, valid_to), and an is_current active flag.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"sort"
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
)

func init() {
	beam.RegisterType(reflect.TypeOf((*CustomerProfileUpdate)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*CustomerDimHistory)(nil)).Elem())
	beam.RegisterDoFn(&keyByCustomerFn{})
	beam.RegisterDoFn(&generateSCD2HistoryFn{})
}

// CustomerProfileUpdate represents an incoming customer state modification.
type CustomerProfileUpdate struct {
	CustomerID string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	FullName   string    `json:"full_name" db:"full_name" beam:"full_name"`
	Tier       string    `json:"tier" db:"tier" beam:"tier"`
	Address    string    `json:"address" db:"address" beam:"address"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at" beam:"updated_at"`
}

// CustomerDimHistory represents the versioned SCD2 record stored in PostgreSQL.
type CustomerDimHistory struct {
	CustomerID string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	Version    int32     `json:"version" db:"version" beam:"version"`
	FullName   string    `json:"full_name" db:"full_name" beam:"full_name"`
	Tier       string    `json:"tier" db:"tier" beam:"tier"`
	Address    string    `json:"address" db:"address" beam:"address"`
	ValidFrom  time.Time `json:"valid_from" db:"valid_from" beam:"valid_from"`
	ValidTo    time.Time `json:"valid_to" db:"valid_to" beam:"valid_to"`
	IsCurrent  bool      `json:"is_current" db:"is_current" beam:"is_current"`
}

// keyByCustomerFn keys each profile update by customer_id for co-grouping.
type keyByCustomerFn struct{}

func (fn *keyByCustomerFn) ProcessElement(u CustomerProfileUpdate, emit func(string, CustomerProfileUpdate)) {
	emit(u.CustomerID, u)
}

// generateSCD2HistoryFn receives all updates for a customer sorted by timestamp
// and constructs sequential SCD2 versioned records.
type generateSCD2HistoryFn struct{}

func (fn *generateSCD2HistoryFn) ProcessElement(customerID string, updates func(*CustomerProfileUpdate) bool, emit func(CustomerDimHistory)) {
	var updateList []CustomerProfileUpdate
	var item CustomerProfileUpdate
	for updates(&item) {
		updateList = append(updateList, item)
	}

	// Sort chronologically by update timestamp
	sort.Slice(updateList, func(i, j int) bool {
		return updateList[i].UpdatedAt.Before(updateList[j].UpdatedAt)
	})

	farFuture := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	n := len(updateList)

	for i, u := range updateList {
		version := int32(i + 1)
		validFrom := u.UpdatedAt.UTC()
		var validTo time.Time
		isCurrent := false

		if i == n-1 {
			// Latest active record
			validTo = farFuture
			isCurrent = true
		} else {
			// Historical superseded record ends when next record starts
			validTo = updateList[i+1].UpdatedAt.UTC()
		}

		emit(CustomerDimHistory{
			CustomerID: customerID,
			Version:    version,
			FullName:   u.FullName,
			Tier:       u.Tier,
			Address:    u.Address,
			ValidFrom:  validFrom,
			ValidTo:    validTo,
			IsCurrent:  isCurrent,
		})
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Sample updates simulating account tier promotions and address changes over time
	sampleUpdates := []CustomerProfileUpdate{
		{CustomerID: "CUST-100", FullName: "Jane Doe", Tier: "STANDARD", Address: "123 Main St", UpdatedAt: baseTime},
		{CustomerID: "CUST-100", FullName: "Jane Doe", Tier: "GOLD", Address: "123 Main St", UpdatedAt: baseTime.Add(30 * 24 * time.Hour)},
		{CustomerID: "CUST-100", FullName: "Jane Doe", Tier: "PLATINUM", Address: "456 Market St", UpdatedAt: baseTime.Add(90 * 24 * time.Hour)},
		{CustomerID: "CUST-200", FullName: "John Smith", Tier: "STANDARD", Address: "789 Pine Ave", UpdatedAt: baseTime.Add(5 * 24 * time.Hour)},
		{CustomerID: "CUST-200", FullName: "John Smith", Tier: "VIP", Address: "789 Pine Ave", UpdatedAt: baseTime.Add(45 * 24 * time.Hour)},
	}

	rawCol := beam.CreateList(s, sampleUpdates)

	// 1. Key by customer ID
	keyedCol := beam.ParDo(s, &keyByCustomerFn{}, rawCol)

	// 2. Group all updates for the same customer
	groupedCol := beam.GroupByKey(s, keyedCol)

	// 3. Generate sequential SCD2 versioned history records
	scd2Col := beam.ParDo(s, &generateSCD2HistoryFn{}, groupedCol)

	// 4. Sink to PostgreSQL dimension history table with upsert on (customer_id, version)
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"customer_id", "version"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.customer_dim_history", writeOpts, scd2Col)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Slowly Changing Dimensions (SCD2) pipeline completed successfully.")
}
