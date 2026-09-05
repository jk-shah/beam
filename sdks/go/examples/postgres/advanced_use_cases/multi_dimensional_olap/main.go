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

// Package main demonstrates an advanced multi-dimensional OLAP aggregation and rollup pipeline
// using Apache Beam with PostgreSQL.
//
// Pattern:
// Ingest granular transactions, aggregate across multiple orthogonal dimensions
// (region and category), compute statistical rollups (total revenue, count, average,
// and max order value), and write atomic multi-dimensional cube records to PostgreSQL.
// Multi-dimensional aggregations summarize high-volume transactions into regional
// and categorical performance metrics for executive reporting and BI dashboards.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
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
)

func init() {
	beam.RegisterType(reflect.TypeOf((*SaleTransaction)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*OlapCubeRow)(nil)).Elem())
	beam.RegisterDoFn(&keyByRegionCategoryFn{})
	beam.RegisterDoFn(&aggregateOlapCubeFn{})
}

// SaleTransaction represents an incoming transactional sales record.
type SaleTransaction struct {
	TransactionID string    `json:"transaction_id" db:"transaction_id" beam:"transaction_id"`
	Region        string    `json:"region" db:"region" beam:"region"`
	Category      string    `json:"category" db:"category" beam:"category"`
	Amount        float64   `json:"amount" db:"amount" beam:"amount"`
	Timestamp     time.Time `json:"timestamp" db:"timestamp" beam:"timestamp"`
}

// OlapCubeRow represents a summary cell in the multi-dimensional sales cube.
type OlapCubeRow struct {
	Region        string    `json:"region" db:"region" beam:"region"`
	Category      string    `json:"category" db:"category" beam:"category"`
	TotalRevenue  float64   `json:"total_revenue" db:"total_revenue" beam:"total_revenue"`
	OrderCount    int64     `json:"order_count" db:"order_count" beam:"order_count"`
	AvgOrderValue float64   `json:"avg_order_value" db:"avg_order_value" beam:"avg_order_value"`
	MaxOrderValue float64   `json:"max_order_value" db:"max_order_value" beam:"max_order_value"`
	ComputedAt    time.Time `json:"computed_at" db:"computed_at" beam:"computed_at"`
}

type keyByRegionCategoryFn struct{}

func (fn *keyByRegionCategoryFn) ProcessElement(t SaleTransaction, emit func(string, SaleTransaction)) {
	key := fmt.Sprintf("%s|%s", t.Region, t.Category)
	emit(key, t)
}

type aggregateOlapCubeFn struct{}

func (fn *aggregateOlapCubeFn) ProcessElement(key string, txns func(*SaleTransaction) bool, emit func(OlapCubeRow)) {
	parts := strings.Split(key, "|")
	if len(parts) != 2 {
		return
	}
	region := parts[0]
	category := parts[1]

	var count int64
	var sum float64
	var maxVal float64 = -1.0

	var t SaleTransaction
	for txns(&t) {
		count++
		sum += t.Amount
		if t.Amount > maxVal {
			maxVal = t.Amount
		}
	}

	if count == 0 {
		return
	}

	avg := sum / float64(count)
	// Round to two decimal places
	sum = math.Round(sum*100) / 100
	avg = math.Round(avg*100) / 100
	maxVal = math.Round(maxVal*100) / 100

	emit(OlapCubeRow{
		Region:        region,
		Category:      category,
		TotalRevenue:  sum,
		OrderCount:    count,
		AvgOrderValue: avg,
		MaxOrderValue: maxVal,
		ComputedAt:    time.Now().UTC(),
	})
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	now := time.Now().UTC()

	// Sample transactions across regions and categories
	sampleData := []SaleTransaction{
		{TransactionID: "T-01", Region: "US-EAST", Category: "ELECTRONICS", Amount: 500.00, Timestamp: now},
		{TransactionID: "T-02", Region: "US-EAST", Category: "ELECTRONICS", Amount: 300.00, Timestamp: now},
		{TransactionID: "T-03", Region: "US-EAST", Category: "FURNITURE", Amount: 120.00, Timestamp: now},
		{TransactionID: "T-04", Region: "US-WEST", Category: "ELECTRONICS", Amount: 750.00, Timestamp: now},
		{TransactionID: "T-05", Region: "US-WEST", Category: "FURNITURE", Amount: 450.00, Timestamp: now},
		{TransactionID: "T-06", Region: "US-WEST", Category: "FURNITURE", Amount: 350.00, Timestamp: now},
		{TransactionID: "T-07", Region: "EU-WEST", Category: "ELECTRONICS", Amount: 900.00, Timestamp: now},
		{TransactionID: "T-08", Region: "EU-WEST", Category: "GROCERY", Amount: 45.00, Timestamp: now},
		{TransactionID: "T-09", Region: "EU-WEST", Category: "GROCERY", Amount: 65.00, Timestamp: now},
	}

	transactionsCol := beam.CreateList(s, sampleData)

	// 1. Key by composite dimension (Region | Category)
	keyedCol := beam.ParDo(s, &keyByRegionCategoryFn{}, transactionsCol)

	// 2. Group all transactions in each dimensional cube slice
	groupedCol := beam.GroupByKey(s, keyedCol)

	// 3. Compute multi-metric aggregations
	cubeCol := beam.ParDo(s, &aggregateOlapCubeFn{}, groupedCol)

	// 4. Sink to PostgreSQL with atomic upsert on (region, category)
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"region", "category"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.olap_sales_cube", writeOpts, cubeCol)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Multi-dimensional OLAP aggregation pipeline completed successfully.")
}
