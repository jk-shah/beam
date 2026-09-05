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

// Package main demonstrates a windowed Top-N ranking per category pipeline
// using Apache Beam with PostgreSQL.
//
// Spark Equivalent:
//
//	val windowSpec = Window.partitionBy("category").orderBy(col("sales_volume").desc)
//	df.withColumn("rank", dense_rank().over(windowSpec))
//	  .filter(col("rank") <= 3)
//	  .write.format("jdbc").mode("overwrite").save()
//
// Use Case:
// Identifying the top-performing products, merchants, or campaigns within each
// discrete partition or department is a fundamental analytical operation across
// merchandising and catalog management.
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
	topK     = flag.Int("top_k", 3, "Number of top ranked items to preserve per category")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*ProductRecord)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*RankedProduct)(nil)).Elem())
	beam.RegisterDoFn(&keyByCategoryFn{})
	beam.RegisterDoFn(&rankTopNProductsFn{})
}

// ProductRecord represents an incoming catalog sales record.
type ProductRecord struct {
	ProductID   string  `json:"product_id" db:"product_id" beam:"product_id"`
	ProductName string  `json:"product_name" db:"product_name" beam:"product_name"`
	Category    string  `json:"category" db:"category" beam:"category"`
	SalesVolume float64 `json:"sales_volume" db:"sales_volume" beam:"sales_volume"`
}

// RankedProduct represents the top-ranked item written to PostgreSQL.
type RankedProduct struct {
	Category     string    `json:"category" db:"category" beam:"category"`
	RankPosition int32     `json:"rank_position" db:"rank_position" beam:"rank_position"`
	ProductID    string    `json:"product_id" db:"product_id" beam:"product_id"`
	ProductName  string    `json:"product_name" db:"product_name" beam:"product_name"`
	SalesVolume  float64   `json:"sales_volume" db:"sales_volume" beam:"sales_volume"`
	UpdatedAt    time.Time `json:"updated_at" db:"updated_at" beam:"updated_at"`
}

type keyByCategoryFn struct{}

func (fn *keyByCategoryFn) ProcessElement(p ProductRecord, emit func(string, ProductRecord)) {
	emit(p.Category, p)
}

type rankTopNProductsFn struct {
	TopK int `json:"top_k"`
}

func (fn *rankTopNProductsFn) ProcessElement(category string, products func(*ProductRecord) bool, emit func(RankedProduct)) {
	var list []ProductRecord
	var p ProductRecord
	for products(&p) {
		list = append(list, p)
	}

	// Sort descending by sales volume
	sort.Slice(list, func(i, j int) bool {
		return list[i].SalesVolume > list[j].SalesVolume
	})

	limit := fn.TopK
	if limit <= 0 {
		limit = 3
	}
	if len(list) < limit {
		limit = len(list)
	}

	now := time.Now().UTC()
	for rank := 1; rank <= limit; rank++ {
		item := list[rank-1]
		emit(RankedProduct{
			Category:     category,
			RankPosition: int32(rank),
			ProductID:    item.ProductID,
			ProductName:  item.ProductName,
			SalesVolume:  item.SalesVolume,
			UpdatedAt:    now,
		})
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	sampleCatalog := []ProductRecord{
		// Electronics
		{ProductID: "PROD-101", ProductName: "Cloud Laptop Pro", Category: "ELECTRONICS", SalesVolume: 185000.00},
		{ProductID: "PROD-102", ProductName: "Noise Cancelling Headphones", Category: "ELECTRONICS", SalesVolume: 42000.00},
		{ProductID: "PROD-103", ProductName: "4K Gaming Monitor", Category: "ELECTRONICS", SalesVolume: 96000.00},
		{ProductID: "PROD-104", ProductName: "Ergonomic Mechanical Keyboard", Category: "ELECTRONICS", SalesVolume: 28000.00},
		// Furniture
		{ProductID: "PROD-201", ProductName: "Standing Desk 60in", Category: "FURNITURE", SalesVolume: 120000.00},
		{ProductID: "PROD-202", ProductName: "Executive Leather Chair", Category: "FURNITURE", SalesVolume: 145000.00},
		{ProductID: "PROD-203", ProductName: "Filing Cabinet Metal", Category: "FURNITURE", SalesVolume: 15000.00},
		{ProductID: "PROD-204", ProductName: "Monitor Arm Dual Mount", Category: "FURNITURE", SalesVolume: 35000.00},
	}

	catalogCol := beam.CreateList(s, sampleCatalog)

	// 1. Key by partition column (Category)
	keyedCol := beam.ParDo(s, &keyByCategoryFn{}, catalogCol)

	// 2. Group within category
	groupedCol := beam.GroupByKey(s, keyedCol)

	// 3. Apply Window Ranking DoFn to extract Top-K products
	rankedCol := beam.ParDo(s, &rankTopNProductsFn{TopK: *topK}, groupedCol)

	// 4. Sink to PostgreSQL with upsert on composite key (category, rank_position)
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"category", "rank_position"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.top_products_by_category", writeOpts, rankedCol)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Top-N window ranking pipeline completed successfully.")
}
