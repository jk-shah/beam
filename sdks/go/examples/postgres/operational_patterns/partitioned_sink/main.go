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

// Package main demonstrates writing to declarative partitioned PostgreSQL tables
// (PARTITION BY RANGE) using composite conflict targets.
//
// Operational Problem:
// In PostgreSQL declarative partitioning, a primary key or unique index on a
// partitioned table MUST include all partitioning key columns.
// If an application or pipeline attempts an upsert with:
//   INSERT INTO orders_partitioned (...) VALUES (...) ON CONFLICT (order_id) DO UPDATE ...
// PostgreSQL rejects the statement with:
//   ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification
//
// Remediation:
//   1. Configure the composite conflict target containing both the entity identifier
//      and the partition key: WithPrimaryKeyColumns("order_id", "order_date").
//   2. Shard data by partition key upstream (beam.KeyBy + beam.Reshuffle) so each
//      worker batch writes to localized partition child tables, eliminating cross-partition
//      lock contention and heap buffer churning.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
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
	table    = flag.String("table", "public.orders_partitioned", "Partitioned target table")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*PartitionedOrder)(nil)).Elem())
	beam.RegisterDoFn(&generatePartitionedOrdersFn{})
	beam.RegisterDoFn(&keyByPartitionDateFn{})
	beam.RegisterDoFn(&flattenPartitionGroupFn{})
}

// PartitionedOrder represents an order record destined for a partitioned table.
type PartitionedOrder struct {
	OrderID     int64     `json:"order_id" db:"order_id" beam:"order_id"`
	OrderDate   string    `json:"order_date" db:"order_date" beam:"order_date"` // YYYY-MM-DD
	CustomerID  string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	TotalAmount float64   `json:"total_amount" db:"total_amount" beam:"total_amount"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at" beam:"updated_at"`
}

type generatePartitionedOrdersFn struct{}

func (fn *generatePartitionedOrdersFn) ProcessElement(id int64, emit func(PartitionedOrder)) {
	// Generate dates across multiple partition boundaries (e.g. 2026-09-01, 2026-09-02)
	dayOffset := id % 3
	dateStr := fmt.Sprintf("2026-09-%02d", dayOffset+1)
	emit(PartitionedOrder{
		OrderID:     id,
		OrderDate:   dateStr,
		CustomerID:  fmt.Sprintf("USER-%04d", id*7%1000),
		TotalAmount: 49.99 * float64(id%5+1),
		UpdatedAt:   time.Now().UTC(),
	})
}

type keyByPartitionDateFn struct{}

func (fn *keyByPartitionDateFn) ProcessElement(order PartitionedOrder, emit func(string, PartitionedOrder)) {
	emit(order.OrderDate, order)
}

type flattenPartitionGroupFn struct{}

func (fn *flattenPartitionGroupFn) ProcessElement(date string, orders func(*PartitionedOrder) bool, emit func(PartitionedOrder)) {
	var o PartitionedOrder
	for orders(&o) {
		emit(o)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// 1. Generate multi-day partition order data
	orderIDs := beam.CreateList(s, []int64{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009})
	orders := beam.ParDo(s, &generatePartitionedOrdersFn{}, orderIDs)

	// 2. Upstream Partition Localization
	// Grouping by OrderDate ensures each worker batch focuses on a single physical child table.
	keyed := beam.ParDo(s, &keyByPartitionDateFn{}, orders)
	grouped := beam.GroupByKey(s, keyed)
	localized := beam.ParDo(s, &flattenPartitionGroupFn{}, grouped)

	// 3. Configure Sink with Composite Conflict Target
	// PostgreSQL requires ON CONFLICT ("order_id", "order_date") for partitioned tables.
	writeOptions := postgresio.NewWriteOptions(
		postgresio.WithHost(*host),
		postgresio.WithPort(*port),
		postgresio.WithDatabase(*database),
		postgresio.WithUsername(*username),
		postgresio.WithPassword(*password),
		postgresio.WithPrimaryKeyColumns("order_id", "order_date"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
		postgresio.WithBatchSize(1000),
	)

	postgresio.Write(s, *table, writeOptions, localized)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
}
