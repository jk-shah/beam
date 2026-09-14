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

// Package main demonstrates database sequence and identity column reconciliation
// after bulk migration or logical replication cutover.
//
// Operational Problem:
// In PostgreSQL, SERIAL, BIGSERIAL, and GENERATED AS IDENTITY columns draw sequence
// numbers using nextval('sequence_name').
// When data is loaded via bulk COPY or replicated via logical replication:
//   1. Explicit primary key values are written into the table.
//   2. PostgreSQL DOES NOT advance sequence counters during replication or COPY.
//   3. When application traffic cuts over to the new database, the first application
//      INSERT calls nextval(), returning a low number (e.g., 1).
//   4. The write fails with:
//      ERROR: duplicate key value violates unique constraint "orders_pkey"
//      Detail: Key (order_id)=(1) already exists.
//
// Remediation:
//   1. Beam computes the global maximum primary key across all processed records.
//   2. Pre-cutover or post-cutover reconciliation emits and executes:
//      SELECT setval(pg_get_serial_sequence('orders', 'order_id'), COALESCE(MAX(order_id), 1) + 1, false);
//   3. Ensures zero primary key collisions when application traffic activates.
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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "postgres", "Database name")
	username = flag.String("username", "beam_test", "Database user")
	password = flag.String("password", "beam_test", "Database password")
	table    = flag.String("table", "public.orders_sequence_demo", "Target table")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*MigratedOrderRecord)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*SequenceReconciliationSQL)(nil)).Elem())
	beam.RegisterDoFn(&extractPrimaryKeyIDFn{})
	beam.RegisterDoFn(&generateSequenceResetSQLFn{})
}

// MigratedOrderRecord represents migrated records with auto-incrementing identity keys.
type MigratedOrderRecord struct {
	OrderID     int64     `json:"order_id" db:"order_id" beam:"order_id"`
	Description string    `json:"description" db:"description" beam:"description"`
	MigratedAt  time.Time `json:"migrated_at" db:"migrated_at" beam:"migrated_at"`
}

// SequenceReconciliationSQL holds the generated DDL/DML statement to adjust sequences.
type SequenceReconciliationSQL struct {
	TableName    string `json:"table_name" db:"table_name" beam:"table_name"`
	ColumnName   string `json:"column_name" db:"column_name" beam:"column_name"`
	MaxID        int64  `json:"max_id" db:"max_id" beam:"max_id"`
	SetValQuery  string `json:"setval_query" db:"setval_query" beam:"setval_query"`
}

type extractPrimaryKeyIDFn struct{}

func (fn *extractPrimaryKeyIDFn) ProcessElement(r MigratedOrderRecord, emit func(int64)) {
	if r.OrderID > 0 {
		emit(r.OrderID)
	}
}

type generateSequenceResetSQLFn struct {
	TargetTable  string
	PrimaryKeyID string
}

func (fn *generateSequenceResetSQLFn) ProcessElement(maxID int64, emit func(SequenceReconciliationSQL)) {
	// Build idempotent setval statement:
	// setval(sequence_name, next_val, is_called = false)
	// Setting is_called to false means the very next nextval() call returns exactly (maxID + 1).
	query := fmt.Sprintf(
		"SELECT setval(pg_get_serial_sequence('%s', '%s'), %d, true);",
		fn.TargetTable,
		fn.PrimaryKeyID,
		maxID,
	)

	emit(SequenceReconciliationSQL{
		TableName:   fn.TargetTable,
		ColumnName:  fn.PrimaryKeyID,
		MaxID:       maxID,
		SetValQuery: query,
	})
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// 1. Ingest migrated records (simulating bulk load + CDC stream)
	records := beam.CreateList(s, []MigratedOrderRecord{
		{OrderID: 100001, Description: "order_alpha", MigratedAt: time.Now().UTC()},
		{OrderID: 100002, Description: "order_beta", MigratedAt: time.Now().UTC()},
		{OrderID: 100050, Description: "order_gamma", MigratedAt: time.Now().UTC()},
	})

	// 2. Sink data to target table
	writeOptions := postgresio.NewWriteOptions(
		postgresio.WithHost(*host),
		postgresio.WithPort(*port),
		postgresio.WithDatabase(*database),
		postgresio.WithUsername(*username),
		postgresio.WithPassword(*password),
		postgresio.WithPrimaryKeyColumns("order_id"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
		postgresio.WithBatchSize(1000),
	)
	postgresio.Write(s, *table, writeOptions, records)

	// 3. Compute Global Maximum Primary Key for Sequence Reconciliation
	ids := beam.ParDo(s, &extractPrimaryKeyIDFn{}, records)
	maxIDCol := stats.Max(s, ids)

	// 4. Generate Sequence Reset SQL
	resetQueries := beam.ParDo(s, &generateSequenceResetSQLFn{
		TargetTable:  *table,
		PrimaryKeyID: "order_id",
	}, maxIDCol)

	// 5. Log reconciliation SQL statement
	beam.ParDo0(s, func(sql SequenceReconciliationSQL) {
		log.Printf("[SEQUENCE RECONCILIATION] target=%s column=%s max_id=%d query=%s",
			sql.TableName, sql.ColumnName, sql.MaxID, sql.SetValQuery)
	}, resetQueries)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
}
