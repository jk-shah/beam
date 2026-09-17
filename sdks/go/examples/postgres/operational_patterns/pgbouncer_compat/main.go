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

// Package main demonstrates writing safely to PostgreSQL through PgBouncer
// operating in transaction pooling mode.
//
// Operational Problem:
// PgBouncer in transaction pooling mode reassigns backend PostgreSQL connections
// to different clients after each COMMIT or ROLLBACK. Standard JDBC or database/sql
// drivers maintain session-level prepared statement caches (e.g., stmt_0, stmt_1)
// or session-scoped temporary tables. When executed through PgBouncer:
//  1. Prepared statement execution fails with "prepared statement does not exist"
//     when dispatched to a different backend server.
//  2. Prepared statement preparation fails with "prepared statement already exists"
//     if another worker already prepared a statement with the same internal name.
//  3. Session-level temporary tables leak across clients or persist indefinitely.
//
// Remediation:
//  1. Configure postgresio.WithPgBouncer(true) to disable prepared statement caching.
//  2. Ensure all staging operations use transaction-scoped temporary tables
//     (ON COMMIT DROP) so state is discarded immediately upon transaction commit.
//  3. Maintain explicit transaction boundaries per sink micro-batch.
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
	host         = flag.String("host", "localhost", "PgBouncer host")
	port         = flag.Int("port", 6432, "PgBouncer port (default 6432)")
	database     = flag.String("database", "beammeup", "Target database name")
	username     = flag.String("username", "beam_transporter", "Database user")
	password     = flag.String("password", "beam_transporter_pass", "Database password")
	table        = flag.String("table", "public.pooled_events", "Target table")
	sslMode      = flag.String("sslmode", "disable", "PostgreSQL SSL mode")
	usePgBouncer = flag.Bool("pgbouncer", true, "Enable PgBouncer transaction-pooler compatibility")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*PooledEventRecord)(nil)).Elem())
	beam.RegisterDoFn(&generatePooledEventsFn{})
	beam.RegisterDoFn(&validatePoolerRecordFn{})
}

// PooledEventRecord represents high-throughput event logs routed through PgBouncer.
type PooledEventRecord struct {
	EventID   int64     `json:"event_id" db:"event_id" beam:"event_id"`
	Source    string    `json:"source" db:"source" beam:"source"`
	Payload   string    `json:"payload" db:"payload" beam:"payload"`
	CreatedAt time.Time `json:"created_at" db:"created_at" beam:"created_at"`
}

type generatePooledEventsFn struct{}

func (fn *generatePooledEventsFn) ProcessElement(id int64, emit func(PooledEventRecord)) {
	emit(PooledEventRecord{
		EventID:   id,
		Source:    fmt.Sprintf("worker-%d", id%8),
		Payload:   fmt.Sprintf("telemetry_payload_batch_%d", id),
		CreatedAt: time.Now().UTC(),
	})
}

type validatePoolerRecordFn struct{}

func (fn *validatePoolerRecordFn) ProcessElement(r PooledEventRecord, emit func(PooledEventRecord)) {
	if r.EventID > 0 && r.Payload != "" {
		emit(r)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// 1. Generate event batch
	eventIDs := beam.CreateList(s, []int64{101, 102, 103, 104, 105, 106, 107, 108})
	events := beam.ParDo(s, &generatePooledEventsFn{}, eventIDs)
	validated := beam.ParDo(s, &validatePoolerRecordFn{}, events)

	// 2. Configure WriteOptions with PgBouncer compatibility
	// Setting WithPgBouncer(true) guarantees:
	// - Prepared statements are suppressed (uses simple protocol or unnamed queries)
	// - Staging tables are created with ON COMMIT DROP
	// - Connection state is completely reset per micro-batch transaction
	writeOptions := postgresio.NewWriteOptions(
		postgresio.WithHost(*host),
		postgresio.WithPort(*port),
		postgresio.WithDatabase(*database),
		postgresio.WithUsername(*username),
		postgresio.WithPassword(*password),
		postgresio.WithSSLMode(*sslMode),
		postgresio.WithPgBouncer(*usePgBouncer),
		postgresio.WithPrimaryKeyColumns("event_id"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
		postgresio.WithBatchSize(1000),
	)

	// 3. Sink records to target table through PgBouncer
	postgresio.Write(s, *table, writeOptions, validated)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
}
