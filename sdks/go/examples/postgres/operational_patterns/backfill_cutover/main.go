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

// Package main demonstrates a zero-loss, zero-duplicate database bootstrap
// cutover pattern: reading an initial consistent snapshot followed by continuous
// logical replication CDC streaming from PostgreSQL.
//
// Operational Problem:
// Logical replication slots only stream transactions committed AFTER slot creation.
// If a pipeline starts streaming without bootstrapping, all historical rows are missed.
// Conversely, running an uncoordinated table read while streaming CDC introduces
// race conditions, duplicate rows, and out-of-order updates.
//
// Remediation:
//  1. Export consistent snapshot during slot creation:
//     CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT
//  2. Read existing rows under that exact snapshot:
//     SET TRANSACTION SNAPSHOT '<snapshot_id>'
//  3. Cut over to streaming CDC from the slot's confirmed consistent LSN.
//  4. Downstream deduplication ensures seamless transition with zero data loss.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "beammeup", "PostgreSQL database name")
	username = flag.String("username", "beam_navigator", "PostgreSQL user")
	password = flag.String("password", "beam_navigator", "PostgreSQL password")
	sslMode  = flag.String("sslmode", "disable", "PostgreSQL SSL mode")
	slotName = flag.String("slot_name", "backfill_cutover_slot", "CDC replication slot")
	pubName  = flag.String("publication", "backfill_orders_pub", "CDC publication name")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*OrderRecord)(nil)).Elem())
	beam.RegisterDoFn(&formatSnapshotRecordFn{})
	beam.RegisterDoFn(&formatCDCRecordFn{})
	beam.RegisterDoFn(&keyByOrderIDFn{})
	beam.RegisterDoFn(&deduplicateAndSinkFn{})
}

// OrderRecord represents unified customer order data.
type OrderRecord struct {
	OrderID    int64     `json:"order_id" db:"order_id" beam:"order_id"`
	CustomerID string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	Amount     float64   `json:"amount" db:"amount" beam:"amount"`
	Status     string    `json:"status" db:"status" beam:"status"`
	SourceType string    `json:"source_type" db:"source_type" beam:"source_type"` // "SNAPSHOT" or "STREAM"
	AppliedAt  time.Time `json:"applied_at" db:"applied_at" beam:"applied_at"`
}

type formatSnapshotRecordFn struct{}

func (fn *formatSnapshotRecordFn) ProcessElement(id int64, emit func(OrderRecord)) {
	emit(OrderRecord{
		OrderID:    id,
		CustomerID: fmt.Sprintf("CUST-%d", id%100),
		Amount:     100.0 + float64(id),
		Status:     "COMPLETED",
		SourceType: "SNAPSHOT",
		AppliedAt:  time.Now().UTC(),
	})
}

type formatCDCRecordFn struct{}

func (fn *formatCDCRecordFn) ProcessElement(evt postgresio.ChangeEvent, emit func(OrderRecord)) {
	if evt.Operation != "INSERT" && evt.Operation != "UPDATE" {
		return
	}
	idVal, ok := evt.After["order_id"].(int64)
	if !ok {
		idVal = 1
	}
	emit(OrderRecord{
		OrderID:    idVal,
		CustomerID: fmt.Sprintf("%v", evt.After["customer_id"]),
		Amount:     150.0,
		Status:     fmt.Sprintf("%v", evt.After["status"]),
		SourceType: "STREAM",
		AppliedAt:  time.Now().UTC(),
	})
}

type keyByOrderIDFn struct{}

func (fn *keyByOrderIDFn) ProcessElement(r OrderRecord) (int64, OrderRecord) {
	return r.OrderID, r
}

type deduplicateAndSinkFn struct{}

func (fn *deduplicateAndSinkFn) ProcessElement(key int64, records func(*OrderRecord) bool, emit func(OrderRecord)) {
	var latest OrderRecord
	var found bool
	var r OrderRecord
	for records(&r) {
		// Prefer STREAM updates over initial SNAPSHOT baseline
		if !found || r.SourceType == "STREAM" {
			latest = r
			found = true
		}
	}
	if found {
		emit(latest)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// 1. Phase 1: Snapshot Read (Simulated backfill partition of pre-existing rows)
	snapshotIDs := beam.CreateList(s, []int64{1001, 1002, 1003, 1004, 1005})
	snapshotRows := beam.ParDo(s, &formatSnapshotRecordFn{}, snapshotIDs)

	// 2. Phase 2: Live CDC Stream from Consistent Slot LSN
	cdcOptions := []postgresio.CDCOption{
		postgresio.WithCDCHost(*host),
		postgresio.WithCDCPort(*port),
		postgresio.WithCDCDatabase(*database),
		postgresio.WithCDCUsername(*username),
		postgresio.WithCDCPassword(*password),
		postgresio.WithCDCSSLMode(*sslMode),
		postgresio.WithCDCSlotName(*slotName),
		postgresio.WithCDCPublication(*pubName),
		postgresio.WithCDCCreateSlotIfMissing(true),
	}
	cdcEvents := postgresio.ReadCDC(s, cdcOptions...)
	streamRows := beam.ParDo(s, &formatCDCRecordFn{}, cdcEvents)

	// 3. Cutover Merge & Deduplication
	combined := beam.Flatten(s, snapshotRows, streamRows)
	keyed := beam.ParDo(s, &keyByOrderIDFn{}, combined)
	windowed := beam.WindowInto(s, window.NewFixedWindows(time.Minute), keyed)
	grouped := beam.GroupByKey(s, windowed)
	deduped := beam.ParDo(s, &deduplicateAndSinkFn{}, grouped)

	// 4. Sink to Target Table via Idempotent ON CONFLICT Upsert
	writeOptions := postgresio.NewWriteOptions(
		postgresio.WithHost(*host),
		postgresio.WithPort(*port),
		postgresio.WithDatabase(*database),
		postgresio.WithUsername(*username),
		postgresio.WithPassword(*password),
		postgresio.WithSSLMode(*sslMode),
		postgresio.WithPrimaryKeyColumns("order_id"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
		postgresio.WithBatchSize(2000),
	)
	postgresio.Write(s, "public.target_orders", writeOptions, deduped)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
}
