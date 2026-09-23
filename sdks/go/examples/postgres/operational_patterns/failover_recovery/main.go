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

// Package main demonstrates High Availability (HA) failover resilience using
// PostgreSQL 17 failover-synchronized replication slots.
//
// Operational Problem:
// In traditional PostgreSQL high-availability architectures (Patroni, repmgr, Cloud SQL HA),
// logical replication slots exist only on the primary instance.
// When an unplanned failover or planned maintenance promotes a standby to primary:
//  1. The logical replication slot disappears from pg_replication_slots.
//  2. Pipelines crash with "replication slot does not exist".
//  3. Re-creating the slot after promotion causes data loss because changes between
//     the old primary's crash and new slot creation are lost.
//
// Remediation:
//  1. PostgreSQL 17+ supports failover-synchronized logical slots:
//     CREATE_REPLICATION_SLOT ... LOGICAL pgoutput (FAILOVER)
//  2. The primary coordinates with standby instances via synchronized_standby_slots.
//  3. Configure postgresio.WithCDCFailoverSlot(true).
//  4. Standbys continuously mirror the slot's confirmed flush LSN.
//  5. Upon standby promotion, the pipeline reconnects to the new primary cluster VIP/endpoint,
//     finds the synchronized slot, and resumes streaming with zero data loss.
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
	clusterEndpoint = flag.String("cluster_endpoint", "localhost", "PostgreSQL HA cluster endpoint/VIP")
	port            = flag.Int("port", 5432, "PostgreSQL port")
	database        = flag.String("database", "beammeup", "Database name")
	username        = flag.String("username", "scotty", "Replication user")
	password        = flag.String("password", "scotty_secret", "Replication password")
	sslMode         = flag.String("sslmode", "disable", "PostgreSQL SSL mode")
	slotName        = flag.String("slot_name", "ha_failover_slot", "Failover-synchronized slot name")
	pubName         = flag.String("publication", "ha_critical_pub", "CDC publication name")
	targetTable     = flag.String("target_table", "public.ha_replicated_orders", "Target table")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*HAEventRecord)(nil)).Elem())
	beam.RegisterDoFn(&haResilienceFilterFn{})
}

func newHAResilienceFilterFn() *haResilienceFilterFn {
	return &haResilienceFilterFn{}
}

// HAEventRecord captures data replicated across HA failover boundaries.
type HAEventRecord struct {
	RecordID     int64     `json:"record_id" db:"record_id" beam:"record_id"`
	Payload      string    `json:"payload" db:"payload" beam:"payload"`
	ConfirmedLSN uint64    `json:"confirmed_lsn" db:"confirmed_lsn" beam:"confirmed_lsn"`
	ReplicatedAt time.Time `json:"replicated_at" db:"replicated_at" beam:"replicated_at"`
}

type haResilienceFilterFn struct{}

func (fn *haResilienceFilterFn) ProcessElement(evt postgresio.ChangeEvent, emit func(HAEventRecord)) {
	if evt.Operation != postgresio.OpInsert && evt.Operation != postgresio.OpUpdate {
		return
	}

	id, _ := evt.After["order_id"].(int64)
	if id == 0 {
		if f, ok := evt.After["order_id"].(float64); ok {
			id = int64(f)
		}
	}
	if id == 0 {
		if v, ok := evt.After["record_id"].(int64); ok {
			id = v
		} else if f, ok := evt.After["record_id"].(float64); ok {
			id = int64(f)
		}
	}

	payload, _ := evt.After["payload"].(string)
	if payload == "" {
		amount, _ := evt.After["total_amount"].(float64)
		payload = fmt.Sprintf("Amount: %.2f", amount)
	}

	emit(HAEventRecord{
		RecordID:     id,
		Payload:      payload,
		ConfirmedLSN: uint64(evt.LSN),
		ReplicatedAt: time.Now().UTC(),
	})
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// 1. Configure Failover-Safe CDC Options (Requires PostgreSQL 17+)
	cdcOptions := []postgresio.CDCOption{
		postgresio.WithCDCHost(*clusterEndpoint),
		postgresio.WithCDCPort(*port),
		postgresio.WithCDCDatabase(*database),
		postgresio.WithCDCUsername(*username),
		postgresio.WithCDCPassword(*password),
		postgresio.WithCDCSSLMode(*sslMode),
		postgresio.WithCDCSlotName(*slotName),
		postgresio.WithCDCPublication(*pubName),
		postgresio.WithCDCCreateSlotIfMissing(true),
		// WithCDCFailoverSlot(true) adds the FAILOVER parameter during slot creation:
		// CREATE_REPLICATION_SLOT ha_failover_slot LOGICAL pgoutput (FAILOVER)
		// This causes physical standbys listed in synchronized_standby_slots to mirror the slot.
		postgresio.WithCDCFailoverSlot(true),
	}

	stream := postgresio.ReadCDC(s, cdcOptions...)
	records := beam.ParDo(s, &haResilienceFilterFn{}, stream)

	// 2. Sink to target table
	writeOptions := postgresio.NewWriteOptions(
		postgresio.WithHost(*clusterEndpoint),
		postgresio.WithPort(*port),
		postgresio.WithDatabase(*database),
		postgresio.WithUsername(*username),
		postgresio.WithPassword(*password),
		postgresio.WithSSLMode(*sslMode),
		postgresio.WithPrimaryKeyColumns("record_id"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
		postgresio.WithBatchSize(1000),
	)
	postgresio.Write(s, *targetTable, writeOptions, records)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
}
