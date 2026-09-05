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

// Package main demonstrates an advanced enterprise data reconciliation and delta diff
// pipeline using Apache Beam with PostgreSQL.
//
// Pattern:
// Ingest snapshot records from both a source table and a target table, execute a full
// outer co-grouping via CoGroupByKey on primary record ID, detect discrepancies
// (MATCH, MISSING_TARGET, MISSING_SOURCE, VALUE_DRIFT), and write the audit log to PostgreSQL.
// Validating data replication accuracy, audit compliance, and identifying value
// drift between operational primary tables and reporting replicas or warehouses.
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
)

func init() {
	beam.RegisterType(reflect.TypeOf((*TableRecord)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*ReconciliationAuditReport)(nil)).Elem())
	beam.RegisterDoFn(&keyRecordByIDFn{})
	beam.RegisterDoFn(&reconcileDatasetsFn{})
}

// TableRecord represents a snapshot row from either the source or target table.
type TableRecord struct {
	RecordID string `json:"record_id" db:"record_id" beam:"record_id"`
	Checksum string `json:"checksum" db:"checksum" beam:"checksum"`
	Payload  string `json:"payload" db:"payload" beam:"payload"`
}

// ReconciliationAuditReport captures the comparison outcome written to PostgreSQL.
type ReconciliationAuditReport struct {
	RecordID             string    `json:"record_id" db:"record_id" beam:"record_id"`
	ReconciliationStatus string    `json:"reconciliation_status" db:"reconciliation_status" beam:"reconciliation_status"`
	SourceChecksum       string    `json:"source_checksum" db:"source_checksum" beam:"source_checksum"`
	TargetChecksum       string    `json:"target_checksum" db:"target_checksum" beam:"target_checksum"`
	DifferenceDetails    string    `json:"difference_details" db:"difference_details" beam:"difference_details"`
	AuditedAt            time.Time `json:"audited_at" db:"audited_at" beam:"audited_at"`
}

type keyRecordByIDFn struct{}

func (fn *keyRecordByIDFn) ProcessElement(r TableRecord, emit func(string, TableRecord)) {
	emit(r.RecordID, r)
}

type reconcileDatasetsFn struct{}

func (fn *reconcileDatasetsFn) ProcessElement(
	recordID string,
	sources func(*TableRecord) bool,
	targets func(*TableRecord) bool,
	emit func(ReconciliationAuditReport),
) {
	var src TableRecord
	hasSrc := sources(&src)

	var tgt TableRecord
	hasTgt := targets(&tgt)

	now := time.Now().UTC()

	if hasSrc && !hasTgt {
		// Record present in source, completely missing in target
		emit(ReconciliationAuditReport{
			RecordID:             recordID,
			ReconciliationStatus: "MISSING_TARGET",
			SourceChecksum:       src.Checksum,
			TargetChecksum:       "NULL",
			DifferenceDetails:    fmt.Sprintf("record %s missing from target table", recordID),
			AuditedAt:            now,
		})
		return
	}

	if !hasSrc && hasTgt {
		// Phantom record in target that does not exist in source
		emit(ReconciliationAuditReport{
			RecordID:             recordID,
			ReconciliationStatus: "MISSING_SOURCE",
			SourceChecksum:       "NULL",
			TargetChecksum:       tgt.Checksum,
			DifferenceDetails:    fmt.Sprintf("record %s exists in target but missing in source", recordID),
			AuditedAt:            now,
		})
		return
	}

	if src.Checksum != tgt.Checksum {
		// Both present but checksum values differ (data corruption or unsynced write)
		emit(ReconciliationAuditReport{
			RecordID:             recordID,
			ReconciliationStatus: "VALUE_DRIFT",
			SourceChecksum:       src.Checksum,
			TargetChecksum:       tgt.Checksum,
			DifferenceDetails:    fmt.Sprintf("checksum mismatch: source=%s vs target=%s", src.Checksum, tgt.Checksum),
			AuditedAt:            now,
		})
		return
	}

	// Perfect match
	emit(ReconciliationAuditReport{
		RecordID:             recordID,
		ReconciliationStatus: "MATCH",
		SourceChecksum:       src.Checksum,
		TargetChecksum:       tgt.Checksum,
		DifferenceDetails:    "source and target records identical",
		AuditedAt:            now,
	})
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	// Source Table Records (e.g. Master Production Database)
	sourceData := []TableRecord{
		{RecordID: "REC-101", Checksum: "a1b2c3d4", Payload: `{"tier":"GOLD","credit":5000}`},
		{RecordID: "REC-102", Checksum: "e5f6g7h8", Payload: `{"tier":"SILVER","credit":2000}`},
		{RecordID: "REC-103", Checksum: "i9j0k1l2", Payload: `{"tier":"PLATINUM","credit":10000}`},
		{RecordID: "REC-104", Checksum: "m3n4o5p6", Payload: `{"tier":"STANDARD","credit":1000}`}, // will be missing in target
	}
	sourceCol := beam.CreateList(s, sourceData)

	// Target Table Records (e.g. Downstream Reporting Warehouse)
	targetData := []TableRecord{
		{RecordID: "REC-101", Checksum: "a1b2c3d4", Payload: `{"tier":"GOLD","credit":5000}`},        // Match
		{RecordID: "REC-102", Checksum: "DIFFERENT_HASH", Payload: `{"tier":"SILVER","credit":1500}`}, // Value Drift
		{RecordID: "REC-103", Checksum: "i9j0k1l2", Payload: `{"tier":"PLATINUM","credit":10000}`},   // Match
		{RecordID: "REC-105", Checksum: "q7r8s9t0", Payload: `{"tier":"GUEST","credit":500}`},         // Missing in Source
	}
	targetCol := beam.CreateList(s, targetData)

	// 1. Key both collections by RecordID
	keyedSource := beam.ParDo(s, &keyRecordByIDFn{}, sourceCol)
	keyedTarget := beam.ParDo(s, &keyRecordByIDFn{}, targetCol)

	// 2. Full Outer Join via CoGroupByKey
	joined := beam.CoGroupByKey(s, keyedSource, keyedTarget)

	// 3. Reconcile differences and emit audit records
	auditCol := beam.ParDo(s, &reconcileDatasetsFn{}, joined)

	// 4. Sink to PostgreSQL data_reconciliation_audit with upsert
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"record_id"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.data_reconciliation_audit", writeOpts, auditCol)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Data reconciliation and table diff pipeline completed successfully.")
}
