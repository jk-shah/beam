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

package postgresio

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestNativeReplicationStream_LivePostgres validates real-wire logical replication
// against an active PostgreSQL instance. It exercises protocol handshake, slot
// creation/startup, physical CopyData frame demuxing, pgoutput tuple decoding,
// transaction boundary management, and client feedback status updates.
func TestNativeReplicationStream_LivePostgres(t *testing.T) {
	connStr := "host=localhost port=5432 user=beam_test password=beam_test dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Skipf("skipping live replication test: failed to open postgres: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("skipping live replication test: postgres unreachable: %v", err)
	}

	const (
		testTable = "test_pipelines.live_cdc_stream_test"
		pubName   = "beam_live_cdc_pub"
		slotName  = "beam_live_stream_slot"
	)

	// Clean up any stale state from prior runs
	_, _ = db.Exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s' AND NOT active)", slotName, slotName))
	_, _ = db.Exec(fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))

	// Setup table with REPLICA IDENTITY FULL and publication
	setupSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			tag VARCHAR(64) NOT NULL,
			score INT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
		ALTER TABLE %s REPLICA IDENTITY FULL;
		CREATE PUBLICATION %s FOR TABLE %s;
	`, testTable, testTable, pubName, testTable)

	if _, err := db.Exec(setupSQL); err != nil {
		t.Fatalf("failed to setup test table/publication: %v", err)
	}

	// Create replication slot
	if _, err := db.Exec(fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slotName)); err != nil {
		t.Fatalf("failed to create logical replication slot: %v", err)
	}

	t.Cleanup(func() {
		cleanupDB, err := sql.Open("postgres", connStr)
		if err == nil {
			defer cleanupDB.Close()
			_, _ = cleanupDB.Exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s' AND NOT active)", slotName, slotName))
			_, _ = cleanupDB.Exec(fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
			_, _ = cleanupDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
		}
	})

	opts := NewCDCOptions(
		WithCDCHost("localhost"),
		WithCDCPort(5432),
		WithCDCDatabase("postgres"),
		WithCDCUsername("beam_test"),
		WithCDCPassword("beam_test"),
		WithCDCSSLMode("disable"),
		WithCDCSlotName(slotName),
		WithCDCPublication(pubName),
		WithCDCHeartbeatInterval(2*time.Second),
		WithCDCStatusInterval(2*time.Second),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	stream, err := NewNativeReplicationStream(ctx, opts)
	if err != nil {
		t.Fatalf("failed to connect native replication stream: %v", err)
	}
	defer stream.Close()

	// 1. Insert initial test rows in a distinct transaction
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	_, err = tx1.Exec(fmt.Sprintf("INSERT INTO %s (tag, score) VALUES ('alpha', 100), ('beta', 200)", testTable))
	if err != nil {
		_ = tx1.Rollback()
		t.Fatalf("failed to insert initial rows: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("failed to commit insert tx: %v", err)
	}

	// 2. Update a row in a second transaction
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	_, err = tx2.Exec(fmt.Sprintf("UPDATE %s SET score = 999 WHERE tag = 'alpha'", testTable))
	if err != nil {
		_ = tx2.Rollback()
		t.Fatalf("failed to update row: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit update tx: %v", err)
	}

	// 3. Delete a row in a third transaction
	tx3, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin tx3: %v", err)
	}
	_, err = tx3.Exec(fmt.Sprintf("DELETE FROM %s WHERE tag = 'beta'", testTable))
	if err != nil {
		_ = tx3.Rollback()
		t.Fatalf("failed to delete row: %v", err)
	}
	if err := tx3.Commit(); err != nil {
		t.Fatalf("failed to commit delete tx: %v", err)
	}

	parser := NewPgOutputParser()
	var receivedEvents []*ChangeEvent
	var lastLSN uint64

	// Read stream until we see all 4 operations (2 inserts, 1 update, 1 delete)
	readDeadline := time.Now().Add(15 * time.Second)
	for len(receivedEvents) < 4 && time.Now().Before(readDeadline) {
		payload, err := stream.NextMessage(ctx)
		if err != nil {
			t.Fatalf("error reading replication message: %v", err)
		}
		if len(payload) == 0 {
			continue
		}

		if payload[0] == 'k' {
			endWAL, _, replyRequested, kErr := ParseKeepAlive(payload)
			if kErr == nil {
				lastLSN = endWAL
				if replyRequested {
					_ = stream.SendStandbyStatus(ctx, StandbyStatus{
						WriteLSN:   endWAL,
						FlushLSN:   endWAL,
						ApplyLSN:   endWAL,
						ClientTime: time.Now(),
					})
				}
			}
			continue
		}

		if payload[0] == 'w' {
			_, endWAL, _, walData, xErr := ParseXLogData(payload)
			if xErr != nil {
				t.Fatalf("failed to parse XLogData: %v", xErr)
			}
			lastLSN = endWAL

			events, pErr := parser.ParseMessages(walData)
			if pErr != nil {
				t.Fatalf("failed to parse pgoutput messages: %v", pErr)
			}
			for _, ev := range events {
				if ev != nil && ev.Table == "live_cdc_stream_test" {
					receivedEvents = append(receivedEvents, ev)
				}
			}
		}
	}

	if len(receivedEvents) != 4 {
		t.Fatalf("expected 4 live change events (2 insert, 1 update, 1 delete), got %d", len(receivedEvents))
	}

	var insertAlpha, insertBeta, updateAlpha, deleteBeta bool
	var prevLSN uint64

	for i, ev := range receivedEvents {
		if ev.LSN == 0 {
			t.Errorf("event %d: expected non-zero LSN", i)
		}
		if ev.LSN < prevLSN {
			t.Errorf("event %d: LSN non-monotonic: current=%d, previous=%d", i, ev.LSN, prevLSN)
		}
		prevLSN = ev.LSN

		switch ev.Operation {
		case OpInsert:
			if ev.After["tag"] == "alpha" && fmt.Sprintf("%v", ev.After["score"]) == "100" {
				insertAlpha = true
			}
			if ev.After["tag"] == "beta" && fmt.Sprintf("%v", ev.After["score"]) == "200" {
				insertBeta = true
			}
		case OpUpdate:
			if ev.After["tag"] == "alpha" && fmt.Sprintf("%v", ev.After["score"]) == "999" {
				updateAlpha = true
				if ev.Before != nil && fmt.Sprintf("%v", ev.Before["score"]) != "100" {
					t.Errorf("expected Before score 100 on update, got %v", ev.Before["score"])
				}
			}
		case OpDelete:
			if ev.Before != nil && ev.Before["tag"] == "beta" {
				deleteBeta = true
			}
		default:
			t.Errorf("unexpected operation: %v", ev.Operation)
		}
	}

	if !insertAlpha || !insertBeta {
		t.Errorf("missing expected insert events: alpha=%v, beta=%v", insertAlpha, insertBeta)
	}
	if !updateAlpha {
		t.Errorf("missing expected update event for alpha")
	}
	if !deleteBeta {
		t.Errorf("missing expected delete event for beta (with REPLICA IDENTITY FULL)")
	}

	// Send standby status acknowledgment
	if lastLSN > 0 {
		statusErr := stream.SendStandbyStatus(ctx, StandbyStatus{
			WriteLSN:   lastLSN,
			FlushLSN:   lastLSN,
			ApplyLSN:   lastLSN,
			ClientTime: time.Now(),
		})
		if statusErr != nil {
			t.Errorf("failed to send standby status acknowledgment: %v", statusErr)
		}
	}
}
