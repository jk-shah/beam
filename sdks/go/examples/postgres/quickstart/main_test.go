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

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
)

func TestQuickstart_InitSQLMatchesProvisioningSurface(t *testing.T) {
	initBytes, err := os.ReadFile("init.sql")
	if err != nil {
		t.Fatalf("failed to read init.sql: %v", err)
	}
	initSQL := string(initBytes)

	// Verify key operational components exist in init.sql
	mustContain := []string{
		`CREATE ROLE "beam_cdc" WITH LOGIN REPLICATION PASSWORD 'secret';`,
		`GRANT CONNECT ON DATABASE "quickstart" TO "beam_cdc";`,
		`GRANT USAGE ON SCHEMA "public" TO "beam_cdc";`,
		`CREATE PUBLICATION "beam_pub" FOR TABLE "public"."orders_source";`,
		`CREATE VIEW "public"."beam_cdc_health" AS`,
		`WHERE s.slot_type = 'logical'`,
		`pg_catalog.pg_is_in_recovery()`,
	}

	for _, fragment := range mustContain {
		if !strings.Contains(initSQL, fragment) {
			t.Errorf("init.sql missing expected fragment:\n%s", fragment)
		}
	}
}

func TestQuickstart_TransformCDCToTarget(t *testing.T) {
	t.Run("insert event emitted", func(t *testing.T) {
		event := postgresio.ChangeEvent{
			Operation: postgresio.OpInsert,
			After: map[string]any{
				"id":          int64(42),
				"customer_id": int64(101),
				"amount":      float64(99.50),
				"status":      "COMPLETED",
			},
		}

		var emitted []targetOrderRow
		emit := func(r targetOrderRow) {
			emitted = append(emitted, r)
		}

		if err := transformCDCToTarget(event, emit); err != nil {
			t.Fatalf("transformCDCToTarget failed: %v", err)
		}

		if len(emitted) != 1 {
			t.Fatalf("expected 1 emitted row, got %d", len(emitted))
		}
		row := emitted[0]
		if row.ID != 42 || row.CustomerID != 101 || row.Amount != 99.50 || row.Status != "COMPLETED" {
			t.Errorf("unexpected emitted row: %+v", row)
		}
		if row.SyncedAt.IsZero() {
			t.Errorf("expected non-zero SyncedAt")
		}
	})

	t.Run("delete event skipped", func(t *testing.T) {
		event := postgresio.ChangeEvent{
			Operation: postgresio.OpDelete,
			Before: map[string]any{
				"id": int64(42),
			},
		}

		var emitted []targetOrderRow
		emit := func(r targetOrderRow) {
			emitted = append(emitted, r)
		}

		if err := transformCDCToTarget(event, emit); err != nil {
			t.Fatalf("transformCDCToTarget failed: %v", err)
		}

		if len(emitted) != 0 {
			t.Errorf("expected 0 emitted rows on delete, got %d", len(emitted))
		}
	})
}
