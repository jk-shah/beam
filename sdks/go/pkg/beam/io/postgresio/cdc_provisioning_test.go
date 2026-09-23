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
	"strings"
	"testing"
)

// --- provisioning script ---

func defaultProvisioningConfig() ProvisioningConfig {
	return ProvisioningConfig{
		Role:        "beam_cdc",
		Database:    "appdb",
		Publication: "beam_pub",
		Tables:      []string{"public.orders", "public.payment_events"},
	}
}

func scriptFor(t *testing.T, cfg ProvisioningConfig) string {
	t.Helper()
	script, err := PostgresProvisioningScript(cfg)
	if err != nil {
		t.Fatalf("PostgresProvisioningScript() err = %v", err)
	}
	return script
}

// TestProvisioningScriptCreatesTheExpectedObjects is the baseline shape.
func TestProvisioningScriptCreatesTheExpectedObjects(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	for _, want := range []string{
		`CREATE ROLE "beam_cdc" WITH LOGIN REPLICATION PASSWORD :'cdc_password';`,
		`GRANT CONNECT ON DATABASE "appdb" TO "beam_cdc";`,
		`CREATE PUBLICATION "beam_pub" FOR TABLE "public"."orders", "public"."payment_events";`,
		`DROP VIEW IF EXISTS "beam_cdc_health";`,
		`CREATE VIEW "beam_cdc_health" AS`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script is missing:\n  %s\n\ngot:\n%s", want, script)
		}
	}
}

// TestProvisioningScriptGrantsConnect covers a failure that is otherwise
// opaque. PUBLIC holds CONNECT by default, so this looks redundant until an
// installation revokes it and the pipeline fails at authentication time with
// nothing pointing at the database grant.
func TestProvisioningScriptGrantsConnect(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if !strings.Contains(script, `GRANT CONNECT ON DATABASE "appdb"`) {
		t.Error("the script does not grant CONNECT on the database")
	}
}

// TestProvisioningScriptOmitsBackfillGrantsByDefault is the least-privilege
// assertion.
//
// Logical decoding reads WAL through the walsender rather than reading tables
// through the executor, so SELECT is not required for change data capture.
// PostgreSQL requires it only to copy initial table data, which this connector
// does not do. Emitting it anyway would teach the wrong baseline, and grants
// are hard to walk back once they are in a runbook.
func TestProvisioningScriptOmitsBackfillGrantsByDefault(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.Contains(trimmed, "GRANT SELECT") || strings.Contains(trimmed, "GRANT USAGE") {
			t.Errorf("a backfill grant is active by default: %q", trimmed)
		}
	}
}

// TestProvisioningScriptBackfillGrantsAreAllOrNothing checks that USAGE and
// SELECT move together. Either alone grants nothing usable, and a partial grant
// produces a confusing permission error.
func TestProvisioningScriptBackfillGrantsAreAllOrNothing(t *testing.T) {
	cfg := defaultProvisioningConfig()
	cfg.IncludeBackfillGrants = true
	script := scriptFor(t, cfg)

	active := map[string]bool{}
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.HasPrefix(trimmed, "GRANT USAGE") {
			active["usage"] = true
		}
		if strings.HasPrefix(trimmed, "GRANT SELECT") {
			active["select"] = true
		}
	}
	if !active["usage"] || !active["select"] {
		t.Errorf("backfill grants were requested but only part was emitted: %v\n\n%s", active, script)
	}
}

// TestProvisioningScriptScopesSelectToNamedTables guards against the grant
// silently widening as tables are added to the schema later.
func TestProvisioningScriptScopesSelectToNamedTables(t *testing.T) {
	cfg := defaultProvisioningConfig()
	cfg.IncludeBackfillGrants = true
	script := scriptFor(t, cfg)

	if strings.Contains(script, "ALL TABLES IN SCHEMA") {
		t.Error("SELECT is granted on ALL TABLES IN SCHEMA, which widens as tables are added")
	}
	if !strings.Contains(script, `GRANT SELECT ON "public"."orders", "public"."payment_events"`) {
		t.Errorf("SELECT is not scoped to the published tables:\n%s", script)
	}
}

// TestProvisioningScriptCarriesNoLiteralPassword keeps the output committable.
func TestProvisioningScriptCarriesNoLiteralPassword(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if !strings.Contains(script, ":'cdc_password'") {
		t.Error("the password is not a psql variable, so the script cannot be committed without a secret")
	}
}

// TestProvisioningScriptRejectsHostileIdentifiers checks that every identifier
// is validated rather than interpolated.
func TestProvisioningScriptRejectsHostileIdentifiers(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ProvisioningConfig)
	}{
		{"role", func(c *ProvisioningConfig) { c.Role = `beam"; DROP DATABASE appdb; --` }},
		{"database", func(c *ProvisioningConfig) { c.Database = `appdb"; DROP ROLE beam_cdc; --` }},
		{"publication", func(c *ProvisioningConfig) { c.Publication = `pub"; --` }},
		{"table", func(c *ProvisioningConfig) { c.Tables = []string{`public.orders"; DROP TABLE users; --`} }},
		{"view name", func(c *ProvisioningConfig) { c.HealthViewName = `v"; DROP TABLE users; --` }},
		{"null byte in role", func(c *ProvisioningConfig) { c.Role = "beam\x00cdc" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultProvisioningConfig()
			tc.mutate(&cfg)
			if script, err := PostgresProvisioningScript(cfg); err == nil {
				t.Errorf("a hostile %s was accepted; script:\n%s", tc.name, script)
			}
		})
	}
}

// TestProvisioningScriptRequiresSchemaQualifiedTables mirrors the constraint
// the connector already enforces on the write path: search_path is pinned to
// pg_catalog,pg_temp, so an unqualified name cannot resolve.
func TestProvisioningScriptRequiresSchemaQualifiedTables(t *testing.T) {
	cfg := defaultProvisioningConfig()
	cfg.Tables = []string{"orders"}

	_, err := PostgresProvisioningScript(cfg)
	if err == nil {
		t.Fatal("an unqualified table name was accepted")
	}
	if !strings.Contains(err.Error(), "schema-qualified") {
		t.Errorf("error does not explain the requirement: %v", err)
	}
}

func TestProvisioningScriptRequiresAtLeastOneTable(t *testing.T) {
	cfg := defaultProvisioningConfig()
	cfg.Tables = nil

	if _, err := PostgresProvisioningScript(cfg); err == nil {
		t.Fatal("a publication with no tables was accepted")
	}
}

// --- the health view ---

// TestHealthViewMeasuresFromRestartLSN pins the retention anchor. Measuring
// from confirmed_flush_lsn would understate what the server is holding and
// would disagree with the circuit breaker.
func TestHealthViewMeasuresFromRestartLSN(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if !strings.Contains(script, "s.restart_lsn) AS retained_bytes") {
		t.Errorf("the view does not measure retention from restart_lsn:\n%s", script)
	}
}

// TestHealthViewHandlesStandbys guards the recovery branch. Without it the view
// raises "recovery is in progress" on a standby and returns no rows at all,
// rather than degrading in one column.
func TestHealthViewHandlesStandbys(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if !strings.Contains(script, "pg_catalog.pg_is_in_recovery()") {
		t.Error("the view does not branch on recovery state and will error on a standby")
	}
	if !strings.Contains(script, "pg_catalog.pg_last_wal_receive_lsn()") {
		t.Error("the view has no standby anchor")
	}

	// The branch must appear exactly once. An earlier revision computed the WAL
	// position twice, once for the byte count and once for the pretty form, and
	// mutation testing showed this test stayed green when one copy was deleted:
	// the asserted string survived in the other. The single occurrence is now
	// part of what is being asserted.
	if n := strings.Count(script, "pg_catalog.pg_is_in_recovery()"); n != 1 {
		t.Errorf("the recovery branch appears %d times; duplicating it lets one copy be removed "+
			"without any assertion noticing", n)
	}
}

// TestHealthViewExposesANonNullHealthColumn is the alerting trap.
//
// Slot invalidation clears restart_lsn, pg_wal_lsn_diff of NULL is NULL, so an
// alert written as "retained_bytes > threshold" evaluates to unknown and stops
// firing exactly when the slot becomes unrecoverable. The health column is
// never NULL, so one predicate covers both the gradual and terminal failures.
func TestHealthViewExposesANonNullHealthColumn(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if !strings.Contains(script, "AS health") {
		t.Fatalf("the view has no health column, so a lost slot silently clears a threshold alert:\n%s", script)
	}
	if !strings.Contains(script, `WHEN s.wal_status = 'lost'       THEN 'lost'`) {
		t.Error("the health column does not classify a lost slot")
	}
}

// TestHealthViewReportsTheXminHorizon covers the failure mode that is invisible
// in a bytes-only view: a stalled slot also blocks VACUUM from removing dead
// catalog tuples.
func TestHealthViewReportsTheXminHorizon(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if !strings.Contains(script, "pg_catalog.age(s.catalog_xmin) AS xmin_horizon_age") {
		t.Errorf("the view does not report the catalog xmin horizon:\n%s", script)
	}
}

// TestHealthViewIsRecreatedNotReplaced keeps the script applicable after the
// view definition changes. CREATE OR REPLACE VIEW cannot add, reorder, retype
// or remove columns.
func TestHealthViewIsRecreatedNotReplaced(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if strings.Contains(script, "CREATE OR REPLACE VIEW") {
		t.Error("the view uses CREATE OR REPLACE, which cannot evolve its column list")
	}
	if !strings.Contains(script, "DROP VIEW IF EXISTS") {
		t.Error("the view is not dropped first, so a changed definition will fail to apply")
	}
}

// TestHealthViewCoversAllLogicalSlots checks the predicate.
//
// Filtering on plugin = 'pgoutput' would exclude logical slots using any other
// output plugin, and every logical slot pins WAL regardless of plugin.
func TestHealthViewCoversAllLogicalSlots(t *testing.T) {
	script := scriptFor(t, defaultProvisioningConfig())

	if !strings.Contains(script, "WHERE s.slot_type = 'logical'") {
		t.Error("the view does not select all logical slots")
	}
	if strings.Contains(script, "plugin = 'pgoutput'") {
		t.Error("the view filters by output plugin and will miss logical slots using another one")
	}
}

// --- SlotHealth ---

// TestSlotHealthClassification checks that the Go reporter and the SQL view
// reach the same conclusion from the same server state.
func TestSlotHealthClassification(t *testing.T) {
	lost := reserved(0)
	lost.WALStatus = "lost"
	lost.RestartLSNUnset = true

	unreserved := reserved(0)
	unreserved.WALStatus = "unreserved"
	unreserved.RestartLSNUnset = true

	inactive := reserved(int64(4 * MiB))
	inactive.Active = false

	cases := []struct {
		name      string
		retention slotRetention
		want      SlotStatus
	}{
		{"healthy", reserved(int64(4 * MiB)), SlotStatusHealthy},
		{"inactive", inactive, SlotStatusInactive},
		{"unreserved", unreserved, SlotStatusUnreserved},
		// Checked before unreserved: invalidation also clears restart_lsn, so a
		// lost slot presents as unreserved and would otherwise be reported as
		// the benign condition.
		{"lost", lost, SlotStatusLost},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := newFakeSlotQuerier(fakeSlotResponse{retention: tc.retention})
			report, err := slotHealthFrom(context.Background(), q, "test_slot")
			if err != nil {
				t.Fatalf("slotHealthFrom() err = %v", err)
			}
			if report.Status != tc.want {
				t.Errorf("Status = %v, want %v", report.Status, tc.want)
			}
			if report.SlotName != "test_slot" {
				t.Errorf("SlotName = %q, want test_slot", report.SlotName)
			}
		})
	}
}

// TestSlotHealthReportsAMissingSlot checks that absence is a status rather than
// an error, so a caller can distinguish "not provisioned" from "cannot reach
// the database".
func TestSlotHealthReportsAMissingSlot(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{err: errSlotNotFound})

	report, err := slotHealthFrom(context.Background(), q, "absent_slot")
	if err != nil {
		t.Fatalf("a missing slot was reported as an error: %v", err)
	}
	if report.Status != SlotStatusMissing {
		t.Errorf("Status = %v, want %v", report.Status, SlotStatusMissing)
	}
}

// TestSlotHealthCarriesTheCostFigures checks the report surfaces both failure
// modes, not only WAL volume.
func TestSlotHealthCarriesTheCostFigures(t *testing.T) {
	retention := reserved(int64(9 * MiB))
	retention.XminHorizonAge = 4242

	q := newFakeSlotQuerier(fakeSlotResponse{retention: retention})
	report, err := slotHealthFrom(context.Background(), q, "test_slot")
	if err != nil {
		t.Fatalf("slotHealthFrom() err = %v", err)
	}

	if report.RetainedBytes != int64(9*MiB) {
		t.Errorf("RetainedBytes = %d, want %d", report.RetainedBytes, int64(9*MiB))
	}
	if report.XminHorizonAge != 4242 {
		t.Errorf("XminHorizonAge = %d, want 4242; catalog bloat is invisible without it", report.XminHorizonAge)
	}
}

func TestProvisioningScriptWithPublicationTableProjectionsAndFilters(t *testing.T) {
	cfg := ProvisioningConfig{
		Role:        "beam_cdc",
		Database:    "finance",
		Publication: "filtered_pub",
		PublicationTables: []PublicationTableConfig{
			{
				TableName: "public.orders",
				Columns:   []string{"order_id", "customer_id", "amount"},
				RowFilter: "amount > 100.0 AND status = 'COMPLETED'",
			},
			{
				TableName: "public.accounts",
				Columns:   []string{"account_id", "balance"},
			},
		},
	}

	script, err := PostgresProvisioningScript(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedPubClause := `CREATE PUBLICATION "filtered_pub" FOR TABLE "public"."orders" ("order_id", "customer_id", "amount") WHERE (amount > 100.0 AND status = 'COMPLETED'), "public"."accounts" ("account_id", "balance");`
	if !strings.Contains(script, expectedPubClause) {
		t.Errorf("expected publication clause:\n  %s\ngot:\n%s", expectedPubClause, script)
	}
}

func TestSanitizeRowFilter(t *testing.T) {
	validFilters := []string{
		"amount > 100",
		"status = 'ACTIVE' AND category_id IN (1, 2, 3)",
		"(dept_id = 10 OR salary >= 50000.00)",
		"",
	}
	for _, f := range validFilters {
		if err := SanitizeRowFilter(f); err != nil {
			t.Errorf("unexpected error for valid filter %q: %v", f, err)
		}
	}

	invalidFilters := []string{
		"amount > 100; DROP TABLE users",
		"status = 'A' -- comment",
		"status = 'A' /* inline comment */",
		"amount > 100 AND 1=1; COMMIT",
		"amount > 100; TRUNCATE orders",
	}
	for _, f := range invalidFilters {
		if err := SanitizeRowFilter(f); err == nil {
			t.Errorf("expected error for dangerous filter %q, got nil", f)
		}
	}
}
