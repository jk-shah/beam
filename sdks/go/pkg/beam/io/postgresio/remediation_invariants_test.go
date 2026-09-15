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

// This file locks in behavior that is already correct today and must not
// regress while the connector is remediated.
//
// It is the counterpart to remediation_acceptance_test.go, which specifies
// behavior that is not yet implemented. Everything here passes against the
// current tree. If a test in this file starts failing, a remediation change
// has broken an existing guarantee -- fix the change, not the test.
//
// Each test names the guarantee it protects and, where applicable, the
// finding that would be reintroduced by regressing it.

package postgresio

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// --- Identifier sanitization (guards against SQL injection via table,
// --- schema, slot and publication names supplied by pipeline authors) ---

// TestSanitizeIdentifierRejectsInjectionVectors ensures identifier validation
// cannot be escaped. Table and publication names reach SQL text directly, so a
// bypass here is a direct SQL injection. The WS-6 move to pgx.Identifier must
// not relax any of these.
func TestSanitizeIdentifierRejectsInjectionVectors(t *testing.T) {
	hostile := []struct {
		name  string
		ident string
	}{
		{"quote_escape_then_drop", `orders"; DROP TABLE users; --`},
		{"bare_double_quote", `or"ders`},
		{"null_byte_truncation", "orders\x00evil"},
		{"statement_separator", "orders; DELETE FROM audit"},
		{"comment_injection", "orders--"},
		{"whitespace_split", "orders orders"},
		{"leading_digit", "1orders"},
		{"hyphen", "order-items"},
		{"empty", ""},
		{"whitespace_only", "   "},
		{"parenthesis", "orders()"},
		{"unicode_lookalike_quote", "orders\u2019; DROP"},
	}

	for _, tc := range hostile {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SanitizeIdentifier(tc.ident)
			if err == nil {
				t.Fatalf("SanitizeIdentifier(%q) accepted a hostile identifier and returned %q; it must reject it", tc.ident, got)
			}
		})
	}
}

// TestSanitizeIdentifierAcceptsLegalNamesAndQuotes verifies the positive path
// still works and that the result is quoted. Quoting is what makes the
// identifier safe once interpolated.
func TestSanitizeIdentifierAcceptsLegalNamesAndQuotes(t *testing.T) {
	legal := map[string]string{
		"orders":        `"orders"`,
		"_private":      `"_private"`,
		"tbl$1":         `"tbl$1"`,
		"Orders":        `"Orders"`,
		"a":             `"a"`,
		"payment_event": `"payment_event"`,
	}

	for in, want := range legal {
		got, err := SanitizeIdentifier(in)
		if err != nil {
			t.Fatalf("SanitizeIdentifier(%q) rejected a legal identifier: %v", in, err)
		}
		if got != want {
			t.Errorf("SanitizeIdentifier(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSanitizeTableIdentifierQuotesEveryComponent ensures both the schema and
// the table are quoted independently. Quoting only the joined string would
// produce "public.orders" as a single identifier, which does not resolve.
func TestSanitizeTableIdentifierQuotesEveryComponent(t *testing.T) {
	got, err := SanitizeTableIdentifier("public.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := `"public"."orders"`; got != want {
		t.Errorf("SanitizeTableIdentifier(public.orders) = %q, want %q", got, want)
	}
}

// TestSanitizeTableIdentifierRejectsMalformed guards the component split. A
// three-part name or an injected component must not pass through.
func TestSanitizeTableIdentifierRejectsMalformed(t *testing.T) {
	bad := []string{
		"db.public.orders",
		`public.orders"; DROP TABLE users; --`,
		`pub"lic.orders`,
		"public.",
		".orders",
		"",
	}

	for _, table := range bad {
		if got, err := SanitizeTableIdentifier(table); err == nil {
			t.Errorf("SanitizeTableIdentifier(%q) accepted malformed input, returned %q", table, got)
		}
	}
}

// --- CDC option validation ---

// TestCDCValidateRejectsUnsafeSlotNames protects the replication slot name,
// which is interpolated into START_REPLICATION and CREATE_REPLICATION_SLOT.
// PostgreSQL slot names are restricted to [a-z0-9_], so the validation is
// stricter than for ordinary identifiers.
func TestCDCValidateRejectsUnsafeSlotNames(t *testing.T) {
	bad := []string{
		"",
		"Beam_Slot",
		"beam-slot",
		"beam slot",
		"beam;drop",
		`beam"slot`,
		strings.Repeat("a", 64),
	}

	for _, slot := range bad {
		opts := NewCDCOptions(
			WithCDCSlotName(slot),
			WithCDCPublication("beam_pub"),
		)
		if err := opts.Validate(); err == nil {
			t.Errorf("Validate() accepted unsafe slot name %q", slot)
		}
	}
}

// TestCDCValidateAcceptsLegalSlotName confirms the positive path, including
// the 63-character PostgreSQL maximum.
func TestCDCValidateAcceptsLegalSlotName(t *testing.T) {
	for _, slot := range []string{"beam_slot", "s", "slot_1", strings.Repeat("a", 63)} {
		opts := NewCDCOptions(
			WithCDCSlotName(slot),
			WithCDCPublication("beam_pub"),
		)
		if err := opts.Validate(); err != nil {
			t.Errorf("Validate() rejected legal slot name %q: %v", slot, err)
		}
	}
}

// TestCDCValidateRejectsHeartbeatAtOrAboveWalSenderTimeout protects an
// availability guarantee that is easy to lose. PostgreSQL terminates a
// replication connection after wal_sender_timeout (default 60s) without a
// standby status update. A heartbeat at or above that value guarantees the
// slot is dropped under idle load, which then stalls WAL release.
func TestCDCValidateRejectsHeartbeatAtOrAboveWalSenderTimeout(t *testing.T) {
	unsafe := []time.Duration{
		0,
		-1 * time.Second,
		60 * time.Second,
		90 * time.Second,
		5 * time.Minute,
	}

	for _, hb := range unsafe {
		opts := NewCDCOptions(
			WithCDCSlotName("beam_slot"),
			WithCDCPublication("beam_pub"),
			WithCDCHeartbeatInterval(hb),
		)
		if err := opts.Validate(); err == nil {
			t.Errorf("Validate() accepted unsafe heartbeat interval %v; must be >0 and <60s", hb)
		}
	}

	safe := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
		WithCDCHeartbeatInterval(10*time.Second),
	)
	if err := safe.Validate(); err != nil {
		t.Errorf("Validate() rejected a safe heartbeat interval: %v", err)
	}
}

// TestCDCValidateRejectsUnsafePublication ensures the publication name is
// sanitized. It is interpolated into the START_REPLICATION options list.
func TestCDCValidateRejectsUnsafePublication(t *testing.T) {
	bad := []string{"", `pub"; DROP`, "pub name", "pub-name"}

	for _, pub := range bad {
		opts := NewCDCOptions(
			WithCDCSlotName("beam_slot"),
			WithCDCPublication(pub),
		)
		if err := opts.Validate(); err == nil {
			t.Errorf("Validate() accepted unsafe publication %q", pub)
		}
	}
}

// --- Credential hygiene ---

// TestCDCOptionsStringRedactsPassword ensures the options struct is safe to
// log. CDCOptions is printed in error paths and pipeline metadata; leaking the
// password there would write database credentials into job logs.
func TestCDCOptionsStringRedactsPassword(t *testing.T) {
	const secret = "sup3rs3cr3t-hunter2"

	opts := NewCDCOptions(
		WithCDCHost("db.internal"),
		WithCDCUsername("beam_cdc"),
		WithCDCPassword(secret),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
	)

	rendered := opts.String()
	if strings.Contains(rendered, secret) {
		t.Fatalf("CDCOptions.String() leaked the password: %s", rendered)
	}
	if !strings.Contains(rendered, "<redacted>") {
		t.Errorf("CDCOptions.String() should mark the password as redacted, got: %s", rendered)
	}
}

// TestCDCOptionsStringDistinguishesEmptyPassword confirms the redaction marker
// is not applied when there is no password, so operators can tell "unset" from
// "hidden" when debugging authentication failures.
func TestCDCOptionsStringDistinguishesEmptyPassword(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
	)
	if rendered := opts.String(); !strings.Contains(rendered, "<none>") {
		t.Errorf("CDCOptions.String() should report an unset password as <none>, got: %s", rendered)
	}
}

// TestSanitizeErrorMessageRedactsCredentials protects the error path that
// feeds metrics and dead-letter queues. Driver errors routinely embed the DSN.
func TestSanitizeErrorMessageRedactsCredentials(t *testing.T) {
	err := fmt.Errorf("dial failed for postgres://beam_cdc:topsecret@10.128.0.7:5432/orders: connection refused")
	got := SanitizeErrorMessage(err)

	if strings.Contains(got, "topsecret") {
		t.Errorf("SanitizeErrorMessage leaked the password: %s", got)
	}
	if strings.Contains(got, "10.128.0.7") {
		t.Errorf("SanitizeErrorMessage leaked the host IP: %s", got)
	}
}

// TestSanitizeErrorMessageRedactsBearerToken covers the IAM / token-provider
// authentication path.
func TestSanitizeErrorMessageRedactsBearerToken(t *testing.T) {
	err := errors.New("auth rejected: Bearer ya29.a0AfB_byC3xd-Token_Value123==")
	got := SanitizeErrorMessage(err)

	if strings.Contains(got, "ya29") {
		t.Errorf("SanitizeErrorMessage leaked a bearer token: %s", got)
	}
}

// TestSanitizeErrorMessageHandlesNil guards a nil dereference on a path that
// runs during failure handling, where a panic would mask the original error.
func TestSanitizeErrorMessageHandlesNil(t *testing.T) {
	if got := SanitizeErrorMessage(nil); got != "" {
		t.Errorf("SanitizeErrorMessage(nil) = %q, want empty string", got)
	}
}

// --- Batch compaction ---

// TestBatchCompactorCollapsesRepeatedKey locks in last-write-wins collapsing.
// WS-5 changes which write wins (newest LSN rather than newest arrival) but
// must not change the fact that one key yields one row per batch. Emitting
// both would break the idempotent-upsert contract the sink relies on.
func TestBatchCompactorCollapsesRepeatedKey(t *testing.T) {
	bc := NewBatchCompactor(100, 1<<20, time.Minute)

	bc.Add("orders:1", []any{int64(1)}, "v1", 10)
	bc.Add("orders:1", []any{int64(1)}, "v2", 10)
	bc.Add("orders:1", []any{int64(1)}, "v3", 10)

	if got := bc.Len(); got != 1 {
		t.Fatalf("expected 1 buffered entry after 3 writes to the same key, got %d", got)
	}

	out := bc.CompactAndSort()
	if len(out) != 1 {
		t.Fatalf("expected 1 compacted record, got %d", len(out))
	}
}

// TestBatchCompactorKeepsDistinctKeys ensures deduplication is scoped to the
// entity key and does not silently drop unrelated rows.
func TestBatchCompactorKeepsDistinctKeys(t *testing.T) {
	bc := NewBatchCompactor(100, 1<<20, time.Minute)

	for i := 0; i < 25; i++ {
		bc.Add(fmt.Sprintf("orders:%d", i), []any{int64(i)}, i, 10)
	}

	if got := bc.Len(); got != 25 {
		t.Fatalf("expected 25 distinct keys to be retained, got %d", got)
	}
}

// TestBatchCompactorDoesNotDeduplicateUnkeyedRows covers append-only writes,
// where no primary key is configured. Collapsing those would lose data.
func TestBatchCompactorDoesNotDeduplicateUnkeyedRows(t *testing.T) {
	bc := NewBatchCompactor(100, 1<<20, time.Minute)

	for i := 0; i < 10; i++ {
		bc.Add("", nil, i, 10)
	}

	if got := bc.Len(); got != 10 {
		t.Fatalf("unkeyed rows must never be deduplicated; expected 10, got %d", got)
	}
}

// TestBatchCompactorSortsByKeyToAvoidDeadlock locks in the deterministic sort.
// Two workers writing overlapping keys in opposite orders deadlock in
// PostgreSQL with SQLSTATE 40P01. A canonical sort makes lock acquisition
// order identical across all workers, which is what prevents it.
func TestBatchCompactorSortsByKeyToAvoidDeadlock(t *testing.T) {
	bc := NewBatchCompactor(100, 1<<20, time.Minute)

	for _, id := range []int64{9, 3, 7, 1, 5} {
		bc.Add(fmt.Sprintf("orders:%d", id), []any{id}, id, 10)
	}

	out := bc.CompactAndSort()
	if len(out) != 5 {
		t.Fatalf("expected 5 records, got %d", len(out))
	}

	for i := 1; i < len(out); i++ {
		prev, ok := out[i-1].(int64)
		if !ok {
			t.Fatalf("unexpected element type %T", out[i-1])
		}
		cur, ok := out[i].(int64)
		if !ok {
			t.Fatalf("unexpected element type %T", out[i])
		}
		if prev > cur {
			t.Fatalf("CompactAndSort must return keys in ascending order to avoid deadlocks; got %v before %v", prev, cur)
		}
	}
}

// TestBatchCompactorResetsAfterFlush ensures buffer state does not leak across
// micro-batches, which would double-write rows from the previous batch.
func TestBatchCompactorResetsAfterFlush(t *testing.T) {
	bc := NewBatchCompactor(100, 1<<20, time.Minute)

	bc.Add("orders:1", []any{int64(1)}, "a", 10)
	bc.Add("orders:2", []any{int64(2)}, "b", 10)
	_ = bc.CompactAndSort()

	if got := bc.Len(); got != 0 {
		t.Fatalf("compactor must be empty after CompactAndSort, got %d buffered", got)
	}

	// The key seen in the previous batch must not be treated as a duplicate now.
	bc.Add("orders:1", []any{int64(1)}, "c", 10)
	if got := bc.Len(); got != 1 {
		t.Fatalf("expected 1 entry in the new batch, got %d", got)
	}
}

// --- ChangeEvent identity ---

// TestChangeEventPrimaryKeyStringIsDeterministic protects the value used for
// Beam keying and Reshuffle partitioning. If it is not stable for a given row,
// updates to that row scatter across bundles and the LWW compaction that
// guarantees ordering stops working.
func TestChangeEventPrimaryKeyStringIsDeterministic(t *testing.T) {
	ev := ChangeEvent{
		Operation:   OpUpdate,
		Schema:      "public",
		Table:       "orders",
		PrimaryKeys: []string{"tenant_id", "order_id"},
		After: map[string]any{
			"order_id":  int64(42),
			"tenant_id": "acme",
			"total":     19.99,
		},
	}

	first := ev.PrimaryKeyString()
	for i := 0; i < 100; i++ {
		if got := ev.PrimaryKeyString(); got != first {
			t.Fatalf("PrimaryKeyString is not deterministic: %q then %q", first, got)
		}
	}

	// Composite keys must be ordered by PrimaryKeys, not by map iteration.
	if want := "public.orders:acme|42"; first != want {
		t.Errorf("PrimaryKeyString() = %q, want %q", first, want)
	}
}

// TestChangeEventPrimaryKeyStringFallsBackToBefore covers DELETE, which
// carries only a before-image. Without this the key would be unstable between
// the UPDATE and the DELETE of the same row.
func TestChangeEventPrimaryKeyStringFallsBackToBefore(t *testing.T) {
	del := ChangeEvent{
		Operation:   OpDelete,
		Schema:      "public",
		Table:       "orders",
		PrimaryKeys: []string{"order_id"},
		Before:      map[string]any{"order_id": int64(42)},
	}

	upd := ChangeEvent{
		Operation:   OpUpdate,
		Schema:      "public",
		Table:       "orders",
		PrimaryKeys: []string{"order_id"},
		After:       map[string]any{"order_id": int64(42)},
	}

	if del.PrimaryKeyString() != upd.PrimaryKeyString() {
		t.Errorf("DELETE and UPDATE of the same row must produce the same key; got %q and %q",
			del.PrimaryKeyString(), upd.PrimaryKeyString())
	}
}

// TestChangeEventFullTableNameQualifies ensures the schema is retained.
// Dropping it would make two same-named tables in different schemas collide in
// the sink routing map.
func TestChangeEventFullTableNameQualifies(t *testing.T) {
	qualified := ChangeEvent{Schema: "reporting", Table: "orders"}
	if got, want := qualified.FullTableName(), "reporting.orders"; got != want {
		t.Errorf("FullTableName() = %q, want %q", got, want)
	}

	bare := ChangeEvent{Table: "orders"}
	if got, want := bare.FullTableName(), "orders"; got != want {
		t.Errorf("FullTableName() = %q, want %q", got, want)
	}
}

// --- Write option defaults ---

// TestWriteOptionDefaultsAreStable pins the sink defaults that appear in
// documentation and in the connection-count formula operators use for
// capacity planning (workers x MaxConnections).
func TestWriteOptionDefaultsAreStable(t *testing.T) {
	opts := NewWriteOptions()

	if opts.Port != 5432 {
		t.Errorf("default port = %d, want 5432", opts.Port)
	}
	if opts.BatchSize != 5000 {
		t.Errorf("default batch size = %d, want 5000", opts.BatchSize)
	}
	if opts.WriteMode != WriteModeUpsert {
		t.Errorf("default write mode = %v, want upsert (idempotent replay is assumed by the CDC path)", opts.WriteMode)
	}
	if opts.FlushInterval != time.Second {
		t.Errorf("default flush interval = %v, want 1s", opts.FlushInterval)
	}
}
