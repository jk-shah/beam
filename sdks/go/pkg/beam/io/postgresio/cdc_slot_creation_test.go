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
	"errors"
	"strings"
	"testing"
)

func TestBuildCreateSlotQuery(t *testing.T) {
	tests := []struct {
		name           string
		slot           string
		plugin         string
		exportSnapshot bool
		twoPhase       bool
		failover       bool

		// serverMajorVersion only matters when failover is requested; the
		// legacy syntax is accepted by every supported server.
		serverMajorVersion int

		want string
	}{
		{
			name:           "default plugin with exported snapshot",
			slot:           "beam_slot",
			plugin:         "",
			exportSnapshot: true,
			want:           "CREATE_REPLICATION_SLOT beam_slot LOGICAL pgoutput EXPORT_SNAPSHOT",
		},
		{
			name:           "explicit plugin without snapshot",
			slot:           "beam_slot_2",
			plugin:         "pgoutput",
			exportSnapshot: false,
			want:           "CREATE_REPLICATION_SLOT beam_slot_2 LOGICAL pgoutput NOEXPORT_SNAPSHOT",
		},
		{
			name:           "two phase precedes snapshot clause",
			slot:           "beam_2pc",
			plugin:         "pgoutput",
			exportSnapshot: true,
			twoPhase:       true,
			want:           "CREATE_REPLICATION_SLOT beam_2pc LOGICAL pgoutput TWO_PHASE EXPORT_SNAPSHOT",
		},
		{
			name:           "alternate output plugin is preserved",
			slot:           "wal2json_slot",
			plugin:         "wal2json",
			exportSnapshot: false,
			want:           "CREATE_REPLICATION_SLOT wal2json_slot LOGICAL wal2json NOEXPORT_SNAPSHOT",
		},
		{
			name:               "failover uses the parenthesized option list",
			slot:               "beam_failover",
			plugin:             "pgoutput",
			exportSnapshot:     true,
			failover:           true,
			serverMajorVersion: 17,
			want:               "CREATE_REPLICATION_SLOT beam_failover LOGICAL pgoutput (FAILOVER, SNAPSHOT 'export')",
		},
		{
			name:               "failover combines with two phase",
			slot:               "beam_failover_2pc",
			plugin:             "pgoutput",
			exportSnapshot:     false,
			twoPhase:           true,
			failover:           true,
			serverMajorVersion: 18,
			want:               "CREATE_REPLICATION_SLOT beam_failover_2pc LOGICAL pgoutput (FAILOVER, TWO_PHASE, SNAPSHOT 'nothing')",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildCreateSlotQuery(tc.slot, tc.plugin, tc.exportSnapshot, tc.twoPhase, tc.failover, tc.serverMajorVersion)
			if err != nil {
				t.Fatalf("buildCreateSlotQuery returned error: %v", err)
			}
			if got != tc.want {
				t.Errorf("query mismatch\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestBuildCreateSlotQueryRejectsInjection is the security case. The slot name
// is interpolated into the replication command unquoted because PostgreSQL's
// replication protocol accepts a bare identifier there, so validation is the
// only barrier against command injection.
func TestBuildCreateSlotQueryRejectsInjection(t *testing.T) {
	bad := []string{
		"",
		"Beam_Slot",                    // uppercase
		"beam slot",                    // space
		"beam-slot",                    // hyphen
		"beam_slot; DROP DATABASE x",   // statement injection
		"beam_slot\"",                  // quote
		"beam_slot'",                   // quote
		strings.Repeat("a", 64),        // 64 chars, one over the limit
		"beam_slot LOGICAL evilplugin", // clause injection
	}

	for _, slot := range bad {
		t.Run(slot, func(t *testing.T) {
			if _, err := buildCreateSlotQuery(slot, "pgoutput", true, false, false, 0); err == nil {
				t.Errorf("buildCreateSlotQuery(%q) accepted an invalid slot name; "+
					"the name is interpolated unquoted, so this is a command injection vector", slot)
			}
		})
	}

	// A 63-character name is exactly at the limit and must be accepted.
	if _, err := buildCreateSlotQuery(strings.Repeat("a", 63), "pgoutput", true, false, false, 0); err != nil {
		t.Errorf("63-character slot name was rejected: %v", err)
	}
}

func TestBuildCreateSlotQueryRejectsInvalidPlugin(t *testing.T) {
	for _, plugin := range []string{"pgoutput; DROP TABLE t", "pg output", "1plugin", "plug\"in"} {
		if _, err := buildCreateSlotQuery("beam_slot", plugin, true, false, false, 0); err == nil {
			t.Errorf("buildCreateSlotQuery accepted invalid output plugin %q", plugin)
		}
	}
}

func TestBuildDropSlotQuery(t *testing.T) {
	got, err := buildDropSlotQuery("beam_slot", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "DROP_REPLICATION_SLOT beam_slot" {
		t.Errorf("got %q", got)
	}

	got, err = buildDropSlotQuery("beam_slot", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "DROP_REPLICATION_SLOT beam_slot WAIT" {
		t.Errorf("got %q", got)
	}

	if _, err := buildDropSlotQuery("bad slot", false); err == nil {
		t.Error("buildDropSlotQuery accepted an invalid slot name")
	}
}

func TestParseCreateSlotResponse(t *testing.T) {
	res, err := parseCreateSlotResponse([]string{
		"beam_slot", "0/16B3748", "00000003-00000002-1", "pgoutput",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.SlotName != "beam_slot" {
		t.Errorf("SlotName = %q", res.SlotName)
	}
	if res.ConsistentLSN != "0/16B3748" {
		t.Errorf("ConsistentLSN = %q", res.ConsistentLSN)
	}
	if res.SnapshotName != "00000003-00000002-1" {
		t.Errorf("SnapshotName = %q", res.SnapshotName)
	}
	if res.OutputPlugin != "pgoutput" {
		t.Errorf("OutputPlugin = %q", res.OutputPlugin)
	}
}

// TestParseCreateSlotResponseNoExportSnapshot covers the NOEXPORT_SNAPSHOT
// case, where the server returns an empty snapshot_name column rather than
// omitting it.
func TestParseCreateSlotResponseNoExportSnapshot(t *testing.T) {
	res, err := parseCreateSlotResponse([]string{"beam_slot", "0/16B3748", "", "pgoutput"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.SnapshotName != "" {
		t.Errorf("expected empty snapshot name, got %q", res.SnapshotName)
	}
	if stmts := res.SnapshotIsolationStatements(); stmts != nil {
		t.Errorf("expected no isolation statements without a snapshot, got %v", stmts)
	}
}

func TestParseCreateSlotResponseRejectsShortRow(t *testing.T) {
	for _, fields := range [][]string{nil, {}, {"beam_slot"}} {
		if _, err := parseCreateSlotResponse(fields); err == nil {
			t.Errorf("parseCreateSlotResponse(%v) accepted a short row; "+
				"a missing consistent_point would silently start the stream at the wrong LSN", fields)
		}
	}
}

// TestSnapshotIsolationStatementsOrdering asserts the two properties that make
// a backfill correct: the transaction must open at REPEATABLE READ before the
// snapshot is adopted, and the snapshot name must be quoted as a literal.
func TestSnapshotIsolationStatementsOrdering(t *testing.T) {
	res := SlotCreationResult{SnapshotName: "00000003-00000002-1"}
	stmts := res.SnapshotIsolationStatements()

	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}
	if !strings.Contains(stmts[0], "REPEATABLE READ") {
		t.Errorf("first statement must set REPEATABLE READ, got %q", stmts[0])
	}
	if !strings.HasPrefix(stmts[0], "BEGIN") {
		t.Errorf("first statement must open the transaction, got %q", stmts[0])
	}
	if !strings.HasPrefix(stmts[1], "SET TRANSACTION SNAPSHOT") {
		t.Errorf("second statement must adopt the snapshot, got %q", stmts[1])
	}
	if !strings.Contains(stmts[1], "'00000003-00000002-1'") {
		t.Errorf("snapshot name must be a quoted literal, got %q", stmts[1])
	}
}

func TestSnapshotIsolationStatementsEscapeQuotes(t *testing.T) {
	res := SlotCreationResult{SnapshotName: "abc'; DROP TABLE users; --"}
	stmts := res.SnapshotIsolationStatements()
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}
	if !strings.Contains(stmts[1], "abc''; DROP TABLE users; --") {
		t.Errorf("single quote was not doubled: %q", stmts[1])
	}
	// The literal must remain balanced: an odd number of quotes would mean the
	// injected text escaped the literal.
	if strings.Count(stmts[1], "'")%2 != 0 {
		t.Errorf("unbalanced quotes in %q", stmts[1])
	}
}

func TestIsDuplicateSlotError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"sqlstate", errors.New("ERROR: 42710: replication slot \"beam_slot\" already exists"), true},
		{"message only", errors.New(`replication slot "beam_slot" already exists`), true},
		{"uppercase message", errors.New(`Replication Slot "x" Already Exists`), true},
		{"unrelated", errors.New("permission denied for database"), false},
		{"table exists is not a slot", errors.New(`relation "orders" already exists`), false},
		{"connection failure", errors.New("connection refused"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDuplicateSlotError(tc.err); got != tc.want {
				t.Errorf("isDuplicateSlotError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestEscapeSQLLiteral(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"plain", "plain"},
		{"it's", "it''s"},
		{"''", "''''"},
		{"a'b'c", "a''b''c"},
	}
	for _, tc := range tests {
		if got := escapeSQLLiteral(tc.in); got != tc.want {
			t.Errorf("escapeSQLLiteral(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestFailoverSlotRequiresPostgres17 checks that an unsupported server is an
// error rather than a silent downgrade.
//
// The point of the option is a slot that survives failover. Creating a slot
// that does not, and reporting success, would leave the operator believing in
// a guarantee they do not have — and they would only discover otherwise during
// the failover itself.
func TestFailoverSlotRequiresPostgres17(t *testing.T) {
	for _, version := range []int{13, 14, 15, 16} {
		_, err := buildCreateSlotQuery("beam_slot", "pgoutput", true, false, true, version)
		if err == nil {
			t.Errorf("buildCreateSlotQuery accepted a failover slot on PostgreSQL %d; "+
				"the server would reject the option and the slot would silently not survive a failover", version)
		}
	}

	if _, err := buildCreateSlotQuery("beam_slot", "pgoutput", true, false, true, 17); err != nil {
		t.Errorf("buildCreateSlotQuery rejected a failover slot on PostgreSQL 17: %v", err)
	}
}
