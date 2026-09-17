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

// Acceptance suite for the PostgreSQLIO connector.
//
// Each test here encodes behavior the connector must have, for a defect that
// has been fixed, so that a future change cannot quietly reintroduce it. The
// suite runs as part of the default `go test ./...` -- it is not build-tagged.
//
// Do not delete or weaken a test to make it pass. Several of these guard
// defects that affect the source database, not just the pipeline.
//
// Tests requiring a live PostgreSQL server (snapshot consistency, WAL
// retention under load, catalog bloat, TOAST round-trips, the PG 13-18 matrix)
// are specified in the engineering plan and belong in a testcontainers-backed
// integration suite; they are intentionally not in this file, which is
// hermetic and runs in milliseconds.

package postgresio

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// sslRequestMagic is the 8-byte PostgreSQL SSLRequest packet: a 4-byte length
// of 8 followed by the request code 80877103.
var sslRequestMagic = []byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f}

// dialObserver captures the first bytes a connection attempt puts on the wire.
//
// It uses net.Pipe rather than a TCP listener so the test is hermetic and
// needs no ports. The server side reads a fixed prefix and then closes, which
// aborts the handshake; the resulting error is irrelevant, the bytes are the
// assertion.
func observeFirstWireBytes(t *testing.T, n int, opts ...CDCOption) []byte {
	t.Helper()

	clientSide, serverSide := net.Pipe()
	observed := make(chan []byte, 1)

	go func() {
		defer func() { _ = serverSide.Close() }()
		buf := make([]byte, n)
		_ = serverSide.SetReadDeadline(time.Now().Add(5 * time.Second))
		read, err := io.ReadFull(serverSide, buf)
		if err != nil {
			observed <- buf[:read]
			return
		}
		observed <- buf
	}()

	dial := func(_ context.Context, _, _ string) (net.Conn, error) {
		return clientSide, nil
	}

	base := []CDCOption{
		WithCDCHost("db.example.com"),
		WithCDCPort(5432),
		WithCDCDatabase("orders"),
		WithCDCUsername("beam_cdc"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
		WithCDCDialFunc(dial),
	}

	cdcOpts := NewCDCOptions(append(base, opts...)...)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Expected to fail: the fake server closes mid-handshake.
	if stream, err := NewNativeReplicationStream(ctx, cdcOpts); err == nil && stream != nil {
		_ = stream.Close()
	}

	select {
	case got := <-observed:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the client to send anything")
		return nil
	}
}

// --- P0-4: transport security ---

// TestCDCDefaultSSLModeIsVerifyFull asserts the default is secure.
//
// FINDING P0-4. NewCDCOptions currently leaves SSLMode as the empty string,
// and the dial path treats empty as "skip TLS entirely", so the default
// configuration streams replication traffic -- including the credentials in
// the startup packet -- in cleartext.
//
// verify-full is the correct default rather than verify-ca because managed
// PostgreSQL providers sign every tenant's certificate with a shared regional
// CA: under verify-ca, a certificate issued to any other customer of the same
// provider validates successfully. The two modes are otherwise identical in
// cost (one hostname match at handshake), and this connector holds a single
// long-lived replication connection, so there is no performance argument.
func TestCDCDefaultSSLModeIsVerifyFull(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
	)

	if opts.SSLMode != "verify-full" {
		t.Errorf("default CDC SSLMode = %q, want \"verify-full\"; an unset or empty mode must never mean plaintext", opts.SSLMode)
	}
}

// TestWriteDefaultSSLModeIsVerifyFull applies the same requirement to the sink.
//
// FINDING P0-4. The sink currently defaults to "disable".
func TestWriteDefaultSSLModeIsVerifyFull(t *testing.T) {
	opts := NewWriteOptions()

	if opts.SSLMode != "verify-full" {
		t.Errorf("default write SSLMode = %q, want \"verify-full\"", opts.SSLMode)
	}
}

// TestCDCNeverConnectsInPlaintextByDefault is the wire-level proof of P0-4.
//
// With no explicit sslmode, the very first bytes the client sends must be the
// SSLRequest packet. If they are a startup packet instead, the connection is
// cleartext and the username, database and password have already been
// transmitted before any negotiation could occur.
func TestCDCNeverConnectsInPlaintextByDefault(t *testing.T) {
	got := observeFirstWireBytes(t, len(sslRequestMagic))

	if len(got) < len(sslRequestMagic) {
		t.Fatalf("client sent only %d bytes, want an %d-byte SSLRequest", len(got), len(sslRequestMagic))
	}

	for i := range sslRequestMagic {
		if got[i] != sslRequestMagic[i] {
			t.Fatalf("client did not send SSLRequest by default: got % x, want % x.\n"+
				"The connection is in cleartext and credentials have already left the process.", got, sslRequestMagic)
		}
	}
}

// TestCDCSendsSSLRequestForEveryVerifyingMode ensures no verifying mode
// silently skips negotiation. This currently passes and must keep passing.
func TestCDCSendsSSLRequestForEveryVerifyingMode(t *testing.T) {
	for _, mode := range []string{"require", "verify-ca", "verify-full"} {
		t.Run(mode, func(t *testing.T) {
			got := observeFirstWireBytes(t, len(sslRequestMagic), WithCDCSSLMode(mode))

			if len(got) < len(sslRequestMagic) {
				t.Fatalf("sslmode=%s sent only %d bytes, want an SSLRequest", mode, len(got))
			}
			for i := range sslRequestMagic {
				if got[i] != sslRequestMagic[i] {
					t.Fatalf("sslmode=%s did not send SSLRequest: got % x", mode, got)
				}
			}
		})
	}
}

// TestSSLModeDisableStillSkipsTLS confirms the explicit opt-out keeps working.
// Operators on a trusted unix socket or an encrypted overlay network need a
// way to turn TLS off; the requirement is that it is explicit, not the default.
func TestSSLModeDisableStillSkipsTLS(t *testing.T) {
	got := observeFirstWireBytes(t, len(sslRequestMagic), WithCDCSSLMode("disable"))

	if len(got) >= len(sslRequestMagic) {
		matches := true
		for i := range sslRequestMagic {
			if got[i] != sslRequestMagic[i] {
				matches = false
				break
			}
		}
		if matches {
			t.Error("sslmode=disable must not negotiate TLS, but an SSLRequest was sent")
		}
	}
}

// TestUnknownSSLModeIsRejected requires a typo to fail loudly at construction.
//
// FINDING P0-4. The dial path compares sslmode against a handful of string
// literals; anything unrecognized falls through to a partially-verifying
// branch. "verify_full" or "VerifyFull" should be a configuration error, not a
// silent downgrade.
func TestUnknownSSLModeIsRejected(t *testing.T) {
	typos := []string{
		"verify_full",
		"VerifyFull",
		"verifyfull",
		"full",
		"on",
		"true",
		"yes",
	}

	for _, mode := range typos {
		opts := NewCDCOptions(
			WithCDCSlotName("beam_slot"),
			WithCDCPublication("beam_pub"),
			WithCDCSSLMode(mode),
		)
		if err := opts.Validate(); err == nil {
			t.Errorf("Validate() accepted unrecognized sslmode %q; it must be a construction-time error", mode)
		}
	}
}

// TestValidSSLModesAreAccepted is the companion to the above: the five libpq
// modes this connector supports must not be rejected by the new validation.
func TestValidSSLModesAreAccepted(t *testing.T) {
	for _, mode := range []string{"disable", "require", "verify-ca", "verify-full"} {
		opts := NewCDCOptions(
			WithCDCSlotName("beam_slot"),
			WithCDCPublication("beam_pub"),
			WithCDCSSLMode(mode),
		)
		if err := opts.Validate(); err != nil {
			t.Errorf("Validate() rejected supported sslmode %q: %v", mode, err)
		}
	}
}

// --- P0-3: authentication ---

// TestSCRAMSHA256IsSupported drives a real handshake against a fake server
// that answers with AuthenticationSASL (auth type 10).
//
// FINDING P0-3. The handshake handles only auth types 0 (ok), 3 (cleartext)
// and 5 (md5), and returns "unsupported authentication type 10" otherwise.
// SCRAM-SHA-256 has been the default for new installations since PostgreSQL
// 14, and md5 is removed in PostgreSQL 18 -- so the connector cannot
// authenticate against a default modern server at all.
func TestSCRAMSHA256IsSupported(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		defer func() { _ = serverSide.Close() }()
		_ = serverSide.SetDeadline(time.Now().Add(5 * time.Second))

		// Read the startup packet: int32 length, then length-4 bytes of body.
		var lenBuf [4]byte
		if _, err := io.ReadFull(serverSide, lenBuf[:]); err != nil {
			return
		}
		bodyLen := int(binary.BigEndian.Uint32(lenBuf[:])) - 4
		if bodyLen < 0 || bodyLen > 1<<16 {
			return
		}
		if _, err := io.ReadFull(serverSide, make([]byte, bodyLen)); err != nil {
			return
		}

		// Reply with AuthenticationSASL advertising SCRAM-SHA-256.
		mechanism := "SCRAM-SHA-256"
		msgLen := 4 + 4 + len(mechanism) + 1 + 1

		msg := make([]byte, 0, msgLen+1)
		msg = append(msg, 'R')
		msg = binary.BigEndian.AppendUint32(msg, uint32(msgLen))
		msg = binary.BigEndian.AppendUint32(msg, 10) // AuthenticationSASL
		msg = append(msg, mechanism...)
		msg = append(msg, 0) // terminate the mechanism string
		msg = append(msg, 0) // terminate the mechanism list

		_, _ = serverSide.Write(msg)

		// Drain whatever the client sends back so it is not blocked on write.
		_, _ = io.ReadFull(serverSide, make([]byte, 1))
	}()

	dial := func(_ context.Context, _, _ string) (net.Conn, error) {
		return clientSide, nil
	}

	opts := NewCDCOptions(
		WithCDCHost("db.example.com"),
		WithCDCDatabase("orders"),
		WithCDCUsername("beam_cdc"),
		WithCDCPassword("correct horse battery staple"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
		WithCDCSSLMode("disable"), // isolate authentication from transport
		WithCDCDialFunc(dial),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := NewNativeReplicationStream(ctx, opts)
	if stream != nil {
		_ = stream.Close()
	}

	<-serverDone

	// The handshake cannot complete against this stub, so an error is
	// expected. What must NOT happen is a refusal to attempt SCRAM at all.
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "unsupported authentication") {
		t.Fatalf("connector rejected SCRAM-SHA-256: %v\n"+
			"SCRAM is the default for PostgreSQL 14+ and md5 is removed in PostgreSQL 18.", err)
	}
}

// TestMD5IsNotTheOnlyPasswordMechanism guards against a regression that would
// re-narrow authentication to md5 only. It is a source-level check because the
// alternative is a full SASL exchange against a live server.
func TestMD5IsNotTheOnlyPasswordMechanism(t *testing.T) {
	src, err := os.ReadFile("cdc_stream.go")
	if err != nil {
		t.Skipf("cannot read cdc_stream.go: %v", err)
	}

	if !strings.Contains(string(src), "SCRAM") {
		t.Error("cdc_stream.go contains no SCRAM handling; PostgreSQL 14+ defaults to SCRAM-SHA-256 and PostgreSQL 18 removes md5")
	}
}

// --- P1-1 / P0-6: LSN-aware conflict resolution ---

// TestCompactorKeepsHighestLSN is the core last-write-wins correctness test.
//
// FINDING P1-1. BatchCompactor.Add overwrites an existing key unconditionally.
// SortKey is captured into the entry and used to sort the flush order, but it
// is never compared during the overwrite. Beam gives no ordering guarantee
// between bundles, so a replayed or reordered older change silently overwrites
// a newer one, and the sink writes a stale row that no subsequent event
// corrects.
func TestCompactorKeepsHighestLSN(t *testing.T) {
	bc := NewBatchCompactor(100, 1<<20, time.Minute)

	newer := ChangeEvent{Operation: OpUpdate, Table: "orders", LSN: 200, After: map[string]any{"status": "shipped"}}
	older := ChangeEvent{Operation: OpUpdate, Table: "orders", LSN: 100, After: map[string]any{"status": "pending"}}

	// Newest arrives first; the stale replay arrives second.
	bc.Add("orders:1", []any{newer.LSN}, newer, 64)
	bc.Add("orders:1", []any{older.LSN}, older, 64)

	out := bc.CompactAndSort()
	if len(out) != 1 {
		t.Fatalf("expected the key to collapse to 1 record, got %d", len(out))
	}

	survivor, ok := out[0].(ChangeEvent)
	if !ok {
		t.Fatalf("unexpected record type %T", out[0])
	}

	if survivor.LSN != 200 {
		t.Errorf("compactor kept LSN %d, want 200. An older change overwrote a newer one; "+
			"the sink will persist %q instead of %q and never self-correct.",
			survivor.LSN, survivor.After["status"], "shipped")
	}
}

// TestCompactorKeepsHighestLSNRegardlessOfArrivalOrder checks the symmetric
// case so the fix cannot be a hardcoded "first write wins".
func TestCompactorKeepsHighestLSNRegardlessOfArrivalOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first uint64
		last  uint64
	}{
		{"ascending", 100, 200},
		{"descending", 200, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bc := NewBatchCompactor(100, 1<<20, time.Minute)

			bc.Add("orders:1", []any{tc.first}, ChangeEvent{LSN: tc.first}, 64)
			bc.Add("orders:1", []any{tc.last}, ChangeEvent{LSN: tc.last}, 64)

			out := bc.CompactAndSort()
			if len(out) != 1 {
				t.Fatalf("expected 1 record, got %d", len(out))
			}

			survivor := out[0].(ChangeEvent)
			want := tc.first
			if tc.last > want {
				want = tc.last
			}
			if survivor.LSN != want {
				t.Errorf("kept LSN %d, want the highest LSN %d", survivor.LSN, want)
			}
		})
	}
}

// TestCompactorBreaksLSNTiesDeterministically covers two mutations to the same
// row inside one transaction, which share a commit LSN.
//
// FINDING P0-6. Without an intra-transaction sequence number the two changes
// are indistinguishable and the surviving row depends on map iteration order.
// WS-5 adds ChangeEvent.TxSeq; once it exists this test should compare
// (LSN, TxSeq) rather than LSN alone.
func TestCompactorBreaksLSNTiesDeterministically(t *testing.T) {
	const sharedLSN uint64 = 500

	results := make(map[string]int)
	for i := 0; i < 50; i++ {
		bc := NewBatchCompactor(100, 1<<20, time.Minute)

		bc.Add("orders:1", []any{sharedLSN, 0}, ChangeEvent{LSN: sharedLSN, After: map[string]any{"status": "pending"}}, 64)
		bc.Add("orders:1", []any{sharedLSN, 1}, ChangeEvent{LSN: sharedLSN, After: map[string]any{"status": "shipped"}}, 64)

		out := bc.CompactAndSort()
		if len(out) != 1 {
			t.Fatalf("expected 1 record, got %d", len(out))
		}
		survivor := out[0].(ChangeEvent)
		results[survivor.After["status"].(string)]++
	}

	if len(results) != 1 {
		t.Fatalf("tie-break is not deterministic across runs: %v", results)
	}
	if results["shipped"] != 50 {
		t.Errorf("expected the later intra-transaction mutation (shipped) to win every time, got %v", results)
	}
}

// --- P0-1: WAL acknowledgment ---

// TestBundleFinalizationShortcutIsAbsent is a source-level guard for the
// single most dangerous property of this connector.
//
// FINDING P0-1, and the meta-finding that the test suite cannot catch it.
// cdc_source.go contains a branch of the form "if bf == nil { advance the
// confirmed LSN directly }". Tests pass a nil BundleFinalization and therefore
// exercise a path where acknowledgment always works. Production passes a
// non-nil value, takes the callback path, and -- because ProcessElement is an
// infinite loop that never returns -- never finalizes a bundle, so the
// callback never fires and confirmed_flush_lsn stays pinned at StartLSN
// forever. WAL accumulates until the primary's volume fills.
//
// Any fix must delete this shortcut so that tests and production share one
// acknowledgment path. This check is deliberately crude: it fails while the
// shortcut exists, and keeps failing if anyone reintroduces it.
func TestBundleFinalizationShortcutIsAbsent(t *testing.T) {
	src, err := os.ReadFile("cdc_source.go")
	if err != nil {
		t.Skipf("cannot read cdc_source.go: %v", err)
	}

	// Normalize spacing so formatting changes cannot hide the pattern.
	normalized := strings.Join(strings.Fields(string(src)), " ")

	for _, pattern := range []string{"if bf == nil", "if bf != nil"} {
		if strings.Contains(normalized, pattern) {
			t.Errorf("cdc_source.go still branches on %q.\n"+
				"Acknowledgment must follow one path in tests and in production; "+
				"a nil-BundleFinalization shortcut makes the suite green while production never "+
				"advances confirmed_flush_lsn and WAL grows without bound.", pattern)
		}
	}
}

// TestProcessElementIsNotAnUnboundedLoop guards the structural fix in WS-2.
//
// FINDING P0-1. A bundle can only finalize if ProcessElement returns. The
// rewritten source must return an sdf.ProcessContinuation at a commit boundary
// rather than looping forever.
func TestProcessElementIsNotAnUnboundedLoop(t *testing.T) {
	src, err := os.ReadFile("cdc_source.go")
	if err != nil {
		t.Skipf("cannot read cdc_source.go: %v", err)
	}

	if !strings.Contains(string(src), "ProcessContinuation") {
		t.Error("cdc_source.go does not return an sdf.ProcessContinuation.\n" +
			"Without returning from ProcessElement the bundle never finalizes, the " +
			"BundleFinalization callback never runs, and the replication slot is never acknowledged.")
	}
}

// TestCDCSourceDeclaresAWatermarkEstimator guards the windowing fix.
//
// FINDING P0-2. The package declares no watermark estimator, so the runner has
// no event-time signal from the CDC source. Downstream fixed or sliding
// windows cannot close reliably, which is why the streaming aggregation
// example cannot be trusted today.
func TestCDCSourceDeclaresAWatermarkEstimator(t *testing.T) {
	src, err := os.ReadFile("cdc_source.go")
	if err != nil {
		t.Skipf("cannot read cdc_source.go: %v", err)
	}

	if !strings.Contains(string(src), "WatermarkEstimator") {
		t.Error("cdc_source.go declares no WatermarkEstimator; downstream windows cannot close")
	}
}

// --- P0-5: slot lifecycle ---

// TestCreateSlotIfMissingIsNotADeadOption guards an exported option that
// currently does nothing.
//
// FINDING P0-5. WithCDCCreateSlotIfMissing sets a field that no code reads.
// The package issues no CREATE_REPLICATION_SLOT and performs no snapshot
// bootstrap, so a pipeline pointed at an existing table silently receives only
// changes that occur after it starts. Pre-existing rows are never emitted and
// nothing warns.
func TestCreateSlotIfMissingIsNotADeadOption(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Skipf("cannot enumerate package sources: %v", err)
	}

	found := false
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			continue
		}
		if strings.Contains(string(src), "CREATE_REPLICATION_SLOT") {
			found = true
			break
		}
	}

	if !found {
		t.Error("no source file issues CREATE_REPLICATION_SLOT.\n" +
			"WithCDCCreateSlotIfMissing is exported but inert, and without slot creation " +
			"there is no exported snapshot, so initial table state is never backfilled.")
	}
}

// --- P1-2: TOAST ---

// TestToastSentinelIsNotAStringLiteral guards against poisoning typed columns.
//
// FINDING P1-2. The parser writes the literal Go string "<unchanged_toast>"
// into the value map for an unchanged TOAST column. For a non-text column that
// value is not merely wrong, it is untypeable, and the cold-start reassembly
// path converts it to nil -- silently nulling large columns in the target.
// Absence must be represented structurally, not by an in-band magic string.
func TestToastSentinelIsNotAStringLiteral(t *testing.T) {
	src, err := os.ReadFile("pgoutput_parser.go")
	if err != nil {
		t.Skipf("cannot read pgoutput_parser.go: %v", err)
	}

	if strings.Contains(string(src), `"<unchanged_toast>"`) {
		t.Error(`pgoutput_parser.go still writes the literal "<unchanged_toast>" into column values.` + "\n" +
			"Use structural absence (omit the column and record it in UnchangedColumns) so the " +
			"sink can generate a partial update instead of overwriting the column.")
	}
}

// --- P1-4 / P2-1: sink hygiene ---

// TestSinkDoesNotUseDeprecatedCopyIn guards the sink write path.
//
// FINDING P2-1. pq.CopyIn is formally deprecated, and it double-quotes an
// identifier that the caller has already quoted. FINDING P1-4 is adjacent: the
// upsert path creates and drops a temporary table per micro-batch, churning
// pg_class and pg_attribute and degrading planning for the whole instance.
func TestSinkDoesNotUseDeprecatedCopyIn(t *testing.T) {
	src, err := os.ReadFile("write.go")
	if err != nil {
		t.Skipf("cannot read write.go: %v", err)
	}

	if strings.Contains(string(src), "pq.CopyIn") {
		t.Error("write.go still uses the deprecated pq.CopyIn; migrate to pgx.CopyFrom, which takes identifier parts and quotes them itself")
	}
}

// TestSinkDoesNotCreateATempTablePerBatch guards against catalog bloat.
//
// FINDING P1-4. The staging table must be created once per session and
// truncated between batches, not created and dropped per flush.
func TestSinkDoesNotCreateATempTablePerBatch(t *testing.T) {
	src, err := os.ReadFile("write.go")
	if err != nil {
		t.Skipf("cannot read write.go: %v", err)
	}

	body := string(src)
	if strings.Contains(body, "CREATE TEMP") && !strings.Contains(body, "TRUNCATE") {
		t.Error("write.go creates a temporary table but never truncates one.\n" +
			"Per-batch CREATE/DROP churns pg_class and pg_attribute, starves autovacuum on the " +
			"catalog, and degrades query planning instance-wide. Create the staging table once " +
			"in Setup and TRUNCATE between batches.")
	}
}

// TestOriginSetupErrorIsNotDiscarded guards bidirectional loop prevention.
//
// FINDING P1-5. The result of pg_replication_origin_xact_setup is assigned to
// blank identifiers. Any error inside a transaction poisons it (SQLSTATE
// 25P02), so the next statement fails with a confusing message, and the
// fallback write path loses origin tagging entirely -- which in a
// bidirectional topology means replication loops.
func TestOriginSetupErrorIsNotDiscarded(t *testing.T) {
	src, err := os.ReadFile("write.go")
	if err != nil {
		t.Skipf("cannot read write.go: %v", err)
	}

	if strings.Contains(string(src), "_, _ = txn.ExecContext") {
		t.Error("write.go discards the result of a transactional Exec.\n" +
			"An error inside a transaction poisons it (25P02); check the error and fail with the required grant named.")
	}
}

// --- P1-3: type fidelity ---

// TestBinaryDecoderHandlesNumericUUIDAndInterval guards decoding.
//
// FINDING P1-3. In binary mode the decoder has no case for NUMERIC (OID 1700),
// UUID (2950) or INTERVAL (1186), so those columns fall through to the default
// branch and are emitted as raw wire bytes. NUMERIC is the money type; silently
// handing back an opaque byte slice is a data-fidelity failure for exactly the
// workload most likely to adopt this connector.
func TestBinaryDecoderHandlesNumericUUIDAndInterval(t *testing.T) {
	src, err := os.ReadFile("pgoutput_parser.go")
	if err != nil {
		t.Skipf("cannot read pgoutput_parser.go: %v", err)
	}

	body := string(src)
	for _, oid := range []struct {
		name string
		code string
	}{
		{"NUMERIC", "1700"},
		{"UUID", "2950"},
		{"INTERVAL", "1186"},
	} {
		if !strings.Contains(body, oid.code) {
			t.Errorf("no binary decoding branch for %s (OID %s); values are emitted as raw []byte", oid.name, oid.code)
		}
	}
}
