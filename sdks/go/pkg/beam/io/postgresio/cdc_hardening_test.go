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
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"testing"
)

// These tests cover the input-validation and resource-release guards added to
// the decode path. Each one drives a frame that the server would never send but
// that a corrupted, truncated or hostile stream can produce. The property under
// test is uniform: the decoder must return an error rather than panic or
// allocate on an attacker-chosen length. A panic in a DoFn fails the bundle,
// and because Beam retries a failed bundle against the same bytes, a panic
// repeats on every retry instead of clearing.

// buildRelationFrame assembles a well-formed Relation ('R') message so that the
// Insert tests below have a registered schema to decode against.
func buildRelationFrame(relID uint32, cols []ColumnDef) []byte {
	var buf bytes.Buffer
	buf.WriteByte('R')
	_ = binary.Write(&buf, binary.BigEndian, relID)
	buf.WriteString("public\000")
	buf.WriteString("t\000")
	buf.WriteByte('d') // replica identity: default
	_ = binary.Write(&buf, binary.BigEndian, int16(len(cols)))
	for _, c := range cols {
		buf.WriteByte(c.Flags)
		buf.WriteString(c.Name + "\000")
		_ = binary.Write(&buf, binary.BigEndian, c.TypeOID)
		_ = binary.Write(&buf, binary.BigEndian, c.TypeModifier)
	}
	return buf.Bytes()
}

// TestParseTupleDataRejectsHostileLengths covers the two length fields inside a
// tuple that are signed on the wire and size an allocation directly.
func TestParseTupleDataRejectsHostileLengths(t *testing.T) {
	const relID = uint32(9001)
	cols := []ColumnDef{{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1}}

	// A field length of -2 is not the NULL (-1) or unchanged-TOAST sentinel, so
	// it reaches make([]byte, length) and panics with "len out of range".
	var negField bytes.Buffer
	negField.WriteByte('I')
	_ = binary.Write(&negField, binary.BigEndian, relID)
	negField.WriteByte('N')
	_ = binary.Write(&negField, binary.BigEndian, int16(1))
	negField.WriteByte('t')                                  // text-format datum
	_ = binary.Write(&negField, binary.BigEndian, int32(-2)) // hostile length

	// A field length of 2GB is well formed but cannot be satisfied by a frame
	// this small. Without the bound it reserves 2GB per frame.
	var hugeField bytes.Buffer
	hugeField.WriteByte('I')
	_ = binary.Write(&hugeField, binary.BigEndian, relID)
	hugeField.WriteByte('N')
	_ = binary.Write(&hugeField, binary.BigEndian, int16(1))
	hugeField.WriteByte('t')
	_ = binary.Write(&hugeField, binary.BigEndian, int32(1<<31-1))

	// A negative column count panics make([]ColumnValue, numCols).
	var negCols bytes.Buffer
	negCols.WriteByte('I')
	_ = binary.Write(&negCols, binary.BigEndian, relID)
	negCols.WriteByte('N')
	_ = binary.Write(&negCols, binary.BigEndian, int16(-1))

	// A column count far larger than the bytes left cannot be honest: every
	// column costs at least its one-byte kind tag.
	var hugeCols bytes.Buffer
	hugeCols.WriteByte('I')
	_ = binary.Write(&hugeCols, binary.BigEndian, relID)
	hugeCols.WriteByte('N')
	_ = binary.Write(&hugeCols, binary.BigEndian, int16(32767))

	tests := []struct {
		name  string
		frame []byte
	}{
		{"NegativeFieldLength", negField.Bytes()},
		{"FieldLengthExceedsFrame", hugeField.Bytes()},
		{"NegativeColumnCount", negCols.Bytes()},
		{"ColumnCountExceedsFrame", hugeCols.Bytes()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewPgOutputParser()
			if _, err := parser.ParseMessages(buildRelationFrame(relID, cols)); err != nil {
				t.Fatalf("relation frame should decode: %v", err)
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decoder panicked on a malformed frame instead of returning an error: %v", r)
				}
			}()
			events, err := parser.ParseMessages(tt.frame)
			if err == nil {
				t.Fatalf("expected an error, got %d events", len(events))
			}
			if len(events) != 0 {
				t.Errorf("no events should be emitted alongside the error, got %d", len(events))
			}
		})
	}
}

// TestParseRelationRejectsHostileColumnCount covers the same exposure in the
// Relation message, whose column count sizes a []ColumnDef.
func TestParseRelationRejectsHostileColumnCount(t *testing.T) {
	for _, tt := range []struct {
		name     string
		numCols  int16
		trailing int
	}{
		{"Negative", -1, 0},
		{"ExceedsFrame", 32767, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteByte('R')
			_ = binary.Write(&buf, binary.BigEndian, uint32(9002))
			buf.WriteString("public\000")
			buf.WriteString("t\000")
			buf.WriteByte('d')
			_ = binary.Write(&buf, binary.BigEndian, tt.numCols)
			buf.Write(make([]byte, tt.trailing))

			parser := NewPgOutputParser()
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Relation decode panicked instead of returning an error: %v", r)
				}
			}()
			if _, err := parser.ParseMessages(buf.Bytes()); err == nil {
				t.Fatal("expected an error for a malformed Relation column count")
			}
		})
	}
}

// TestDecodeBinaryArrayRejectsHostileLengths covers the binary array decoder,
// which both allocates from wire lengths and indexes fixed-width items
// directly.
func TestDecodeBinaryArrayRejectsHostileLengths(t *testing.T) {
	// header writes the five int32 array header fields.
	header := func(buf *bytes.Buffer, elemOID, dimLen int32) {
		_ = binary.Write(buf, binary.BigEndian, int32(1)) // ndim
		_ = binary.Write(buf, binary.BigEndian, int32(0)) // flags
		_ = binary.Write(buf, binary.BigEndian, elemOID)
		_ = binary.Write(buf, binary.BigEndian, dimLen)
		_ = binary.Write(buf, binary.BigEndian, int32(1)) // lower bound
	}

	var negDim bytes.Buffer
	header(&negDim, 23, -1)

	var hugeDim bytes.Buffer
	header(&hugeDim, 23, 1<<30)

	var negItem bytes.Buffer
	header(&negItem, 23, 1)
	_ = binary.Write(&negItem, binary.BigEndian, int32(-7))

	var hugeItem bytes.Buffer
	header(&hugeItem, 23, 1)
	_ = binary.Write(&hugeItem, binary.BigEndian, int32(1<<31-1))

	// An INT4 item declared as 2 bytes passes the remaining-bytes check but is
	// then handed to binary.BigEndian.Uint32, which indexes itemBytes[:4] and
	// panics with a slice bounds error.
	var shortInt4 bytes.Buffer
	header(&shortInt4, 23, 1)
	_ = binary.Write(&shortInt4, binary.BigEndian, int32(2))
	shortInt4.Write([]byte{0x00, 0x01})

	// Same for INT8, which needs 8 bytes.
	var shortInt8 bytes.Buffer
	header(&shortInt8, 20, 1)
	_ = binary.Write(&shortInt8, binary.BigEndian, int32(4))
	shortInt8.Write([]byte{0x00, 0x00, 0x00, 0x01})

	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"NegativeDimension", negDim.Bytes()},
		{"DimensionExceedsBuffer", hugeDim.Bytes()},
		{"NegativeItemLength", negItem.Bytes()},
		{"ItemLengthExceedsBuffer", hugeItem.Bytes()},
		{"Int4ItemTooShort", shortInt4.Bytes()},
		{"Int8ItemTooShort", shortInt8.Bytes()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodeBinaryArray panicked instead of returning an error: %v", r)
				}
			}()
			got, err := DecodeBinaryArray(tt.data)
			if err == nil {
				t.Fatalf("expected an error, got %v", got)
			}
		})
	}
}

// TestSpooledTransactionLimit verifies that reassembling a streamed in-progress
// transaction fails once it would exceed the configured buffer, rather than
// growing until the worker is killed.
//
// PostgreSQL streams an in-progress transaction so that neither side holds all
// of it at once. The parser gives that property back by re-accumulating the
// transaction to emit atomically at Stream Commit, so the bound is what keeps a
// single large transaction from sizing worker memory.
func TestSpooledTransactionLimit(t *testing.T) {
	const relID = uint32(9003)
	const xid = uint32(555)
	cols := []ColumnDef{
		{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1},
		{Name: "payload", TypeOID: 25, TypeModifier: -1},
	}

	// insertFrame builds an Insert carrying a payload of the requested size.
	insertFrame := func(payload string) []byte {
		var buf bytes.Buffer
		buf.WriteByte('I')
		_ = binary.Write(&buf, binary.BigEndian, relID)
		buf.WriteByte('N')
		_ = binary.Write(&buf, binary.BigEndian, int16(2))
		buf.WriteByte('t')
		_ = binary.Write(&buf, binary.BigEndian, int32(1))
		buf.WriteString("1")
		buf.WriteByte('t')
		_ = binary.Write(&buf, binary.BigEndian, int32(len(payload)))
		buf.WriteString(payload)
		return buf.Bytes()
	}

	streamStart := func() []byte {
		var buf bytes.Buffer
		buf.WriteByte('S')
		_ = binary.Write(&buf, binary.BigEndian, xid)
		buf.WriteByte(1) // first segment
		return buf.Bytes()
	}

	newStreamingParser := func(t *testing.T, limit int64) *PgOutputParser {
		t.Helper()
		p := NewPgOutputParser()
		p.SetMaxSpooledBytes(limit)
		if _, err := p.ParseMessages(buildRelationFrame(relID, cols)); err != nil {
			t.Fatalf("relation frame should decode: %v", err)
		}
		if _, err := p.ParseMessages(streamStart()); err != nil {
			t.Fatalf("stream start should decode: %v", err)
		}
		return p
	}

	t.Run("ErrorsOnceLimitExceeded", func(t *testing.T) {
		// 4 KiB is below the per-event base cost, so the second insert must
		// trip the limit even though neither insert is individually large.
		p := newStreamingParser(t, 4096)
		payload := strings.Repeat("x", 2048)

		var sawError bool
		for i := 0; i < 16; i++ {
			events, err := p.ParseMessages(insertFrame(payload))
			if err != nil {
				sawError = true
				if !strings.Contains(err.Error(), "streamed transaction buffer") {
					t.Errorf("error should identify the spool limit, got: %v", err)
				}
				break
			}
			if len(events) != 0 {
				t.Fatalf("a streamed insert must be spooled, not emitted; got %d events", len(events))
			}
		}
		if !sawError {
			t.Fatal("expected the spool limit to be enforced within 16 inserts")
		}
	})

	t.Run("StreamAbortReleasesBudget", func(t *testing.T) {
		// Aborting must return the budget. If it did not, a long-lived stream
		// that rolls back repeatedly would ratchet into permanent failure.
		p := newStreamingParser(t, 1<<20)
		payload := strings.Repeat("x", 4096)
		for i := 0; i < 8; i++ {
			if _, err := p.ParseMessages(insertFrame(payload)); err != nil {
				t.Fatalf("insert %d should spool: %v", i, err)
			}
		}
		spooledBefore := p.spooledBytes
		if spooledBefore <= 0 {
			t.Fatalf("expected a non-zero spool charge, got %d", spooledBefore)
		}

		var abort bytes.Buffer
		abort.WriteByte('A')
		_ = binary.Write(&abort, binary.BigEndian, xid)
		_ = binary.Write(&abort, binary.BigEndian, uint32(0))
		if _, err := p.ParseMessages(abort.Bytes()); err != nil {
			t.Fatalf("stream abort should decode: %v", err)
		}
		if p.spooledBytes != 0 {
			t.Errorf("stream abort must release the whole charge, %d bytes still held", p.spooledBytes)
		}
		if len(p.spooledBytesByXID) != 0 {
			t.Errorf("per-xid accounting leaked %d entries", len(p.spooledBytesByXID))
		}
	})

	t.Run("StreamCommitReleasesBudget", func(t *testing.T) {
		p := newStreamingParser(t, 1<<20)
		payload := strings.Repeat("x", 4096)
		for i := 0; i < 8; i++ {
			if _, err := p.ParseMessages(insertFrame(payload)); err != nil {
				t.Fatalf("insert %d should spool: %v", i, err)
			}
		}

		var commit bytes.Buffer
		commit.WriteByte('c')
		_ = binary.Write(&commit, binary.BigEndian, xid)
		commit.WriteByte(0)
		_ = binary.Write(&commit, binary.BigEndian, uint64(7000)) // commit LSN
		_ = binary.Write(&commit, binary.BigEndian, uint64(7008)) // end LSN
		events, err := p.ParseMessages(commit.Bytes())
		if err != nil {
			t.Fatalf("stream commit should decode: %v", err)
		}
		if len(events) != 8 {
			t.Fatalf("expected 8 spooled events at commit, got %d", len(events))
		}
		if p.spooledBytes != 0 {
			t.Errorf("stream commit must release the whole charge, %d bytes still held", p.spooledBytes)
		}
	})

	t.Run("OversizedTxnSkipReclaimsHeapAndSkipsOnCommit", func(t *testing.T) {
		p := newStreamingParser(t, 4096)
		p.SetOversizedTxnPolicy(OversizedTxnSkip)
		payload := strings.Repeat("x", 2048)

		// Send inserts exceeding the 4096-byte limit
		for i := 0; i < 8; i++ {
			events, err := p.ParseMessages(insertFrame(payload))
			if err != nil {
				t.Fatalf("OversizedTxnSkip must not error on overflow, got: %v", err)
			}
			if len(events) != 0 {
				t.Fatalf("streamed insert should not emit early, got %d events", len(events))
			}
		}

		// Buffer must be immediately refunded upon crossing the limit
		if p.spooledBytes != 0 {
			t.Errorf("OversizedTxnSkip must free spooled bytes immediately, still holds %d", p.spooledBytes)
		}
		if !p.oversizedXIDs[xid] {
			t.Errorf("expected xid %d to be tracked in oversizedXIDs", xid)
		}

		// Stream commit must clean up tracking and emit 0 events
		var commit bytes.Buffer
		commit.WriteByte('c')
		_ = binary.Write(&commit, binary.BigEndian, xid)
		commit.WriteByte(0)
		_ = binary.Write(&commit, binary.BigEndian, uint64(8000))
		_ = binary.Write(&commit, binary.BigEndian, uint64(8008))
		events, err := p.ParseMessages(commit.Bytes())
		if err != nil {
			t.Fatalf("stream commit for skipped txn should decode cleanly, got: %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("stream commit must not emit partial events for skipped txn, got %d", len(events))
		}
		if p.oversizedXIDs[xid] {
			t.Errorf("expected xid %d to be removed from oversizedXIDs after commit", xid)
		}
		if p.LastCommittedLSN() != 8008 {
			t.Errorf("expected LastCommittedLSN 8008 after skipped commit, got %d", p.LastCommittedLSN())
		}
	})

	t.Run("SubxidAbortPreservesTopLevelTransaction", func(t *testing.T) {
		p := newStreamingParser(t, 1<<20)
		payload := strings.Repeat("x", 1024)

		if _, err := p.ParseMessages(insertFrame(payload)); err != nil {
			t.Fatalf("failed to spool initial insert: %v", err)
		}

		// Issue Stream Abort for a nested subxid (savepoint rollback)
		var subAbort bytes.Buffer
		subAbort.WriteByte('A')
		_ = binary.Write(&subAbort, binary.BigEndian, xid)
		_ = binary.Write(&subAbort, binary.BigEndian, uint32(9999)) // subxid != xid
		if _, err := p.ParseMessages(subAbort.Bytes()); err != nil {
			t.Fatalf("subxid abort should decode cleanly: %v", err)
		}

		// Top-level transaction must remain intact in spooled buffer
		if len(p.spooledTransactions[xid]) != 1 {
			t.Fatalf("subxid abort must not purge top-level transaction; expected 1 event, got %d", len(p.spooledTransactions[xid]))
		}

		// Full abort with xid == subxid purges it
		var fullAbort bytes.Buffer
		fullAbort.WriteByte('A')
		_ = binary.Write(&fullAbort, binary.BigEndian, xid)
		_ = binary.Write(&fullAbort, binary.BigEndian, xid) // subxid == xid
		if _, err := p.ParseMessages(fullAbort.Bytes()); err != nil {
			t.Fatalf("full abort should decode cleanly: %v", err)
		}
		if len(p.spooledTransactions[xid]) != 0 {
			t.Errorf("full abort must purge transaction, still holds %d", len(p.spooledTransactions[xid]))
		}
	})

	t.Run("LimitDisabledByNonPositive", func(t *testing.T) {
		p := newStreamingParser(t, 0)
		payload := strings.Repeat("x", 4096)
		for i := 0; i < 64; i++ {
			if _, err := p.ParseMessages(insertFrame(payload)); err != nil {
				t.Fatalf("a non-positive limit must disable the bound, insert %d failed: %v", i, err)
			}
		}
	})
}

// countingStream records how many times the session's underlying connection was
// closed, so the refcount tests can distinguish "released" from "closed".
type countingStream struct {
	mu     sync.Mutex
	closes int
}

func (c *countingStream) NextMessage(ctx context.Context) ([]byte, error) { return nil, nil }

func (c *countingStream) SendStandbyStatus(ctx context.Context, status StandbyStatus) error {
	return nil
}

func (c *countingStream) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}

func (c *countingStream) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// TestTeardownReleasesSharedSessionByRefcount verifies the ownership rule for a
// session shared between DoFn instances on one worker.
//
// Teardown previously ran its release only when Options.StreamFactory was set,
// a field injected by tests, so on a real pipeline nothing was released: the
// walsender stayed attached and the slot stayed active until the process
// exited, which blocks restart and failover. The replacement must close exactly
// once, when the last holder leaves, and must not close while another instance
// is still reading.
func TestTeardownReleasesSharedSessionByRefcount(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("beam_refcount_slot"),
		WithCDCPublication("beam_refcount_pub"),
		WithCDCHost("localhost"),
		WithCDCDatabase("testdb"),
	)
	key := cdcSessionKey(opts)

	stream := &countingStream{}
	session := &cdcSession{
		stream: stream,
		parser: NewPgOutputParser(),
		done:   make(chan struct{}),
	}
	session.refs.Store(2)

	globalCDCSessionsMu.Lock()
	globalCDCSessions[key] = session
	globalCDCSessionsMu.Unlock()
	t.Cleanup(func() {
		globalCDCSessionsMu.Lock()
		delete(globalCDCSessions, key)
		globalCDCSessionsMu.Unlock()
	})

	first := &cdcSourceFn{Options: opts, session: session}
	second := &cdcSourceFn{Options: opts, session: session}

	if err := first.Teardown(); err != nil {
		t.Fatalf("first Teardown: %v", err)
	}
	if got := stream.closeCount(); got != 0 {
		t.Fatalf("session must stay open while another holder remains, closed %d times", got)
	}
	globalCDCSessionsMu.Lock()
	_, stillPublished := globalCDCSessions[key]
	globalCDCSessionsMu.Unlock()
	if !stillPublished {
		t.Error("session must stay published while another holder remains")
	}

	if err := second.Teardown(); err != nil {
		t.Fatalf("second Teardown: %v", err)
	}
	if got := stream.closeCount(); got != 1 {
		t.Fatalf("last holder must close the session exactly once, closed %d times", got)
	}

	// The session must be unpublished before it is closed, so a concurrent
	// ensureSession cannot adopt a connection that is being torn down.
	globalCDCSessionsMu.Lock()
	_, stillPublishedAfter := globalCDCSessions[key]
	globalCDCSessionsMu.Unlock()
	if stillPublishedAfter {
		t.Error("a closed session must not remain in the global session map")
	}

	// Teardown is idempotent: a runner may call it more than once, and the
	// closeOnce guard must keep that from double-closing the connection.
	if err := first.Teardown(); err != nil {
		t.Fatalf("repeat Teardown: %v", err)
	}
	if got := stream.closeCount(); got != 1 {
		t.Errorf("repeat Teardown must not close again, closed %d times", got)
	}
}

// TestTeardownClosesSoleSession covers the common single-instance case.
func TestTeardownClosesSoleSession(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("beam_sole_slot"),
		WithCDCPublication("beam_sole_pub"),
	)
	key := cdcSessionKey(opts)

	stream := &countingStream{}
	session := &cdcSession{
		stream: stream,
		parser: NewPgOutputParser(),
		done:   make(chan struct{}),
	}
	session.refs.Store(1)

	globalCDCSessionsMu.Lock()
	globalCDCSessions[key] = session
	globalCDCSessionsMu.Unlock()
	t.Cleanup(func() {
		globalCDCSessionsMu.Lock()
		delete(globalCDCSessions, key)
		globalCDCSessionsMu.Unlock()
	})

	fn := &cdcSourceFn{Options: opts, session: session}
	if err := fn.Teardown(); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	if got := stream.closeCount(); got != 1 {
		t.Errorf("sole holder must close the session, closed %d times", got)
	}
	if !session.isClosed.Load() {
		t.Error("session should be marked closed")
	}
}
