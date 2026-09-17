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
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// --- pgoutput message builders, so ordering can be asserted end to end ---

func pgBeginMessage(lsn uint64, xid uint32) []byte {
	var buf bytes.Buffer
	buf.WriteByte('B')
	binary.Write(&buf, binary.BigEndian, lsn)
	binary.Write(&buf, binary.BigEndian, int64(0)) // commit time
	binary.Write(&buf, binary.BigEndian, xid)
	return buf.Bytes()
}

func pgCommitMessage(commitLSN, endLSN uint64) []byte {
	var buf bytes.Buffer
	buf.WriteByte('C')
	buf.WriteByte(0) // flags
	binary.Write(&buf, binary.BigEndian, commitLSN)
	binary.Write(&buf, binary.BigEndian, endLSN)
	binary.Write(&buf, binary.BigEndian, int64(0))
	return buf.Bytes()
}

// pgRelationMessage declares a two-column table: id (int4, key) and val (text).
func pgRelationMessage(relID uint32, schema, table string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('R')
	binary.Write(&buf, binary.BigEndian, relID)
	buf.WriteString(schema)
	buf.WriteByte(0)
	buf.WriteString(table)
	buf.WriteByte(0)
	buf.WriteByte('d')                              // replica identity default
	binary.Write(&buf, binary.BigEndian, uint16(2)) // column count

	buf.WriteByte(1) // flags: part of key
	buf.WriteString("id")
	buf.WriteByte(0)
	binary.Write(&buf, binary.BigEndian, uint32(23)) // int4
	binary.Write(&buf, binary.BigEndian, int32(-1))

	buf.WriteByte(0)
	buf.WriteString("val")
	buf.WriteByte(0)
	binary.Write(&buf, binary.BigEndian, uint32(25)) // text
	binary.Write(&buf, binary.BigEndian, int32(-1))

	return buf.Bytes()
}

func pgInsertMessage(relID uint32, id, val string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('I')
	binary.Write(&buf, binary.BigEndian, relID)
	buf.WriteByte('N')
	writeTupleData(&buf, id, val)
	return buf.Bytes()
}

func pgUpdateMessage(relID uint32, id, val string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('U')
	binary.Write(&buf, binary.BigEndian, relID)
	buf.WriteByte('N')
	writeTupleData(&buf, id, val)
	return buf.Bytes()
}

func writeTupleData(buf *bytes.Buffer, values ...string) {
	binary.Write(buf, binary.BigEndian, uint16(len(values)))
	for _, v := range values {
		buf.WriteByte('t')
		binary.Write(buf, binary.BigEndian, uint32(len(v)))
		buf.WriteString(v)
	}
}

// TestParserAssignsIntraTransactionSequence is the behavioral proof for
// FINDING P0-6. It decodes a real transaction containing several changes to the
// same row and asserts that the events carry distinct, increasing sequence
// numbers despite sharing one LSN.
func TestParserAssignsIntraTransactionSequence(t *testing.T) {
	p := NewPgOutputParser()

	if _, err := p.ParseMessages(pgBeginMessage(0x16B3748, 501)); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := p.ParseMessages(pgRelationMessage(1, "public", "orders")); err != nil {
		t.Fatalf("Relation: %v", err)
	}

	var events []*ChangeEvent
	for _, val := range []string{"first", "second", "third"} {
		evs, err := p.ParseMessages(pgUpdateMessage(1, "1", val))
		if err != nil {
			t.Fatalf("Update %s: %v", val, err)
		}
		events = append(events, evs...)
	}

	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}

	// All three share the BEGIN record's LSN. This is the premise of the
	// finding: if it ever stopped holding, LSN alone would suffice.
	for i, ev := range events {
		if ev.LSN != 0x16B3748 {
			t.Errorf("event %d LSN = %x, want 16B3748", i, ev.LSN)
		}
	}

	for i, ev := range events {
		if ev.TxSeq != uint32(i) {
			t.Errorf("event %d TxSeq = %d, want %d", i, ev.TxSeq, i)
		}
	}

	// The sequence is what makes the dedup identifier unique. Without it all
	// three collide and a deduplicating runner keeps only one.
	seen := map[string]bool{}
	for _, ev := range events {
		if seen[ev.EventID] {
			t.Errorf("duplicate EventID %q; a deduplicating runner would drop this change", ev.EventID)
		}
		seen[ev.EventID] = true
	}
}

// TestParserResetsSequencePerTransaction asserts the counter is scoped to a
// transaction rather than growing for the life of the stream.
func TestParserResetsSequencePerTransaction(t *testing.T) {
	p := NewPgOutputParser()
	if _, err := p.ParseMessages(pgRelationMessage(1, "public", "orders")); err != nil {
		t.Fatalf("Relation: %v", err)
	}

	for tx := 0; tx < 3; tx++ {
		lsn := uint64(0x1000 * (tx + 1))
		if _, err := p.ParseMessages(pgBeginMessage(lsn, uint32(600+tx))); err != nil {
			t.Fatalf("Begin: %v", err)
		}

		for i := 0; i < 2; i++ {
			evs, err := p.ParseMessages(pgInsertMessage(1, fmt.Sprint(i), "v"))
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if len(evs) != 1 {
				t.Fatalf("got %d events", len(evs))
			}
			if evs[0].TxSeq != uint32(i) {
				t.Errorf("transaction %d event %d: TxSeq = %d, want %d", tx, i, evs[0].TxSeq, i)
			}
		}

		if _, err := p.ParseMessages(pgCommitMessage(lsn, lsn+8)); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	// The counter map must not retain finished transactions.
	if n := len(p.txSeqByXID); n != 0 {
		t.Errorf("txSeqByXID retained %d entries after all transactions committed; "+
			"a long-lived stream would leak one entry per transaction", n)
	}
}

// --- spooler ordering ---

// TestSortChangeEventsRestoresReplicationOrder feeds the spooler's sort a
// deliberately shuffled transaction and asserts the original order is
// recovered. BagState returns its contents in an unspecified order, so this is
// the property the sink depends on.
func TestSortChangeEventsRestoresReplicationOrder(t *testing.T) {
	original := make([]ChangeEvent, 0, 12)
	for seq := 0; seq < 6; seq++ {
		original = append(original, ChangeEvent{
			LSN:       0x1000,
			TxSeq:     uint32(seq),
			Operation: OpUpdate,
			Table:     "orders",
		})
	}
	for seq := 0; seq < 6; seq++ {
		original = append(original, ChangeEvent{
			LSN:       0x2000,
			TxSeq:     uint32(seq),
			Operation: OpUpdate,
			Table:     "orders",
		})
	}

	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 200; trial++ {
		shuffled := make([]ChangeEvent, len(original))
		copy(shuffled, original)
		rng.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})

		sortChangeEvents(shuffled)

		for i := range original {
			if shuffled[i].LSN != original[i].LSN || shuffled[i].TxSeq != original[i].TxSeq {
				t.Fatalf("trial %d position %d: got (LSN=%x, TxSeq=%d), want (LSN=%x, TxSeq=%d)",
					trial, i, shuffled[i].LSN, shuffled[i].TxSeq, original[i].LSN, original[i].TxSeq)
			}
		}
	}
}

// TestSortChangeEventsOrdersByLSNBeforeSequence asserts LSN dominates, so a
// later transaction's first change never precedes an earlier transaction's
// last change.
func TestSortChangeEventsOrdersByLSNBeforeSequence(t *testing.T) {
	events := []ChangeEvent{
		{LSN: 0x2000, TxSeq: 0},
		{LSN: 0x1000, TxSeq: 9},
	}
	sortChangeEvents(events)

	if events[0].LSN != 0x1000 {
		t.Errorf("a later transaction sorted ahead of an earlier one: %+v", events)
	}
	if events[0].TxSeq != 9 {
		t.Errorf("expected the earlier transaction's change first, got TxSeq %d", events[0].TxSeq)
	}
}

// TestSortChangeEventsIsStable protects events written before TxSeq existed,
// which all carry zero. They must keep arrival order rather than be shuffled.
func TestSortChangeEventsIsStable(t *testing.T) {
	events := []ChangeEvent{
		{LSN: 0x1000, TxSeq: 0, EventID: "a"},
		{LSN: 0x1000, TxSeq: 0, EventID: "b"},
		{LSN: 0x1000, TxSeq: 0, EventID: "c"},
	}
	sortChangeEvents(events)

	for i, want := range []string{"a", "b", "c"} {
		if events[i].EventID != want {
			t.Errorf("position %d = %q, want %q; equal keys must preserve arrival order",
				i, events[i].EventID, want)
		}
	}
}

// TestSortChangeEventsHandlesDegenerateInput guards the boundary cases a sort
// comparator is easy to get wrong on.
func TestSortChangeEventsHandlesDegenerateInput(t *testing.T) {
	sortChangeEvents(nil)
	sortChangeEvents([]ChangeEvent{})

	one := []ChangeEvent{{LSN: 5, TxSeq: 3}}
	sortChangeEvents(one)
	if one[0].LSN != 5 || one[0].TxSeq != 3 {
		t.Error("single-element slice was modified")
	}

	// LSN values above 2^32 must compare as unsigned 64-bit, not truncate.
	big := []ChangeEvent{
		{LSN: 0x1_0000_0000, TxSeq: 0},
		{LSN: 0x0_FFFF_FFFF, TxSeq: 0},
	}
	sortChangeEvents(big)
	if big[0].LSN != 0x0_FFFF_FFFF {
		t.Errorf("LSN comparison truncated above 2^32: %+v", big)
	}
}

// TestStreamedTransactionGetsCommitLSN covers the streamed path, which has no
// BEGIN record. Events must be stamped with the commit LSN rather than
// inheriting whatever LSN the previously decoded transaction left behind.
func TestStreamedTransactionGetsCommitLSN(t *testing.T) {
	p := NewPgOutputParser()
	if _, err := p.ParseMessages(pgRelationMessage(1, "public", "orders")); err != nil {
		t.Fatalf("Relation: %v", err)
	}

	// A prior ordinary transaction leaves a stale LSN in parser state.
	if _, err := p.ParseMessages(pgBeginMessage(0xAAAA, 700)); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := p.ParseMessages(pgInsertMessage(1, "1", "old")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, err := p.ParseMessages(pgCommitMessage(0xAAAA, 0xAAB0)); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Now a streamed transaction, which sends no BEGIN.
	var streamStart bytes.Buffer
	streamStart.WriteByte('S')
	binary.Write(&streamStart, binary.BigEndian, uint32(701))
	streamStart.WriteByte(1)
	if _, err := p.ParseMessages(streamStart.Bytes()); err != nil {
		t.Fatalf("StreamStart: %v", err)
	}

	for _, v := range []string{"a", "b"} {
		evs, err := p.ParseMessages(pgInsertMessage(1, "2", v))
		if err != nil {
			t.Fatalf("streamed Insert: %v", err)
		}
		if len(evs) != 0 {
			t.Fatalf("streamed changes must be spooled, got %d emitted", len(evs))
		}
	}

	var streamCommit bytes.Buffer
	streamCommit.WriteByte('c')
	binary.Write(&streamCommit, binary.BigEndian, uint32(701))
	streamCommit.WriteByte(0)
	binary.Write(&streamCommit, binary.BigEndian, uint64(0xBBBB)) // commit LSN
	binary.Write(&streamCommit, binary.BigEndian, uint64(0xBBC0)) // end LSN
	binary.Write(&streamCommit, binary.BigEndian, int64(0))

	events, err := p.ParseMessages(streamCommit.Bytes())
	if err != nil {
		t.Fatalf("StreamCommit: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events on stream commit, want 2", len(events))
	}

	for i, ev := range events {
		if ev.LSN != 0xBBBB {
			t.Errorf("event %d LSN = %x, want BBBB; the event kept the previous transaction's "+
				"stale LSN and would sort into the wrong transaction", i, ev.LSN)
		}
		if ev.TxSeq != uint32(i) {
			t.Errorf("event %d TxSeq = %d, want %d", i, ev.TxSeq, i)
		}
	}
}

// TestStreamedTransactionsInterleaveWithoutSequenceCollision is why the counter
// is keyed by XID. Two streamed transactions arrive in alternating segments; a
// single shared counter would restart on each resumed segment and assign the
// same sequence twice within one transaction.
func TestStreamedTransactionsInterleaveWithoutSequenceCollision(t *testing.T) {
	p := NewPgOutputParser()
	if _, err := p.ParseMessages(pgRelationMessage(1, "public", "orders")); err != nil {
		t.Fatalf("Relation: %v", err)
	}

	startStream := func(xid uint32, first bool) {
		var buf bytes.Buffer
		buf.WriteByte('S')
		binary.Write(&buf, binary.BigEndian, xid)
		if first {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		if _, err := p.ParseMessages(buf.Bytes()); err != nil {
			t.Fatalf("StreamStart(%d): %v", xid, err)
		}
	}
	stopStream := func() {
		if _, err := p.ParseMessages([]byte{'E'}); err != nil {
			t.Fatalf("StreamStop: %v", err)
		}
	}
	commitStream := func(xid uint32, commitLSN uint64) []ChangeEvent {
		var buf bytes.Buffer
		buf.WriteByte('c')
		binary.Write(&buf, binary.BigEndian, xid)
		buf.WriteByte(0)
		binary.Write(&buf, binary.BigEndian, commitLSN)
		binary.Write(&buf, binary.BigEndian, commitLSN+8)
		binary.Write(&buf, binary.BigEndian, int64(0))
		evs, err := p.ParseMessages(buf.Bytes())
		if err != nil {
			t.Fatalf("StreamCommit(%d): %v", xid, err)
		}
		out := make([]ChangeEvent, 0, len(evs))
		for _, e := range evs {
			out = append(out, *e)
		}
		return out
	}

	// xid 801 segment 1, xid 802 segment 1, xid 801 segment 2, xid 802 segment 2.
	for _, step := range []struct {
		xid   uint32
		first bool
		vals  []string
	}{
		{801, true, []string{"a1", "a2"}},
		{802, true, []string{"b1"}},
		{801, false, []string{"a3"}},
		{802, false, []string{"b2", "b3"}},
	} {
		startStream(step.xid, step.first)
		for _, v := range step.vals {
			if _, err := p.ParseMessages(pgInsertMessage(1, "1", v)); err != nil {
				t.Fatalf("Insert %s: %v", v, err)
			}
		}
		stopStream()
	}

	for _, tc := range []struct {
		xid       uint32
		commitLSN uint64
		want      int
	}{
		{801, 0xC000, 3},
		{802, 0xD000, 3},
	} {
		events := commitStream(tc.xid, tc.commitLSN)
		if len(events) != tc.want {
			t.Fatalf("xid %d: got %d events, want %d", tc.xid, len(events), tc.want)
		}

		seen := map[uint32]bool{}
		for i, ev := range events {
			if seen[ev.TxSeq] {
				t.Errorf("xid %d: TxSeq %d assigned twice; interleaved segments collided",
					tc.xid, ev.TxSeq)
			}
			seen[ev.TxSeq] = true
			if ev.TxSeq != uint32(i) {
				t.Errorf("xid %d event %d: TxSeq = %d, want %d", tc.xid, i, ev.TxSeq, i)
			}
		}
	}

	if n := len(p.txSeqByXID); n != 0 {
		t.Errorf("txSeqByXID retained %d entries after both streams committed", n)
	}
}

// TestStreamAbortReleasesSequenceCounter asserts an aborted transaction does
// not leave state behind.
func TestStreamAbortReleasesSequenceCounter(t *testing.T) {
	p := NewPgOutputParser()
	if _, err := p.ParseMessages(pgRelationMessage(1, "public", "orders")); err != nil {
		t.Fatalf("Relation: %v", err)
	}

	var start bytes.Buffer
	start.WriteByte('S')
	binary.Write(&start, binary.BigEndian, uint32(900))
	start.WriteByte(1)
	if _, err := p.ParseMessages(start.Bytes()); err != nil {
		t.Fatalf("StreamStart: %v", err)
	}
	if _, err := p.ParseMessages(pgInsertMessage(1, "1", "doomed")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var abort bytes.Buffer
	abort.WriteByte('A')
	binary.Write(&abort, binary.BigEndian, uint32(900))
	binary.Write(&abort, binary.BigEndian, uint32(900))
	if _, err := p.ParseMessages(abort.Bytes()); err != nil {
		t.Fatalf("StreamAbort: %v", err)
	}

	if n := len(p.txSeqByXID); n != 0 {
		t.Errorf("txSeqByXID retained %d entries after abort", n)
	}
	if n := len(p.spooledTransactions); n != 0 {
		t.Errorf("spooledTransactions retained %d entries after abort", n)
	}
}

// TestEventIDDistinguishesRepeatedRowUpdates is the dedup-collision case stated
// directly: same row, same transaction, therefore same LSN, XID, table and
// primary key. Only the sequence number separates them.
func TestEventIDDistinguishesRepeatedRowUpdates(t *testing.T) {
	base := ChangeEvent{
		Operation:     OpUpdate,
		Schema:        "public",
		Table:         "orders",
		LSN:           0x16B3748,
		TransactionID: 501,
		PrimaryKeys:   []string{"id"},
		Before:        map[string]any{"id": 1},
		After:         map[string]any{"id": 1},
		CommitTime:    time.Unix(0, 0),
	}

	first := base
	first.TxSeq = 0
	second := base
	second.TxSeq = 1

	if first.EventIDString() == second.EventIDString() {
		t.Errorf("two changes to the same row in one transaction share EventID %q; "+
			"a deduplicating runner would discard the second", first.EventIDString())
	}
}
