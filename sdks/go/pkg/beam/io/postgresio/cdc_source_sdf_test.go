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
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
)

// --- harness ---

type sdfHarness struct {
	fn     *cdcSourceFn
	stream *MockReplicationStream
	bf     *mockBundleFinalizer
	we     *sdf.ManualWatermarkEstimator
	rt     *sdf.LockRTracker

	emitted    []ChangeEvent
	eventTimes []beam.EventTime
}

// testCheckpointInterval keeps the idle path fast. Production defaults to
// DefaultCheckpointInterval.
const testCheckpointInterval = 150 * time.Millisecond

// newSDFHarness wires the source to a scripted replication stream and returns
// everything a test needs to drive one ProcessElement call.
//
// idleWhenDrained selects what happens after the scripted messages run out:
// true models a connection that stays open but goes quiet, false models the
// server ending the copy stream.
func newSDFHarness(t *testing.T, idleWhenDrained bool, messages ...[]byte) *sdfHarness {
	t.Helper()

	stream := NewMockReplicationStream(messages...)
	stream.SetIdleWhenDrained(idleWhenDrained)

	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(50*time.Millisecond),
		WithCDCCheckpointInterval(testCheckpointInterval),
		WithCDCStreamFactory(func(context.Context, CDCOptions) (ReplicationStream, error) {
			return stream, nil
		}),
	)

	fn := newCDCSourceFn(opts)
	fn.preflightDone = true
	fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) {
		return newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)}), nil
	}
	t.Cleanup(func() { _ = fn.Teardown() })

	// A transaction that never commits is a deliberate scenario in a few tests
	// below. Shorten the stall budget so they finish in milliseconds instead of
	// the two minutes production allows.
	previousStallTimeout := transactionStallTimeout
	transactionStallTimeout = 4 * testCheckpointInterval
	t.Cleanup(func() { transactionStallTimeout = previousStallTimeout })

	h := &sdfHarness{
		fn:     fn,
		stream: stream,
		bf:     &mockBundleFinalizer{},
		we:     fn.CreateWatermarkEstimator(),
		rt:     fn.CreateTracker(fn.CreateInitialRestriction(1)),
	}
	return h
}

func (h *sdfHarness) run(ctx context.Context, t *testing.T) sdf.ProcessContinuation {
	t.Helper()
	cont, err := h.fn.ProcessElement(ctx, h.we, h.bf, h.rt, 1, func(et beam.EventTime, e ChangeEvent) {
		h.eventTimes = append(h.eventTimes, et)
		h.emitted = append(h.emitted, e)
	})
	if err != nil {
		t.Fatalf("ProcessElement returned an error: %v", err)
	}
	return cont
}

// runExpectingStall drives one ProcessElement call that is expected to fail
// because a transaction never completed. It returns the error.
func (h *sdfHarness) runExpectingStall(ctx context.Context, t *testing.T) error {
	t.Helper()
	_, err := h.fn.ProcessElement(ctx, h.we, h.bf, h.rt, 1, func(et beam.EventTime, e ChangeEvent) {
		h.eventTimes = append(h.eventTimes, et)
		h.emitted = append(h.emitted, e)
	})
	if err == nil {
		t.Fatal("ProcessElement returned successfully with a transaction still open; " +
			"the restriction would be checkpointed midway through a transaction and its tail lost")
	}
	return err
}

// registeredCallbacks reports how many acknowledgment callbacks the invocation
// handed to the runner.
func (h *sdfHarness) registeredCallbacks() int {
	h.bf.mu.Lock()
	defer h.bf.mu.Unlock()
	return len(h.bf.callbacks)
}

func (h *sdfHarness) confirmedLSN(t *testing.T) uint64 {
	t.Helper()
	session := h.fn.currentSession()
	if session == nil {
		t.Fatal("expected an open replication session")
	}
	return session.confirmedFlushLSN.Load()
}

// keepalivePayload builds a server keepalive ('k') frame.
func keepalivePayload(walEnd uint64, replyRequested bool) []byte {
	var buf bytes.Buffer
	buf.WriteByte('k')
	_ = binary.Write(&buf, binary.BigEndian, walEnd)
	_ = binary.Write(&buf, binary.BigEndian, GoTimeToPg(time.Now()))
	if replyRequested {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	return buf.Bytes()
}

// --- P0-1: the source must return so bundles can finalize ---

// TestProcessElementReturnsWhenTheStreamIsIdle is the behavioural counterpart
// to the source-level guard in remediation_acceptance_test.go.
//
// FINDING P0-1. The previous implementation looped forever reading the stream.
// A bundle cannot finalize until ProcessElement returns, so the acknowledgment
// callback never ran and confirmed_flush_lsn stayed pinned at its starting
// value while the primary accumulated WAL.
func TestProcessElementReturnsWhenTheStreamIsIdle(t *testing.T) {
	h := newSDFHarness(t, true) // connected, no data

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan sdf.ProcessContinuation, 1)
	go func() { done <- h.run(ctx, t) }()

	select {
	case cont := <-done:
		if !cont.ShouldResume() {
			t.Error("an idle stream should yield a resuming continuation, not a terminal one")
		}
	case <-time.After(testCheckpointInterval + 10*time.Second):
		t.Fatal("ProcessElement did not return on an idle stream; " +
			"the bundle can never finalize and the replication slot is never acknowledged")
	}
}

// TestAcknowledgmentRequiresBundleFinalization pins the ordering that protects
// against data loss: the confirmed LSN may only move after the runner reports
// the bundle's output durable.
func TestAcknowledgmentRequiresBundleFinalization(t *testing.T) {
	commitTime := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	h := newSDFHarness(t, true,
		buildMockBeginPayload(900, commitTime, 7),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockCommitPayload(900, 900, commitTime),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.run(ctx, t)

	if len(h.emitted) != 1 {
		t.Fatalf("expected 1 event, got %d", len(h.emitted))
	}
	if got := h.confirmedLSN(t); got != 0 {
		t.Fatalf("confirmed LSN is %d before finalization, want 0", got)
	}

	if err := h.bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	if got := h.confirmedLSN(t); got != 900 {
		t.Fatalf("confirmed LSN is %d after finalization, want 900", got)
	}
}

// TestAcknowledgmentReportsTheConfirmedLSNToTheServer checks the value that
// actually governs WAL retention. flush_lsn and apply_lsn must carry the
// finalized position; reporting the received position instead would let the
// server discard WAL for records the pipeline has not committed.
func TestAcknowledgmentReportsTheConfirmedLSNToTheServer(t *testing.T) {
	commitTime := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	h := newSDFHarness(t, true,
		buildMockBeginPayload(1500, commitTime, 3),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockCommitPayload(1500, 1500, commitTime),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.run(ctx, t)

	session := h.fn.currentSession()
	if session == nil {
		t.Fatal("expected an open session")
	}

	// Before finalization the reported flush position must not include the
	// unacknowledged record.
	session.sendStatus(ctx)
	statuses := h.stream.GetStatusLog()
	last := statuses[len(statuses)-1]
	if last.FlushLSN >= 1500 {
		t.Errorf("flush_lsn reported as %d before finalization; the server would be free to discard WAL for records the pipeline has not committed", last.FlushLSN)
	}
	if last.WriteLSN < 1500 {
		t.Errorf("write_lsn reported as %d, want at least 1500: the received position is advisory and should reflect what was read", last.WriteLSN)
	}

	if err := h.bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	session.sendStatus(ctx)
	statuses = h.stream.GetStatusLog()
	last = statuses[len(statuses)-1]
	if last.FlushLSN != 1500 || last.ApplyLSN != 1500 {
		t.Errorf("after finalization flush/apply are %d/%d, want 1500/1500", last.FlushLSN, last.ApplyLSN)
	}
}

// TestQuietPublicationStillAdvancesAcknowledgment covers the case that breaks
// production databases most often: a slot whose publication covers only
// low-traffic tables.
//
// The server keeps sending keepalives as the global WAL advances. If the source
// only ever acknowledges positions it saw change records for, the slot pins WAL
// for the entire database's write traffic even though the pipeline has nothing
// to do.
func TestQuietPublicationStillAdvancesAcknowledgment(t *testing.T) {
	h := newSDFHarness(t, true, keepalivePayload(7_000_000, false))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.run(ctx, t)

	if len(h.emitted) != 0 {
		t.Fatalf("expected no events, got %d", len(h.emitted))
	}
	if err := h.bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	if got := h.confirmedLSN(t); got != 7_000_000 {
		t.Fatalf("confirmed LSN is %d, want 7000000: a keepalive with no buffered transaction is safe to acknowledge, and refusing to do so pins WAL on a quiet publication", got)
	}
}

// TestKeepaliveIsNotAcknowledgedWhileATransactionIsBuffered is the safety side
// of the previous test.
//
// A streamed transaction is delivered in segments and its events are withheld
// until commit. A keepalive arriving mid-transaction reports a WAL position
// beyond records the parser is still holding, so acknowledging it would let the
// server discard WAL the pipeline needs after a restart.
func TestKeepaliveIsNotAcknowledgedWhileATransactionIsBuffered(t *testing.T) {
	h := newSDFHarness(t, true,
		pgStreamStartMessage(11),
		pgRelationMessage(42, "public", "orders"),
		pgInsertMessage(42, "1", "pending"),
		keepalivePayload(9_000_000, false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The streamed transaction never commits, so the stream goes silent with
	// the parser holding its events. The invocation cannot return here without
	// abandoning them, so it fails the bundle instead.
	h.runExpectingStall(ctx, t)

	if n := h.registeredCallbacks(); n != 0 {
		t.Fatalf("%d acknowledgment callbacks were registered while a transaction was still buffered; "+
			"finalizing would advance the slot past records the parser is still holding", n)
	}
	if err := h.bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	for _, status := range h.stream.GetStatusLog() {
		if status.FlushLSN >= 9_000_000 {
			t.Fatalf("reported flush_lsn %d is past a transaction still buffered in the parser; those records would be lost after a restart", status.FlushLSN)
		}
	}
}

// TestKeepaliveIsNotAcknowledgedMidOrdinaryTransaction covers the same hazard
// for a transaction that is not streamed.
//
// pgoutput transmits a non-streamed transaction only after it commits, as
// Begin, changes, Commit. The walsender can interleave a keepalive between
// those frames, and that keepalive's position is at or beyond the
// transaction's commit LSN. Acknowledging it before the Commit arrives tells
// the server the transaction is fully consumed; a restart at that point loses
// the changes that had not yet been received.
//
// A predicate that only tracks streamed transactions reports "nothing
// buffered" here and lets the acknowledgment through.
func TestKeepaliveIsNotAcknowledgedMidOrdinaryTransaction(t *testing.T) {
	commitTime := time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)
	h := newSDFHarness(t, true,
		buildMockBeginPayload(4000, commitTime, 21),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		// The keepalive lands between the Begin and the Commit, reporting a WAL
		// position well past the transaction's own LSN.
		keepalivePayload(9_000_000, false),
		buildMockInsertPayload(42, []string{"2"}),
		buildMockCommitPayload(4000, 4000, commitTime),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.run(ctx, t)

	if len(h.emitted) != 2 {
		t.Fatalf("expected both rows of the transaction, got %d", len(h.emitted))
	}
	if err := h.bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	if got := h.confirmedLSN(t); got != 4000 {
		t.Fatalf("confirmed LSN is %d, want 4000: the keepalive arrived while the transaction was only partially received, "+
			"so its position must not be acknowledged, and the transaction's own LSN must be once the Commit arrives", got)
	}
}

// TestCommitClearsTheBufferedTransactionFlag is the counterpart: once the
// transaction is complete the source must resume acknowledging, otherwise the
// flag introduced above would pin the slot forever.
func TestCommitClearsTheBufferedTransactionFlag(t *testing.T) {
	commitTime := time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)
	h := newSDFHarness(t, true,
		buildMockBeginPayload(4000, commitTime, 21),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockCommitPayload(4000, 4100, commitTime),
		keepalivePayload(9_000_000, false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.run(ctx, t)

	session := h.fn.currentSession()
	if session == nil {
		t.Fatal("expected an open session")
	}
	if session.parser.HasBufferedTransaction() {
		t.Fatal("the parser still reports a buffered transaction after Commit; the slot would never advance again")
	}
	if err := h.bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	if got := session.confirmedFlushLSN.Load(); got != 9_000_000 {
		t.Fatalf("confirmed LSN is %d after a completed transaction and a keepalive, want 9000000", got)
	}
}

// --- P0-2: watermark ---

// TestWatermarkAdvancesToTheCommitTimestamp checks that the source produces an
// event-time signal.
//
// FINDING P0-2. Without a watermark the runner has no event-time input from the
// source, so downstream fixed and sliding windows never close.
func TestWatermarkAdvancesToTheCommitTimestamp(t *testing.T) {
	commitTime := time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)
	h := newSDFHarness(t, true,
		buildMockBeginPayload(2000, commitTime, 5),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockCommitPayload(2000, 2000, commitTime),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.run(ctx, t)

	if len(h.emitted) != 1 {
		t.Fatalf("expected 1 event, got %d", len(h.emitted))
	}
	if got := h.we.CurrentWatermark(); !got.Equal(commitTime) {
		t.Errorf("watermark is %v, want the transaction commit time %v", got, commitTime)
	}
	if want := mtime.FromTime(commitTime); h.eventTimes[0] != want {
		t.Errorf("element event time is %v, want %v", h.eventTimes[0], want)
	}
}

// TestWatermarkNeverRegresses guards the monotonicity the runner assumes.
// Commit timestamps are not globally ordered across concurrent transactions, so
// a naive assignment would move the watermark backwards and cause the runner to
// treat already-windowed data as late.
func TestWatermarkNeverRegresses(t *testing.T) {
	later := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	earlier := later.Add(-2 * time.Hour)

	we := &sdf.ManualWatermarkEstimator{}
	advanceWatermark(we, later)
	advanceWatermark(we, earlier)

	if got := we.CurrentWatermark(); !got.Equal(later) {
		t.Errorf("watermark regressed to %v after observing an earlier commit; want it held at %v", got, later)
	}
}

// --- restriction and claiming ---

// TestRestrictionIsUnbounded checks that the source produces an unbounded
// PCollection. A bounded restriction would let the runner decide the source is
// complete and shut the pipeline down.
func TestRestrictionIsUnbounded(t *testing.T) {
	fn := newCDCSourceFn(NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
	))

	rest := fn.CreateInitialRestriction(1)
	if rest.End != math.MaxInt64 {
		t.Errorf("restriction end is %d, want math.MaxInt64 so the range is treated as growable", rest.End)
	}

	rt := fn.CreateTracker(rest)
	if rt.IsBounded() {
		t.Error("the restriction tracker reports bounded; the CDC source is an unbounded read and the runner would terminate the pipeline")
	}
}

// TestRestrictionIsNeverSplit pins the single-consumer constraint. PostgreSQL
// permits one connection per replication slot, so handing part of the LSN range
// to a second worker cannot work.
func TestRestrictionIsNeverSplit(t *testing.T) {
	fn := newCDCSourceFn(NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
	))

	rest := fn.CreateInitialRestriction(1)
	splits := fn.SplitRestriction(1, rest)
	if len(splits) != 1 || splits[0] != rest {
		t.Errorf("SplitRestriction returned %v, want exactly the input restriction: a replication slot admits one consumer", splits)
	}
}

// TestOneClaimPerTransaction checks the claim discipline.
//
// pgoutput stamps every change in a transaction with the LSN of its BEGIN
// record, so a claim per event would attempt to claim the same position twice.
// offsetrange rejects a non-increasing claim and puts the tracker into an error
// state, which fails the bundle.
func TestOneClaimPerTransaction(t *testing.T) {
	commitTime := time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)
	h := newSDFHarness(t, true,
		buildMockBeginPayload(3000, commitTime, 9),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockInsertPayload(42, []string{"2"}),
		buildMockInsertPayload(42, []string{"3"}),
		buildMockCommitPayload(3000, 3000, commitTime),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.run(ctx, t)

	if len(h.emitted) != 3 {
		t.Fatalf("expected 3 events from one transaction, got %d", len(h.emitted))
	}
	if err := h.rt.GetError(); err != nil {
		t.Fatalf("restriction tracker errored: %v; every change in a pgoutput transaction carries the same LSN, so the position must be claimed once per transaction", err)
	}
}

// TestCheckpointResumesAfterTheLastClaimedLSN checks that a self-checkpoint
// hands the runner a residual that starts where this invocation stopped, and
// leaves no gap that would silently drop records.
func TestCheckpointResumesAfterTheLastClaimedLSN(t *testing.T) {
	fn := newCDCSourceFn(NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
	))

	rt := fn.CreateTracker(offsetrange.Restriction{Start: 100, End: math.MaxInt64})
	if !rt.TryClaim(int64(500)) {
		t.Fatalf("claiming 500 failed: %v", rt.GetError())
	}

	_, residual, err := rt.TrySplit(0)
	if err != nil {
		t.Fatalf("TrySplit: %v", err)
	}
	res, ok := residual.(offsetrange.Restriction)
	if !ok {
		t.Fatalf("residual has type %T, want offsetrange.Restriction", residual)
	}
	if res.Start != 501 {
		t.Errorf("residual starts at %d, want 501 (one past the last claimed LSN)", res.Start)
	}
	if res.End != math.MaxInt64 {
		t.Errorf("residual ends at %d, want math.MaxInt64 so the resumed range stays unbounded", res.End)
	}
	if !rt.IsDone() {
		t.Error("the primary should be done after a checkpointing split, otherwise the harness reports data loss")
	}
}

// --- serialization ---

// TestSourceOptionsSurviveSerialization guards a failure that is invisible on
// the direct runner.
//
// A structural DoFn is shipped to workers as JSON (graphx.encodeFn calls
// jsonx.Marshal on the receiver). JSON ignores unexported fields, so holding
// the configuration in one would deliver a zero-valued CDCOptions to every
// distributed runner: no host, no slot name, no publication.
func TestSourceOptionsSurviveSerialization(t *testing.T) {
	original := newCDCSourceFn(NewCDCOptions(
		WithCDCHost("db.example.com"),
		WithCDCPort(6543),
		WithCDCDatabase("shop"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
	))

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshalling the DoFn failed: %v", err)
	}

	var decoded cdcSourceFn
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling the DoFn failed: %v", err)
	}

	if decoded.Options.SlotName != "beam_slot" {
		t.Errorf("slot name after a round trip is %q, want %q: the worker would connect with no slot configured", decoded.Options.SlotName, "beam_slot")
	}
	if decoded.Options.Publication != "beam_pub" {
		t.Errorf("publication after a round trip is %q, want %q", decoded.Options.Publication, "beam_pub")
	}
	if decoded.Options.Host != "db.example.com" || decoded.Options.Port != 6543 || decoded.Options.Database != "shop" {
		t.Errorf("connection settings did not survive a round trip: %+v", decoded.Options)
	}
}

// --- framing ---

// TestIdleReadDeadlineDoesNotDesynchronizeTheStream exercises the interaction
// between the bounded read and the wire protocol.
//
// The source gives every read a deadline so ProcessElement can return. If that
// deadline could expire midway through a frame, the unread tail of the frame
// would be parsed as the next frame's header and the connection would be
// unusable. The deadline must therefore apply only while waiting for a frame to
// begin.
func TestIdleReadDeadlineDoesNotDesynchronizeTheStream(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	stream := &NativeReplicationStream{conn: client}

	// First read: nothing is sent, so the deadline expires cleanly.
	idleCtx, cancelIdle := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelIdle()
	if _, err := stream.NextMessage(idleCtx); !isIdleTimeout(err) {
		t.Fatalf("expected an idle timeout with nothing on the wire, got %v", err)
	}

	// Second read: a frame is written in two pieces with a gap longer than the
	// first deadline. Once the frame has started the read must run to
	// completion regardless of that deadline.
	body := []byte("hello-wal")
	frame := make([]byte, 5+len(body))
	frame[0] = 'd'
	binary.BigEndian.PutUint32(frame[1:5], uint32(4+len(body)))
	copy(frame[5:], body)

	go func() {
		_, _ = server.Write(frame[:1])
		time.Sleep(250 * time.Millisecond)
		_, _ = server.Write(frame[1:])
	}()

	readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRead()
	payload, err := stream.NextMessage(readCtx)
	if err != nil {
		t.Fatalf("reading a frame that arrived in pieces failed: %v", err)
	}
	if string(payload) != string(body) {
		t.Errorf("payload is %q, want %q: the frame was reassembled incorrectly", payload, body)
	}

	// Third read: the connection must still be usable and correctly framed.
	go func() {
		second := make([]byte, 5+4)
		second[0] = 'd'
		binary.BigEndian.PutUint32(second[1:5], 8)
		copy(second[5:], "next")
		_, _ = server.Write(second)
	}()

	nextCtx, cancelNext := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelNext()
	payload, err = stream.NextMessage(nextCtx)
	if err != nil {
		t.Fatalf("the connection was left unusable after an idle timeout: %v", err)
	}
	if string(payload) != "next" {
		t.Errorf("payload is %q, want %q: the stream lost frame alignment", payload, "next")
	}
}

// TestEndOfStreamSchedulesAReconnect checks that a server-side CopyDone does
// not terminate the source. A replication connection can end for reasons that
// do not mean the pipeline is finished, such as a failover.
func TestEndOfStreamSchedulesAReconnect(t *testing.T) {
	h := newSDFHarness(t, false) // drained immediately, reports io.EOF

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cont := h.run(ctx, t)

	if !cont.ShouldResume() {
		t.Error("end of stream produced a terminal continuation; the source would never reconnect")
	}
	if h.fn.currentSession() != nil {
		t.Error("the closed session was retained; the next invocation would read from a dead connection")
	}
}

// --- transaction boundary discipline ---

// pacedStream delivers scripted frames with a configurable pause before each
// one, so a test can choose where in the stream the checkpoint deadline
// expires.
type pacedStream struct {
	messages [][]byte
	pauses   []time.Duration

	mu        sync.Mutex
	idx       int
	waited    []bool
	statuses  []StandbyStatus
	closed    chan struct{}
	closeOnce sync.Once
}

func newPacedStream(messages [][]byte, pauses []time.Duration) *pacedStream {
	return &pacedStream{
		messages: messages,
		pauses:   pauses,
		waited:   make([]bool, len(messages)),
		closed:   make(chan struct{}),
	}
}

func (s *pacedStream) NextMessage(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	idx := s.idx
	var pause time.Duration
	if idx < len(s.messages) && !s.waited[idx] {
		pause = s.pauses[idx]
	}
	s.mu.Unlock()

	if idx >= len(s.messages) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.closed:
			return nil, io.EOF
		}
	}

	if pause > 0 {
		timer := time.NewTimer(pause)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			// The caller gave up waiting. Record the wait as served so a
			// later attempt finds the frame already on the wire, which is how
			// a real stream behaves across successive invocations.
			s.mu.Lock()
			s.waited[idx] = true
			s.mu.Unlock()
			return nil, ctx.Err()
		case <-s.closed:
			return nil, io.EOF
		}
		s.mu.Lock()
		s.waited[idx] = true
		s.mu.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.idx++
	return s.messages[idx], nil
}

func (s *pacedStream) SendStandbyStatus(_ context.Context, status StandbyStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = append(s.statuses, status)
	return nil
}

func (s *pacedStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

// TestProcessElementDoesNotReturnMidTransaction is the counterpart to
// TestProcessElementReturnsWhenTheStreamIsIdle, and the two together define
// when the source is allowed to give up control.
//
// The restriction tracker addresses positions as a single int64 LSN, and every
// change in a pgoutput transaction carries the LSN of its Begin record. A
// residual restriction created partway through a transaction therefore starts
// one past that shared LSN, and PostgreSQL skips a transaction whose commit LSN
// precedes the requested start position. The remainder of the interrupted
// transaction is never redelivered.
//
// The checkpoint deadline is consequently advisory while a transaction is open:
// the invocation must read on to the Commit frame before returning.
func TestProcessElementDoesNotReturnMidTransaction(t *testing.T) {
	commitTime := time.Date(2026, 7, 2, 11, 0, 0, 0, time.UTC)
	messages := [][]byte{
		buildMockBeginPayload(6000, commitTime, 31),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockInsertPayload(42, []string{"2"}),
		buildMockCommitPayload(6000, 6000, commitTime),
	}
	// The gap before the second row outlasts the checkpoint interval, so a
	// deadline check that ignored transaction state would return here.
	pauses := []time.Duration{0, 0, 0, 2 * testCheckpointInterval, 0}

	stream := newPacedStream(messages, pauses)
	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(50*time.Millisecond),
		WithCDCCheckpointInterval(testCheckpointInterval),
		WithCDCStreamFactory(func(context.Context, CDCOptions) (ReplicationStream, error) {
			return stream, nil
		}),
	)

	fn := newCDCSourceFn(opts)
	t.Cleanup(func() { _ = fn.Teardown() })

	previousStallTimeout := transactionStallTimeout
	transactionStallTimeout = 10 * testCheckpointInterval
	t.Cleanup(func() { transactionStallTimeout = previousStallTimeout })

	bf := &mockBundleFinalizer{}
	we := fn.CreateWatermarkEstimator()
	rt := fn.CreateTracker(fn.CreateInitialRestriction(1))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var emitted []ChangeEvent
	if _, err := fn.ProcessElement(ctx, we, bf, rt, 1, func(_ beam.EventTime, e ChangeEvent) {
		emitted = append(emitted, e)
	}); err != nil {
		t.Fatalf("ProcessElement returned an error: %v", err)
	}

	if len(emitted) != 2 {
		t.Fatalf("emitted %d of the transaction's 2 rows; the invocation returned partway through a "+
			"transaction and the remainder would not be redelivered", len(emitted))
	}
	if err := bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	session := fn.currentSession()
	if session == nil {
		t.Fatal("expected an open session")
	}
	if got := session.confirmedFlushLSN.Load(); got != 6000 {
		t.Fatalf("confirmed LSN is %d, want 6000 once the whole transaction is emitted and finalized", got)
	}
}

// TestCheckpointedRestrictionResumesWithoutLoss is the execution half of
// TestCheckpointResumesAfterTheLastClaimedLSN.
//
// That test only inspects the residual restriction. This one runs
// ProcessElement against it, which is where a claim discipline that permitted
// a mid-transaction checkpoint would fail: the resumed tracker starts one past
// the previously claimed LSN, so a transaction still in flight at checkpoint
// time could neither be claimed nor redelivered.
func TestCheckpointedRestrictionResumesWithoutLoss(t *testing.T) {
	commitTime := time.Date(2026, 7, 4, 14, 0, 0, 0, time.UTC)
	relation := buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
		{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
	})
	messages := [][]byte{
		buildMockBeginPayload(7000, commitTime, 51),
		relation,
		buildMockInsertPayload(42, []string{"1"}),
		buildMockCommitPayload(7000, 7000, commitTime),
		buildMockBeginPayload(8000, commitTime, 52),
		buildMockInsertPayload(42, []string{"2"}),
		buildMockCommitPayload(8000, 8000, commitTime),
	}
	// The gap sits on a transaction boundary, so the first invocation is free
	// to return there.
	pauses := []time.Duration{0, 0, 0, 0, 2 * testCheckpointInterval, 0, 0}

	stream := newPacedStream(messages, pauses)
	fn := newCDCSourceFn(NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(50*time.Millisecond),
		WithCDCCheckpointInterval(testCheckpointInterval),
		WithCDCStreamFactory(func(context.Context, CDCOptions) (ReplicationStream, error) {
			return stream, nil
		}),
	))
	t.Cleanup(func() { _ = fn.Teardown() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var emitted []ChangeEvent
	emit := func(_ beam.EventTime, e ChangeEvent) { emitted = append(emitted, e) }

	bf := &mockBundleFinalizer{}
	we := fn.CreateWatermarkEstimator()
	rt := fn.CreateTracker(fn.CreateInitialRestriction(1))

	if _, err := fn.ProcessElement(ctx, we, bf, rt, 1, emit); err != nil {
		t.Fatalf("first invocation: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("first invocation emitted %d events, want 1", len(emitted))
	}

	_, residual, err := rt.TrySplit(0)
	if err != nil {
		t.Fatalf("TrySplit: %v", err)
	}
	res, ok := residual.(offsetrange.Restriction)
	if !ok {
		t.Fatalf("residual has type %T, want offsetrange.Restriction", residual)
	}
	if res.Start != 7001 {
		t.Fatalf("residual starts at %d, want 7001", res.Start)
	}

	// Resume on the residual. The session is reused, exactly as it would be
	// when the runner schedules the residual back onto the same worker.
	resumed := fn.CreateTracker(res)
	if _, err := fn.ProcessElement(ctx, we, bf, resumed, 1, emit); err != nil {
		t.Fatalf("resumed invocation: %v", err)
	}
	if err := resumed.GetError(); err != nil {
		t.Fatalf("the resumed tracker errored: %v; a checkpoint left the source unable to claim the positions it went on to emit", err)
	}
	if len(emitted) != 2 {
		t.Fatalf("emitted %d events across both invocations, want 2: the checkpoint dropped a transaction", len(emitted))
	}
	if err := bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	session := fn.currentSession()
	if session == nil {
		t.Fatal("expected an open session")
	}
	if got := session.confirmedFlushLSN.Load(); got != 8000 {
		t.Fatalf("confirmed LSN is %d, want 8000 after both transactions were emitted and finalized", got)
	}
}

// TestPartialTransactionIsNotAcknowledged covers the acknowledgment side of the
// same hazard. A transaction can be cut short by the server ending the copy
// stream rather than by this invocation choosing to return.
//
// Its events already carry the commit LSN, so folding them into the
// acknowledgment candidate would advance the slot past a transaction the
// pipeline only partly received.
func TestPartialTransactionIsNotAcknowledged(t *testing.T) {
	commitTime := time.Date(2026, 7, 3, 11, 0, 0, 0, time.UTC)
	// idleWhenDrained is false: the server ends the copy stream immediately
	// after the second transaction's first row.
	h := newSDFHarness(t, false,
		buildMockBeginPayload(1000, commitTime, 41),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockCommitPayload(1000, 1000, commitTime),
		buildMockBeginPayload(2000, commitTime, 42),
		buildMockInsertPayload(42, []string{"2"}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open the session up front so it stays reachable after the end of stream
	// drops it from the DoFn.
	session, err := h.fn.ensureSession(ctx, 1)
	if err != nil {
		t.Fatalf("ensureSession: %v", err)
	}

	h.run(ctx, t)

	if err := h.bf.FinalizeBundle(); err != nil {
		t.Fatalf("FinalizeBundle: %v", err)
	}
	if got := session.confirmedFlushLSN.Load(); got != 1000 {
		t.Fatalf("confirmed LSN is %d, want 1000: only the transaction that committed may be acknowledged, "+
			"and acknowledging 2000 would tell the server a half-received transaction was consumed", got)
	}
}

// --- transport failure detection ---

// TestMidFrameStallIsReportedAsAConnectionFailure checks that a peer that stops
// sending partway through a frame is distinguished from a quiet one.
//
// io.ReadFull does not observe context cancellation, so leaving the remainder
// of a frame deadline-free blocks the read, and with it ProcessElement, for as
// long as the kernel keeps the socket open. Reporting the expiry as an idle
// timeout would be equally wrong: the connection is desynchronized at that
// point, because the unread tail would be parsed as the next frame's header.
func TestMidFrameStallIsReportedAsAConnectionFailure(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	previousFrameTimeout := frameReadTimeout
	frameReadTimeout = 150 * time.Millisecond
	t.Cleanup(func() { frameReadTimeout = previousFrameTimeout })

	stream := &NativeReplicationStream{conn: client}

	// One byte of a frame and then silence.
	go func() { _, _ = server.Write([]byte{'d'}) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := stream.NextMessage(ctx)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, errStreamDesynchronized) {
			t.Fatalf("error is %v, want errStreamDesynchronized: a frame that stops midway leaves the connection unusable", err)
		}
		if isIdleTimeout(err) {
			t.Fatal("a mid-frame stall was classified as an idle timeout; the source would keep reading from a desynchronized connection")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the read never returned; io.ReadFull does not observe context cancellation, so the frame remainder needs its own deadline")
	}
}

// blockingWriteStream models a TCP connection to an unresponsive peer: a write
// blocks until the socket is closed.
type blockingWriteStream struct {
	closed    chan struct{}
	writing   chan struct{}
	closeOnce sync.Once
	writeOnce sync.Once
}

func newBlockingWriteStream() *blockingWriteStream {
	return &blockingWriteStream{closed: make(chan struct{}), writing: make(chan struct{})}
}

func (s *blockingWriteStream) NextMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *blockingWriteStream) SendStandbyStatus(context.Context, StandbyStatus) error {
	s.writeOnce.Do(func() { close(s.writing) })
	<-s.closed
	return errors.New("connection closed")
}

func (s *blockingWriteStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

// TestTeardownCompletesWhileAKeepaliveWriteIsBlocked pins the shutdown order.
//
// The keepalive goroutine writes standby status to the socket. Closing the
// socket is what forces a blocked write to return, so teardown must close
// before it waits. Cancelling first and then waiting deadlocks against a write
// that cannot observe cancellation, which leaks the goroutine and stalls the
// worker.
func TestTeardownCompletesWhileAKeepaliveWriteIsBlocked(t *testing.T) {
	stream := newBlockingWriteStream()
	fn := newCDCSourceFn(NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(10*time.Millisecond),
		WithCDCStreamFactory(func(context.Context, CDCOptions) (ReplicationStream, error) {
			return stream, nil
		}),
	))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := fn.ensureSession(ctx, 1); err != nil {
		t.Fatalf("ensureSession: %v", err)
	}

	select {
	case <-stream.writing:
	case <-time.After(5 * time.Second):
		t.Fatal("the keepalive goroutine never attempted a write")
	}

	done := make(chan error, 1)
	go func() { done <- fn.Teardown() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Teardown did not complete while a keepalive write was blocked; " +
			"the connection must be closed before waiting for the goroutine")
	}
}

// pgStreamStartMessage builds a Stream Start ('S') frame, which opens a
// streamed (in-progress) transaction.
func pgStreamStartMessage(xid uint32) []byte {
	var buf bytes.Buffer
	buf.WriteByte('S')
	_ = binary.Write(&buf, binary.BigEndian, xid)
	buf.WriteByte(1) // first segment
	return buf.Bytes()
}
