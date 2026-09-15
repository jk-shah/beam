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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
)

type mockBundleFinalizer struct {
	callbacks []func() error
	mu        sync.Mutex
}

func (m *mockBundleFinalizer) RegisterCallback(_ time.Duration, cb func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callbacks = append(m.callbacks, cb)
}

func (m *mockBundleFinalizer) FinalizeBundle() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cb := range m.callbacks {
		if err := cb(); err != nil {
			return err
		}
	}
	return nil
}

func TestReadCDCPipelineGraphConstruction(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	opts := []CDCOption{
		WithCDCSlotName("beam_test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCStartLSN(100),
		WithCDCHeartbeatInterval(5 * time.Second),
	}
	events := ReadCDC(s, opts...)

	if events.Type().Type() != reflect.TypeOf(ChangeEvent{}) {
		t.Errorf("expected PCollection of ChangeEvent, got %v", events.Type().Type())
	}
	_ = p
}

// TestCDCSourceRegistersAsASplittableDoFn asserts the property directly rather
// than inferring it from the absence of a panic during graph construction.
//
// If the SDK classified this as an ordinary DoFn, ProcessElement's
// ProcessContinuation would be ignored, the source would run once to
// completion, and the watermark estimator would never be consulted.
func TestCDCSourceRegistersAsASplittableDoFn(t *testing.T) {
	fn, err := graph.NewDoFn(
		newCDCSourceFn(NewCDCOptions(
			WithCDCSlotName("beam_test_slot"),
			WithCDCPublication("test_pub"),
		)),
		graph.NumMainInputs(graph.MainSingle),
	)
	if err != nil {
		t.Fatalf("the SDK rejected the CDC source as a DoFn: %v", err)
	}
	if !fn.IsSplittable() {
		t.Error("the CDC source is not recognized as splittable; its ProcessContinuation would be ignored and it would never self-checkpoint")
	}
	sdfn := (*graph.SplittableDoFn)(fn)
	if !sdfn.IsWatermarkEstimating() {
		t.Error("the CDC source declares no watermark estimator to the SDK; downstream windows would never close")
	}
}

func TestReadCDCPanicsOnInvalidSlotName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected ReadCDC to panic on invalid slot name, but it did not")
		}
	}()

	_, s := beam.NewPipelineWithRoot()
	_ = ReadCDC(s,
		WithCDCSlotName("invalid;drop table;"),
		WithCDCPublication("pub"),
	)
}

func TestCDCSourceFnExecutionWithMockStream(t *testing.T) {
	fixedTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	// Prepare mock replication stream messages
	beginPayload := buildMockBeginPayload(500, fixedTime, 1)
	relCols := []ColumnDef{
		{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		{Flags: 0, Name: "name", TypeOID: 25, TypeModifier: -1},
	}
	relPayload := buildMockRelationPayload(1001, "public", "customers", 'd', relCols)
	insertPayload := buildMockInsertPayload(1001, []string{"1", "Alice"})

	// Server keepalive with reply requested
	var keepAliveBuf bytes.Buffer
	keepAliveBuf.WriteByte('k')
	_ = binary.Write(&keepAliveBuf, binary.BigEndian, uint64(600))
	_ = binary.Write(&keepAliveBuf, binary.BigEndian, GoTimeToPg(fixedTime))
	keepAliveBuf.WriteByte(1) // reply requested

	mockStream := NewMockReplicationStream(
		beginPayload,
		relPayload,
		insertPayload,
		// The Commit closes the transaction. Without it the source has only
		// part of a transaction and is not permitted to acknowledge anything.
		buildMockCommitPayload(500, 500, fixedTime),
		keepAliveBuf.Bytes(),
	)
	// Stay connected once the scripted messages run out, so the source returns
	// on its checkpoint interval rather than on end of stream. That keeps the
	// session open for the acknowledgment assertions below.
	mockStream.SetIdleWhenDrained(true)

	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(50*time.Millisecond),
		WithCDCCheckpointInterval(150*time.Millisecond),
		WithCDCStreamFactory(func(ctx context.Context, o CDCOptions) (ReplicationStream, error) {
			return mockStream, nil
		}),
	)

	fn := newCDCSourceFn(opts)
	fn.preflightDone = true
	fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) {
		return newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)}), nil
	}
	defer func() { _ = fn.Teardown() }()
	bf := &mockBundleFinalizer{}

	var emitted []ChangeEvent
	emit := func(_ beam.EventTime, e ChangeEvent) {
		emitted = append(emitted, e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	we := fn.CreateWatermarkEstimator()
	rt := fn.CreateTracker(fn.CreateInitialRestriction(1))

	if _, err := fn.ProcessElement(ctx, we, bf, rt, 1, emit); err != nil {
		t.Fatalf("unexpected error executing cdcSourceFn: %v", err)
	}

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted ChangeEvent, got %d", len(emitted))
	}

	evt := emitted[0]
	if evt.Operation != OpInsert || evt.Table != "customers" || evt.After["name"] != "Alice" {
		t.Errorf("unexpected event content: %+v", evt)
	}
	if evt.EventID == "" {
		t.Errorf("expected non-empty EventID on emitted ChangeEvent, got empty string")
	}
	if ChangeEventKeyFn(evt) != evt.EventID {
		t.Errorf("ChangeEventKeyFn mismatch: got %q, want %q", ChangeEventKeyFn(evt), evt.EventID)
	}

	// The slot must not be acknowledged before the bundle's output is durable.
	session := fn.currentSession()
	if session == nil {
		t.Fatal("expected the source to hold a session after ProcessElement")
	}
	if got := session.confirmedFlushLSN.Load(); got != 0 {
		t.Fatalf("confirmed LSN advanced to %d before bundle finalization; "+
			"acknowledging ahead of a durable commit loses data on restart", got)
	}

	// Finalize bundle: triggers callback advancing confirmedFlushLSN
	if err := bf.FinalizeBundle(); err != nil {
		t.Fatalf("failed to finalize bundle: %v", err)
	}
	if got := session.confirmedFlushLSN.Load(); got == 0 {
		t.Fatal("confirmed LSN did not advance after bundle finalization; " +
			"the replication slot would never be acknowledged and WAL would grow without bound")
	}

	// Verify that mockStream received status updates (both from keepalive reply and ticker)
	statuses := mockStream.GetStatusLog()
	if len(statuses) == 0 {
		t.Errorf("expected mockStream to receive standby status updates, got none")
	}
}

func TestChangeEventDeterministicEventID(t *testing.T) {
	ev1 := ChangeEvent{
		Operation:     OpInsert,
		Schema:        "public",
		Table:         "orders",
		LSN:           50001,
		TransactionID: 42,
		PrimaryKeys:   []string{"order_id"},
		After:         map[string]any{"order_id": int64(101), "amount": 99.5},
	}
	ev1.PopulateEventID()

	ev2 := ChangeEvent{
		Operation:     OpInsert,
		Schema:        "public",
		Table:         "orders",
		LSN:           50001,
		TransactionID: 42,
		PrimaryKeys:   []string{"order_id"},
		After:         map[string]any{"order_id": int64(101), "amount": 99.5},
	}
	ev2.PopulateEventID()

	if ev1.EventID != ev2.EventID {
		t.Errorf("expected deterministic EventID equality, got %q != %q", ev1.EventID, ev2.EventID)
	}

	// Ensure different primary key or LSN yields different EventID
	ev3 := ChangeEvent{
		Operation:     OpInsert,
		Schema:        "public",
		Table:         "orders",
		LSN:           50002,
		TransactionID: 42,
		PrimaryKeys:   []string{"order_id"},
		After:         map[string]any{"order_id": int64(102), "amount": 99.5},
	}
	ev3.PopulateEventID()

	if ev1.EventID == ev3.EventID {
		t.Errorf("expected distinct EventIDs for different LSNs/records, got collision %q", ev1.EventID)
	}

	// The identifier is LSN:TransactionID:TxSeq:Table:PrimaryKey. TxSeq is
	// present because LSN, XID, table and primary key are all identical for two
	// changes to the same row within one transaction.
	expectedKey := "50001:42:0:public.orders:public.orders:101"
	if ev1.EventID != expectedKey {
		t.Errorf("unexpected EventID format: got %q, want %q", ev1.EventID, expectedKey)
	}
}

func TestCdcSourceFn_RestrictionMethods(t *testing.T) {
	fn := &cdcSourceFn{
		Options: CDCOptions{
			StartLSN: 1000,
		},
	}

	rest := offsetrange.Restriction{Start: 100, End: 500}
	size := fn.RestrictionSize(0, rest)
	if size < 0 {
		t.Errorf("expected non-negative restriction size, got %f", size)
	}

	tracker := fn.CreateTracker(rest)
	if tracker == nil {
		t.Fatalf("expected non-nil tracker")
	}

	truncated := fn.TruncateRestriction(tracker, 0)
	if truncated.Start != 0 || truncated.End != 0 {
		t.Errorf("expected empty restriction on drain, got %+v", truncated)
	}
}

