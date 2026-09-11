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
		keepAliveBuf.Bytes(),
	)

	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(50*time.Millisecond),
		WithCDCStreamFactory(func(ctx context.Context, o CDCOptions) (ReplicationStream, error) {
			return mockStream, nil
		}),
	)

	fn := newCDCSourceFn(opts)
	bf := &mockBundleFinalizer{}

	var emitted []ChangeEvent
	emit := func(e ChangeEvent) {
		emitted = append(emitted, e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := fn.ProcessElement(ctx, bf, 1, emit)
	if err != nil {
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

	// Finalize bundle: triggers callback advancing confirmedCommittedLSN
	if err := bf.FinalizeBundle(); err != nil {
		t.Fatalf("failed to finalize bundle: %v", err)
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

	expectedKey := "50001:42:public.orders:public.orders:101"
	if ev1.EventID != expectedKey {
		t.Errorf("unexpected EventID format: got %q, want %q", ev1.EventID, expectedKey)
	}
}
