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
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

type mockBagStateProvider struct {
	bag map[string][]any
}

func newMockBagStateProvider() *mockBagStateProvider {
	return &mockBagStateProvider{bag: make(map[string][]any)}
}

func (m *mockBagStateProvider) ReadBagState(id string) ([]any, []state.Transaction, error) {
	return m.bag[id], nil, nil
}

func (m *mockBagStateProvider) WriteBagState(val state.Transaction) error {
	m.bag[val.Key] = append(m.bag[val.Key], val.Val)
	return nil
}

func (m *mockBagStateProvider) ClearBagState(val state.Transaction) error {
	delete(m.bag, val.Key)
	return nil
}

func (m *mockBagStateProvider) ReadValueState(id string) (any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockBagStateProvider) WriteValueState(val state.Transaction) error { return nil }
func (m *mockBagStateProvider) ClearValueState(val state.Transaction) error { return nil }
func (m *mockBagStateProvider) CreateAccumulatorFn(id string) reflectx.Func { return nil }
func (m *mockBagStateProvider) AddInputFn(id string) reflectx.Func          { return nil }
func (m *mockBagStateProvider) MergeAccumulatorsFn(id string) reflectx.Func { return nil }
func (m *mockBagStateProvider) ExtractOutputFn(id string) reflectx.Func     { return nil }
func (m *mockBagStateProvider) ReadMapStateValue(id string, key any) (any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockBagStateProvider) ReadMapStateKeys(id string) ([]any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockBagStateProvider) WriteMapState(val state.Transaction) error    { return nil }
func (m *mockBagStateProvider) ClearMapStateKey(val state.Transaction) error { return nil }
func (m *mockBagStateProvider) ClearMapState(val state.Transaction) error    { return nil }
func (m *mockBagStateProvider) ReadOrderedListState(id string) ([]any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockBagStateProvider) WriteOrderedListState(val state.Transaction) error { return nil }
func (m *mockBagStateProvider) ClearOrderedListState(val state.Transaction) error { return nil }

func TestTransactionMessageConstructorsAndProperties(t *testing.T) {
	msgCommit := OfCommit(500)
	if msgCommit.TransactionID != 500 || msgCommit.Type != MessageTypeCommit {
		t.Errorf("unexpected commit message: %+v", msgCommit)
	}

	msgAbort := OfAbort(600)
	if msgAbort.TransactionID != 600 || msgAbort.Type != MessageTypeAbort {
		t.Errorf("unexpected abort message: %+v", msgAbort)
	}

	event := ChangeEvent{Table: "orders", Operation: OpInsert}
	msgMut := OfMutation(700, event)
	if msgMut.TransactionID != 700 || msgMut.Type != MessageTypeMutation || msgMut.Event == nil {
		t.Errorf("unexpected mutation message: %+v", msgMut)
	}
}

func TestSpooledMutationsEmittedOnCommit(t *testing.T) {
	sp := newMockBagStateProvider()
	spooler := newInFlightTransactionSpoolerFn()

	now := time.Now().UTC()
	e1 := ChangeEvent{Operation: OpInsert, Table: "orders", LSN: 301, CommitTime: now}
	e2 := ChangeEvent{Operation: OpInsert, Table: "orders", LSN: 302, CommitTime: now}

	var emitted []ChangeEvent
	emit := func(e ChangeEvent) {
		emitted = append(emitted, e)
	}

	// 1. Spool 2 mutations for XID 700
	_ = spooler.ProcessElement(sp, 700, OfMutation(700, e1), emit)
	_ = spooler.ProcessElement(sp, 700, OfMutation(700, e2), emit)

	if len(emitted) != 0 {
		t.Fatalf("expected 0 emitted events before commit, got %d", len(emitted))
	}

	// 2. Commit XID 700 -> emits spooled events
	_ = spooler.ProcessElement(sp, 700, OfCommit(700), emit)

	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted events after commit, got %d", len(emitted))
	}
	if emitted[0].LSN != 301 || emitted[1].LSN != 302 {
		t.Errorf("unexpected emitted events: %+v", emitted)
	}
}

func TestSpooledMutationsDiscardedOnAbort(t *testing.T) {
	sp := newMockBagStateProvider()
	spooler := newInFlightTransactionSpoolerFn()

	now := time.Now().UTC()
	e1 := ChangeEvent{Operation: OpInsert, Table: "orders", LSN: 401, CommitTime: now}

	var emitted []ChangeEvent
	emit := func(e ChangeEvent) {
		emitted = append(emitted, e)
	}

	// 1. Spool 1 mutation for XID 800
	_ = spooler.ProcessElement(sp, 800, OfMutation(800, e1), emit)

	// 2. Abort XID 800 -> discards spooled events
	_ = spooler.ProcessElement(sp, 800, OfAbort(800), emit)

	if len(emitted) != 0 {
		t.Fatalf("expected 0 emitted events after abort, got %d", len(emitted))
	}

	// Verify bag is empty
	bagItems, _, _ := sp.ReadBagState("spooled_mutations")
	if len(bagItems) != 0 {
		t.Errorf("expected bag state to be cleared after abort, got %d items", len(bagItems))
	}
}
