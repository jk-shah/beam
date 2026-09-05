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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

type mockStateProvider struct {
	values map[string]any
}

func newMockStateProvider() *mockStateProvider {
	return &mockStateProvider{values: make(map[string]any)}
}

func (m *mockStateProvider) ReadValueState(id string) (any, []state.Transaction, error) {
	val := m.values[id]
	return val, nil, nil
}

func (m *mockStateProvider) WriteValueState(val state.Transaction) error {
	m.values[val.Key] = val.Val
	return nil
}

func (m *mockStateProvider) ClearValueState(val state.Transaction) error {
	delete(m.values, val.Key)
	return nil
}

func (m *mockStateProvider) ReadBagState(id string) ([]any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockStateProvider) WriteBagState(val state.Transaction) error { return nil }
func (m *mockStateProvider) ClearBagState(val state.Transaction) error { return nil }
func (m *mockStateProvider) CreateAccumulatorFn(id string) reflectx.Func { return nil }
func (m *mockStateProvider) AddInputFn(id string) reflectx.Func          { return nil }
func (m *mockStateProvider) MergeAccumulatorsFn(id string) reflectx.Func { return nil }
func (m *mockStateProvider) ExtractOutputFn(id string) reflectx.Func     { return nil }
func (m *mockStateProvider) ReadMapStateValue(id string, key any) (any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockStateProvider) ReadMapStateKeys(id string) ([]any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockStateProvider) WriteMapState(val state.Transaction) error    { return nil }
func (m *mockStateProvider) ClearMapStateKey(val state.Transaction) error { return nil }
func (m *mockStateProvider) ClearMapState(val state.Transaction) error    { return nil }
func (m *mockStateProvider) ReadOrderedListState(id string) ([]any, []state.Transaction, error) {
	return nil, nil, nil
}
func (m *mockStateProvider) WriteOrderedListState(val state.Transaction) error { return nil }
func (m *mockStateProvider) ClearOrderedListState(val state.Transaction) error { return nil }

func TestToastReassemblyLifecycle(t *testing.T) {
	sp := newMockStateProvider()
	fn := newToastReassemblyFn()

	now := time.Now().UTC()
	insertEvent := ChangeEvent{
		Operation:   OpInsert,
		Schema:      "public",
		Table:       "articles",
		CommitTime:  now,
		LSN:         1000,
		PrimaryKeys: []string{"id"},
		After: map[string]any{
			"id":        int64(42),
			"title":     "Apache Beam in Go",
			"body":      "A very large out-of-line TOAST document with thousands of characters...",
			"view_cnt":  int64(0),
		},
	}

	var emitted []ChangeEvent
	emit := func(_ string, e ChangeEvent) {
		emitted = append(emitted, e)
	}

	key := insertEvent.PrimaryKeyString()

	// 1. Process INSERT: caches row in state
	if err := fn.ProcessElement(sp, key, insertEvent, emit); err != nil {
		t.Fatalf("failed to process insert: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted insert event, got %d", len(emitted))
	}

	// 2. Process UPDATE where 'body' is unchanged TOAST ('<unchanged_toast>')
	updateEvent := ChangeEvent{
		Operation:   OpUpdate,
		Schema:      "public",
		Table:       "articles",
		CommitTime:  now.Add(time.Second),
		LSN:         1050,
		PrimaryKeys: []string{"id"},
		After: map[string]any{
			"id":        int64(42),
			"title":     "Apache Beam in Go (Updated Title)",
			"body":      unchangedToastMarker,
			"view_cnt":  int64(10),
		},
	}

	if err := fn.ProcessElement(sp, key, updateEvent, emit); err != nil {
		t.Fatalf("failed to process update: %v", err)
	}
	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted events, got %d", len(emitted))
	}

	updated := emitted[1]
	if updated.After["title"] != "Apache Beam in Go (Updated Title)" {
		t.Errorf("title not updated: %v", updated.After["title"])
	}
	// Body should have been patched from state
	if updated.After["body"] != "A very large out-of-line TOAST document with thousands of characters..." {
		t.Errorf("body TOAST datum was not patched from state, got %v", updated.After["body"])
	}
	if updated.After["view_cnt"] != int64(10) {
		t.Errorf("view_cnt not updated: %v", updated.After["view_cnt"])
	}

	// 3. Process DELETE: evicts row from state
	deleteEvent := ChangeEvent{
		Operation:   OpDelete,
		Schema:      "public",
		Table:       "articles",
		CommitTime:  now.Add(2 * time.Second),
		LSN:         1100,
		PrimaryKeys: []string{"id"},
		Before: map[string]any{
			"id": int64(42),
		},
	}
	if err := fn.ProcessElement(sp, key, deleteEvent, emit); err != nil {
		t.Fatalf("failed to process delete: %v", err)
	}
	if len(emitted) != 3 {
		t.Fatalf("expected 3 emitted events, got %d", len(emitted))
	}

	// Verify state cleared
	cached, ok, _ := fn.RowState.Read(sp)
	if ok || len(cached.After) > 0 {
		t.Errorf("expected state to be cleared after delete, got %+v", cached)
	}
}

func TestReassembleToastAndPartitionPipelineConstruction(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	events := beam.Create(s, ChangeEvent{
		Operation:   OpInsert,
		Schema:      "public",
		Table:       "users",
		PrimaryKeys: []string{"id"},
		After:       map[string]any{"id": int64(1)},
	})

	partitioned := PartitionByPrimaryKey(s, events)
	reassembled := ReassembleToast(s, partitioned)

	if reassembled.Type().Type() != reflect.TypeOf(ChangeEvent{}) {
		t.Errorf("unexpected collection type: %v", reassembled.Type().Type())
	}
	_ = p
}
