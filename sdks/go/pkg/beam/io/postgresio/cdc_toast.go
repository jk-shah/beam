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
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
)

func init() {
	beam.RegisterDoFn(&toastReassemblyFn{})
	beam.RegisterDoFn(&keyByPrimaryKeyFn{})
	beam.RegisterDoFn(&dropKeyFn{})
}

// toastReassemblyFn is a stateful Beam DoFn that caches baseline rows on INSERT
// and patches unchanged out-of-line TOAST columns ('u') on UPDATE.
type toastReassemblyFn struct {
	RowState state.Value[ChangeEvent]
}

// NewToastReassemblyFn creates a new stateful TOAST reassembly DoFn.
func newToastReassemblyFn() *toastReassemblyFn {
	return &toastReassemblyFn{
		RowState: state.MakeValueState[ChangeEvent]("pg_cdc_toast_baseline"),
	}
}

// ProcessElement processes keyed ChangeEvents and patches unchanged TOAST fields.
func (fn *toastReassemblyFn) ProcessElement(sp state.Provider, key string, event ChangeEvent, emit func(string, ChangeEvent)) error {
	switch event.Operation {
	case OpInsert:
		_ = fn.RowState.Write(sp, event)
		emit(key, event)

	case OpUpdate:
		cached, ok, err := fn.RowState.Read(sp)
		if err == nil && ok && len(event.UnchangedColumns) > 0 {
			// Unchanged TOAST columns are absent from After. Patch in the
			// last value this pipeline observed for the row where one exists.
			//
			// A column with no cached value is deliberately left absent
			// rather than set to nil: on a cold start the pipeline has never
			// seen the row, and writing NULL would erase a value the source
			// still holds. Leaving it absent lets the sink omit it from the
			// generated UPDATE.
			var stillUnknown []string
			patched := false

			for _, colName := range event.UnchangedColumns {
				if cachedVal, found := cached.After[colName]; found {
					if event.After == nil {
						event.After = make(map[string]any, len(event.UnchangedColumns))
					}
					event.After[colName] = cachedVal
					patched = true
				} else {
					stillUnknown = append(stillUnknown, colName)
				}
			}
			event.UnchangedColumns = stillUnknown

			if patched && len(event.Before) == 0 && len(cached.After) > 0 {
				event.Before = make(map[string]any, len(cached.After))
				for k, v := range cached.After {
					event.Before[k] = v
				}
			}
		}

		_ = fn.RowState.Write(sp, event)
		emit(key, event)

	case OpDelete:
		_ = fn.RowState.Clear(sp)
		emit(key, event)

	default:
		emit(key, event)
	}
	return nil
}

type keyByPrimaryKeyFn struct{}

func (fn *keyByPrimaryKeyFn) ProcessElement(event ChangeEvent) (string, ChangeEvent) {
	return event.PrimaryKeyString(), event
}

type dropKeyFn struct{}

func (fn *dropKeyFn) ProcessElement(_ string, event ChangeEvent) ChangeEvent {
	return event
}

// ReassembleToast applies stateful TOAST reassembly across a PCollection of ChangeEvents.
func ReassembleToast(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("postgresio.ReassembleToast")
	keyed := beam.ParDo(s, &keyByPrimaryKeyFn{}, col)
	reassembled := beam.ParDo(s, newToastReassemblyFn(), keyed)
	return beam.ParDo(s, &dropKeyFn{}, reassembled)
}

// PartitionByPrimaryKey keys a PCollection of ChangeEvents by their composite primary key
// and applies beam.Reshuffle to distribute processing evenly across cluster workers.
func PartitionByPrimaryKey(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("postgresio.PartitionByPrimaryKey")
	keyed := beam.ParDo(s, &keyByPrimaryKeyFn{}, col)
	reshuffled := beam.Reshuffle(s, keyed)
	return beam.ParDo(s, &dropKeyFn{}, reshuffled)
}
