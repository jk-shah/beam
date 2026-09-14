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

const unchangedToastMarker = "<unchanged_toast>"

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
		if err == nil && ok {
			hasToast := false
			for colName, val := range event.After {
				if s, isStr := val.(string); isStr && s == unchangedToastMarker {
					hasToast = true
					if cachedVal, found := cached.After[colName]; found {
						event.After[colName] = cachedVal
					} else {
						event.After[colName] = nil
					}
				}
			}
			if hasToast {
				// Also update Before map if not present
				if len(event.Before) == 0 && len(cached.After) > 0 {
					event.Before = make(map[string]any, len(cached.After))
					for k, v := range cached.After {
						event.Before[k] = v
					}
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
