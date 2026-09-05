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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func TestDemuxFilterTransforms(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	e1 := ChangeEvent{Schema: "public", Table: "orders", Operation: OpInsert, Origin: "master_node"}
	e2 := ChangeEvent{Schema: "public", Table: "users", Operation: OpInsert, Origin: "self_worker"}
	e3 := ChangeEvent{Schema: "audit", Table: "logs", Operation: OpInsert, Origin: "master_node"}

	input := beam.Create(s, e1, e2, e3)

	orders := FilterByTable(s, "orders", input)
	auditSchema := FilterBySchema(s, "audit", input)
	noLoopEvents := FilterByOrigin(s, "self_worker", input)

	if orders.Type().Type() != reflect.TypeOf(ChangeEvent{}) {
		t.Errorf("unexpected orders type: %v", orders.Type().Type())
	}
	if auditSchema.Type().Type() != reflect.TypeOf(ChangeEvent{}) {
		t.Errorf("unexpected auditSchema type: %v", auditSchema.Type().Type())
	}
	if noLoopEvents.Type().Type() != reflect.TypeOf(ChangeEvent{}) {
		t.Errorf("unexpected noLoopEvents type: %v", noLoopEvents.Type().Type())
	}
	_ = p
}

func TestFilterByOriginLogic(t *testing.T) {
	fn := &filterByOriginFn{SelfOrigin: "replica_node_1"}
	var emitted []ChangeEvent
	emit := func(e ChangeEvent) {
		emitted = append(emitted, e)
	}

	// 1. Event from self should be dropped to prevent replication loop
	selfEvent := ChangeEvent{Table: "orders", Origin: "replica_node_1"}
	fn.ProcessElement(selfEvent, emit)
	if len(emitted) != 0 {
		t.Errorf("expected self event to be dropped, but got %d emitted", len(emitted))
	}

	// 2. Event from remote node should be emitted
	remoteEvent := ChangeEvent{Table: "orders", Origin: "primary_node"}
	fn.ProcessElement(remoteEvent, emit)
	if len(emitted) != 1 {
		t.Errorf("expected remote event to be emitted, but got %d", len(emitted))
	}
}
