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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*filterByTableFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*filterBySchemaFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*filterByOriginFn)(nil)).Elem())
}

type filterByTableFn struct {
	TargetTable string
}

func (fn *filterByTableFn) ProcessElement(event ChangeEvent, emit func(ChangeEvent)) {
	if event.Table == fn.TargetTable || event.FullTableName() == fn.TargetTable {
		emit(event)
	}
}

type filterBySchemaFn struct {
	TargetSchema string
}

func (fn *filterBySchemaFn) ProcessElement(event ChangeEvent, emit func(ChangeEvent)) {
	if event.Schema == fn.TargetSchema {
		emit(event)
	}
}

type filterByOriginFn struct {
	SelfOrigin string
}

func (fn *filterByOriginFn) ProcessElement(event ChangeEvent, emit func(ChangeEvent)) {
	// Drop events originating from self to prevent bidirectional replication loops
	if fn.SelfOrigin != "" && event.Origin == fn.SelfOrigin {
		return
	}
	emit(event)
}

// FilterByTable filters a PCollection of ChangeEvents to only those matching targetTable.
func FilterByTable(s beam.Scope, targetTable string, col beam.PCollection) beam.PCollection {
	s = s.Scope("postgresio.FilterByTable")
	return beam.ParDo(s, &filterByTableFn{TargetTable: targetTable}, col)
}

// FilterBySchema filters a PCollection of ChangeEvents to only those matching targetSchema.
func FilterBySchema(s beam.Scope, targetSchema string, col beam.PCollection) beam.PCollection {
	s = s.Scope("postgresio.FilterBySchema")
	return beam.ParDo(s, &filterBySchemaFn{TargetSchema: targetSchema}, col)
}

// FilterByOrigin filters out ChangeEvents originating from selfOrigin for loop-free bi-directional replication.
func FilterByOrigin(s beam.Scope, selfOrigin string, col beam.PCollection) beam.PCollection {
	s = s.Scope("postgresio.FilterByOrigin")
	return beam.ParDo(s, &filterByOriginFn{SelfOrigin: selfOrigin}, col)
}
