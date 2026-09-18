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

package main

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestExtractPrimaryKeyIDFn(t *testing.T) {
	fn := &extractPrimaryKeyIDFn{}
	var emitted []int64
	emit := func(id int64) {
		emitted = append(emitted, id)
	}

	fn.ProcessElement(MigratedOrderRecord{OrderID: 5001}, emit)
	fn.ProcessElement(MigratedOrderRecord{OrderID: 0}, emit) // Should be skipped

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted id, got %d", len(emitted))
	}
	if emitted[0] != 5001 {
		t.Errorf("expected 5001, got %d", emitted[0])
	}
}

func TestGenerateSequenceResetSQLFn(t *testing.T) {
	fn := &generateSequenceResetSQLFn{
		TargetTable:  "public.orders",
		PrimaryKeyID: "order_id",
	}

	var emitted []SequenceReconciliationSQL
	emit := func(s SequenceReconciliationSQL) {
		emitted = append(emitted, s)
	}

	fn.ProcessElement(1234567, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 statement emitted, got %d", len(emitted))
	}
	stmt := emitted[0]
	if stmt.MaxID != 1234567 {
		t.Errorf("expected MaxID 1234567, got %d", stmt.MaxID)
	}
	expectedSub := "SELECT setval(pg_get_serial_sequence('public.orders', 'order_id'), 1234567, true);"
	if !strings.Contains(stmt.SetValQuery, expectedSub) {
		t.Errorf("expected query to contain %q, got %q", expectedSub, stmt.SetValQuery)
	}
}
