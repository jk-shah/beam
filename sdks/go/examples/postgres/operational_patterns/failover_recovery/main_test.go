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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestHAResilienceFilterFn(t *testing.T) {
	fn := newHAResilienceFilterFn()

	t.Run("insert event with confirmed LSN", func(t *testing.T) {
		var emitted []HAEventRecord
		emit := func(r HAEventRecord) {
			emitted = append(emitted, r)
		}

		evt := postgresio.ChangeEvent{
			Operation: postgresio.OpInsert,
			LSN:       987654321,
			After: map[string]any{
				"record_id": int64(42),
				"payload":   "critical_financial_transaction",
			},
		}

		fn.ProcessElement(evt, emit)

		if len(emitted) != 1 {
			t.Fatalf("expected 1 record emitted, got %d", len(emitted))
		}
		r := emitted[0]
		if r.RecordID != 42 {
			t.Errorf("expected RecordID 42, got %d", r.RecordID)
		}
		if r.ConfirmedLSN != 987654321 {
			t.Errorf("expected LSN 987654321, got %d", r.ConfirmedLSN)
		}
		if r.Payload != "critical_financial_transaction" {
			t.Errorf("expected payload match, got %s", r.Payload)
		}
	})

	t.Run("skip non-mutation events", func(t *testing.T) {
		var emitted []HAEventRecord
		emit := func(r HAEventRecord) {
			emitted = append(emitted, r)
		}

		evt := postgresio.ChangeEvent{
			Operation: postgresio.OpTruncate,
			LSN:       987654400,
		}

		fn.ProcessElement(evt, emit)

		if len(emitted) != 0 {
			t.Fatalf("expected 0 records emitted for truncate, got %d", len(emitted))
		}
	})
}

func TestCDCFailoverOptionConfig(t *testing.T) {
	var opts postgresio.CDCOptions
	opt := postgresio.WithCDCFailoverSlot(true)
	opt(&opts)

	if !opts.FailoverSlot {
		t.Errorf("expected FailoverSlot to be true")
	}
}
