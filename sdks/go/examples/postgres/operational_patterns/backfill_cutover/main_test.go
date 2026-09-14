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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}


func TestFormatSnapshotRecordFn(t *testing.T) {
	fn := &formatSnapshotRecordFn{}
	var emitted []OrderRecord
	emit := func(r OrderRecord) {
		emitted = append(emitted, r)
	}

	fn.ProcessElement(1001, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted record, got %d", len(emitted))
	}
	record := emitted[0]
	if record.OrderID != 1001 {
		t.Errorf("expected OrderID 1001, got %d", record.OrderID)
	}
	if record.SourceType != "SNAPSHOT" {
		t.Errorf("expected SourceType SNAPSHOT, got %s", record.SourceType)
	}
	if record.Status != "COMPLETED" {
		t.Errorf("expected Status COMPLETED, got %s", record.Status)
	}
}

func TestFormatCDCRecordFn(t *testing.T) {
	fn := &formatCDCRecordFn{}

	t.Run("insert operation", func(t *testing.T) {
		var emitted []OrderRecord
		emit := func(r OrderRecord) {
			emitted = append(emitted, r)
		}

		evt := postgresio.ChangeEvent{
			Operation: postgresio.OpInsert,
			After: map[string]any{
				"order_id":    int64(2001),
				"customer_id": "CUST-99",
				"amount":      250.75,
				"status":      "PROCESSING",
			},
		}

		fn.ProcessElement(evt, emit)

		if len(emitted) != 1 {
			t.Fatalf("expected 1 emitted record, got %d", len(emitted))
		}
		record := emitted[0]
		if record.OrderID != 2001 {
			t.Errorf("expected OrderID 2001, got %d", record.OrderID)
		}
		if record.SourceType != "STREAM" {
			t.Errorf("expected SourceType STREAM, got %s", record.SourceType)
		}
		if record.Status != "PROCESSING" {
			t.Errorf("expected Status PROCESSING, got %s", record.Status)
		}
	})

	t.Run("ignore delete operation", func(t *testing.T) {
		var emitted []OrderRecord
		emit := func(r OrderRecord) {
			emitted = append(emitted, r)
		}

		evt := postgresio.ChangeEvent{
			Operation: postgresio.OpDelete,
			Before: map[string]any{
				"order_id": int64(2001),
			},
		}

		fn.ProcessElement(evt, emit)

		if len(emitted) != 0 {
			t.Errorf("expected 0 emitted records for OpDelete, got %d", len(emitted))
		}
	})
}

func TestDeduplicateAndSinkFn(t *testing.T) {
	fn := &deduplicateAndSinkFn{}

	snapshotRecord := OrderRecord{
		OrderID:    1001,
		CustomerID: "CUST-1",
		Amount:     100.0,
		Status:     "PENDING",
		SourceType: "SNAPSHOT",
		AppliedAt:  time.Now().Add(-1 * time.Hour),
	}
	streamRecord := OrderRecord{
		OrderID:    1001,
		CustomerID: "CUST-1",
		Amount:     125.0,
		Status:     "COMPLETED",
		SourceType: "STREAM",
		AppliedAt:  time.Now(),
	}

	records := []OrderRecord{snapshotRecord, streamRecord}
	idx := 0
	recordsIterator := func(out *OrderRecord) bool {
		if idx < len(records) {
			*out = records[idx]
			idx++
			return true
		}
		return false
	}

	var emitted []OrderRecord
	emit := func(r OrderRecord) {
		emitted = append(emitted, r)
	}

	fn.ProcessElement(1001, recordsIterator, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted record, got %d", len(emitted))
	}
	winner := emitted[0]
	if winner.SourceType != "STREAM" {
		t.Errorf("expected STREAM record to supersede SNAPSHOT, got %s", winner.SourceType)
	}
	if winner.Status != "COMPLETED" {
		t.Errorf("expected Status COMPLETED, got %s", winner.Status)
	}
	if winner.Amount != 125.0 {
		t.Errorf("expected Amount 125.0, got %f", winner.Amount)
	}
}
