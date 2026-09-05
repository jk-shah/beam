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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func TestPgOIDToArrowType(t *testing.T) {
	tests := []struct {
		oid      uint32
		expected arrow.DataType
	}{
		{16, arrow.FixedWidthTypes.Boolean},
		{21, arrow.PrimitiveTypes.Int16},
		{23, arrow.PrimitiveTypes.Int32},
		{20, arrow.PrimitiveTypes.Int64},
		{700, arrow.PrimitiveTypes.Float32},
		{701, arrow.PrimitiveTypes.Float64},
		{17, arrow.BinaryTypes.Binary},
		{1082, arrow.FixedWidthTypes.Date32},
		{1114, arrow.FixedWidthTypes.Timestamp_us},
		{1184, arrow.FixedWidthTypes.Timestamp_us},
		{25, arrow.BinaryTypes.String},
		{1043, arrow.BinaryTypes.String},
		{114, arrow.BinaryTypes.String},
		{3802, arrow.BinaryTypes.String},
		{1700, arrow.BinaryTypes.String},
		{2950, arrow.BinaryTypes.String},
		{99999, arrow.BinaryTypes.String},
	}

	for _, tt := range tests {
		got := PgOIDToArrowType(tt.oid)
		if got.ID() != tt.expected.ID() {
			t.Errorf("PgOIDToArrowType(%d) = %v; want %v", tt.oid, got, tt.expected)
		}
	}
}

func TestArrowBatchRecordBinaryCoder(t *testing.T) {
	record := ArrowBatchRecord{
		SchemaID:  "0123456789abcdef",
		Namespace: "public",
		TableName: "orders",
		RowCount:  1500,
		Payload:   []byte{0x00, 0x01, 0x02, 0x03, 0xFF},
		Metadata: map[string]string{
			"first_lsn": "123456",
			"last_lsn":  "654321",
		},
	}

	encoded, err := encodeArrowBatchRecord(record)
	if err != nil {
		t.Fatalf("encodeArrowBatchRecord failed: %v", err)
	}

	decoded, err := decodeArrowBatchRecord(encoded)
	if err != nil {
		t.Fatalf("decodeArrowBatchRecord failed: %v", err)
	}

	if decoded.SchemaID != record.SchemaID {
		t.Errorf("SchemaID mismatch: got %s, want %s", decoded.SchemaID, record.SchemaID)
	}
	if decoded.Namespace != record.Namespace {
		t.Errorf("Namespace mismatch: got %s, want %s", decoded.Namespace, record.Namespace)
	}
	if decoded.TableName != record.TableName {
		t.Errorf("TableName mismatch: got %s, want %s", decoded.TableName, record.TableName)
	}
	if decoded.RowCount != record.RowCount {
		t.Errorf("RowCount mismatch: got %d, want %d", decoded.RowCount, record.RowCount)
	}
	if string(decoded.Payload) != string(record.Payload) {
		t.Errorf("Payload mismatch: got %v, want %v", decoded.Payload, record.Payload)
	}
	if decoded.Metadata["first_lsn"] != "123456" || decoded.Metadata["last_lsn"] != "654321" {
		t.Errorf("Metadata mismatch: got %v", decoded.Metadata)
	}
}

func TestTableBufferAppendAndMaterialize(t *testing.T) {
	checkedAlloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() {
		if checkedAlloc.CurrentAlloc() != 0 {
			t.Errorf("Memory leak detected! Remaining allocated bytes: %d", checkedAlloc.CurrentAlloc())
		}
	}()

	sampleEvent := ChangeEvent{
		Operation: OpInsert,
		Schema:    "public",
		Table:     "users",
		LSN:       1001,
		After: map[string]any{
			"id":         int32(42),
			"username":   "alice",
			"is_active":  true,
			"score":      float64(98.5),
			"created_at": time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
			"avatar":     []byte{0xDE, 0xAD, 0xBE, 0xEF},
			"notes":      nil,
		},
		ColumnTypes: map[string]uint32{
			"id":         23,
			"username":   25,
			"is_active":  16,
			"score":      701,
			"created_at": 1184,
			"avatar":     17,
			"notes":      25,
		},
	}

	hash, cols, schema := ComputeSchemaHash(sampleEvent)
	buf := newTableBuffer(checkedAlloc, hash, "public", "users", cols, schema)

	// Append 5 records
	for i := 0; i < 5; i++ {
		ev := sampleEvent
		ev.LSN = uint64(1001 + i)
		ev.After["id"] = int32(42 + i)
		if err := buf.Append(ev); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if buf.rowCount != 5 {
		t.Fatalf("Expected rowCount 5, got %d", buf.rowCount)
	}

	batchRecord, err := buf.MaterializeAndReset()
	if err != nil {
		t.Fatalf("MaterializeAndReset failed: %v", err)
	}

	if batchRecord.RowCount != 5 {
		t.Errorf("Expected batch record row count 5, got %d", batchRecord.RowCount)
	}
	if len(batchRecord.Payload) == 0 {
		t.Fatalf("Expected non-empty IPC payload")
	}
	if batchRecord.Metadata["first_lsn"] != "1001" {
		t.Errorf("Expected first_lsn 1001, got %s", batchRecord.Metadata["first_lsn"])
	}
	if batchRecord.Metadata["last_lsn"] != "1005" {
		t.Errorf("Expected last_lsn 1005, got %s", batchRecord.Metadata["last_lsn"])
	}

	// Verify builder is reset and ready for reuse
	if buf.rowCount != 0 {
		t.Errorf("Expected rowCount reset to 0, got %d", buf.rowCount)
	}

	// Clean up buffer
	buf.Release()
}

func TestArrowBatcherBoundedMultiTablePool(t *testing.T) {
	checkedAlloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() {
		if checkedAlloc.CurrentAlloc() != 0 {
			t.Errorf("Memory leak detected! Remaining allocated bytes: %d", checkedAlloc.CurrentAlloc())
		}
	}()

	batcher := newArrowBatcherFn(WithArrowMaxBatchRows(5))
	batcher.alloc = checkedAlloc
	batcher.Setup()
	defer batcher.Teardown()

	var emittedBatches []ArrowBatchRecord
	emit := func(r ArrowBatchRecord) {
		emittedBatches = append(emittedBatches, r)
	}

	// Interleave 12 records across 3 tables: orders, items, users
	tables := []string{"orders", "items", "users"}
	for i := 0; i < 15; i++ {
		table := tables[i%3]
		ev := ChangeEvent{
			Operation: OpInsert,
			Schema:    "public",
			Table:     table,
			LSN:       uint64(2000 + i),
			After: map[string]any{
				"id":   int32(i),
				"name": fmt.Sprintf("%s_%d", table, i),
			},
		}
		if err := batcher.ProcessElement(context.Background(), ev, emit); err != nil {
			t.Fatalf("ProcessElement failed: %v", err)
		}
	}

	// Each table received 5 records, which equals MaxBatchRows(5).
	// Exactly 3 batches (one for each table) should have been emitted without any thrashing!
	if len(emittedBatches) != 3 {
		t.Fatalf("Expected 3 batches emitted for 3 tables, got %d", len(emittedBatches))
	}

	seenTables := make(map[string]int)
	for _, b := range emittedBatches {
		seenTables[b.TableName]++
		if b.RowCount != 5 {
			t.Errorf("Expected batch size 5 for table %s, got %d", b.TableName, b.RowCount)
		}
	}

	for _, table := range tables {
		if seenTables[table] != 1 {
			t.Errorf("Expected 1 batch for table %s, got %d", table, seenTables[table])
		}
	}
}

func TestArrowBatcherFinishBundleEvacuation(t *testing.T) {
	batcher := newArrowBatcherFn(WithArrowMaxBatchRows(100))
	batcher.Setup()
	defer batcher.Teardown()

	var emitted []ArrowBatchRecord
	emit := func(r ArrowBatchRecord) {
		emitted = append(emitted, r)
	}

	// Send 7 records (less than MaxBatchRows 100)
	for i := 0; i < 7; i++ {
		ev := ChangeEvent{
			Operation: OpInsert,
			Schema:    "public",
			Table:     "customers",
			LSN:       uint64(5000 + i),
			After: map[string]any{
				"id":    int32(i),
				"email": fmt.Sprintf("cust%d@example.com", i),
			},
		}
		if err := batcher.ProcessElement(context.Background(), ev, emit); err != nil {
			t.Fatalf("ProcessElement failed: %v", err)
		}
	}

	// Nothing emitted yet
	if len(emitted) != 0 {
		t.Fatalf("Expected 0 batches emitted before FinishBundle, got %d", len(emitted))
	}

	// Call FinishBundle
	if err := batcher.FinishBundle(emit); err != nil {
		t.Fatalf("FinishBundle failed: %v", err)
	}

	// Residual records must be completely evacuated!
	if len(emitted) != 1 {
		t.Fatalf("Expected 1 evacuated batch after FinishBundle, got %d", len(emitted))
	}
	if emitted[0].RowCount != 7 {
		t.Errorf("Expected evacuated batch to have 7 rows, got %d", emitted[0].RowCount)
	}
	if emitted[0].TableName != "customers" {
		t.Errorf("Expected table 'customers', got %s", emitted[0].TableName)
	}
}

func TestArrowRoundtripSerialization(t *testing.T) {
	checkedAlloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() {
		if checkedAlloc.CurrentAlloc() != 0 {
			t.Errorf("Memory leak in roundtrip test! %d bytes remaining", checkedAlloc.CurrentAlloc())
		}
	}()

	now := time.Date(2026, 9, 5, 14, 30, 0, 123000, time.UTC)
	originalEvents := []ChangeEvent{
		{
			Operation: OpInsert,
			Schema:    "analytics",
			Table:     "metrics",
			LSN:       8881,
			After: map[string]any{
				"id":        int32(1),
				"metric":    "cpu_usage",
				"value":     float64(75.25),
				"active":    true,
				"timestamp": now,
				"raw_bytes": []byte{0x01, 0x02, 0x03},
				"optional":  "present",
			},
		},
		{
			Operation: OpInsert,
			Schema:    "analytics",
			Table:     "metrics",
			LSN:       8882,
			After: map[string]any{
				"id":        int32(2),
				"metric":    "memory_usage",
				"value":     float64(42.10),
				"active":    false,
				"timestamp": now.Add(time.Second),
				"raw_bytes": []byte{0x04, 0x05},
				"optional":  nil, // test null preservation
			},
		},
	}

	batcher := newArrowBatcherFn(WithArrowMaxBatchRows(10))
	batcher.alloc = checkedAlloc
	batcher.Setup()
	defer batcher.Teardown()

	var batches []ArrowBatchRecord
	for _, ev := range originalEvents {
		if err := batcher.ProcessElement(context.Background(), ev, func(r ArrowBatchRecord) {
			batches = append(batches, r)
		}); err != nil {
			t.Fatalf("ProcessElement failed: %v", err)
		}
	}
	if err := batcher.FinishBundle(func(r ArrowBatchRecord) {
		batches = append(batches, r)
	}); err != nil {
		t.Fatalf("FinishBundle failed: %v", err)
	}

	if len(batches) != 1 {
		t.Fatalf("Expected 1 batch, got %d", len(batches))
	}

	// Reconvert back from Arrow IPC payload to ChangeEvents
	decoder := &fromArrowBatchesFn{}
	var decodedEvents []ChangeEvent
	if err := decoder.ProcessElement(context.Background(), batches[0], func(ev ChangeEvent) {
		decodedEvents = append(decodedEvents, ev)
	}); err != nil {
		t.Fatalf("fromArrowBatchesFn failed: %v", err)
	}

	if len(decodedEvents) != 2 {
		t.Fatalf("Expected 2 decoded events, got %d", len(decodedEvents))
	}

	// Verify row 1 values
	r1 := decodedEvents[0].After
	if r1["id"] != int32(1) {
		t.Errorf("Row 1 id: got %v, want 1", r1["id"])
	}
	if r1["metric"] != "cpu_usage" {
		t.Errorf("Row 1 metric: got %v, want cpu_usage", r1["metric"])
	}
	if r1["value"] != float64(75.25) {
		t.Errorf("Row 1 value: got %v, want 75.25", r1["value"])
	}
	if r1["active"] != true {
		t.Errorf("Row 1 active: got %v, want true", r1["active"])
	}
	if r1["timestamp"] != now {
		t.Errorf("Row 1 timestamp: got %v, want %v", r1["timestamp"], now)
	}
	if string(r1["raw_bytes"].([]byte)) != string([]byte{0x01, 0x02, 0x03}) {
		t.Errorf("Row 1 raw_bytes: got %v", r1["raw_bytes"])
	}
	if r1["optional"] != "present" {
		t.Errorf("Row 1 optional: got %v, want present", r1["optional"])
	}

	// Verify row 2 null preservation
	r2 := decodedEvents[1].After
	if r2["id"] != int32(2) {
		t.Errorf("Row 2 id: got %v, want 2", r2["id"])
	}
	if r2["optional"] != nil {
		t.Errorf("Row 2 optional: got %v, want nil (null preservation)", r2["optional"])
	}
}

func TestArrowPayloadSafetyLimit(t *testing.T) {
	// Create oversized payload record
	oversized := ArrowBatchRecord{
		SchemaID:  "oversized",
		Namespace: "public",
		TableName: "bombs",
		RowCount:  1,
		Payload:   make([]byte, MaxAllowedIPCMessageBytes+1),
	}

	decoder := &fromArrowBatchesFn{}
	err := decoder.ProcessElement(context.Background(), oversized, func(_ ChangeEvent) {})
	if err == nil || !strings.Contains(err.Error(), "exceeds safety limit") {
		t.Errorf("Expected safety limit error, got: %v", err)
	}
}

func BenchmarkArrowBatching(b *testing.B) {
	alloc := memory.NewGoAllocator()
	ev := ChangeEvent{
		Operation: OpInsert,
		Schema:    "public",
		Table:     "bench_orders",
		LSN:       9999,
		After: map[string]any{
			"order_id":    int64(123456789),
			"customer_id": int32(42),
			"total":       float64(299.99),
			"status":      "SHIPPED",
			"created_at":  time.Now().UTC(),
		},
		ColumnTypes: map[string]uint32{
			"order_id":    20,
			"customer_id": 23,
			"total":       701,
			"status":      25,
			"created_at":  1184,
		},
	}

	hash, cols, schema := ComputeSchemaHash(ev)
	buf := newTableBuffer(alloc, hash, "public", "bench_orders", cols, schema)
	defer buf.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := buf.Append(ev); err != nil {
			b.Fatal(err)
		}
		if buf.rowCount >= 1000 {
			if _, err := buf.MaterializeAndReset(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
