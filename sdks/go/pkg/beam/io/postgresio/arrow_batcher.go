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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterDoFn(&arrowBatcherFn{})
	beam.RegisterType(reflect.TypeOf((*tableBuffer)(nil)))
}

var ipcBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 64*1024))
	},
}

// tableBuffer maintains the Arrow RecordBuilder and schema context for a specific table topology.
type tableBuffer struct {
	alloc        memory.Allocator
	schemaHash   uint64
	namespace    string
	tableName    string
	cols         []string
	schema       *arrow.Schema
	builder      *array.RecordBuilder
	rowCount     int64
	currentBytes int64
	firstLSN     uint64
	lastLSN      uint64
}

func newTableBuffer(
	alloc memory.Allocator,
	hash uint64,
	namespace string,
	tableName string,
	cols []string,
	schema *arrow.Schema,
) *tableBuffer {
	return &tableBuffer{
		alloc:      alloc,
		schemaHash: hash,
		namespace:  namespace,
		tableName:  tableName,
		cols:       cols,
		schema:     schema,
		builder:    array.NewRecordBuilder(alloc, schema),
	}
}

// Append appends a single ChangeEvent row into the column builders.
func (b *tableBuffer) Append(event ChangeEvent) error {
	source := event.After
	if len(source) == 0 {
		source = event.Before
	}

	for i, col := range b.cols {
		val := source[col]
		AppendColumnValue(b.builder.Field(i), val)
		// Estimate incremental bytes
		if valStr, ok := val.(string); ok {
			b.currentBytes += int64(len(valStr))
		} else if valBytes, ok := val.([]byte); ok {
			b.currentBytes += int64(len(valBytes))
		} else {
			b.currentBytes += 8
		}
	}

	if b.firstLSN == 0 || (event.LSN > 0 && event.LSN < b.firstLSN) {
		b.firstLSN = event.LSN
	}
	if event.LSN > b.lastLSN {
		b.lastLSN = event.LSN
	}
	b.rowCount++
	return nil
}

// MaterializeAndReset converts buffered columns into an Arrow RecordBatch, serializes to Arrow IPC
// stream bytes, and resets the builder length to 0 while preserving internal slice capacity.
func (b *tableBuffer) MaterializeAndReset() (ArrowBatchRecord, error) {
	if b.rowCount == 0 {
		return ArrowBatchRecord{}, nil
	}

	record := b.builder.NewRecord()
	defer record.Release()

	buf := ipcBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer ipcBufferPool.Put(buf)

	writer := ipc.NewWriter(buf, ipc.WithSchema(b.schema), ipc.WithAllocator(b.alloc))
	if err := writer.Write(record); err != nil {
		_ = writer.Close()
		return ArrowBatchRecord{}, fmt.Errorf("failed to write Arrow record batch to IPC stream: %w", err)
	}
	if err := writer.Close(); err != nil {
		return ArrowBatchRecord{}, fmt.Errorf("failed to finalize Arrow IPC writer: %w", err)
	}

	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())

	meta := map[string]string{
		"first_lsn": fmt.Sprintf("%d", b.firstLSN),
		"last_lsn":  fmt.Sprintf("%d", b.lastLSN),
	}

	out := ArrowBatchRecord{
		SchemaID:  fmt.Sprintf("%016x", b.schemaHash),
		Namespace: b.namespace,
		TableName: b.tableName,
		RowCount:  b.rowCount,
		Payload:   payload,
		Metadata:  meta,
	}

	b.rowCount = 0
	b.currentBytes = 0
	b.firstLSN = 0
	b.lastLSN = 0

	return out, nil
}

// Release releases memory allocated by the record builder.
func (b *tableBuffer) Release() {
	if b.builder != nil {
		b.builder.Release()
		b.builder = nil
	}
}

// arrowBatcherFn is a Beam DoFn that transforms ChangeEvents into columnar ArrowBatchRecords.
type arrowBatcherFn struct {
	Options   ArrowBatchOptions
	alloc     memory.Allocator
	buffers   map[uint64]*tableBuffer
	schemaLRU []uint64
}

func newArrowBatcherFn(opts ...ArrowBatchOption) *arrowBatcherFn {
	cfg := NewDefaultArrowBatchOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &arrowBatcherFn{
		Options: cfg,
	}
}

func (fn *arrowBatcherFn) Setup() {
	if fn.alloc == nil {
		fn.alloc = memory.NewGoAllocator()
	}
	fn.buffers = make(map[uint64]*tableBuffer)
	fn.schemaLRU = make([]uint64, 0, fn.Options.MaxActiveSchemas)
}

func (fn *arrowBatcherFn) ProcessElement(ctx context.Context, event ChangeEvent, emit func(ArrowBatchRecord)) error {
	schemaHash, cols, schema := ComputeSchemaHash(event)

	buf, exists := fn.buffers[schemaHash]
	if !exists {
		// Enforce schema pool capacity limit
		if len(fn.buffers) >= fn.Options.MaxActiveSchemas && len(fn.schemaLRU) > 0 {
			oldestHash := fn.schemaLRU[0]
			fn.schemaLRU = fn.schemaLRU[1:]
			if oldBuf, ok := fn.buffers[oldestHash]; ok {
				if oldBuf.rowCount > 0 {
					rec, err := oldBuf.MaterializeAndReset()
					if err != nil {
						return err
					}
					emit(rec)
				}
				oldBuf.Release()
				delete(fn.buffers, oldestHash)
			}
		}

		buf = newTableBuffer(fn.alloc, schemaHash, event.Schema, event.Table, cols, schema)
		fn.buffers[schemaHash] = buf
		fn.schemaLRU = append(fn.schemaLRU, schemaHash)
	}

	if err := buf.Append(event); err != nil {
		return err
	}

	if buf.rowCount >= int64(fn.Options.MaxBatchRows) || buf.currentBytes >= int64(fn.Options.MaxBatchBytes) {
		rec, err := buf.MaterializeAndReset()
		if err != nil {
			return err
		}
		emit(rec)
	}

	return nil
}

func (fn *arrowBatcherFn) FinishBundle(emit func(ArrowBatchRecord)) error {
	for _, buf := range fn.buffers {
		if buf.rowCount > 0 {
			rec, err := buf.MaterializeAndReset()
			if err != nil {
				return err
			}
			emit(rec)
		}
	}
	return nil
}

func (fn *arrowBatcherFn) Teardown() {
	for _, buf := range fn.buffers {
		buf.Release()
	}
	fn.buffers = make(map[uint64]*tableBuffer)
	fn.schemaLRU = nil
}

// ToArrowBatches transforms a PCollection of ChangeEvents into a PCollection of ArrowBatchRecords.
func ToArrowBatches(s beam.Scope, col beam.PCollection, opts ...ArrowBatchOption) beam.PCollection {
	s = s.Scope("postgresio.ToArrowBatches")
	return beam.ParDo(s, newArrowBatcherFn(opts...), col)
}
