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
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/lib/pq"
)

func TestCoders_Roundtrip(t *testing.T) {
	// 1. FailedRow Coder
	failedRow := FailedRow{
		Row:          map[string]any{"id": "test_id"},
		ErrorMessage: "test_error",
		SqlState:     "23505",
	}
	encFailed, err := encodeFailedRow(failedRow)
	if err != nil {
		t.Fatalf("encodeFailedRow() err = %v", err)
	}
	decFailed, err := decodeFailedRow(encFailed)
	if err != nil {
		t.Fatalf("decodeFailedRow() err = %v", err)
	}
	if decFailed.ErrorMessage != failedRow.ErrorMessage || decFailed.SqlState != failedRow.SqlState {
		t.Errorf("FailedRow decode mismatch: got %+v, want %+v", decFailed, failedRow)
	}

	// 2. ChangeEvent Coder
	ce := ChangeEvent{
		Operation:     OpInsert,
		Schema:        "public",
		Table:         "users",
		CommitTime:    time.Now().UTC(),
		LSN:           12345,
		TransactionID: 100,
		PrimaryKeys:   []string{"id"},
		After: map[string]any{
			"id":   int64(101),
			"name": "alice",
		},
	}
	encCE, err := encodeChangeEvent(ce)
	if err != nil {
		t.Fatalf("encodeChangeEvent() err = %v", err)
	}
	decCE, err := decodeChangeEvent(encCE)
	if err != nil {
		t.Fatalf("decodeChangeEvent() err = %v", err)
	}
	if decCE.Table != ce.Table || decCE.LSN != ce.LSN {
		t.Errorf("ChangeEvent decode mismatch: got %+v, want %+v", decCE, ce)
	}
	if idVal, ok := decCE.After["id"].(int64); !ok || idVal != 101 {
		t.Errorf("ChangeEvent After['id'] type coerced: got %T (%v), want int64(101)", decCE.After["id"], decCE.After["id"])
	}

	// 3. TransactionMessage Coder
	txMsg := OfMutation(999, ce)
	encTx, err := encodeTxMessage(txMsg)
	if err != nil {
		t.Fatalf("encodeTxMessage() err = %v", err)
	}
	decTx, err := decodeTxMessage(encTx)
	if err != nil {
		t.Fatalf("decodeTxMessage() err = %v", err)
	}
	if decTx.TransactionID != txMsg.TransactionID || decTx.Event == nil || decTx.Event.Table != txMsg.Event.Table {
		t.Errorf("TransactionMessage decode mismatch: got %+v, want %+v", decTx, txMsg)
	}
}

func TestWrite_GoTypeToPgArrayType(t *testing.T) {
	tests := []struct {
		val  any
		want string
	}{
		{nil, "text[]"},
		{int64(1), "bigint[]"},
		{int(1), "integer[]"},
		{int32(1), "integer[]"},
		{int16(1), "smallint[]"},
		{float64(1.5), "double precision[]"},
		{float32(1.5), "real[]"},
		{true, "boolean[]"},
		{"string", "text[]"},
		{[]byte{1, 2}, "bytea[]"},
		{[]string{"a"}, "text[]"},
		{time.Now(), "timestamptz[]"},
	}

	for _, tt := range tests {
		var typ reflect.Type
		if tt.val != nil {
			typ = reflect.TypeOf(tt.val)
		}
		got := goTypeToPgArrayType(typ)
		if got != tt.want {
			t.Errorf("goTypeToPgArrayType(%v) = %q, want %q", typ, got, tt.want)
		}
	}

	// Pointer unwrapping
	var ptrInt int64 = 42
	gotPtr := goTypeToPgArrayType(reflect.TypeOf(&ptrInt))
	if gotPtr != "bigint[]" {
		t.Errorf("goTypeToPgArrayType(*int64) = %q, want bigint[]", gotPtr)
	}

	// Struct fallback
	type Custom struct{ A int }
	gotStruct := goTypeToPgArrayType(reflect.TypeOf(Custom{}))
	if gotStruct != "text[]" {
		t.Errorf("goTypeToPgArrayType(Custom{}) = %q, want text[]", gotStruct)
	}
}

func TestWrite_ExtractSqlState(t *testing.T) {
	if got := extractSqlState(nil); got != "" {
		t.Errorf("extractSqlState(nil) = %q, want empty", got)
	}
	pqErr := &pq.Error{Code: "23505"}
	if got := extractSqlState(pqErr); got != "23505" {
		t.Errorf("extractSqlState(pqErr) = %q, want 23505", got)
	}
	genErr := errors.New("connection reset")
	if got := extractSqlState(genErr); got != "UNKNOWN" {
		t.Errorf("extractSqlState(genErr) = %q, want UNKNOWN", got)
	}
}

func TestCompactor_CompareSingleKey(t *testing.T) {
	if got := compareSingleKey(nil, nil); got != 0 {
		t.Errorf("compareSingleKey(nil, nil) = %d, want 0", got)
	}
	if got := compareSingleKey(nil, 1); got != -1 {
		t.Errorf("compareSingleKey(nil, 1) = %d, want -1", got)
	}
	if got := compareSingleKey(1, nil); got != 1 {
		t.Errorf("compareSingleKey(1, nil) = %d, want 1", got)
	}

	// int
	if got := compareSingleKey(1, 2); got != -1 {
		t.Errorf("compareSingleKey(1, 2) = %d, want -1", got)
	}
	if got := compareSingleKey(2, 1); got != 1 {
		t.Errorf("compareSingleKey(2, 1) = %d, want 1", got)
	}
	if got := compareSingleKey(1, 1); got != 0 {
		t.Errorf("compareSingleKey(1, 1) = %d, want 0", got)
	}

	// int64
	if got := compareSingleKey(int64(10), int64(20)); got != -1 {
		t.Errorf("compareSingleKey(10, 20) = %d, want -1", got)
	}
	if got := compareSingleKey(int64(20), int64(10)); got != 1 {
		t.Errorf("compareSingleKey(20, 10) = %d, want 1", got)
	}

	// string
	if got := compareSingleKey("a", "b"); got != -1 {
		t.Errorf("compareSingleKey('a', 'b') = %d, want -1", got)
	}

	// float64
	if got := compareSingleKey(1.5, 2.5); got != -1 {
		t.Errorf("compareSingleKey(1.5, 2.5) = %d, want -1", got)
	}
	if got := compareSingleKey(2.5, 1.5); got != 1 {
		t.Errorf("compareSingleKey(2.5, 1.5) = %d, want 1", got)
	}

	// []byte
	if got := compareSingleKey([]byte{1}, []byte{2}); got != -1 {
		t.Errorf("compareSingleKey([]byte) = %d, want -1", got)
	}

	// fallback string representation
	type KeyType int
	if got := compareSingleKey(KeyType(1), KeyType(2)); got != -1 {
		t.Errorf("compareSingleKey(KeyType) = %d, want -1", got)
	}
}

func TestCompactor_ExtractPrimaryKeys_Variations(t *testing.T) {
	// Empty pkCols
	if k, s := ExtractPrimaryKeys("anything", nil); k != "" || s != nil {
		t.Errorf("ExtractPrimaryKeys(empty) = (%q, %v), want empty", k, s)
	}

	// Struct by exact name
	type TestUser struct {
		ID   int64
		Name string
	}
	u := TestUser{ID: 101, Name: "bob"}
	k, vals := ExtractPrimaryKeys(u, []string{"ID"})
	if k != "101|" || len(vals) != 1 || vals[0] != int64(101) {
		t.Errorf("ExtractPrimaryKeys(exact) = (%q, %v)", k, vals)
	}

	// Pointer to struct with EqualFold
	k2, vals2 := ExtractPrimaryKeys(&u, []string{"id"})
	if k2 != "101|" || len(vals2) != 1 {
		t.Errorf("ExtractPrimaryKeys(EqualFold) = (%q, %v)", k2, vals2)
	}

	// Struct with db tag
	type TaggedUser struct {
		UserKey int64 `db:"user_id"`
	}
	tu := TaggedUser{UserKey: 777}
	k3, vals3 := ExtractPrimaryKeys(tu, []string{"user_id"})
	if k3 != "777|" || len(vals3) != 1 || vals3[0] != int64(777) {
		t.Errorf("ExtractPrimaryKeys(db tag) = (%q, %v)", k3, vals3)
	}

	// Struct missing column
	k4, vals4 := ExtractPrimaryKeys(u, []string{"MissingCol"})
	if k4 != "nil|" || len(vals4) != 1 || vals4[0] != nil {
		t.Errorf("ExtractPrimaryKeys(missing col) = (%q, %v)", k4, vals4)
	}

	// Map lookup
	m := map[string]any{"id": "order_123", "region": "US"}
	k5, vals5 := ExtractPrimaryKeys(m, []string{"id", "region"})
	if k5 != "order_123|US|" || len(vals5) != 2 {
		t.Errorf("ExtractPrimaryKeys(map) = (%q, %v)", k5, vals5)
	}

	// Map missing key
	k6, vals6 := ExtractPrimaryKeys(m, []string{"missing_key"})
	if k6 != "nil|" || len(vals6) != 1 || vals6[0] != nil {
		t.Errorf("ExtractPrimaryKeys(map missing) = (%q, %v)", k6, vals6)
	}

	// Non-struct, non-map kind
	if k7, vals7 := ExtractPrimaryKeys(42, []string{"id"}); k7 != "" || vals7 != nil {
		t.Errorf("ExtractPrimaryKeys(primitive) = (%q, %v), want empty", k7, vals7)
	}
}

func TestDemux_ProcessElement(t *testing.T) {
	ev1 := ChangeEvent{Schema: "public", Table: "orders"}
	ev2 := ChangeEvent{Schema: "analytics", Table: "orders"}

	// 1. filterByTableFn
	tableFn := &filterByTableFn{TargetTable: "public.orders"}
	var emittedTable []ChangeEvent
	tableFn.ProcessElement(ev1, func(e ChangeEvent) { emittedTable = append(emittedTable, e) })
	tableFn.ProcessElement(ev2, func(e ChangeEvent) { emittedTable = append(emittedTable, e) })
	if len(emittedTable) != 1 || emittedTable[0].Schema != "public" {
		t.Errorf("filterByTableFn emitted %d events, want 1", len(emittedTable))
	}

	// 2. filterBySchemaFn
	schemaFn := &filterBySchemaFn{TargetSchema: "analytics"}
	var emittedSchema []ChangeEvent
	schemaFn.ProcessElement(ev1, func(e ChangeEvent) { emittedSchema = append(emittedSchema, e) })
	schemaFn.ProcessElement(ev2, func(e ChangeEvent) { emittedSchema = append(emittedSchema, e) })
	if len(emittedSchema) != 1 || emittedSchema[0].Schema != "analytics" {
		t.Errorf("filterBySchemaFn emitted %d events, want 1", len(emittedSchema))
	}

	// 3. filterByOriginFn
	originFn := &filterByOriginFn{SelfOrigin: "remote_origin"}
	var emittedOrigin []ChangeEvent
	originFn.ProcessElement(ChangeEvent{Origin: "remote_origin"}, func(e ChangeEvent) { emittedOrigin = append(emittedOrigin, e) })
	originFn.ProcessElement(ChangeEvent{Origin: "local_origin"}, func(e ChangeEvent) { emittedOrigin = append(emittedOrigin, e) })
	originFn.ProcessElement(ChangeEvent{Origin: ""}, func(e ChangeEvent) { emittedOrigin = append(emittedOrigin, e) })
	if len(emittedOrigin) != 2 {
		t.Errorf("filterByOriginFn emitted %d events, want 2", len(emittedOrigin))
	}
}

func TestToast_KeyFunctions(t *testing.T) {
	keyFn := &keyByPrimaryKeyFn{}
	ev := ChangeEvent{
		Table:       "users",
		PrimaryKeys: []string{"id"},
		After:       map[string]any{"id": "user_42"},
	}
	k, v := keyFn.ProcessElement(ev)
	if k != "users:user_42" || v.PrimaryKeys[0] != "id" {
		t.Errorf("keyByPrimaryKeyFn output mismatch: k=%q, v=%+v", k, v)
	}

	dropFn := &dropKeyFn{}
	dropped := dropFn.ProcessElement("ignored_key", ev)
	if dropped.PrimaryKeys[0] != "id" {
		t.Errorf("dropKeyFn output mismatch: %+v", dropped)
	}
}

func TestOptions_BuildersAndTokenProvider(t *testing.T) {
	// Write options builders
	dummyDial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, nil
	}
	wOpts := NewWriteOptions(
		WithSSLMode("require"),
		WithDialFunc(dummyDial),
	)
	if wOpts.SSLMode != "require" {
		t.Errorf("SSLMode = %q, want require", wOpts.SSLMode)
	}
	if wOpts.DialFunc == nil {
		t.Errorf("DialFunc is nil")
	}

	// CDC options builders
	dummyToken := NewStaticTokenProvider("secret_token")
	if pw, err := dummyToken.GetPassword(context.Background()); err != nil || pw != "secret_token" {
		t.Errorf("dummyToken.GetPassword() = %q, err = %v, want secret_token", pw, err)
	}

	cdcOpts := NewCDCOptions(
		WithCDCCreateSlotIfMissing(true),
		WithCDCReplicaIdentityFull(true),
		WithCDCTokenProvider(dummyToken),
		WithCDCDialFunc(dummyDial),
	)
	if !cdcOpts.CreateSlotIfMissing {
		t.Errorf("CreateSlotIfMissing = false, want true")
	}
	if !cdcOpts.ReplicaIdentityFull {
		t.Errorf("ReplicaIdentityFull = false, want true")
	}
	if cdcOpts.TokenProvider == nil {
		t.Errorf("TokenProvider is nil")
	}
	if cdcOpts.DialFunc == nil {
		t.Errorf("DialFunc is nil")
	}
}

func TestArrow_OptionsAndFullTableName(t *testing.T) {
	opts := NewDefaultArrowBatchOptions()
	optFn1 := WithArrowMaxBatchBytes(1024 * 1024)
	optFn1(&opts)
	optFn2 := WithArrowMaxActiveSchemas(32)
	optFn2(&opts)

	if opts.MaxBatchBytes != 1024*1024 {
		t.Errorf("MaxBatchBytes = %d, want 1MB", opts.MaxBatchBytes)
	}
	if opts.MaxActiveSchemas != 32 {
		t.Errorf("MaxActiveSchemas = %d, want 32", opts.MaxActiveSchemas)
	}

	// ChangeEvent FullTableName
	ce1 := ChangeEvent{Schema: "public", Table: "orders"}
	if got := ce1.FullTableName(); got != "public.orders" {
		t.Errorf("ce1.FullTableName() = %q, want public.orders", got)
	}
	ce2 := ChangeEvent{Table: "orders"}
	if got := ce2.FullTableName(); got != "orders" {
		t.Errorf("ce2.FullTableName() = %q, want orders", got)
	}

	// ArrowBatchRecord FullTableName
	rec1 := ArrowBatchRecord{Namespace: "analytics", TableName: "metrics"}
	if got := rec1.FullTableName(); got != "analytics.metrics" {
		t.Errorf("rec1.FullTableName() = %q, want analytics.metrics", got)
	}
	rec2 := ArrowBatchRecord{TableName: "metrics"}
	if got := rec2.FullTableName(); got != "metrics" {
		t.Errorf("rec2.FullTableName() = %q, want metrics", got)
	}
}

func TestArrow_FromArrowBatches_PipelineConstruction(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()
	col := beam.Create(s, ArrowBatchRecord{
		Namespace: "public",
		TableName: "users",
		RowCount:  0,
		Payload:   []byte{},
	})
	events := FromArrowBatches(s, col)
	if !events.IsValid() {
		t.Errorf("FromArrowBatches returned invalid PCollection")
	}
}

func TestArrow_FromArrowBatches_Execution(t *testing.T) {
	pool := memory.NewGoAllocator()
	bld := array.NewRecordBuilder(pool, arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer bld.Release()

	bld.Field(0).(*array.Int64Builder).Append(42)
	bld.Field(1).(*array.StringBuilder).Append("alice")
	rec := bld.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc.Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc.Close failed: %v", err)
	}

	batch := ArrowBatchRecord{
		Namespace: "public",
		TableName: "users",
		RowCount:  1,
		Payload:   buf.Bytes(),
		Metadata:  map[string]string{"first_lsn": "999"},
	}

	fn := &fromArrowBatchesFn{}
	var emitted []ChangeEvent
	err := fn.ProcessElement(context.Background(), batch, func(e ChangeEvent) {
		emitted = append(emitted, e)
	})
	if err != nil {
		t.Fatalf("ProcessElement() err = %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitted))
	}
	if emitted[0].Table != "users" || emitted[0].LSN != 999 {
		t.Errorf("emitted event mismatch: %+v", emitted[0])
	}
	if emitted[0].After["id"] != int64(42) || emitted[0].After["name"] != "alice" {
		t.Errorf("emitted event row mismatch: %+v", emitted[0].After)
	}
}

func TestArrow_AppendColumnValue_AllTypes(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 1. BooleanBuilder
	bb := array.NewBooleanBuilder(pool)
	defer bb.Release()
	AppendColumnValue(bb, nil)
	AppendColumnValue(bb, true)
	AppendColumnValue(bb, "t")
	AppendColumnValue(bb, 123) // fallback to null
	if bb.Len() != 4 {
		t.Errorf("BooleanBuilder len = %d, want 4", bb.Len())
	}

	// 2. Int16Builder
	i16b := array.NewInt16Builder(pool)
	defer i16b.Release()
	AppendColumnValue(i16b, int16(1))
	AppendColumnValue(i16b, int(2))
	AppendColumnValue(i16b, int32(3))
	AppendColumnValue(i16b, int64(4))
	AppendColumnValue(i16b, float64(5.0))
	AppendColumnValue(i16b, "invalid")
	if i16b.Len() != 6 {
		t.Errorf("Int16Builder len = %d, want 6", i16b.Len())
	}

	// 3. Int32Builder
	i32b := array.NewInt32Builder(pool)
	defer i32b.Release()
	AppendColumnValue(i32b, int32(10))
	AppendColumnValue(i32b, int(20))
	AppendColumnValue(i32b, int16(30))
	AppendColumnValue(i32b, int64(40))
	AppendColumnValue(i32b, float64(50.0))
	if i32b.Len() != 5 {
		t.Errorf("Int32Builder len = %d, want 5", i32b.Len())
	}

	// 4. Int64Builder
	i64b := array.NewInt64Builder(pool)
	defer i64b.Release()
	AppendColumnValue(i64b, int64(100))
	AppendColumnValue(i64b, int(200))
	AppendColumnValue(i64b, float64(300.0))
	if i64b.Len() != 3 {
		t.Errorf("Int64Builder len = %d, want 3", i64b.Len())
	}

	// 5. Float32Builder
	f32b := array.NewFloat32Builder(pool)
	defer f32b.Release()
	AppendColumnValue(f32b, float32(1.5))
	AppendColumnValue(f32b, float64(2.5))
	AppendColumnValue(f32b, int(3))
	if f32b.Len() != 3 {
		t.Errorf("Float32Builder len = %d, want 3", f32b.Len())
	}

	// 6. Float64Builder
	f64b := array.NewFloat64Builder(pool)
	defer f64b.Release()
	AppendColumnValue(f64b, float64(1.25))
	AppendColumnValue(f64b, float32(2.25))
	AppendColumnValue(f64b, int64(3))
	if f64b.Len() != 3 {
		t.Errorf("Float64Builder len = %d, want 3", f64b.Len())
	}

	// 7. StringBuilder
	sb := array.NewStringBuilder(pool)
	defer sb.Release()
	AppendColumnValue(sb, "hello")
	AppendColumnValue(sb, 123)
	if sb.Len() != 2 {
		t.Errorf("StringBuilder len = %d, want 2", sb.Len())
	}

	// 8. BinaryBuilder
	binb := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	defer binb.Release()
	AppendColumnValue(binb, []byte{1, 2, 3})
	AppendColumnValue(binb, "bytes")
	if binb.Len() != 2 {
		t.Errorf("BinaryBuilder len = %d, want 2", binb.Len())
	}
}

func TestPgOutputParser_ParseTextValue_AllOIDs(t *testing.T) {
	// JSON/JSONB (114, 3802)
	if v := parseTextValue(114, `{"a": 1}`); v != `{"a": 1}` {
		t.Errorf("parseTextValue(114) = %v", v)
	}
	if v := parseTextValue(3802, `{"b": 2}`); v != `{"b": 2}` {
		t.Errorf("parseTextValue(3802) = %v", v)
	}

	// bool (16)
	if v := parseTextValue(16, "t"); v != true {
		t.Errorf("parseTextValue(16, t) = %v", v)
	}
	if v := parseTextValue(16, "f"); v != false {
		t.Errorf("parseTextValue(16, f) = %v", v)
	}

	// int8 (20)
	if v := parseTextValue(20, "9223372036854775807"); v != int64(9223372036854775807) {
		t.Errorf("parseTextValue(20) = %v", v)
	}

	// int2 (21)
	if v := parseTextValue(21, "32767"); v != int16(32767) {
		t.Errorf("parseTextValue(21) = %v", v)
	}

	// int4 (23)
	if v := parseTextValue(23, "2147483647"); v != int32(2147483647) {
		t.Errorf("parseTextValue(23) = %v", v)
	}

	// float4 (700)
	if v := parseTextValue(700, "3.14"); v != float32(3.14) {
		t.Errorf("parseTextValue(700) = %v", v)
	}

	// float8 (701)
	if v := parseTextValue(701, "2.718281828459045"); v != float64(2.718281828459045) {
		t.Errorf("parseTextValue(701) = %v", v)
	}

	// timestamp/timestamptz (1114, 1184)
	tsVal := parseTextValue(1184, "2026-09-06T12:00:00Z")
	if _, ok := tsVal.(time.Time); !ok {
		t.Errorf("parseTextValue(1184) = %v, want time.Time", tsVal)
	}

	// Array OID
	arrVal := parseTextValue(1007, "{10,20,30}")
	if arr, ok := arrVal.([]any); !ok || len(arr) != 3 {
		t.Errorf("parseTextValue(1007) = %v, want []any of len 3", arrVal)
	}

	// Point (600)
	ptVal := parseTextValue(600, "(1.5,2.5)")
	if _, ok := ptVal.(PgPoint); !ok {
		t.Errorf("parseTextValue(600) = %v, want PgPoint", ptVal)
	}

	// Fallback text
	if v := parseTextValue(99999, "unhandled_text"); v != "unhandled_text" {
		t.Errorf("parseTextValue(fallback) = %v", v)
	}
}

func TestCDCStream_NativeReplicationStream_MockTCP(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		// 1. Read StartupMessage
		var lenBuf [4]byte
		if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		body := make([]byte, length-4)
		if _, err := io.ReadFull(serverConn, body); err != nil {
			return
		}

		// 2. Respond with AuthenticationOk ('R', length 8, code 0)
		authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
		if _, err := serverConn.Write(authOk); err != nil {
			return
		}

		// 3. Respond with ReadyForQuery ('Z', length 5, status 'I')
		ready := []byte{'Z', 0, 0, 0, 5, 'I'}
		if _, err := serverConn.Write(ready); err != nil {
			return
		}

		// 4. Read Query message ('Q') for search_path isolation
		var qType [1]byte
		if _, err := io.ReadFull(serverConn, qType[:]); err != nil {
			return
		}
		if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
			return
		}
		qLen := binary.BigEndian.Uint32(lenBuf[:])
		qBody := make([]byte, qLen-4)
		if _, err := io.ReadFull(serverConn, qBody); err != nil {
			return
		}

		// Respond with ReadyForQuery ('Z') for search_path query
		if _, err := serverConn.Write(ready); err != nil {
			return
		}

		// 5. Read START_REPLICATION query ('Q')
		if _, err := io.ReadFull(serverConn, qType[:]); err != nil {
			return
		}
		if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
			return
		}
		qLen2 := binary.BigEndian.Uint32(lenBuf[:])
		qBody2 := make([]byte, qLen2-4)
		if _, err := io.ReadFull(serverConn, qBody2); err != nil {
			return
		}

		// 6. Respond with CopyBothResponse ('W', len 7, format 0, cols 0)
		copyBoth := []byte{'W', 0, 0, 0, 7, 0, 0, 0}
		if _, err := serverConn.Write(copyBoth); err != nil {
			return
		}

		// 7. Send CopyData keepalive ('d', length 22, 'k', ...)
		keepAlive := []byte{
			'd', 0, 0, 0, 22,
			'k',
			0, 0, 0, 0, 0, 0, 0x10, 0x00, // walEnd (0x1000)
			0, 0, 0, 0, 0, 0, 0, 0, // serverTime
			1, // replyRequested = 1
		}
		if _, err := serverConn.Write(keepAlive); err != nil {
			return
		}

		// 7. Read client StandbyStatusUpdate ('d', length 39, 'r', ...)
		var dType [1]byte
		if _, err := io.ReadFull(serverConn, dType[:]); err != nil {
			return
		}
		if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
			return
		}
		dLen := binary.BigEndian.Uint32(lenBuf[:])
		dBody := make([]byte, dLen-4)
		if _, err := io.ReadFull(serverConn, dBody); err != nil {
			return
		}
	}()

	opts := NewCDCOptions(
		WithCDCHost("localhost"),
		WithCDCPort(5432),
		WithCDCDatabase("testdb"),
		WithCDCUsername("beam_test"),
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCSSLMode("disable"),
		WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}),
	)

	stream, err := NewNativeReplicationStream(ctx, opts)
	if err != nil {
		t.Fatalf("NewNativeReplicationStream() err = %v", err)
	}
	defer stream.Close()

	// Read message (should receive the keepalive)
	payload, err := stream.NextMessage(ctx)
	if err != nil {
		t.Fatalf("stream.NextMessage() err = %v", err)
	}
	if len(payload) == 0 || payload[0] != 'k' {
		t.Errorf("expected keepalive 'k', got payload len=%d", len(payload))
	}

	// Send standby status
	status := StandbyStatus{
		WriteLSN: 0x1000,
		FlushLSN: 0x1000,
		ApplyLSN: 0x1000,
	}
	if err := stream.SendStandbyStatus(ctx, status); err != nil {
		t.Errorf("stream.SendStandbyStatus() err = %v", err)
	}

	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Errorf("server mock timed out")
	}
}

func TestCDCStream_AuthCleartextAndMD5(t *testing.T) {
	t.Run("CleartextAuth", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			// 1. StartupMessage
			var lenBuf [4]byte
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			length := binary.BigEndian.Uint32(lenBuf[:])
			body := make([]byte, length-4)
			if _, err := io.ReadFull(serverConn, body); err != nil {
				return
			}

			// 2. Respond with AuthCleartext ('R', length 8, code 3)
			authReq := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 3}
			if _, err := serverConn.Write(authReq); err != nil {
				return
			}

			// 3. Read password ('p')
			var pType [1]byte
			if _, err := io.ReadFull(serverConn, pType[:]); err != nil || pType[0] != 'p' {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			pLen := binary.BigEndian.Uint32(lenBuf[:])
			pBody := make([]byte, pLen-4)
			if _, err := io.ReadFull(serverConn, pBody); err != nil {
				return
			}

			// 4. Respond with AuthOk ('R', length 8, code 0)
			authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
			if _, err := serverConn.Write(authOk); err != nil {
				return
			}

			// 5. ReadyForQuery ('Z')
			ready := []byte{'Z', 0, 0, 0, 5, 'I'}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// 6. Read search_path Query ('Q')
			if _, err := io.ReadFull(serverConn, pType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen := binary.BigEndian.Uint32(lenBuf[:])
			qBody := make([]byte, qLen-4)
			if _, err := io.ReadFull(serverConn, qBody); err != nil {
				return
			}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// 7. Read START_REPLICATION ('Q')
			if _, err := io.ReadFull(serverConn, pType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen2 := binary.BigEndian.Uint32(lenBuf[:])
			qBody2 := make([]byte, qLen2-4)
			if _, err := io.ReadFull(serverConn, qBody2); err != nil {
				return
			}

			// 8. Respond with CopyBothResponse ('W')
			copyBoth := []byte{'W', 0, 0, 0, 7, 0, 0, 0}
			if _, err := serverConn.Write(copyBoth); err != nil {
				return
			}
		}()

		opts := NewCDCOptions(
			WithCDCHost("localhost"),
			WithCDCUsername("beam_test"),
			WithCDCPassword("secret123"),
			WithCDCSlotName("test_slot"),
			WithCDCPublication("test_pub"),
			WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
				return clientConn, nil
			}),
		)

		stream, err := NewNativeReplicationStream(ctx, opts)
		if err != nil {
			t.Fatalf("NewNativeReplicationStream() err = %v", err)
		}
		_ = stream.Close()
		<-serverDone
	})

	t.Run("MD5Auth", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			var lenBuf [4]byte
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			length := binary.BigEndian.Uint32(lenBuf[:])
			body := make([]byte, length-4)
			if _, err := io.ReadFull(serverConn, body); err != nil {
				return
			}

			// Respond with AuthMD5 ('R', length 12, code 5, salt 'abcd')
			authReq := []byte{'R', 0, 0, 0, 12, 0, 0, 0, 5, 'a', 'b', 'c', 'd'}
			if _, err := serverConn.Write(authReq); err != nil {
				return
			}

			// Read password ('p')
			var pType [1]byte
			if _, err := io.ReadFull(serverConn, pType[:]); err != nil || pType[0] != 'p' {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			pLen := binary.BigEndian.Uint32(lenBuf[:])
			pBody := make([]byte, pLen-4)
			if _, err := io.ReadFull(serverConn, pBody); err != nil {
				return
			}

			// Verify token starts with "md5"
			if !strings.HasPrefix(string(pBody), "md5") {
				return
			}

			// Respond with AuthOk
			authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
			if _, err := serverConn.Write(authOk); err != nil {
				return
			}
			ready := []byte{'Z', 0, 0, 0, 5, 'I'}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// Read search_path Query ('Q')
			if _, err := io.ReadFull(serverConn, pType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen := binary.BigEndian.Uint32(lenBuf[:])
			qBody := make([]byte, qLen-4)
			if _, err := io.ReadFull(serverConn, qBody); err != nil {
				return
			}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// Read START_REPLICATION ('Q')
			if _, err := io.ReadFull(serverConn, pType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen2 := binary.BigEndian.Uint32(lenBuf[:])
			qBody2 := make([]byte, qLen2-4)
			if _, err := io.ReadFull(serverConn, qBody2); err != nil {
				return
			}

			// Respond with CopyBothResponse ('W')
			copyBoth := []byte{'W', 0, 0, 0, 7, 0, 0, 0}
			if _, err := serverConn.Write(copyBoth); err != nil {
				return
			}
		}()

		opts := NewCDCOptions(
			WithCDCHost("localhost"),
			WithCDCUsername("beam_test"),
			WithCDCPassword("secret123"),
			WithCDCSlotName("test_slot"),
			WithCDCPublication("test_pub"),
			WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
				return clientConn, nil
			}),
		)

		stream, err := NewNativeReplicationStream(ctx, opts)
		if err != nil {
			t.Fatalf("NewNativeReplicationStream() err = %v", err)
		}
		_ = stream.Close()
		<-serverDone
	})
}

func TestCDCStream_StartupErrorResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		var lenBuf [4]byte
		if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		body := make([]byte, length-4)
		if _, err := io.ReadFull(serverConn, body); err != nil {
			return
		}

		// Respond with ErrorResponse ('E', length 14, "FATAL_ERROR")
		errMsg := "FATAL_ERROR\x00"
		errPkt := make([]byte, 5+len(errMsg))
		errPkt[0] = 'E'
		binary.BigEndian.PutUint32(errPkt[1:5], uint32(len(errPkt)-1))
		copy(errPkt[5:], errMsg)
		_, _ = serverConn.Write(errPkt)
	}()

	opts := NewCDCOptions(
		WithCDCHost("localhost"),
		WithCDCUsername("beam_test"),
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}),
	)

	_, err := NewNativeReplicationStream(ctx, opts)
	if err == nil {
		t.Fatalf("expected error from startup ErrorResponse, got nil")
	}
	if !strings.Contains(err.Error(), "FATAL_ERROR") {
		t.Errorf("expected FATAL_ERROR in message, got %v", err)
	}
}

func TestCDCStream_NextMessage_EOFAndError(t *testing.T) {
	t.Run("CopyDoneEOF", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		stream := &NativeReplicationStream{
			conn: clientConn,
		}

		go func() {
			// Write CopyDone ('c', length 4)
			pkt := []byte{'c', 0, 0, 0, 4}
			_, _ = serverConn.Write(pkt)
		}()

		_, err := stream.NextMessage(ctx)
		if err != io.EOF {
			t.Errorf("NextMessage on CopyDone = %v, want io.EOF", err)
		}
	})

	t.Run("StreamErrorResponse", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		stream := &NativeReplicationStream{
			conn: clientConn,
		}

		go func() {
			// Write ErrorResponse ('E')
			errMsg := "replication terminated"
			errPkt := make([]byte, 5+len(errMsg))
			errPkt[0] = 'E'
			binary.BigEndian.PutUint32(errPkt[1:5], uint32(len(errPkt)-1))
			copy(errPkt[5:], errMsg)
			_, _ = serverConn.Write(errPkt)
		}()

		_, err := stream.NextMessage(ctx)
		if err == nil || !strings.Contains(err.Error(), "replication terminated") {
			t.Errorf("NextMessage on 'E' = %v, want replication terminated error", err)
		}
	})
}

func TestCDCDecoders_BinaryArrayAndJSONB(t *testing.T) {
	// JSONB
	t.Run("BinaryJSONB", func(t *testing.T) {
		// Empty
		res, err := DecodeBinaryJSONB(nil)
		if err != nil || res != "" {
			t.Errorf("DecodeBinaryJSONB(nil) = (%q, %v)", res, err)
		}

		// Valid version 1
		validJSONB := []byte{1, '{', '"', 'a', '"', ':', '1', '}'}
		res, err = DecodeBinaryJSONB(validJSONB)
		if err != nil || res != `{"a":1}` {
			t.Errorf("DecodeBinaryJSONB(valid) = (%q, %v)", res, err)
		}

		// Invalid version
		invalidJSONB := []byte{2, '{', '}'}
		_, err = DecodeBinaryJSONB(invalidJSONB)
		if err == nil {
			t.Errorf("DecodeBinaryJSONB(version 2) expected error, got nil")
		}
	})

	// BinaryArray
	t.Run("BinaryArray", func(t *testing.T) {
		// Empty / 0-dim
		res, err := DecodeBinaryArray(nil)
		if err != nil || len(res) != 0 {
			t.Errorf("DecodeBinaryArray(nil) = (%v, %v)", res, err)
		}

		zeroDim := make([]byte, 4) // ndim = 0
		res, err = DecodeBinaryArray(zeroDim)
		if err != nil || len(res) != 0 {
			t.Errorf("DecodeBinaryArray(ndim=0) = (%v, %v)", res, err)
		}

		// Construct 1-dim INT4 array: [100, NULL, 200]
		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.BigEndian, int32(1))   // ndim = 1
		_ = binary.Write(buf, binary.BigEndian, int32(0))   // flags = 0
		_ = binary.Write(buf, binary.BigEndian, int32(23))  // elemOID = 23 (INT4)
		_ = binary.Write(buf, binary.BigEndian, int32(3))   // dimLen = 3
		_ = binary.Write(buf, binary.BigEndian, int32(1))   // dimLbound = 1
		// Item 0: 100
		_ = binary.Write(buf, binary.BigEndian, int32(4))
		_ = binary.Write(buf, binary.BigEndian, int32(100))
		// Item 1: NULL (len = -1)
		_ = binary.Write(buf, binary.BigEndian, int32(-1))
		// Item 2: 200
		_ = binary.Write(buf, binary.BigEndian, int32(4))
		_ = binary.Write(buf, binary.BigEndian, int32(200))

		arr, err := DecodeBinaryArray(buf.Bytes())
		if err != nil {
			t.Fatalf("DecodeBinaryArray(int4) err = %v", err)
		}
		if len(arr) != 3 || arr[0] != 100 || arr[1] != nil || arr[2] != 200 {
			t.Errorf("DecodeBinaryArray(int4) = %v", arr)
		}

		// Construct 1-dim TEXT array: ["hello", "world"]
		bufText := new(bytes.Buffer)
		_ = binary.Write(bufText, binary.BigEndian, int32(1))   // ndim = 1
		_ = binary.Write(bufText, binary.BigEndian, int32(0))   // flags = 0
		_ = binary.Write(bufText, binary.BigEndian, int32(25))  // elemOID = 25 (TEXT)
		_ = binary.Write(bufText, binary.BigEndian, int32(2))   // dimLen = 2
		_ = binary.Write(bufText, binary.BigEndian, int32(1))   // dimLbound = 1
		// Item 0: "hello"
		_ = binary.Write(bufText, binary.BigEndian, int32(5))
		bufText.WriteString("hello")
		// Item 1: "world"
		_ = binary.Write(bufText, binary.BigEndian, int32(5))
		bufText.WriteString("world")

		textArr, err := DecodeBinaryArray(bufText.Bytes())
		if err != nil {
			t.Fatalf("DecodeBinaryArray(text) err = %v", err)
		}
		if len(textArr) != 2 || textArr[0] != "hello" || textArr[1] != "world" {
			t.Errorf("DecodeBinaryArray(text) = %v", textArr)
		}
	})
}

func TestCDCDecoders_RangeVariations(t *testing.T) {
	// Empty
	rEmpty, err := DecodeRange("empty")
	if err != nil || !rEmpty.IsEmpty {
		t.Errorf("DecodeRange('empty') = (%v, %v)", rEmpty, err)
	}

	// [10, 50)
	r1, err := DecodeRange("[10, 50)")
	if err != nil || !r1.LowerInclusive || r1.UpperInclusive || r1.Lower != 10 || r1.Upper != 50 {
		t.Errorf("DecodeRange('[10, 50)') = (%v, %v)", r1, err)
	}

	// (100, 200]
	r2, err := DecodeRange("(100, 200]")
	if err != nil || r2.LowerInclusive || !r2.UpperInclusive || r2.Lower != 100 || r2.Upper != 200 {
		t.Errorf("DecodeRange('(100, 200]') = (%v, %v)", r2, err)
	}

	// Unbounded lower: [, 50]
	r3, err := DecodeRange("[, 50]")
	if err != nil || !r3.IsLowerUnbounded || r3.Upper != 50 {
		t.Errorf("DecodeRange('[, 50]') = (%v, %v)", r3, err)
	}

	// Unbounded upper: [25, )
	r4, err := DecodeRange("[25, )")
	if err != nil || !r4.IsUpperUnbounded || r4.Lower != 25 {
		t.Errorf("DecodeRange('[25, )') = (%v, %v)", r4, err)
	}

	// Error cases
	if _, err := DecodeRange("ab"); err == nil {
		t.Errorf("expected error for too short range")
	}
}

func TestLSNRangeTracker_MonotonicAndSplit(t *testing.T) {
	tracker := NewLSNRangeTracker(StartingFrom(100))

	// Out of bounds
	if tracker.TryClaim(99) {
		t.Errorf("TryClaim(99) on [100, inf) should be false")
	}

	// Monotonic claims
	if !tracker.TryClaim(100) {
		t.Errorf("TryClaim(100) should succeed")
	}
	if tracker.TryClaim(100) {
		t.Errorf("TryClaim(100) again should fail (non-monotonic)")
	}
	if tracker.TryClaim(95) {
		t.Errorf("TryClaim(95) should fail (regressive)")
	}
	if !tracker.TryClaim(150) {
		t.Errorf("TryClaim(150) should succeed")
	}

	// Split
	primary, residual, ok := tracker.TrySplit(0.5)
	if !ok {
		t.Fatalf("TrySplit(0.5) failed")
	}
	if primary.FromLSN != 100 || primary.ToLSN != 151 {
		t.Errorf("primary = %v, want [100, 151)", primary)
	}
	if residual.FromLSN != 151 || residual.ToLSN != UnboundedStopLSN {
		t.Errorf("residual = %v, want [151, inf)", residual)
	}

	// TrySplit on unclaimed tracker
	unclaimed := NewLSNRangeTracker(StartingFrom(200))
	if _, _, ok := unclaimed.TrySplit(0.5); ok {
		t.Errorf("TrySplit on unclaimed tracker should return false")
	}
}

func TestWrite_EstimateElementSize(t *testing.T) {
	// Nil
	if sz := estimateElementSize(nil); sz != 64 {
		t.Errorf("estimateElementSize(nil) = %d, want 64", sz)
	}

	// Nil pointer
	var ptr *string
	if sz := estimateElementSize(ptr); sz != 64 {
		t.Errorf("estimateElementSize(nil ptr) = %d, want 64", sz)
	}

	// Primitive
	if sz := estimateElementSize(12345); sz != 64 {
		t.Errorf("estimateElementSize(int) = %d, want 64", sz)
	}

	// Struct with string and slice
	type sampleStruct struct {
		Name string
		Data []byte
		Num  int
	}
	sample := sampleStruct{
		Name: strings.Repeat("A", 100),
		Data: make([]byte, 200),
		Num:  42,
	}
	sz := estimateElementSize(sample)
	if sz < 300 {
		t.Errorf("estimateElementSize(sample) = %d, want >= 300", sz)
	}

	// Pointer to struct
	szPtr := estimateElementSize(&sample)
	if szPtr != sz {
		t.Errorf("estimateElementSize(&sample) = %d, want %d", szPtr, sz)
	}
}

func TestWriteFn_Teardown(t *testing.T) {
	fn := &writeFn{}
	// Teardown with nil db should be a safe no-op
	fn.Teardown()
}

func TestCDCStream_BuildStartReplicationQuery_VersionNegotiation(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
	)

	// Case 1: Server Version >= 19 -> proto_version '4', binary 'true', streaming 'parallel'
	streamPG19 := &NativeReplicationStream{
		opts:               opts,
		serverMajorVersion: 19,
		serverVersion:      "19.0",
	}
	queryPG19 := streamPG19.BuildStartReplicationQuery("0/0")
	if !strings.Contains(queryPG19, "proto_version '4'") {
		t.Errorf("PG19 query expected proto_version '4', got: %s", queryPG19)
	}
	if !strings.Contains(queryPG19, "binary 'true'") {
		t.Errorf("PG19 query expected binary 'true', got: %s", queryPG19)
	}
	if !strings.Contains(queryPG19, "streaming 'parallel'") {
		t.Errorf("PG19 query expected streaming 'parallel', got: %s", queryPG19)
	}

	// Case 2: Server Version 18 (Older) -> reverts cleanly to proto_version '1', no binary, no streaming
	streamPG18 := &NativeReplicationStream{
		opts:               opts,
		serverMajorVersion: 18,
		serverVersion:      "18.6",
	}
	queryPG18 := streamPG18.BuildStartReplicationQuery("0/0")
	if !strings.Contains(queryPG18, "proto_version '1'") {
		t.Errorf("PG18 query expected proto_version '1', got: %s", queryPG18)
	}
	if strings.Contains(queryPG18, "binary 'true'") {
		t.Errorf("PG18 query should not contain binary 'true', got: %s", queryPG18)
	}
	if strings.Contains(queryPG18, "streaming 'parallel'") {
		t.Errorf("PG18 query should not contain streaming 'parallel', got: %s", queryPG18)
	}

	// Case 3: Server Version 0 (unknown/unparsed) -> reverts to older proto_version '1'
	streamUnknown := &NativeReplicationStream{
		opts:               opts,
		serverMajorVersion: 0,
	}
	queryUnknown := streamUnknown.BuildStartReplicationQuery("0/0")
	if !strings.Contains(queryUnknown, "proto_version '1'") || strings.Contains(queryUnknown, "binary 'true'") {
		t.Errorf("Unknown version query should revert to proto_version '1', got: %s", queryUnknown)
	}

	// Case 4: PG19 with originFilter 'none'
	optsOrigin := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
		WithCDCOriginFilter("none"),
	)
	streamPG19Origin := &NativeReplicationStream{
		opts:               optsOrigin,
		serverMajorVersion: 19,
	}
	queryPG19Origin := streamPG19Origin.BuildStartReplicationQuery("0/0")
	if !strings.Contains(queryPG19Origin, "origin 'none'") {
		t.Errorf("PG19 with originFilter 'none' expected origin 'none', got: %s", queryPG19Origin)
	}

	// Case 5: Explicit overrides on PG19
	optsOverride := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
		WithCDCProtoVersion(2),
		WithCDCBinaryMode(false),
		WithCDCStreamingMode("off"),
	)
	streamOverride := &NativeReplicationStream{
		opts:               optsOverride,
		serverMajorVersion: 19,
	}
	queryOverride := streamOverride.BuildStartReplicationQuery("0/0")
	if !strings.Contains(queryOverride, "proto_version '2'") {
		t.Errorf("expected overridden proto_version '2', got: %s", queryOverride)
	}
	if strings.Contains(queryOverride, "binary 'true'") {
		t.Errorf("expected binary 'true' to be disabled by override, got: %s", queryOverride)
	}
	if strings.Contains(queryOverride, "streaming") {
		t.Errorf("expected streaming to be disabled by override, got: %s", queryOverride)
	}
}

func TestCDCStream_ParameterStatus_WireNegotiation(t *testing.T) {
	t.Run("PG19_NegotiatesProto4BinaryAndParallelStreaming", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			// 1. StartupMessage
			var lenBuf [4]byte
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			length := binary.BigEndian.Uint32(lenBuf[:])
			body := make([]byte, length-4)
			if _, err := io.ReadFull(serverConn, body); err != nil {
				return
			}

			// 2. AuthOk ('R')
			authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
			if _, err := serverConn.Write(authOk); err != nil {
				return
			}

			// 3. ParameterStatus ('S') for server_version = "19.1"
			paramPayload := []byte("server_version\x0019.1\x00")
			paramPkt := make([]byte, 5+len(paramPayload))
			paramPkt[0] = 'S'
			binary.BigEndian.PutUint32(paramPkt[1:5], uint32(len(paramPkt)-1))
			copy(paramPkt[5:], paramPayload)
			if _, err := serverConn.Write(paramPkt); err != nil {
				return
			}

			// 4. ReadyForQuery ('Z')
			ready := []byte{'Z', 0, 0, 0, 5, 'I'}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// 5. Read search_path query ('Q')
			var qType [1]byte
			if _, err := io.ReadFull(serverConn, qType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen := binary.BigEndian.Uint32(lenBuf[:])
			qBody := make([]byte, qLen-4)
			if _, err := io.ReadFull(serverConn, qBody); err != nil {
				return
			}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// 6. Read START_REPLICATION query ('Q')
			if _, err := io.ReadFull(serverConn, qType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen2 := binary.BigEndian.Uint32(lenBuf[:])
			qBody2 := make([]byte, qLen2-4)
			if _, err := io.ReadFull(serverConn, qBody2); err != nil {
				return
			}
			repQueryStr := string(qBody2)
			if !strings.Contains(repQueryStr, "proto_version '4'") {
				t.Errorf("wire query expected proto_version '4', got: %s", repQueryStr)
			}
			if !strings.Contains(repQueryStr, "binary 'true'") {
				t.Errorf("wire query expected binary 'true', got: %s", repQueryStr)
			}
			if !strings.Contains(repQueryStr, "streaming 'parallel'") {
				t.Errorf("wire query expected streaming 'parallel', got: %s", repQueryStr)
			}

			// 7. Respond with CopyBothResponse ('W')
			copyBoth := []byte{'W', 0, 0, 0, 7, 0, 0, 0}
			_, _ = serverConn.Write(copyBoth)
		}()

		opts := NewCDCOptions(
			WithCDCHost("localhost"),
			WithCDCUsername("beam_test"),
			WithCDCSlotName("test_slot"),
			WithCDCPublication("test_pub"),
			WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
				return clientConn, nil
			}),
		)

		stream, err := NewNativeReplicationStream(ctx, opts)
		if err != nil {
			t.Fatalf("NewNativeReplicationStream() err = %v", err)
		}
		defer stream.Close()

		if stream.ServerMajorVersion() != 19 {
			t.Errorf("stream.ServerMajorVersion() = %d, want 19", stream.ServerMajorVersion())
		}
		if stream.ServerVersion() != "19.1" {
			t.Errorf("stream.ServerVersion() = %q, want '19.1'", stream.ServerVersion())
		}

		<-serverDone
	})

	t.Run("PG18_RevertsToOlderProto1AndOmitBinaryParallelStreaming", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			var lenBuf [4]byte
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			length := binary.BigEndian.Uint32(lenBuf[:])
			body := make([]byte, length-4)
			if _, err := io.ReadFull(serverConn, body); err != nil {
				return
			}

			// AuthOk
			authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
			if _, err := serverConn.Write(authOk); err != nil {
				return
			}

			// ParameterStatus ('S') for server_version = "18.6 (Debian 18.6-1)"
			paramPayload := []byte("server_version\x0018.6 (Debian 18.6-1)\x00")
			paramPkt := make([]byte, 5+len(paramPayload))
			paramPkt[0] = 'S'
			binary.BigEndian.PutUint32(paramPkt[1:5], uint32(len(paramPkt)-1))
			copy(paramPkt[5:], paramPayload)
			if _, err := serverConn.Write(paramPkt); err != nil {
				return
			}

			// ReadyForQuery
			ready := []byte{'Z', 0, 0, 0, 5, 'I'}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// search_path
			var qType [1]byte
			if _, err := io.ReadFull(serverConn, qType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen := binary.BigEndian.Uint32(lenBuf[:])
			qBody := make([]byte, qLen-4)
			if _, err := io.ReadFull(serverConn, qBody); err != nil {
				return
			}
			if _, err := serverConn.Write(ready); err != nil {
				return
			}

			// START_REPLICATION query
			if _, err := io.ReadFull(serverConn, qType[:]); err != nil {
				return
			}
			if _, err := io.ReadFull(serverConn, lenBuf[:]); err != nil {
				return
			}
			qLen2 := binary.BigEndian.Uint32(lenBuf[:])
			qBody2 := make([]byte, qLen2-4)
			if _, err := io.ReadFull(serverConn, qBody2); err != nil {
				return
			}
			repQueryStr := string(qBody2)
			if !strings.Contains(repQueryStr, "proto_version '1'") {
				t.Errorf("PG18 wire query expected proto_version '1', got: %s", repQueryStr)
			}
			if strings.Contains(repQueryStr, "binary 'true'") {
				t.Errorf("PG18 wire query should NOT contain binary 'true', got: %s", repQueryStr)
			}
			if strings.Contains(repQueryStr, "streaming 'parallel'") {
				t.Errorf("PG18 wire query should NOT contain streaming 'parallel', got: %s", repQueryStr)
			}

			// Respond with CopyBothResponse ('W')
			copyBoth := []byte{'W', 0, 0, 0, 7, 0, 0, 0}
			_, _ = serverConn.Write(copyBoth)
		}()

		opts := NewCDCOptions(
			WithCDCHost("localhost"),
			WithCDCUsername("beam_test"),
			WithCDCSlotName("test_slot"),
			WithCDCPublication("test_pub"),
			WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
				return clientConn, nil
			}),
		)

		stream, err := NewNativeReplicationStream(ctx, opts)
		if err != nil {
			t.Fatalf("NewNativeReplicationStream() err = %v", err)
		}
		defer stream.Close()

		if stream.ServerMajorVersion() != 18 {
			t.Errorf("stream.ServerMajorVersion() = %d, want 18", stream.ServerMajorVersion())
		}
		if stream.ServerVersion() != "18.6 (Debian 18.6-1)" {
			t.Errorf("stream.ServerVersion() = %q", stream.ServerVersion())
		}

		<-serverDone
	})
}

func TestPgOutputParser_ParseBinaryValue_AllTypes(t *testing.T) {
	// bool (16)
	if v := parseBinaryValue(16, []byte{1}); v != true {
		t.Errorf("parseBinaryValue(16, [1]) = %v, want true", v)
	}
	if v := parseBinaryValue(16, []byte{0}); v != false {
		t.Errorf("parseBinaryValue(16, [0]) = %v, want false", v)
	}

	// int2 (21)
	var int2Buf [2]byte
	binary.BigEndian.PutUint16(int2Buf[:], 42)
	if v := parseBinaryValue(21, int2Buf[:]); v != int16(42) {
		t.Errorf("parseBinaryValue(21) = %v, want 42", v)
	}

	// int4 (23)
	var int4Buf [4]byte
	binary.BigEndian.PutUint32(int4Buf[:], 123456)
	if v := parseBinaryValue(23, int4Buf[:]); v != int32(123456) {
		t.Errorf("parseBinaryValue(23) = %v, want 123456", v)
	}

	// int8 (20)
	var int8Buf [8]byte
	binary.BigEndian.PutUint64(int8Buf[:], 9876543210)
	if v := parseBinaryValue(20, int8Buf[:]); v != int64(9876543210) {
		t.Errorf("parseBinaryValue(20) = %v, want 9876543210", v)
	}

	// float8 (701)
	var f8Buf [8]byte
	binary.BigEndian.PutUint64(f8Buf[:], 0x400921fb54442d18) // ~3.141592653589793
	if v := parseBinaryValue(701, f8Buf[:]); v != 3.141592653589793 {
		t.Errorf("parseBinaryValue(701) = %v", v)
	}

	// text (25)
	if v := parseBinaryValue(25, []byte("postgres_binary_test")); v != "postgres_binary_test" {
		t.Errorf("parseBinaryValue(25) = %v", v)
	}

	// point (600)
	var ptBuf [16]byte
	binary.BigEndian.PutUint64(ptBuf[0:8], 0x3ff0000000000000) // 1.0
	binary.BigEndian.PutUint64(ptBuf[8:16], 0x4000000000000000) // 2.0
	if pt, ok := parseBinaryValue(600, ptBuf[:]).(PgPoint); !ok || pt.X != 1.0 || pt.Y != 2.0 {
		t.Errorf("parseBinaryValue(600) = %v, want PgPoint{1.0, 2.0}", pt)
	}

	// bytea (17)
	bData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if v := parseBinaryValue(17, bData); !bytes.Equal(v.([]byte), bData) {
		t.Errorf("parseBinaryValue(17) = %v", v)
	}

	// JSONB (3802)
	jsonbData := append([]byte{1}, []byte(`{"key":"val"}`)...)
	if v := parseBinaryValue(3802, jsonbData); v != `{"key":"val"}` {
		t.Errorf("parseBinaryValue(3802) = %v", v)
	}
}

func TestPgOutputParser_MultiTableTruncate(t *testing.T) {
	parser := NewPgOutputParser()
	parser.RegisterRelation(&RelationDef{
		RelationID:   101,
		Namespace:    "public",
		RelationName: "table_a",
	})
	parser.RegisterRelation(&RelationDef{
		RelationID:   102,
		Namespace:    "public",
		RelationName: "table_b",
	})
	parser.RegisterRelation(&RelationDef{
		RelationID:   103,
		Namespace:    "analytics",
		RelationName: "table_c",
	})

	// Construct 'T' message with 3 relations
	var buf bytes.Buffer
	buf.WriteByte('T')
	_ = binary.Write(&buf, binary.BigEndian, uint32(3)) // 3 relations
	buf.WriteByte(0)                                    // options
	_ = binary.Write(&buf, binary.BigEndian, uint32(101))
	_ = binary.Write(&buf, binary.BigEndian, uint32(102))
	_ = binary.Write(&buf, binary.BigEndian, uint32(103))

	events, err := parser.ParseMessages(buf.Bytes())
	if err != nil {
		t.Fatalf("ParseMessages('T') err = %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 truncate events, got %d", len(events))
	}
	if events[0].Table != "table_a" || events[1].Table != "table_b" || events[2].Table != "table_c" {
		t.Errorf("unexpected truncate events tables: %+v", events)
	}
	for i, e := range events {
		if e.Operation != OpTruncate {
			t.Errorf("event %d operation = %v, want OpTruncate", i, e.Operation)
		}
	}
}

func TestWrite_EmptyPKUpsertQuery(t *testing.T) {
	fn := &writeFn{
		Table:          "users",
		columns:        []string{"id", "email"},
		PrimaryKeyCols: []string{}, // Empty primary keys
		Options: WriteOptions{
			WriteMode: WriteModeUpsert,
		},
		Type: beam.EncodedType{T: reflect.TypeOf(struct {
			ID    int64  `db:"id"`
			Email string `db:"email"`
		}{})},
	}

	batch := []any{
		struct {
			ID    int64  `db:"id"`
			Email string `db:"email"`
		}{ID: 1, Email: "test@example.com"},
	}

	query, _, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("buildUnnestQuery err = %v", err)
	}
	if strings.Contains(query, "ON CONFLICT ()") {
		t.Errorf("query contains invalid syntax ON CONFLICT (): %s", query)
	}
	if !strings.Contains(query, "ON CONFLICT DO NOTHING") {
		t.Errorf("query should contain ON CONFLICT DO NOTHING: %s", query)
	}
}

func TestWrite_PQDialerAdapter(t *testing.T) {
	dialed := false
	dialer := &pqDialerAdapter{
		dialFunc: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialed = true
			c1, _ := net.Pipe()
			return c1, nil
		},
	}

	c, err := dialer.Dial("tcp", "127.0.0.1:5432")
	if err != nil || c == nil || !dialed {
		t.Errorf("Dial failed: %v", err)
	}
	if c != nil {
		_ = c.Close()
	}

	cTimeout, err := dialer.DialTimeout("tcp", "127.0.0.1:5432", 1*time.Second)
	if err != nil || cTimeout == nil {
		t.Errorf("DialTimeout failed: %v", err)
	}
	if cTimeout != nil {
		_ = cTimeout.Close()
	}
}


