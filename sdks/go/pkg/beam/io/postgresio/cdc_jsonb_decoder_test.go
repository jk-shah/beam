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
	"encoding/binary"
	"testing"
)

func TestTextJsonDecoding(t *testing.T) {
	parser := NewPgOutputParser()
	cols := []ColumnDef{
		{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1},
		{Flags: 0, Name: "payload", TypeOID: 114, TypeModifier: -1}, // 114 = JSON
	}
	relPayload := buildMockRelationPayload(17001, "public", "orders", 'd', cols)
	_, _ = parser.ParseMessage(relPayload)

	jsonStr := `{"customer":"Alice","amount":99.99}`
	insertPayload := buildMockInsertPayload(17001, []string{"1", jsonStr})

	event, err := parser.ParseMessage(insertPayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Insert: %v", err)
	}
	if event == nil || event.After["payload"] != jsonStr {
		t.Errorf("expected payload %s, got %v", jsonStr, event.After["payload"])
	}
}

func TestBinaryJsonbDecoding(t *testing.T) {
	parser := NewPgOutputParser()
	cols := []ColumnDef{
		{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1},
		{Flags: 0, Name: "attributes", TypeOID: 3802, TypeModifier: -1}, // 3802 = JSONB
	}
	relPayload := buildMockRelationPayload(17002, "public", "profiles", 'd', cols)
	_, _ = parser.ParseMessage(relPayload)

	jsonContent := `{"tier":"gold","active":true}`
	rawBytes := []byte(jsonContent)
	jsonbBinary := append([]byte{1}, rawBytes...) // version header 1 + json content

	// Build raw 'I' message with binary format column for col 2
	var buf bytes.Buffer
	buf.WriteByte('I')
	_ = binary.Write(&buf, binary.BigEndian, uint32(17002))
	buf.WriteByte('N')
	_ = binary.Write(&buf, binary.BigEndian, int16(2))

	// Col 1: text "42"
	buf.WriteByte('t')
	_ = binary.Write(&buf, binary.BigEndian, int32(2))
	buf.WriteString("42")

	// Col 2: binary jsonb
	buf.WriteByte('b')
	_ = binary.Write(&buf, binary.BigEndian, int32(len(jsonbBinary)))
	buf.Write(jsonbBinary)

	event, err := parser.ParseMessage(buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error parsing Insert with binary jsonb: %v", err)
	}
	if event == nil {
		t.Fatalf("expected non-nil event")
	}
	if event.After["id"] != int32(42) {
		t.Errorf("expected id 42, got %v", event.After["id"])
	}
	if event.After["attributes"] != jsonContent {
		t.Errorf("expected stripped JSONB string %s, got %v", jsonContent, event.After["attributes"])
	}
}
