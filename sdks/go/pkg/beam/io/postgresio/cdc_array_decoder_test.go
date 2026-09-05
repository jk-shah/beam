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
	"math"
	"testing"
)

func TestTextIntegerArrayDecoding(t *testing.T) {
	textPayload := "{10,20,30,NULL,50}"
	result, err := DecodeTextArray(textPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 items, got %d", len(result))
	}
	if result[0] != 10 || result[1] != 20 || result[2] != 30 || result[3] != nil || result[4] != 50 {
		t.Errorf("unexpected elements: %+v", result)
	}
}

func TestTextStringArrayWithQuotesAndCommas(t *testing.T) {
	textPayload := `{"apple","banana,with,comma","quote\"escaped",NULL,"orange"}`
	result, err := DecodeTextArray(textPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 items, got %d", len(result))
	}
	if result[0] != "apple" || result[1] != "banana,with,comma" || result[2] != "quote\"escaped" || result[3] != nil || result[4] != "orange" {
		t.Errorf("unexpected elements: %+v", result)
	}
}

func TestBinaryInt32ArrayDecoding(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(1))  // ndim
	_ = binary.Write(&buf, binary.BigEndian, int32(1))  // flags
	_ = binary.Write(&buf, binary.BigEndian, int32(23)) // elemOID (INT4)
	_ = binary.Write(&buf, binary.BigEndian, int32(3))  // dimLen
	_ = binary.Write(&buf, binary.BigEndian, int32(1))  // dimLbound

	// Element 1: 100
	_ = binary.Write(&buf, binary.BigEndian, int32(4))
	_ = binary.Write(&buf, binary.BigEndian, int32(100))

	// Element 2: NULL (-1)
	_ = binary.Write(&buf, binary.BigEndian, int32(-1))

	// Element 3: 300
	_ = binary.Write(&buf, binary.BigEndian, int32(4))
	_ = binary.Write(&buf, binary.BigEndian, int32(300))

	result, err := DecodeBinaryArray(buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	if result[0] != 100 || result[1] != nil || result[2] != 300 {
		t.Errorf("unexpected binary array elements: %+v", result)
	}
}

func TestBinaryDoubleArrayDecoding(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(1))   // ndim
	_ = binary.Write(&buf, binary.BigEndian, int32(0))   // flags
	_ = binary.Write(&buf, binary.BigEndian, int32(701)) // elemOID (FLOAT8)
	_ = binary.Write(&buf, binary.BigEndian, int32(2))   // dimLen
	_ = binary.Write(&buf, binary.BigEndian, int32(1))   // dimLbound

	_ = binary.Write(&buf, binary.BigEndian, int32(8))
	_ = binary.Write(&buf, binary.BigEndian, math.Float64bits(3.14159))

	_ = binary.Write(&buf, binary.BigEndian, int32(8))
	_ = binary.Write(&buf, binary.BigEndian, math.Float64bits(2.71828))

	result, err := DecodeBinaryArray(buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result))
	}
	d1 := result[0].(float64)
	d2 := result[1].(float64)
	if math.Abs(d1-3.14159) > 0.0001 || math.Abs(d2-2.71828) > 0.0001 {
		t.Errorf("unexpected float values: %v, %v", d1, d2)
	}
}

func TestEmptyArray(t *testing.T) {
	res1, err := DecodeTextArray("{}")
	if err != nil || len(res1) != 0 {
		t.Errorf("expected empty array, got %v, err %v", res1, err)
	}

	res2, err := DecodeBinaryArray(nil)
	if err != nil || len(res2) != 0 {
		t.Errorf("expected empty binary array, got %v, err %v", res2, err)
	}
}
