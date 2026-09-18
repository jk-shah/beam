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

import "testing"

// These tests exercise parseBinaryValue, the pgoutput replication-stream entry
// point that dispatches a column's wire bytes to the binary decoders. The
// decoders themselves are covered by binary_types_test.go; the cases here
// assert that the CDC parser actually routes through them rather than handing
// raw bytes to the pipeline. The payload helpers (encodeNumeric,
// encodeInterval) are shared with binary_types_test.go.

// TestParseBinaryValueDecodesNumericInsteadOfReturningBytes is the
// integration-level assertion for P1-3: the value reaching the pipeline must
// no longer be a raw byte slice.
func TestParseBinaryValueDecodesNumericInsteadOfReturningBytes(t *testing.T) {
	payload := encodeNumeric(0, numericPositive, 2, []uint16{19, 9900})

	got := parseBinaryValue(1700, payload)

	if raw, isBytes := got.([]byte); isBytes {
		t.Fatalf("NUMERIC still decodes to raw bytes % x; monetary data is unreadable downstream", raw)
	}
	if got != "19.99" {
		t.Errorf("parseBinaryValue(NUMERIC) = %v (%T), want \"19.99\"", got, got)
	}
}

func TestParseBinaryValueDecodesUUID(t *testing.T) {
	raw := []byte{
		0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8,
		0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11,
	}

	got := parseBinaryValue(2950, raw)
	if _, isBytes := got.([]byte); isBytes {
		t.Fatal("UUID still decodes to raw bytes")
	}
	if got != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" {
		t.Errorf("parseBinaryValue(UUID) = %v, want the canonical text form", got)
	}
}

func TestParseBinaryValueDecodesInterval(t *testing.T) {
	got := parseBinaryValue(1186, encodeInterval(3_600_000_000, 0, 0))
	if _, isBytes := got.([]byte); isBytes {
		t.Fatal("INTERVAL still decodes to raw bytes")
	}
	iv, ok := got.(PgInterval)
	if !ok {
		t.Fatalf("parseBinaryValue(INTERVAL) returned %T, want PgInterval", got)
	}
	if iv.Microthings != 3_600_000_000 {
		t.Errorf("interval microseconds = %d, want 3600000000", iv.Microthings)
	}
}
