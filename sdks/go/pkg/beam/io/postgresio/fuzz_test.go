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
	"strings"
	"testing"
)

func FuzzParseXLogData(f *testing.F) {
	// Seed valid and edge-case byte sequences
	f.Add([]byte{0x77, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00})
	f.Add([]byte{'B', 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00})
	f.Add([]byte{'C', 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00})
	f.Add([]byte{'I', 0x00, 0x00, 0x00, 0x01, 'N'})
	f.Add([]byte{'U', 0x00, 0x00, 0x00, 0x01, 'N'})
	f.Add([]byte{'D', 0x00, 0x00, 0x00, 0x01, 'K'})
	f.Add([]byte{'T', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, payload []byte) {
		parser := NewPgOutputParser()
		// Must safely parse or return error, never panic
		_, _ = parser.ParseMessages(payload)
	})
}

func FuzzSanitizeIdentifier(f *testing.F) {
	f.Add("orders")
	f.Add("public.orders")
	f.Add("table\"with\"quotes")
	f.Add("table; DROP DATABASE postgres; --")
	f.Add("schema.\"table; DROP TABLE users;\"")
	f.Add("")
	f.Add("   ")
	f.Add("user_accounts_123")

	f.Fuzz(func(t *testing.T, ident string) {
		sanitized, err := SanitizeTableIdentifier(ident)
		if err == nil && sanitized != "" {
			if !strings.HasPrefix(sanitized, "\"") && !strings.Contains(sanitized, "\".\"") {
				t.Errorf("SanitizeTableIdentifier(%q) did not produce quoted identifier: %q", ident, sanitized)
			}
		}

		sanitizedCol, err := SanitizeIdentifier(ident)
		if err == nil && sanitizedCol != "" {
			if !strings.HasPrefix(sanitizedCol, "\"") || !strings.HasSuffix(sanitizedCol, "\"") {
				t.Errorf("SanitizeIdentifier(%q) did not produce quoted identifier: %q", ident, sanitizedCol)
			}
		}

		_ = SanitizeRowFilter(ident)
	})
}

func FuzzDecodeBinaryVector(f *testing.F) {
	f.Add([]byte{0x00, 0x03, 0x00, 0x00, 0x3f, 0x80, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x40, 0x40, 0x00, 0x00})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0x00})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeBinaryVector(data)
	})
}
