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

// containsString reports whether s appears in list.
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// --- P1-2: unchanged TOAST columns ---

// TestValuesToMapOmitsUnchangedToastColumns is the core contract.
//
// FINDING P1-2. The parser previously wrote the literal string
// "<unchanged_toast>" into the value map. For a JSONB, NUMERIC[] or BIGINT
// column that is a type violation which either panics during schema coercion
// or is written to the sink as literal text.
func TestValuesToMapOmitsUnchangedToastColumns(t *testing.T) {
	values := []ColumnValue{
		{Name: "id", Value: int64(1)},
		{Name: "title", Value: "hello"},
		{Name: "big_json", IsToastUnchanged: true},
		{Name: "deleted_at", IsNull: true},
	}

	m, unchanged := valuesToMap(values)

	if _, present := m["big_json"]; present {
		t.Errorf("unchanged TOAST column must not appear in the value map, got %v", m["big_json"])
	}
	if !containsString(unchanged, "big_json") {
		t.Errorf("unchanged columns = %v, want it to contain big_json", unchanged)
	}

	// A genuinely NULL column is different from a withheld one and must still
	// be present with a nil value so the sink writes NULL.
	val, present := m["deleted_at"]
	if !present {
		t.Error("an explicitly NULL column must remain present in the map")
	}
	if val != nil {
		t.Errorf("NULL column = %v, want nil", val)
	}

	if m["id"] != int64(1) || m["title"] != "hello" {
		t.Errorf("ordinary columns were altered: %v", m)
	}
}

// TestValuesToMapReportsNoUnchangedColumnsWhenAllPresent keeps the common path
// allocation-free of spurious entries.
func TestValuesToMapReportsNoUnchangedColumnsWhenAllPresent(t *testing.T) {
	m, unchanged := valuesToMap([]ColumnValue{
		{Name: "a", Value: 1},
		{Name: "b", Value: 2},
	})

	if len(unchanged) != 0 {
		t.Errorf("unchanged = %v, want empty", unchanged)
	}
	if len(m) != 2 {
		t.Errorf("value map = %v, want 2 entries", m)
	}
}

// TestNoToastSentinelStringSurvivesInValues is a direct guard against the
// placeholder reappearing anywhere in a decoded row.
func TestNoToastSentinelStringSurvivesInValues(t *testing.T) {
	m, _ := valuesToMap([]ColumnValue{
		{Name: "payload", IsToastUnchanged: true},
	})

	for col, v := range m {
		if s, isStr := v.(string); isStr && s == "<unchanged_toast>" {
			t.Errorf("column %q still carries the placeholder sentinel", col)
		}
	}
}
