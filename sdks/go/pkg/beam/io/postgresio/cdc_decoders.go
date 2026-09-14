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
	"fmt"
	"math"
	"strconv"
	"strings"
)

// PgRange represents a PostgreSQL range type (e.g. int4range, daterange, numrange, tsrange).
type PgRange struct {
	Lower            any  `beam:"lower" json:"lower"`
	Upper            any  `beam:"upper" json:"upper"`
	LowerInclusive   bool `beam:"lower_inclusive" json:"lower_inclusive"`
	UpperInclusive   bool `beam:"upper_inclusive" json:"upper_inclusive"`
	IsEmpty          bool `beam:"is_empty" json:"is_empty"`
	IsLowerUnbounded bool `beam:"is_lower_unbounded" json:"is_lower_unbounded"`
	IsUpperUnbounded bool `beam:"is_upper_unbounded" json:"is_upper_unbounded"`
}

// PgPoint represents a PostgreSQL 2D geometric point (x, y).
type PgPoint struct {
	X float64 `beam:"x" json:"x"`
	Y float64 `beam:"y" json:"y"`
}

// DecodeTextArray parses standard PostgreSQL array string literals (e.g. "{1,2,3}", "{\"a\",\"b\"}").
func DecodeTextArray(payload string) ([]any, error) {
	s := strings.TrimSpace(payload)
	if s == "{}" || s == "" {
		return []any{}, nil
	}
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("invalid array literal: %s", s)
	}

	inner := s[1 : len(s)-1]
	var items []any
	var cur strings.Builder
	inQuotes := false
	escaped := false

	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if escaped {
			cur.WriteByte(c)
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}
		if c == '"' {
			inQuotes = !inQuotes
			continue
		}
		if c == ',' && !inQuotes {
			val := cur.String()
			items = append(items, parseArrayItem(val))
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}

	val := cur.String()
	items = append(items, parseArrayItem(val))
	return items, nil
}

func parseArrayItem(s string) any {
	if s == "NULL" {
		return nil
	}
	// Attempt integer parse
	if intVal, err := strconv.ParseInt(s, 10, 64); err == nil {
		if intVal >= math.MinInt32 && intVal <= math.MaxInt32 {
			return int(intVal)
		}
		return intVal
	}
	// Attempt float parse
	if floatVal, err := strconv.ParseFloat(s, 64); err == nil && strings.Contains(s, ".") {
		return floatVal
	}
	// Attempt boolean parse
	if s == "t" || s == "true" {
		return true
	}
	if s == "f" || s == "false" {
		return false
	}
	return s
}

// DecodeBinaryArray parses PostgreSQL binary format arrays.
func DecodeBinaryArray(data []byte) ([]any, error) {
	if len(data) == 0 {
		return []any{}, nil
	}
	r := bytes.NewReader(data)
	var ndim, flags, elemOID, dimLen, dimLbound int32
	if err := binary.Read(r, binary.BigEndian, &ndim); err != nil {
		return nil, err
	}
	if ndim == 0 {
		return []any{}, nil
	}
	if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &elemOID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &dimLen); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &dimLbound); err != nil {
		return nil, err
	}

	result := make([]any, dimLen)
	for i := 0; i < int(dimLen); i++ {
		var itemLen int32
		if err := binary.Read(r, binary.BigEndian, &itemLen); err != nil {
			return nil, err
		}
		if itemLen == -1 {
			result[i] = nil
			continue
		}
		itemBytes := make([]byte, itemLen)
		if _, err := r.Read(itemBytes); err != nil {
			return nil, err
		}

		switch elemOID {
		case 23: // INT4
			result[i] = int(int32(binary.BigEndian.Uint32(itemBytes)))
		case 20: // INT8
			result[i] = int64(binary.BigEndian.Uint64(itemBytes))
		case 701: // FLOAT8
			bits := binary.BigEndian.Uint64(itemBytes)
			result[i] = math.Float64frombits(bits)
		case 700: // FLOAT4
			bits := binary.BigEndian.Uint32(itemBytes)
			result[i] = float64(math.Float32frombits(bits))
		case 25: // TEXT
			result[i] = string(itemBytes)
		default:
			result[i] = itemBytes
		}
	}
	return result, nil
}

// DecodeBinaryJSONB decodes PostgreSQL binary JSONB datum by validating and stripping
// the 1-byte version header (0x01).
func DecodeBinaryJSONB(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	if data[0] != 1 {
		return "", fmt.Errorf("unsupported JSONB binary version %d (expected 1)", data[0])
	}
	return string(data[1:]), nil
}

// DecodeRange parses standard PostgreSQL range syntax (e.g. "[10,50)", "(100,200]", "empty", "[,50]").
func DecodeRange(s string) (PgRange, error) {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "empty") {
		return PgRange{IsEmpty: true}, nil
	}
	if len(s) < 3 {
		return PgRange{}, fmt.Errorf("invalid range format: %s", s)
	}

	lowerInc := s[0] == '['
	upperInc := s[len(s)-1] == ']'
	inner := s[1 : len(s)-1]

	parts := strings.Split(inner, ",")
	if len(parts) != 2 {
		return PgRange{}, fmt.Errorf("range requires exactly two bounds: %s", s)
	}

	lowerStr := strings.TrimSpace(parts[0])
	upperStr := strings.TrimSpace(parts[1])

	var lowerVal, upperVal any
	lowerUnbounded := lowerStr == ""
	upperUnbounded := upperStr == ""

	if !lowerUnbounded {
		lowerVal = parseArrayItem(lowerStr)
	}
	if !upperUnbounded {
		upperVal = parseArrayItem(upperStr)
	}

	return PgRange{
		Lower:            lowerVal,
		Upper:            upperVal,
		LowerInclusive:   lowerInc,
		UpperInclusive:   upperInc,
		IsLowerUnbounded: lowerUnbounded,
		IsUpperUnbounded: upperUnbounded,
	}, nil
}

// DecodePoint parses standard PostgreSQL Point syntax "(x, y)".
func DecodePoint(s string) (PgPoint, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return PgPoint{}, fmt.Errorf("invalid point format: %s", s)
	}
	x, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return PgPoint{}, fmt.Errorf("invalid x coordinate: %w", err)
	}
	y, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return PgPoint{}, fmt.Errorf("invalid y coordinate: %w", err)
	}
	return PgPoint{X: x, Y: y}, nil
}
