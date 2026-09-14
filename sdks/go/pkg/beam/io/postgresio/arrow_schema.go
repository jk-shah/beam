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
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
)

// PgOIDToArrowType maps PostgreSQL internal type OIDs to corresponding Arrow DataTypes.
func PgOIDToArrowType(typeOID uint32) arrow.DataType {
	switch typeOID {
	case 16: // bool
		return arrow.FixedWidthTypes.Boolean
	case 21: // int2 (smallint)
		return arrow.PrimitiveTypes.Int16
	case 23: // int4 (integer)
		return arrow.PrimitiveTypes.Int32
	case 20: // int8 (bigint)
		return arrow.PrimitiveTypes.Int64
	case 700: // float4 (real)
		return arrow.PrimitiveTypes.Float32
	case 701: // float8 (double precision)
		return arrow.PrimitiveTypes.Float64
	case 17: // bytea
		return arrow.BinaryTypes.Binary
	case 1082: // date
		return arrow.FixedWidthTypes.Date32
	case 1114, 1184: // timestamp, timestamptz
		return arrow.FixedWidthTypes.Timestamp_us
	case 18, 19, 25, 1043: // char, name, text, varchar
		return arrow.BinaryTypes.String
	case 114, 3802: // json, jsonb
		return arrow.BinaryTypes.String
	case 1700: // numeric / decimal
		return arrow.BinaryTypes.String
	case 2950: // uuid
		return arrow.BinaryTypes.String
	default:
		return arrow.BinaryTypes.String
	}
}

// InferArrowType maps dynamic Go runtime values to Arrow DataTypes when OIDs are absent.
func InferArrowType(val any) arrow.DataType {
	if val == nil {
		return arrow.BinaryTypes.String
	}
	switch val.(type) {
	case bool:
		return arrow.FixedWidthTypes.Boolean
	case int16:
		return arrow.PrimitiveTypes.Int16
	case int32:
		return arrow.PrimitiveTypes.Int32
	case int, int64:
		return arrow.PrimitiveTypes.Int64
	case float32:
		return arrow.PrimitiveTypes.Float32
	case float64:
		return arrow.PrimitiveTypes.Float64
	case []byte:
		return arrow.BinaryTypes.Binary
	case time.Time:
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		return arrow.BinaryTypes.String
	}
}

// ComputeSchemaHash generates a deterministic 64-bit FNV-1a hash representing the table
// schema topology (namespace, table, sorted column names, and resolved Arrow type IDs).
func ComputeSchemaHash(event ChangeEvent) (uint64, []string, *arrow.Schema) {
	cols := make([]string, 0, len(event.After))
	source := event.After
	if len(source) == 0 {
		source = event.Before
	}
	for col := range source {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	fields := make([]arrow.Field, len(cols))
	hasher := fnv.New64a()
	hasher.Write([]byte(event.Schema))
	hasher.Write([]byte("."))
	hasher.Write([]byte(event.Table))
	hasher.Write([]byte(":"))

	for i, col := range cols {
		val := source[col]
		var dt arrow.DataType
		if event.ColumnTypes != nil {
			if oid, ok := event.ColumnTypes[col]; ok && oid > 0 {
				dt = PgOIDToArrowType(oid)
			}
		}
		if dt == nil {
			dt = InferArrowType(val)
		}

		fields[i] = arrow.Field{
			Name:     col,
			Type:     dt,
			Nullable: true,
		}

		hasher.Write([]byte(col))
		hasher.Write([]byte(fmt.Sprintf("%d;", dt.ID())))
	}

	meta := arrow.NewMetadata([]string{"schema", "table"}, []string{event.Schema, event.Table})
	schema := arrow.NewSchema(fields, &meta)
	return hasher.Sum64(), cols, schema
}

// AppendColumnValue appends a single cell into the appropriate typed Arrow builder.
func AppendColumnValue(b array.Builder, val any) {
	if val == nil {
		b.AppendNull()
		return
	}

	switch builder := b.(type) {
	case *array.BooleanBuilder:
		switch v := val.(type) {
		case bool:
			builder.Append(v)
		case string:
			builder.Append(v == "t" || v == "true" || v == "1")
		default:
			builder.AppendNull()
		}

	case *array.Int16Builder:
		switch v := val.(type) {
		case int16:
			builder.Append(v)
		case int:
			builder.Append(int16(v))
		case int32:
			builder.Append(int16(v))
		case int64:
			builder.Append(int16(v))
		case float64:
			builder.Append(int16(v))
		default:
			builder.AppendNull()
		}

	case *array.Int32Builder:
		switch v := val.(type) {
		case int32:
			builder.Append(v)
		case int:
			builder.Append(int32(v))
		case int16:
			builder.Append(int32(v))
		case int64:
			builder.Append(int32(v))
		case float64:
			builder.Append(int32(v))
		default:
			builder.AppendNull()
		}

	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			builder.Append(v)
		case int:
			builder.Append(int64(v))
		case int32:
			builder.Append(int64(v))
		case int16:
			builder.Append(int64(v))
		case float64:
			builder.Append(int64(v))
		default:
			builder.AppendNull()
		}

	case *array.Float32Builder:
		switch v := val.(type) {
		case float32:
			builder.Append(v)
		case float64:
			builder.Append(float32(v))
		case int:
			builder.Append(float32(v))
		default:
			builder.AppendNull()
		}

	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			builder.Append(v)
		case float32:
			builder.Append(float64(v))
		case int:
			builder.Append(float64(v))
		case int64:
			builder.Append(float64(v))
		default:
			builder.AppendNull()
		}

	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			builder.Append(v)
		case []byte:
			builder.Append(string(v))
		default:
			builder.Append(fmt.Sprintf("%v", v))
		}

	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			builder.Append(v)
		case string:
			builder.Append([]byte(v))
		default:
			builder.AppendNull()
		}

	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			builder.Append(arrow.Timestamp(v.UnixMicro()))
		case string:
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				builder.Append(arrow.Timestamp(t.UnixMicro()))
			} else if t, err := time.Parse(time.RFC3339, v); err == nil {
				builder.Append(arrow.Timestamp(t.UnixMicro()))
			} else {
				builder.AppendNull()
			}
		default:
			builder.AppendNull()
		}

	case *array.Date32Builder:
		switch v := val.(type) {
		case time.Time:
			days := int32(v.Unix() / 86400)
			builder.Append(arrow.Date32(days))
		default:
			builder.AppendNull()
		}

	default:
		// Fallback to null or generic append
		b.AppendNull()
	}
}
