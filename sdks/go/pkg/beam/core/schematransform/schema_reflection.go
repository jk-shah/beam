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

package schematransform

import (
	"fmt"
	"reflect"
	"strings"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

const maxRecursionDepth = 10

// ConfigToBeamSchema reflects a Go struct configuration type and converts it into
// an Apache Beam Schema protobuf representation for portable cross-language discovery.
func ConfigToBeamSchema(t reflect.Type) (*pipepb.Schema, error) {
	if t == nil {
		return nil, fmt.Errorf("configuration type cannot be nil")
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("configuration type must be a struct, got %v", t.Kind())
	}

	fields, err := reflectFields(t, 0)
	if err != nil {
		return nil, err
	}

	return &pipepb.Schema{
		Fields: fields,
		Id:     fmt.Sprintf("beam:schema:go:%s", t.Name()),
	}, nil
}

func reflectFields(t reflect.Type, depth int) ([]*pipepb.Field, error) {
	if depth > maxRecursionDepth {
		return nil, fmt.Errorf("schema reflection recursion depth exceeded limit (%d)", maxRecursionDepth)
	}

	var fields []*pipepb.Field
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}

		name, isSecret, ignore := parseFieldTag(sf)
		if ignore {
			continue
		}

		fieldType, isNullable, err := reflectFieldType(sf.Type, depth+1)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", sf.Name, err)
		}

		var options []*pipepb.Option
		if isSecret {
			options = append(options, &pipepb.Option{
				Name: "beam:schema:option:secret:v1",
				Type: &pipepb.FieldType{
					TypeInfo: &pipepb.FieldType_AtomicType{
						AtomicType: pipepb.AtomicType_BOOLEAN,
					},
				},
				Value: &pipepb.FieldValue{
					FieldValue: &pipepb.FieldValue_AtomicValue{
						AtomicValue: &pipepb.AtomicTypeValue{
							Value: &pipepb.AtomicTypeValue_Boolean{
								Boolean: true,
							},
						},
					},
				},
			})
		}

		fieldType.Nullable = isNullable
		fields = append(fields, &pipepb.Field{
			Name:        name,
			Description: sf.Tag.Get("doc"),
			Type:        fieldType,
			Id:          int32(len(fields)),
			Options:     options,
		})
	}

	return fields, nil
}

func parseFieldTag(sf reflect.StructField) (string, bool, bool) {
	tag := sf.Tag.Get("beam")
	if tag == "-" {
		return "", false, true
	}
	if tag == "" {
		tag = sf.Tag.Get("json")
	}
	if tag == "-" {
		return "", false, true
	}

	parts := strings.Split(tag, ",")
	name := parts[0]
	if name == "" {
		name = toSnakeCase(sf.Name)
	}

	isSecret := false
	for _, p := range parts[1:] {
		if p == "secret" {
			isSecret = true
		}
	}

	return name, isSecret, false
}

func reflectFieldType(t reflect.Type, depth int) (*pipepb.FieldType, bool, error) {
	isNullable := false
	if t.Kind() == reflect.Ptr {
		isNullable = true
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Bool:
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: pipepb.AtomicType_BOOLEAN,
			},
		}, isNullable, nil

	case reflect.Int8, reflect.Int16:
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: pipepb.AtomicType_INT16,
			},
		}, isNullable, nil

	case reflect.Int32, reflect.Int:
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: pipepb.AtomicType_INT32,
			},
		}, isNullable, nil

	case reflect.Int64:
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: pipepb.AtomicType_INT64,
			},
		}, isNullable, nil

	case reflect.Float32:
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: pipepb.AtomicType_FLOAT,
			},
		}, isNullable, nil

	case reflect.Float64:
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: pipepb.AtomicType_DOUBLE,
			},
		}, isNullable, nil

	case reflect.String:
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: pipepb.AtomicType_STRING,
			},
		}, isNullable, nil

	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return &pipepb.FieldType{
				TypeInfo: &pipepb.FieldType_AtomicType{
					AtomicType: pipepb.AtomicType_BYTES,
				},
			}, isNullable, nil
		}
		elemType, _, err := reflectFieldType(t.Elem(), depth+1)
		if err != nil {
			return nil, false, err
		}
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_ArrayType{
				ArrayType: &pipepb.ArrayType{
					ElementType: elemType,
				},
			},
		}, isNullable, nil

	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, false, fmt.Errorf("map keys must be strings, got %v", t.Key().Kind())
		}
		valType, _, err := reflectFieldType(t.Elem(), depth+1)
		if err != nil {
			return nil, false, err
		}
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_MapType{
				MapType: &pipepb.MapType{
					KeyType: &pipepb.FieldType{
						TypeInfo: &pipepb.FieldType_AtomicType{
							AtomicType: pipepb.AtomicType_STRING,
						},
					},
					ValueType: valType,
				},
			},
		}, isNullable, nil

	case reflect.Struct:
		subFields, err := reflectFields(t, depth+1)
		if err != nil {
			return nil, false, err
		}
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_RowType{
				RowType: &pipepb.RowType{
					Schema: &pipepb.Schema{
						Fields: subFields,
					},
				},
			},
		}, isNullable, nil

	default:
		return nil, false, fmt.Errorf("unsupported Go type kind %v for schema reflection", t.Kind())
	}
}

func toSnakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}
