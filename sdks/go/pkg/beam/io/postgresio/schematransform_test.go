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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

func TestPostgreSqlWriteConfig_Validation(t *testing.T) {
	tests := []struct {
		name      string
		cfg       PostgreSqlWriteConfig
		expectErr bool
	}{
		{
			name: "valid config",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Table:    "users",
				Username: "testuser",
			},
			expectErr: false,
		},
		{
			name: "missing host",
			cfg: PostgreSqlWriteConfig{
				Database: "testdb",
				Table:    "users",
				Username: "testuser",
			},
			expectErr: true,
		},
		{
			name: "missing database",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Table:    "users",
				Username: "testuser",
			},
			expectErr: true,
		},
		{
			name: "missing table",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Database: "testdb",
				Username: "testuser",
			},
			expectErr: true,
		},
		{
			name: "missing username",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Database: "testdb",
				Table:    "users",
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.expectErr {
				t.Errorf("Validate() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestPostgreSqlReadCDCConfig_Validation(t *testing.T) {
	tests := []struct {
		name      string
		cfg       PostgreSqlReadCDCConfig
		expectErr bool
	}{
		{
			name: "valid config row format",
			cfg: PostgreSqlReadCDCConfig{
				Host:         "localhost",
				Port:         5432,
				Database:     "testdb",
				SlotName:     "slot1",
				Publication:  "pub1",
				Username:     "testuser",
				OutputFormat: "row",
			},
			expectErr: false,
		},
		{
			name: "valid config arrow format",
			cfg: PostgreSqlReadCDCConfig{
				Host:         "localhost",
				Port:         5432,
				Database:     "testdb",
				SlotName:     "slot1",
				Publication:  "pub1",
				Username:     "testuser",
				OutputFormat: "arrow",
			},
			expectErr: false,
		},
		{
			name: "invalid output format",
			cfg: PostgreSqlReadCDCConfig{
				Host:         "localhost",
				Port:         5432,
				Database:     "testdb",
				SlotName:     "slot1",
				Publication:  "pub1",
				Username:     "testuser",
				OutputFormat: "parquet", // unsupported format
			},
			expectErr: true,
		},
		{
			name: "missing slot name",
			cfg: PostgreSqlReadCDCConfig{
				Host:        "localhost",
				Database:    "testdb",
				Publication: "pub1",
				Username:    "testuser",
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.expectErr {
				t.Errorf("Validate() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestPostgreSqlSchemaTransforms_Registration(t *testing.T) {
	reg := schematransform.DefaultRegistry()

	// 1. Verify Write Provider
	writeProvider, writeSchema, ok := reg.Get(WriteSchemaTransformURN)
	if !ok {
		t.Fatalf("expected %s to be registered in DefaultRegistry", WriteSchemaTransformURN)
	}
	if writeProvider.Identifier() != WriteSchemaTransformURN {
		t.Errorf("identifier mismatch: got %s", writeProvider.Identifier())
	}
	if writeSchema == nil || len(writeSchema.GetFields()) == 0 {
		t.Fatalf("expected non-empty schema for write provider")
	}

	// Verify password field has secret option
	var foundPassword bool
	for _, f := range writeSchema.GetFields() {
		if f.GetName() == "password" {
			foundPassword = true
			if len(f.GetOptions()) == 0 || f.GetOptions()[0].GetName() != "beam:schema:option:secret:v1" {
				t.Errorf("expected secret option on password field")
			}
		}
	}
	if !foundPassword {
		t.Errorf("missing password field in write schema")
	}

	// 2. Verify ReadCDC Provider
	cdcProvider, cdcSchema, ok := reg.Get(ReadCDCSchemaTransformURN)
	if !ok {
		t.Fatalf("expected %s to be registered in DefaultRegistry", ReadCDCSchemaTransformURN)
	}
	if cdcProvider.Identifier() != ReadCDCSchemaTransformURN {
		t.Errorf("identifier mismatch: got %s", cdcProvider.Identifier())
	}
	if cdcSchema == nil || len(cdcSchema.GetFields()) == 0 {
		t.Fatalf("expected non-empty schema for cdc provider")
	}

	// Verify tables field is array of strings
	var foundTables bool
	for _, f := range cdcSchema.GetFields() {
		if f.GetName() == "tables" {
			foundTables = true
			arr := f.GetType().GetArrayType()
			if arr == nil || arr.GetElementType().GetAtomicType() != pipepb.AtomicType_STRING {
				t.Errorf("expected tables to be array of string, got: %+v", f.GetType())
			}
		}
	}
	if !foundTables {
		t.Errorf("missing tables field in cdc schema")
	}
}

func TestPostgreSqlWriteTransform_BuildTransform(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	inputCol := beam.Create(s, "row1", "row2")
	inputs := map[string]beam.PCollection{
		schematransform.MainInputTag: inputCol,
	}

	cfg := PostgreSqlWriteConfig{
		Host:         "localhost",
		Port:         5432,
		Database:     "testdb",
		Table:        "users",
		Username:     "testuser",
		ConflictKeys: []string{"id"},
	}

	tf := &postgreSqlWriteTransform{cfg: cfg}
	outputs, err := tf.BuildTransform(s, inputs)
	if err != nil {
		t.Fatalf("BuildTransform failed: %v", err)
	}

	if _, ok := outputs[schematransform.MainOutputTag]; !ok {
		t.Errorf("missing main output tag in write transform outputs")
	}
	if _, ok := outputs[schematransform.ErrorOutputTag]; !ok {
		t.Errorf("missing error output tag in write transform outputs")
	}
}

func TestPostgreSqlReadCDCTransform_BuildTransform(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	cfgRow := PostgreSqlReadCDCConfig{
		Host:         "localhost",
		Port:         5432,
		Database:     "testdb",
		SlotName:     "slot1",
		Publication:  "pub1",
		Username:     "testuser",
		OutputFormat: "row",
	}

	tfRow := &postgreSqlReadCDCTransform{cfg: cfgRow}
	outputsRow, err := tfRow.BuildTransform(s, nil)
	if err != nil {
		t.Fatalf("BuildTransform row failed: %v", err)
	}
	if _, ok := outputsRow[schematransform.MainOutputTag]; !ok {
		t.Errorf("missing main output tag in row cdc transform outputs")
	}

	cfgArrow := PostgreSqlReadCDCConfig{
		Host:           "localhost",
		Port:           5432,
		Database:       "testdb",
		SlotName:       "slot1",
		Publication:    "pub1",
		Username:       "testuser",
		OutputFormat:   "arrow",
		ArrowBatchRows: 1024,
	}

	tfArrow := &postgreSqlReadCDCTransform{cfg: cfgArrow}
	outputsArrow, err := tfArrow.BuildTransform(s, nil)
	if err != nil {
		t.Fatalf("BuildTransform arrow failed: %v", err)
	}
	if _, ok := outputsArrow[schematransform.MainOutputTag]; !ok {
		t.Errorf("missing main output tag in arrow cdc transform outputs")
	}
}
