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
	"os"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// Tests for the PostgreSQL ReadCDC SchemaTransform declared in
// schematransform_cdc.go. TestYAMLReference_ByteEquality lives here because
// the generated reference covers every registered provider, so it only
// matches the golden document once the CDC provider is registered.

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

func TestPostgreSqlReadCDCSchemaTransform_Registration(t *testing.T) {
	reg := schematransform.DefaultRegistry()
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

func TestPostgreSqlReadCDCProvider_MetadataAndFactory(t *testing.T) {
	// 2. Read CDC Provider
	rp := &postgreSqlReadCDCProvider{}
	if got := rp.Identifier(); got != ReadCDCSchemaTransformURN {
		t.Errorf("rp.Identifier() = %q, want %q", got, ReadCDCSchemaTransformURN)
	}
	if got := rp.Description(); got == "" {
		t.Errorf("rp.Description() is empty")
	}
	if in := rp.InputCollectionNames(); in != nil {
		t.Errorf("rp.InputCollectionNames() = %v, want nil", in)
	}
	if out := rp.OutputCollectionNames(); len(out) != 1 || out[0] != schematransform.MainOutputTag {
		t.Errorf("rp.OutputCollectionNames() = %v, want [%q]", out, schematransform.MainOutputTag)
	}
	readCfg := PostgreSqlReadCDCConfig{
		Host:        "localhost",
		Database:    "testdb",
		SlotName:    "slot_test",
		Publication: "pub_test",
		Username:    "beam_test",
	}
	rtf, err := rp.CreateTransform(readCfg)
	if err != nil {
		t.Fatalf("rp.CreateTransform() err = %v, want nil", err)
	}
	if rtf == nil {
		t.Fatalf("rp.CreateTransform() returned nil transform")
	}
}

func TestCDCOptions_PasswordResolution(t *testing.T) {
	t.Setenv("BEAM_TEST_SECRET_ENV", "cdc_env_secret_pw")
	t.Setenv("PGPASSWORD", "cdc_pgpass_fallback")
	opts := CDCOptions{
		Password:       "cdc_direct_pw",
		PasswordEnvVar: "BEAM_TEST_SECRET_ENV",
	}
	if got := opts.ResolvePassword(); got != "cdc_env_secret_pw" {
		t.Errorf("ResolvePassword() = %q, want cdc_env_secret_pw", got)
	}

	optsNoEnvVar := CDCOptions{
		Password: "cdc_direct_pw",
	}
	if got := optsNoEnvVar.ResolvePassword(); got != "cdc_direct_pw" {
		t.Errorf("ResolvePassword() = %q, want cdc_direct_pw", got)
	}

	optsEmpty := CDCOptions{}
	if got := optsEmpty.ResolvePassword(); got != "cdc_pgpass_fallback" {
		t.Errorf("ResolvePassword() = %q, want cdc_pgpass_fallback", got)
	}
}

func TestYAMLReference_ByteEquality(t *testing.T) {
	got := GenerateYAMLReference()
	if os.Getenv("UPDATE_YAML_REF") == "1" {
		if err := os.WriteFile("YAML_REFERENCE.md", []byte(got), 0644); err != nil {
			t.Fatalf("failed to update YAML_REFERENCE.md: %v", err)
		}
		return
	}
	want, err := os.ReadFile("YAML_REFERENCE.md")
	if err != nil {
		t.Fatalf("failed to read YAML_REFERENCE.md: %v", err)
	}
	if string(want) != got {
		t.Fatalf("YAML_REFERENCE.md content drift detected.\n--- Want ---\n%s\n--- Got ---\n%s", string(want), got)
	}
}
