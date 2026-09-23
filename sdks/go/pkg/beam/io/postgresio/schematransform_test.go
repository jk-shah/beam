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
				Table:    "public.users",
				Username: "testuser",
			},
			expectErr: false,
		},
		{
			name: "missing host",
			cfg: PostgreSqlWriteConfig{
				Database: "testdb",
				Table:    "public.users",
				Username: "testuser",
			},
			expectErr: true,
		},
		{
			name: "missing database",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Table:    "public.users",
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
				Table:    "public.users",
			},
			expectErr: true,
		},
		{
			// The sink pins search_path to pg_catalog,pg_temp, so an
			// unqualified name cannot resolve to a user table. Rejecting it
			// during validation keeps the failure inside the SchemaTransform
			// error channel instead of surfacing as a panic during expansion.
			name: "unqualified table",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Database: "testdb",
				Table:    "users",
				Username: "testuser",
			},
			expectErr: true,
		},
		{
			name: "invalid table syntax",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Database: "testdb",
				Table:    `public."bad"table"`,
				Username: "testuser",
			},
			expectErr: true,
		},
		{
			name: "invalid write mode",
			cfg: PostgreSqlWriteConfig{
				Host:      "localhost",
				Database:  "testdb",
				Table:     "public.users",
				Username:  "testuser",
				WriteMode: "INVALID",
			},
			expectErr: true,
		},
		{
			name: "write mode UPDATE without conflict keys",
			cfg: PostgreSqlWriteConfig{
				Host:      "localhost",
				Database:  "testdb",
				Table:     "public.users",
				Username:  "testuser",
				WriteMode: "UPDATE",
			},
			expectErr: true,
		},
		{
			name: "write mode MERGE with pgbouncer",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				WriteMode:    "MERGE",
				UsePgBouncer: true,
			},
			expectErr: true,
		},
		{
			name: "replication origin with pgbouncer",
			cfg: PostgreSqlWriteConfig{
				Host:              "localhost",
				Database:          "testdb",
				Table:             "public.users",
				Username:          "testuser",
				ReplicationOrigin: "my_origin",
				UsePgBouncer:      true,
			},
			expectErr: true,
		},
		{
			name: "invalid replication origin name",
			cfg: PostgreSqlWriteConfig{
				Host:              "localhost",
				Database:          "testdb",
				Table:             "public.users",
				Username:          "testuser",
				ReplicationOrigin: "bad name with spaces!",
			},
			expectErr: true,
		},
		{
			name: "invalid sslmode",
			cfg: PostgreSqlWriteConfig{
				Host:     "localhost",
				Database: "testdb",
				Table:    "public.users",
				Username: "testuser",
				SSLMode:  "bogus",
			},
			expectErr: true,
		},
		{
			name: "invalid conflict key",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				ConflictKeys: []string{`bad"id`},
			},
			expectErr: true,
		},
		{
			name: "invalid update field",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				ConflictKeys: []string{"id"},
				UpdateFields: []string{`bad"field`},
			},
			expectErr: true,
		},
		{
			name: "valid with update fields",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				ConflictKeys: []string{"id"},
				UpdateFields: []string{"name", "status"},
			},
			expectErr: false,
		},
		{
			// INSERT has no SET clause, so update_fields is read by nothing.
			name: "update fields under INSERT",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				WriteMode:    "INSERT",
				UpdateFields: []string{"name"},
			},
			expectErr: true,
		},
		{
			// MERGE derives its own SET clause from the key and op column.
			name: "update fields under MERGE",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				WriteMode:    "MERGE",
				ConflictKeys: []string{"id"},
				UpdateFields: []string{"name"},
			},
			expectErr: true,
		},
		{
			// An empty write_mode with no conflict_keys resolves to INSERT, so
			// update_fields would be dropped without the rule catching it.
			name: "update fields with defaulted mode and no conflict keys",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				UpdateFields: []string{"name"},
			},
			expectErr: true,
		},
		{
			name: "update fields under UPDATE",
			cfg: PostgreSqlWriteConfig{
				Host:         "localhost",
				Database:     "testdb",
				Table:        "public.users",
				Username:     "testuser",
				WriteMode:    "UPDATE",
				ConflictKeys: []string{"id"},
				UpdateFields: []string{"name"},
			},
			expectErr: false,
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

	// Verify Write Provider
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
		Table:        "public.users",
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

func TestPostgreSqlSchemaTransformProviders_MetadataAndFactory(t *testing.T) {
	// Write Provider
	wp := &postgreSqlWriteProvider{}
	if got := wp.Identifier(); got != WriteSchemaTransformURN {
		t.Errorf("wp.Identifier() = %q, want %q", got, WriteSchemaTransformURN)
	}
	if got := wp.Description(); got == "" {
		t.Errorf("wp.Description() is empty")
	}
	if in := wp.InputCollectionNames(); len(in) != 1 || in[0] != schematransform.MainInputTag {
		t.Errorf("wp.InputCollectionNames() = %v, want [%q]", in, schematransform.MainInputTag)
	}
	if out := wp.OutputCollectionNames(); len(out) != 2 {
		t.Errorf("wp.OutputCollectionNames() len = %d, want 2", len(out))
	}
	writeCfg := PostgreSqlWriteConfig{
		Host:     "localhost",
		Database: "testdb",
		Table:    "public.users",
		Username: "beam_test",
	}
	tf, err := wp.CreateTransform(writeCfg)
	if err != nil {
		t.Fatalf("wp.CreateTransform() err = %v, want nil", err)
	}
	if tf == nil {
		t.Fatalf("wp.CreateTransform() returned nil transform")
	}
}

func TestSchemaTransformURNs_Canonical(t *testing.T) {
	wantWrite := "beam:schematransform:org.apache.beam:postgres_write:v1"
	if WriteSchemaTransformURN != wantWrite {
		t.Errorf("WriteSchemaTransformURN = %q, want %q", WriteSchemaTransformURN, wantWrite)
	}

	wantCDC := "beam:schematransform:org.apache.beam:postgres_read_cdc:v1"
	if ReadCDCSchemaTransformURN != wantCDC {
		t.Errorf("ReadCDCSchemaTransformURN = %q, want %q", ReadCDCSchemaTransformURN, wantCDC)
	}

	if WriteSchemaTransformURN == ReadCDCSchemaTransformURN {
		t.Fatalf("write and CDC URNs must be distinct: %q", WriteSchemaTransformURN)
	}
}

func TestOptions_PasswordResolution(t *testing.T) {
	t.Run("WriteOptions", func(t *testing.T) {
		// 1. PasswordEnvVar takes highest precedence
		t.Setenv("BEAM_TEST_SECRET_ENV", "env_secret_pw")
		t.Setenv("PGPASSWORD", "pgpass_fallback")
		opts := WriteOptions{
			Password:       "direct_pw",
			PasswordEnvVar: "BEAM_TEST_SECRET_ENV",
		}
		if got := opts.ResolvePassword(); got != "env_secret_pw" {
			t.Errorf("ResolvePassword() = %q, want env_secret_pw", got)
		}

		// 2. Direct password takes second precedence
		optsNoEnvVar := WriteOptions{
			Password: "direct_pw",
		}
		if got := optsNoEnvVar.ResolvePassword(); got != "direct_pw" {
			t.Errorf("ResolvePassword() = %q, want direct_pw", got)
		}

		// 3. PGPASSWORD fallback
		optsEmpty := WriteOptions{}
		if got := optsEmpty.ResolvePassword(); got != "pgpass_fallback" {
			t.Errorf("ResolvePassword() = %q, want pgpass_fallback", got)
		}
	})
}

func TestPostgreSqlWriteTransform_BuildTransform_StructuredError(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()
	in := beam.Create(s, "test")

	// Missing input collection
	tf := &postgreSqlWriteTransform{cfg: PostgreSqlWriteConfig{
		Host:     "localhost",
		Database: "testdb",
		Table:    "public.users",
		Username: "testuser",
	}}
	_, err := tf.BuildTransform(s, map[string]beam.PCollection{})
	if err == nil {
		t.Error("expected error when input collection is missing, got nil")
	}

	// Misconfiguration that would otherwise panic inside Write
	inputs := map[string]beam.PCollection{schematransform.MainInputTag: in}
	tfMergePgBouncer := &postgreSqlWriteTransform{cfg: PostgreSqlWriteConfig{
		Host:         "localhost",
		Database:     "testdb",
		Table:        "public.users",
		Username:     "testuser",
		WriteMode:    "MERGE",
		UsePgBouncer: true,
	}}
	_, err = tfMergePgBouncer.BuildTransform(s, inputs)
	if err == nil {
		t.Error("expected structured error for MERGE + PgBouncer, got nil")
	}
}
