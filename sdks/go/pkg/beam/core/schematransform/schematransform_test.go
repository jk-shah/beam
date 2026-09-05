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
	"errors"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

type sampleTestConfig struct {
	Host        string   `beam:"host" doc:"Database server hostname"`
	Port        int32    `beam:"port" doc:"Database server port"`
	Username    string   `beam:"username"`
	Password    string   `beam:"password,secret"`
	UseSSL      bool     `beam:"use_ssl"`
	Tags        []string `beam:"tags"`
	TimeoutSecs *float64 `beam:"timeout_secs"`
}

func (c sampleTestConfig) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
	}
	if c.Port <= 0 {
		return errors.New("port must be positive")
	}
	return nil
}

type dummyTransform struct{}

func (d *dummyTransform) BuildTransform(s beam.Scope, inputs map[string]beam.PCollection) (map[string]beam.PCollection, error) {
	return inputs, nil
}

type sampleTestProvider struct{}

func (p *sampleTestProvider) Identifier() string {
	return "beam:schematransform:org.apache.beam:test_provider:v1"
}

func (p *sampleTestProvider) Description() string {
	return "Sample test SchemaTransform provider"
}

func (p *sampleTestProvider) InputCollectionNames() []string {
	return []string{MainInputTag}
}

func (p *sampleTestProvider) OutputCollectionNames() []string {
	return []string{MainOutputTag, ErrorOutputTag}
}

func (p *sampleTestProvider) CreateTransform(config sampleTestConfig) (SchemaTransform, error) {
	return &dummyTransform{}, nil
}

func TestConfigToBeamSchema(t *testing.T) {
	schema, err := ConfigToBeamSchema(reflect.TypeOf(sampleTestConfig{}))
	if err != nil {
		t.Fatalf("ConfigToBeamSchema failed: %v", err)
	}

	if len(schema.Fields) != 7 {
		t.Fatalf("expected 7 fields, got %d", len(schema.Fields))
	}

	fieldsMap := make(map[string]*pipepb.Field)
	for _, f := range schema.Fields {
		fieldsMap[f.Name] = f
	}

	// 1. Verify string field with doc
	hostField, ok := fieldsMap["host"]
	if !ok || hostField.Type.GetAtomicType() != pipepb.AtomicType_STRING {
		t.Errorf("host field mismatch: %+v", hostField)
	}
	if hostField.Description != "Database server hostname" {
		t.Errorf("host description mismatch: %s", hostField.Description)
	}

	// 2. Verify int32 field
	portField, ok := fieldsMap["port"]
	if !ok || portField.Type.GetAtomicType() != pipepb.AtomicType_INT32 {
		t.Errorf("port field mismatch: %+v", portField)
	}

	// 3. Verify secret masking option
	passField, ok := fieldsMap["password"]
	if !ok {
		t.Fatalf("missing password field")
	}
	if len(passField.Options) == 0 || passField.Options[0].Name != "beam:schema:option:secret:v1" {
		t.Errorf("expected secret option on password field, got: %+v", passField.Options)
	}

	// 4. Verify slice / array field
	tagsField, ok := fieldsMap["tags"]
	if !ok || tagsField.Type.GetArrayType() == nil || tagsField.Type.GetArrayType().ElementType.GetAtomicType() != pipepb.AtomicType_STRING {
		t.Errorf("tags field mismatch: %+v", tagsField)
	}

	// 5. Verify pointer / nullable field
	timeoutField, ok := fieldsMap["timeout_secs"]
	if !ok || !timeoutField.Type.Nullable || timeoutField.Type.GetAtomicType() != pipepb.AtomicType_DOUBLE {
		t.Errorf("timeout_secs nullable mismatch: %+v", timeoutField)
	}
}

func TestRegistryOperations(t *testing.T) {
	reg := NewRegistry()
	provider := &sampleTestProvider{}

	err := RegisterTyped(reg, provider)
	if err != nil {
		t.Fatalf("RegisterTyped failed: %v", err)
	}

	// Duplicate registration should fail
	errDup := RegisterTyped(reg, provider)
	if errDup == nil {
		t.Fatalf("expected duplicate registration error")
	}

	// Retrieve provider
	p, schema, found := reg.Get(provider.Identifier())
	if !found {
		t.Fatalf("provider not found in registry")
	}
	if p.Identifier() != provider.Identifier() {
		t.Errorf("provider ID mismatch: got %s, want %s", p.Identifier(), provider.Identifier())
	}
	if schema == nil || len(schema.Fields) != 7 {
		t.Errorf("cached schema mismatch: %+v", schema)
	}

	// Test validation failure
	invalidCfg := sampleTestConfig{
		Host: "",
		Port: -1,
	}
	_, errVal := p.CreateTransform(invalidCfg)
	if errVal == nil {
		t.Fatalf("expected validation error for invalid config")
	}

	// Test valid creation
	validCfg := sampleTestConfig{
		Host: "localhost",
		Port: 5432,
	}
	tf, errCreate := p.CreateTransform(validCfg)
	if errCreate != nil {
		t.Fatalf("unexpected error creating transform: %v", errCreate)
	}
	if tf == nil {
		t.Fatalf("expected non-nil SchemaTransform")
	}
}
