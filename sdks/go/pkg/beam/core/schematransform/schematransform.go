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
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// Standard PCollection tag constants matching Apache Beam cross-language conventions.
const (
	MainInputTag   = "input"
	MainOutputTag  = "output"
	ErrorOutputTag = "errors"
)

// SchemaTransform represents a portable, schema-aware transform operating on PCollections of Rows.
type SchemaTransform interface {
	BuildTransform(s beam.Scope, inputs map[string]beam.PCollection) (map[string]beam.PCollection, error)
}

// Provider represents a type-erased factory that discovers and creates SchemaTransforms.
type Provider interface {
	Identifier() string
	Description() string
	ConfigType() reflect.Type
	InputCollectionNames() []string
	OutputCollectionNames() []string
	CreateTransform(config any) (SchemaTransform, error)
}

// TypedProvider provides type-safe configuration binding using Go generics.
type TypedProvider[ConfigT any] interface {
	Identifier() string
	Description() string
	InputCollectionNames() []string
	OutputCollectionNames() []string
	CreateTransform(config ConfigT) (SchemaTransform, error)
}

// Validatable is an optional interface implemented by configuration structs for pre-expansion validation.
type Validatable interface {
	Validate() error
}

type typedProviderAdapter[ConfigT any] struct {
	inner TypedProvider[ConfigT]
}

func (a *typedProviderAdapter[ConfigT]) Identifier() string {
	return a.inner.Identifier()
}

func (a *typedProviderAdapter[ConfigT]) Description() string {
	return a.inner.Description()
}

func (a *typedProviderAdapter[ConfigT]) ConfigType() reflect.Type {
	var zero ConfigT
	return reflect.TypeOf(zero)
}

func (a *typedProviderAdapter[ConfigT]) InputCollectionNames() []string {
	return a.inner.InputCollectionNames()
}

func (a *typedProviderAdapter[ConfigT]) OutputCollectionNames() []string {
	return a.inner.OutputCollectionNames()
}

func (a *typedProviderAdapter[ConfigT]) CreateTransform(config any) (SchemaTransform, error) {
	if cfg, ok := config.(ConfigT); ok {
		if val, hasValidation := any(cfg).(Validatable); hasValidation {
			if err := val.Validate(); err != nil {
				return nil, fmt.Errorf("configuration validation failed for %s: %w", a.inner.Identifier(), err)
			}
		}
		return a.inner.CreateTransform(cfg)
	}
	return nil, fmt.Errorf("invalid config type: expected %v, got %T", a.ConfigType(), config)
}

type cachedProvider struct {
	provider    Provider
	schemaProto *pipepb.Schema
}

// Registry maintains thread-safe registration of SchemaTransform providers and their pre-computed schemas.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]*cachedProvider
}

// NewRegistry creates a new, empty SchemaTransform provider registry.
func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]*cachedProvider),
	}
}

var defaultRegistry = NewRegistry()

// DefaultRegistry returns the global default SchemaTransform provider registry.
func DefaultRegistry() *Registry {
	return defaultRegistry
}

// Register registers an untyped SchemaTransform provider into the target registry.
func (r *Registry) Register(p Provider) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := p.Identifier()
	if id == "" {
		return fmt.Errorf("provider identifier cannot be empty")
	}
	if _, exists := r.providers[id]; exists {
		return fmt.Errorf("schematransform provider with identifier '%s' already registered", id)
	}

	schemaProto, err := ConfigToBeamSchema(p.ConfigType())
	if err != nil {
		return fmt.Errorf("failed to reflect configuration schema for %s: %w", id, err)
	}

	r.providers[id] = &cachedProvider{
		provider:    p,
		schemaProto: schemaProto,
	}
	return nil
}

// RegisterTyped registers a generic, type-safe SchemaTransform provider into the target registry.
func RegisterTyped[ConfigT any](r *Registry, p TypedProvider[ConfigT]) error {
	return r.Register(&typedProviderAdapter[ConfigT]{inner: p})
}

// GlobalRegisterTyped registers a generic SchemaTransform provider into the default global registry.
func GlobalRegisterTyped[ConfigT any](p TypedProvider[ConfigT]) {
	if err := RegisterTyped(defaultRegistry, p); err != nil {
		panic(fmt.Sprintf("failed to register SchemaTransform %s: %v", p.Identifier(), err))
	}
}

// Get retrieves a registered SchemaTransform provider and its pre-computed schema.
func (r *Registry) Get(id string) (Provider, *pipepb.Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.providers[id]
	if !ok {
		return nil, nil, false
	}
	return entry.provider, entry.schemaProto, true
}

// List returns all registered providers in the registry.
func (r *Registry) List() []Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Provider, 0, len(r.providers))
	for _, entry := range r.providers {
		out = append(out, entry.provider)
	}
	return out
}

// GetAllWithSchemas returns all registered providers mapped to their pre-computed schemas.
func (r *Registry) GetAllWithSchemas() map[string]struct {
	Provider Provider
	Schema   *pipepb.Schema
} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]struct {
		Provider Provider
		Schema   *pipepb.Schema
	}, len(r.providers))

	for id, entry := range r.providers {
		out[id] = struct {
			Provider Provider
			Schema   *pipepb.Schema
		}{
			Provider: entry.provider,
			Schema:   entry.schemaProto,
		}
	}
	return out
}
