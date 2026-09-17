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

package expansion

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
	jobmanagement_v1 "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type testMockConfig struct {
	Endpoint string `beam:"endpoint"`
	MaxBatch int    `beam:"max_batch"`
}

func (c testMockConfig) Validate() error {
	if c.MaxBatch < 0 {
		return fmt.Errorf("max_batch cannot be negative")
	}
	return nil
}

type testMockTransform struct {
	cfg testMockConfig
}

func (t *testMockTransform) BuildTransform(s beam.Scope, inputs map[string]beam.PCollection) (map[string]beam.PCollection, error) {
	out := beam.Create(s, "item1", "item2")
	return map[string]beam.PCollection{
		"output": out,
	}, nil
}

type testMockProvider struct{}

func (p *testMockProvider) Identifier() string {
	return "beam:schematransform:test:mock:v1"
}

func (p *testMockProvider) Description() string {
	return "A test mock schema transform"
}

func (p *testMockProvider) InputCollectionNames() []string {
	return []string{"input"}
}

func (p *testMockProvider) OutputCollectionNames() []string {
	return []string{"output"}
}

func (p *testMockProvider) CreateTransform(cfg testMockConfig) (schematransform.SchemaTransform, error) {
	return &testMockTransform{cfg: cfg}, nil
}

func setupTestServer(t *testing.T, authToken string) (*grpc.ClientConn, *Server, func()) {
	t.Helper()
	reg := schematransform.NewRegistry()

	if err := schematransform.RegisterTyped[testMockConfig](reg, &testMockProvider{}); err != nil {
		t.Fatalf("failed to register mock provider: %v", err)
	}

	opts := []ServerOption{WithRegistry(reg)}
	if authToken != "" {
		opts = append(opts, WithAuthToken(authToken))
	}
	srv := NewServer(opts...)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on random port: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(MaxGrpcMessageSize),
		grpc.MaxSendMsgSize(MaxGrpcMessageSize),
	)
	jobmanagement_v1.RegisterExpansionServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to grpc server: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
	}

	return conn, srv, cleanup
}

func TestExpansionService_DiscoverSchemaTransform(t *testing.T) {
	conn, _, cleanup := setupTestServer(t, "")
	defer cleanup()

	client := jobmanagement_v1.NewExpansionServiceClient(conn)
	resp, err := client.DiscoverSchemaTransform(context.Background(), &jobmanagement_v1.DiscoverSchemaTransformRequest{})
	if err != nil {
		t.Fatalf("DiscoverSchemaTransform RPC failed: %v", err)
	}

	configs := resp.GetSchemaTransformConfigs()
	if len(configs) == 0 {
		t.Fatalf("expected at least 1 schema transform config, got 0")
	}

	mockCfg, ok := configs["beam:schematransform:test:mock:v1"]
	if !ok {
		t.Fatalf("missing registered URN in discovery response: %v", configs)
	}

	if mockCfg.GetDescription() != "A test mock schema transform" {
		t.Errorf("unexpected description: %s", mockCfg.GetDescription())
	}

	if len(mockCfg.GetInputPcollectionNames()) != 1 || mockCfg.GetInputPcollectionNames()[0] != "input" {
		t.Errorf("unexpected inputs: %v", mockCfg.GetInputPcollectionNames())
	}

	if len(mockCfg.GetOutputPcollectionNames()) != 1 || mockCfg.GetOutputPcollectionNames()[0] != "output" {
		t.Errorf("unexpected outputs: %v", mockCfg.GetOutputPcollectionNames())
	}

	schema := mockCfg.GetConfigSchema()
	if schema == nil || len(schema.GetFields()) != 2 {
		t.Fatalf("unexpected schema fields: %v", schema)
	}
}

func TestExpansionService_Expand(t *testing.T) {
	conn, _, cleanup := setupTestServer(t, "")
	defer cleanup()

	client := jobmanagement_v1.NewExpansionServiceClient(conn)

	cfg := testMockConfig{
		Endpoint: "localhost:5432",
		MaxBatch: 500,
	}
	payloadBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	req := &jobmanagement_v1.ExpansionRequest{
		Transform: &pipepb.PTransform{
			UniqueName: "TestExpansionNode",
			Spec: &pipepb.FunctionSpec{
				Urn:     "beam:schematransform:test:mock:v1",
				Payload: payloadBytes,
			},
		},
		Namespace: "subgraph_ns_",
	}

	resp, err := client.Expand(context.Background(), req)
	if err != nil {
		t.Fatalf("Expand RPC failed: %v", err)
	}

	if resp.GetError() != "" {
		t.Fatalf("unexpected expansion error: %s", resp.GetError())
	}

	if resp.GetComponents() == nil {
		t.Fatalf("expected components in expansion response, got nil")
	}

	transforms := resp.GetComponents().GetTransforms()
	if len(transforms) == 0 {
		t.Errorf("expected transforms in components, got none")
	}

	// Verify namespace prefixing
	for id := range transforms {
		if len(id) < len("subgraph_ns_") || id[:len("subgraph_ns_")] != "subgraph_ns_" {
			t.Errorf("transform ID %q does not have expected namespace prefix", id)
		}
	}
}

func TestExpansionService_ValidationFailure(t *testing.T) {
	conn, _, cleanup := setupTestServer(t, "")
	defer cleanup()

	client := jobmanagement_v1.NewExpansionServiceClient(conn)

	cfg := testMockConfig{
		Endpoint: "localhost:5432",
		MaxBatch: -10, // Triggers validation error
	}
	payloadBytes, _ := json.Marshal(cfg)

	req := &jobmanagement_v1.ExpansionRequest{
		Transform: &pipepb.PTransform{
			UniqueName: "InvalidNode",
			Spec: &pipepb.FunctionSpec{
				Urn:     "beam:schematransform:test:mock:v1",
				Payload: payloadBytes,
			},
		},
	}

	resp, err := client.Expand(context.Background(), req)
	if err != nil {
		t.Fatalf("Expand RPC failed: %v", err)
	}

	if resp.GetError() == "" {
		t.Fatalf("expected validation error in expansion response, got none")
	}
}

func TestExpansionService_AuthToken(t *testing.T) {
	secretToken := "super-secure-token-12345"
	conn, _, cleanup := setupTestServer(t, secretToken)
	defer cleanup()

	client := jobmanagement_v1.NewExpansionServiceClient(conn)

	// Call without token should fail
	_, err := client.DiscoverSchemaTransform(context.Background(), &jobmanagement_v1.DiscoverSchemaTransformRequest{})
	if err == nil {
		t.Fatalf("expected error without auth token, got success")
	}

	// Call with correct token should succeed
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+secretToken))
	resp, err := client.DiscoverSchemaTransform(ctx, &jobmanagement_v1.DiscoverSchemaTransformRequest{})
	if err != nil {
		t.Fatalf("expected success with correct auth token, got: %v", err)
	}
	if len(resp.GetSchemaTransformConfigs()) == 0 {
		t.Errorf("expected schema transform configs, got none")
	}
}
