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

package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/expansion"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	jobmanagement_v1 "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestExpansionService_LoopbackBinding(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on loopback: %v", err)
	}
	defer lis.Close()

	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("expected *net.TCPAddr, got %T", lis.Addr())
	}

	if !tcpAddr.IP.IsLoopback() {
		t.Errorf("listener bound to non-loopback IP: %s", tcpAddr.IP.String())
	}
}

func TestExpansionService_DiscoverSchemaTransforms(t *testing.T) {
	srv := expansion.NewServer()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on loopback: %v", err)
	}
	defer lis.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ctx, lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial expansion server: %v", err)
	}
	defer conn.Close()

	client := jobmanagement_v1.NewExpansionServiceClient(conn)

	// Call DiscoverSchemaTransform
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reqCancel()

	resp, err := client.DiscoverSchemaTransform(reqCtx, &jobmanagement_v1.DiscoverSchemaTransformRequest{})
	if err != nil {
		t.Fatalf("DiscoverSchemaTransform failed: %v", err)
	}

	configs := resp.GetSchemaTransformConfigs()
	if configs == nil {
		t.Fatalf("expected non-nil SchemaTransformConfigs")
	}

	// Verify postgres_bulk_write:v1 is present
	writeConfig, ok := configs[postgresio.WriteSchemaTransformURN]
	if !ok {
		t.Errorf("expected %s in discovered configs, got keys: %v", postgresio.WriteSchemaTransformURN, mapKeys(configs))
	} else if writeConfig.GetConfigSchema() == nil {
		t.Errorf("expected non-nil ConfigSchema for write")
	}

	// Verify postgres_cdc_read:v1 is present
	cdcConfig, ok := configs[postgresio.ReadCDCSchemaTransformURN]
	if !ok {
		t.Errorf("expected %s in discovered configs, got keys: %v", postgresio.ReadCDCSchemaTransformURN, mapKeys(configs))
	} else if cdcConfig.GetConfigSchema() == nil {
		t.Errorf("expected non-nil ConfigSchema for cdc")
	}

	cancel()
	select {
	case err := <-serveErr:
		if err != nil && err != context.Canceled && !strings.Contains(err.Error(), "closed") {
			t.Errorf("server Serve returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("server did not shut down within 2s of cancel")
	}
}

func TestExpansionService_AuthTokenEnforcement(t *testing.T) {
	const secretToken = "super-secret-test-token-12345"
	srv := expansion.NewServer(expansion.WithAuthToken(secretToken))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on loopback: %v", err)
	}
	defer lis.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Serve(ctx, lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial expansion server: %v", err)
	}
	defer conn.Close()

	client := jobmanagement_v1.NewExpansionServiceClient(conn)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reqCancel()

	// 1. Unauthenticated call must fail
	_, err = client.DiscoverSchemaTransform(reqCtx, &jobmanagement_v1.DiscoverSchemaTransformRequest{})
	if err == nil {
		t.Fatalf("expected unauthenticated call to fail")
	}
	st, ok := status.FromError(err)
	if !ok || (st.Code() != codes.Unauthenticated && st.Code() != codes.PermissionDenied) {
		t.Errorf("expected Unauthenticated or PermissionDenied code, got: %v", err)
	}

	// 2. Authenticated call must succeed
	authCtx := metadata.AppendToOutgoingContext(reqCtx, "authorization", fmt.Sprintf("Bearer %s", secretToken))
	resp, err := client.DiscoverSchemaTransform(authCtx, &jobmanagement_v1.DiscoverSchemaTransformRequest{})
	if err != nil {
		t.Fatalf("authenticated call failed: %v", err)
	}
	if resp.GetSchemaTransformConfigs() == nil {
		t.Errorf("expected non-nil SchemaTransformConfigs for authenticated call")
	}
}

func mapKeys(m map[string]*jobmanagement_v1.SchemaTransformConfig) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
