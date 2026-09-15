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

// Package main provides a standalone gRPC expansion service for Apache Beam Go SchemaTransforms.
//
// Cross-language pipelines constructed in Python or YAML can execute this binary as a subprocess
// to dynamically expand Go SchemaTransforms into portable pipeline fragments.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/expansion"

	// Register postgresio SchemaTransforms into DefaultRegistry.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
)

var (
	host           = flag.String("host", "127.0.0.1", "Host or IP address to bind the expansion service to (default: loopback 127.0.0.1).")
	port           = flag.Int("port", 0, "Port to listen on (0 chooses an ephemeral port).")
	authToken      = flag.String("auth_token", "", "Optional bearer authentication token for incoming gRPC calls.")
	idleTimeout    = flag.Duration("idle_timeout", 0, "Optional idle timeout duration before automated graceful shutdown (0 disables).")
	containerImage = flag.String("container_image", "", "Override worker container image (defaults to BEAM_GO_WORKER_CONTAINER_IMAGE or apache/beam_go_sdk:latest).")
)

func main() {
	flag.Parse()

	if *containerImage != "" {
		_ = os.Setenv("BEAM_GO_WORKER_CONTAINER_IMAGE", *containerImage)
	}

	// Terminate immediately if parent process closes stdin (e.g. Python subprocess killed).
	expansion.StartStdinMonitor(nil)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	var opts []expansion.ServerOption
	if *authToken != "" {
		opts = append(opts, expansion.WithAuthToken(*authToken))
	}
	if *idleTimeout > 0 {
		opts = append(opts, expansion.WithIdleTimeout(*idleTimeout))
	}

	server := expansion.NewServer(opts...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	if err := server.Serve(ctx, lis); err != nil && err != context.Canceled {
		log.Fatalf("expansion service failed: %v", err)
	}
}
