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
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	jobmanagement_v1 "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// MaxGrpcMessageSize is the maximum size (32MB) for pipeline protos in expansion requests.
	MaxGrpcMessageSize = 32 * 1024 * 1024

	// DefaultWorkerContainerImage is the fallback container image if no environment override is provided.
	DefaultWorkerContainerImage = "apache/beam_go_sdk:latest"
)

// Server implements jobmanagement_v1.ExpansionServiceServer in Go.
type Server struct {
	jobmanagement_v1.UnimplementedExpansionServiceServer

	registry    *schematransform.Registry
	authToken   string
	idleTimeout time.Duration
	lastActive  time.Time
	mu          sync.Mutex
}

// ServerOption configures the expansion Server.
type ServerOption func(*Server)

// WithRegistry configures the SchemaTransform registry backing the expansion service.
func WithRegistry(r *schematransform.Registry) ServerOption {
	return func(s *Server) {
		s.registry = r
	}
}

// WithAuthToken configures bearer token authentication for incoming gRPC calls.
func WithAuthToken(token string) ServerOption {
	return func(s *Server) {
		s.authToken = token
	}
}

// WithIdleTimeout configures an idle shutdown duration for the server.
func WithIdleTimeout(d time.Duration) ServerOption {
	return func(s *Server) {
		s.idleTimeout = d
	}
}

// NewServer creates a new Go ExpansionServiceServer.
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		registry:   schematransform.DefaultRegistry(),
		lastActive: time.Now(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) verifyAuth(ctx context.Context) error {
	if s.authToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get("authorization")
	if len(tokens) == 0 {
		return status.Errorf(codes.Unauthenticated, "missing authorization token")
	}
	expected := fmt.Sprintf("Bearer %s", s.authToken)
	if tokens[0] != expected && tokens[0] != s.authToken {
		return status.Errorf(codes.PermissionDenied, "invalid authorization token")
	}
	return nil
}

func (s *Server) recordActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActive = time.Now()
}

// DiscoverSchemaTransform returns registered SchemaTransforms and their pre-computed schemas.
func (s *Server) DiscoverSchemaTransform(
	ctx context.Context,
	req *jobmanagement_v1.DiscoverSchemaTransformRequest,
) (*jobmanagement_v1.DiscoverSchemaTransformResponse, error) {
	if err := s.verifyAuth(ctx); err != nil {
		return nil, err
	}
	s.recordActivity()

	configs := make(map[string]*jobmanagement_v1.SchemaTransformConfig)
	entries := s.registry.GetAllWithSchemas()

	for id, entry := range entries {
		configs[id] = &jobmanagement_v1.SchemaTransformConfig{
			ConfigSchema:           entry.Schema,
			InputPcollectionNames:  entry.Provider.InputCollectionNames(),
			OutputPcollectionNames: entry.Provider.OutputCollectionNames(),
			Description:            entry.Provider.Description(),
		}
	}

	return &jobmanagement_v1.DiscoverSchemaTransformResponse{
		SchemaTransformConfigs: configs,
	}, nil
}

// Expand compiles and translates a Go SchemaTransform into a portable pipeline subgraph.
func (s *Server) Expand(
	ctx context.Context,
	req *jobmanagement_v1.ExpansionRequest,
) (*jobmanagement_v1.ExpansionResponse, error) {
	if err := s.verifyAuth(ctx); err != nil {
		return nil, err
	}
	s.recordActivity()

	if req.GetTransform() == nil || req.GetTransform().GetSpec() == nil {
		return &jobmanagement_v1.ExpansionResponse{
			Error: "ExpansionRequest missing transform spec",
		}, nil
	}

	urn := req.GetTransform().GetSpec().GetUrn()
	provider, _, exists := s.registry.Get(urn)
	if !exists {
		return &jobmanagement_v1.ExpansionResponse{
			Error: fmt.Sprintf("unrecognized SchemaTransform URN: '%s'", urn),
		}, nil
	}

	// 1. Unmarshal configuration payload
	configVal := reflect.New(provider.ConfigType()).Interface()
	payload := req.GetTransform().GetSpec().GetPayload()
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, configVal); err != nil {
			return &jobmanagement_v1.ExpansionResponse{
				Error: fmt.Sprintf("failed to decode configuration payload for %s: %v", urn, err),
			}, nil
		}
	}

	// 2. Validate configuration
	if val, ok := configVal.(schematransform.Validatable); ok {
		if err := val.Validate(); err != nil {
			return &jobmanagement_v1.ExpansionResponse{
				Error: fmt.Sprintf("configuration validation failed for %s: %v", urn, err),
			}, nil
		}
	}

	// 3. Create SchemaTransform
	tf, err := provider.CreateTransform(reflect.ValueOf(configVal).Elem().Interface())
	if err != nil {
		return &jobmanagement_v1.ExpansionResponse{
			Error: fmt.Sprintf("failed to instantiate SchemaTransform %s: %v", urn, err),
		}, nil
	}

	// 4. Build isolated pipeline subgraph (Zero-state scope isolation)
	pipeline := beam.NewPipeline()
	root := pipeline.Root()
	inputs := make(map[string]beam.PCollection)
	outputs, err := tf.BuildTransform(root, inputs)
	if err != nil {
		return &jobmanagement_v1.ExpansionResponse{
			Error: fmt.Sprintf("failed to build transform subgraph for %s: %v", urn, err),
		}, nil
	}

	// 5. Resolve worker container environment
	containerImage := os.Getenv("BEAM_GO_WORKER_CONTAINER_IMAGE")
	if containerImage == "" {
		containerImage = DefaultWorkerContainerImage
	}

	dockerPayload := protox.MustEncode(&pipepb.DockerPayload{
		ContainerImage: containerImage,
	})

	env := &pipepb.Environment{
		Urn:     "beam:env:docker:v1",
		Payload: dockerPayload,
		Capabilities: []string{
			"beam:coder:row:v1",
			"beam:protocol:progress_reporting:v0",
		},
	}

	// 6. Marshal graph into model pipeline
	edges, _, err := pipeline.Build()
	if err != nil {
		return &jobmanagement_v1.ExpansionResponse{
			Error: fmt.Sprintf("failed to compile pipeline graph: %v", err),
		}, nil
	}

	marshaledPipe, err := graphx.Marshal(edges, &graphx.Options{
		Environment: env,
	})
	if err != nil {
		return &jobmanagement_v1.ExpansionResponse{
			Error: fmt.Sprintf("failed to marshal pipeline components: %v", err),
		}, nil
	}

	// 7. Namespace components if request specifies a namespace
	components := marshaledPipe.Components
	if req.GetNamespace() != "" {
		components = namespaceComponents(components, req.GetNamespace())
	}

	// Construct root composite PTransform
	expandedTransform := &pipepb.PTransform{
		UniqueName: req.GetTransform().GetUniqueName(),
		Spec:       req.GetTransform().GetSpec(),
		Inputs:     req.GetTransform().GetInputs(),
		Outputs:    make(map[string]string),
	}

	for tag := range outputs {
		expandedTransform.Outputs[tag] = fmt.Sprintf("%s%s", req.GetNamespace(), tag)
	}

	return &jobmanagement_v1.ExpansionResponse{
		Components:   components,
		Transform:    expandedTransform,
		Requirements: marshaledPipe.Requirements,
	}, nil
}

func namespaceComponents(c *pipepb.Components, ns string) *pipepb.Components {
	if c == nil || ns == "" {
		return c
	}
	out := &pipepb.Components{
		Transforms:          make(map[string]*pipepb.PTransform, len(c.Transforms)),
		Pcollections:        make(map[string]*pipepb.PCollection, len(c.Pcollections)),
		WindowingStrategies: make(map[string]*pipepb.WindowingStrategy, len(c.WindowingStrategies)),
		Coders:              make(map[string]*pipepb.Coder, len(c.Coders)),
		Environments:        make(map[string]*pipepb.Environment, len(c.Environments)),
	}

	for id, t := range c.Transforms {
		out.Transforms[fmt.Sprintf("%s%s", ns, id)] = t
	}
	for id, p := range c.Pcollections {
		out.Pcollections[fmt.Sprintf("%s%s", ns, id)] = p
	}
	for id, w := range c.WindowingStrategies {
		out.WindowingStrategies[fmt.Sprintf("%s%s", ns, id)] = w
	}
	for id, coder := range c.Coders {
		out.Coders[fmt.Sprintf("%s%s", ns, id)] = coder
	}
	for id, env := range c.Environments {
		out.Environments[fmt.Sprintf("%s%s", ns, id)] = env
	}

	return out
}

// Serve starts the gRPC expansion service server on the provided listener.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(MaxGrpcMessageSize),
		grpc.MaxSendMsgSize(MaxGrpcMessageSize),
	)

	jobmanagement_v1.RegisterExpansionServiceServer(grpcServer, s)

	// Emit readiness handshake string
	port := lis.Addr().(*net.TCPAddr).Port
	handshake := FormatHandshake(port, s.authToken)
	fmt.Print(handshake)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
