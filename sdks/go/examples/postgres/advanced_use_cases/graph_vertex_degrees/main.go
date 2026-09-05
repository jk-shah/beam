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

// Package main demonstrates an advanced graph degree and topological centrality pipeline
// using Apache Beam with PostgreSQL.
//
// Pattern:
// Ingest directed edges from PostgreSQL, decompose each edge into directional degree
// and weight contributions, aggregate contributions per vertex via distributed GroupByKey,
// and compute in-degree, out-degree, total degree, and average edge weight.
// Computing network topology, node influence, and connectivity in fraud detection
// networks, telecommunication graphs, and recommendation engines.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "postgres", "PostgreSQL database name")
	username = flag.String("username", "beam_test", "PostgreSQL user")
	password = flag.String("password", "beam_test", "PostgreSQL password")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*GraphEdge)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*DegreeContribution)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*NodeCentralitySummary)(nil)).Elem())
	beam.RegisterDoFn(&emitEdgeContributionsFn{})
	beam.RegisterDoFn(&aggregateNodeCentralityFn{})
}

// GraphEdge represents a directed weighted edge in a graph topology.
type GraphEdge struct {
	SourceNode string  `json:"source_node" db:"source_node" beam:"source_node"`
	TargetNode string  `json:"target_node" db:"target_node" beam:"target_node"`
	Weight     float64 `json:"weight" db:"weight" beam:"weight"`
}

// DegreeContribution captures directional degree and edge weight for a node.
type DegreeContribution struct {
	InDegreeDelta  int32   `json:"in_degree_delta" beam:"in_degree_delta"`
	OutDegreeDelta int32   `json:"out_degree_delta" beam:"out_degree_delta"`
	Weight         float64 `json:"weight" beam:"weight"`
}

// NodeCentralitySummary represents the computed graph metrics stored in PostgreSQL.
type NodeCentralitySummary struct {
	NodeID        string    `json:"node_id" db:"node_id" beam:"node_id"`
	InDegree      int32     `json:"in_degree" db:"in_degree" beam:"in_degree"`
	OutDegree     int32     `json:"out_degree" db:"out_degree" beam:"out_degree"`
	TotalDegree   int32     `json:"total_degree" db:"total_degree" beam:"total_degree"`
	AvgEdgeWeight float64   `json:"avg_edge_weight" db:"avg_edge_weight" beam:"avg_edge_weight"`
	ComputedAt    time.Time `json:"computed_at" db:"computed_at" beam:"computed_at"`
}

// emitEdgeContributionsFn generates bidirectional degree contributions for source and target nodes.
type emitEdgeContributionsFn struct{}

func (fn *emitEdgeContributionsFn) ProcessElement(e GraphEdge, emit func(string, DegreeContribution)) {
	// Source node has an outgoing edge
	emit(e.SourceNode, DegreeContribution{
		InDegreeDelta:  0,
		OutDegreeDelta: 1,
		Weight:         e.Weight,
	})
	// Target node has an incoming edge
	emit(e.TargetNode, DegreeContribution{
		InDegreeDelta:  1,
		OutDegreeDelta: 0,
		Weight:         e.Weight,
	})
}

// aggregateNodeCentralityFn sums incoming, outgoing, and total degrees and average weights.
type aggregateNodeCentralityFn struct{}

func (fn *aggregateNodeCentralityFn) ProcessElement(nodeID string, contribs func(*DegreeContribution) bool, emit func(NodeCentralitySummary)) {
	var inDeg int32
	var outDeg int32
	var totalWeight float64
	var edgeCount int32

	var c DegreeContribution
	for contribs(&c) {
		inDeg += c.InDegreeDelta
		outDeg += c.OutDegreeDelta
		totalWeight += c.Weight
		edgeCount++
	}

	var avgWeight float64
	if edgeCount > 0 {
		avgWeight = math.Round((totalWeight/float64(edgeCount))*10000) / 10000
	}

	emit(NodeCentralitySummary{
		NodeID:        nodeID,
		InDegree:      inDeg,
		OutDegree:     outDeg,
		TotalDegree:   inDeg + outDeg,
		AvgEdgeWeight: avgWeight,
		ComputedAt:    time.Now().UTC(),
	})
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	// Sample network edges (e.g. transaction flow between accounts or routing hubs)
	sampleEdges := []GraphEdge{
		{SourceNode: "HUB-A", TargetNode: "HUB-B", Weight: 1.5},
		{SourceNode: "HUB-A", TargetNode: "HUB-C", Weight: 2.0},
		{SourceNode: "HUB-B", TargetNode: "HUB-C", Weight: 1.0},
		{SourceNode: "HUB-C", TargetNode: "HUB-D", Weight: 3.5},
		{SourceNode: "HUB-B", TargetNode: "HUB-D", Weight: 2.5},
		{SourceNode: "HUB-D", TargetNode: "HUB-A", Weight: 4.0}, // feedback loop
	}

	edgesCol := beam.CreateList(s, sampleEdges)

	// 1. Emit directional degree contributions
	contribCol := beam.ParDo(s, &emitEdgeContributionsFn{}, edgesCol)

	// 2. Group all incident edges per vertex
	groupedCol := beam.GroupByKey(s, contribCol)

	// 3. Aggregate vertex degree and centrality metrics
	centralityCol := beam.ParDo(s, &aggregateNodeCentralityFn{}, groupedCol)

	// 4. Sink to PostgreSQL graph_node_centrality with upsert
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"node_id"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.graph_node_centrality", writeOpts, centralityCol)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Graph node degree centrality pipeline completed successfully.")
}
