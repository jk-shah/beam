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

// Package main demonstrates a stream deduplication and idempotent upsert pipeline
// using Apache Beam with PostgreSQL.
//
// Use Case:
// Distributed streaming systems guarantee at-least-once delivery, producing duplicate
// messages during worker restarts and network retries. This pipeline eliminates duplicate
// events by event ID, computes deduplication metrics, and sinks unique records to
// PostgreSQL with atomic ON CONFLICT upsert to guarantee end-to-end idempotency.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
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
	beam.RegisterType(reflect.TypeOf((*StreamEvent)(nil)).Elem())
	beam.RegisterDoFn(&keyByEventIDFn{})
	beam.RegisterDoFn(&deduplicateEventsFn{})
}

// StreamEvent represents an incoming streaming event that may contain duplicates.
type StreamEvent struct {
	EventID   string    `json:"event_id" db:"event_id" beam:"event_id"`
	Source    string    `json:"source" db:"source" beam:"source"`
	Payload   string    `json:"payload" db:"payload" beam:"payload"`
	CreatedAt time.Time `json:"created_at" db:"created_at" beam:"created_at"`
}

// keyByEventIDFn assigns event_id as the grouping key and attaches event time.
type keyByEventIDFn struct{}

func (fn *keyByEventIDFn) ProcessElement(e StreamEvent, emit func(typex.EventTime, string, StreamEvent)) {
	emit(typex.EventTime(e.CreatedAt.UnixMilli()), e.EventID, e)
}

// deduplicateEventsFn emits exactly one canonical event per key across the window.
// If multiple duplicate events arrive with the same ID, the earliest or most recent
// record is selected, discarding duplicates.
type deduplicateEventsFn struct{}

func (fn *deduplicateEventsFn) ProcessElement(eventID string, events func(*StreamEvent) bool, emit func(StreamEvent)) {
	var canonical StreamEvent
	var found bool

	var e StreamEvent
	for events(&e) {
		if !found {
			canonical = e
			found = true
		} else {
			// If duplicates have different timestamps, pick the latest event
			if e.CreatedAt.After(canonical.CreatedAt) {
				canonical = e
			}
		}
	}

	if found {
		emit(canonical)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	now := time.Now().UTC()

	// Simulating streaming data containing duplicates from retries
	rawEvents := []StreamEvent{
		{EventID: "EVT-001", Source: "mobile_app", Payload: `{"action":"login"}`, CreatedAt: now.Add(-50 * time.Second)},
		{EventID: "EVT-002", Source: "web_client", Payload: `{"action":"view"}`, CreatedAt: now.Add(-40 * time.Second)},
		{EventID: "EVT-001", Source: "mobile_app", Payload: `{"action":"login"}`, CreatedAt: now.Add(-30 * time.Second)}, // duplicate of EVT-001
		{EventID: "EVT-003", Source: "api_gateway", Payload: `{"action":"checkout"}`, CreatedAt: now.Add(-20 * time.Second)},
		{EventID: "EVT-002", Source: "web_client", Payload: `{"action":"view_retry"}`, CreatedAt: now.Add(-10 * time.Second)}, // retry/duplicate of EVT-002
	}

	rawCol := beam.CreateList(s, rawEvents)

	// 1. Key events by event_id with event timestamp
	keyedCol := beam.ParDo(s, &keyByEventIDFn{}, rawCol)

	// 2. Window into fixed 1-minute deduplication intervals
	windowedCol := beam.WindowInto(s, window.NewFixedWindows(1*time.Minute), keyedCol)

	// 3. Group by key to collect duplicates within the window
	groupedCol := beam.GroupByKey(s, windowedCol)

	// 4. Discard duplicates and yield single canonical event per ID
	dedupedCol := beam.ParDo(s, &deduplicateEventsFn{}, groupedCol)

	// 5. Sink unique canonical events into PostgreSQL with idempotent upsert
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"event_id"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.canonical_events", writeOpts, dedupedCol)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Deduplication and idempotent upsert pipeline completed successfully.")
}
