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

// Package main demonstrates zero-downtime schema evolution handling during
// continuous PostgreSQL CDC streaming.
//
// Operational Problem:
// Production databases evolve schemas dynamically (e.g. ALTER TABLE orders ADD COLUMN loyalty_tier VARCHAR).
// Logical replication streams relation metadata (RelationMessage) whenever DDL occurs.
// Rigid pipelines crash or drop records when new, unknown columns arrive or when
// older records lack newly required attributes.
//
// Remediation:
//   1. Implement a resilient dynamic DoFn that inspects ChangeEvent tuple attributes.
//   2. Fallback to schema-aware defaults when columns are missing from older records.
//   3. Capture newly introduced columns into a flexible evolution bag / JSON attribute.
//   4. Increment the schema_drift_detected_total Beam metric to notify operations
//      of upstream DDL migrations.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "beammeup", "PostgreSQL database name")
	username = flag.String("username", "beam_navigator", "PostgreSQL user")
	password = flag.String("password", "beam_navigator", "PostgreSQL password")
	sslMode  = flag.String("sslmode", "disable", "PostgreSQL SSL mode")
	table    = flag.String("table", "public.evolved_orders", "Target table")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*EvolvedOrderRecord)(nil)).Elem())
	beam.RegisterDoFn(&schemaAdaptiveTransformFn{})
}

// EvolvedOrderRecord represents an order record capable of accommodating schema evolution.
type EvolvedOrderRecord struct {
	OrderID     int64     `json:"order_id" db:"order_id" beam:"order_id"`
	CustomerID  string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	Amount      float64   `json:"amount" db:"amount" beam:"amount"`
	LoyaltyTier string    `json:"loyalty_tier" db:"loyalty_tier" beam:"loyalty_tier"` // Added in v2
	ExtraFields string    `json:"extra_fields" db:"extra_fields" beam:"extra_fields"` // JSON bag for future columns
	SchemaVer   int       `json:"schema_ver" db:"schema_ver" beam:"schema_ver"`
	SyncedAt    time.Time `json:"synced_at" db:"synced_at" beam:"synced_at"`
}

type schemaAdaptiveTransformFn struct {
	driftCounter *metrics.Counter
}

func newSchemaAdaptiveTransformFn() *schemaAdaptiveTransformFn {
	return &schemaAdaptiveTransformFn{
		driftCounter: metrics.NewCounter("postgresio_cdc", "schema_drift_detected_total"),
	}
}

func (fn *schemaAdaptiveTransformFn) ProcessElement(ctx context.Context, evt postgresio.ChangeEvent, emit func(EvolvedOrderRecord)) {
	if evt.Operation != postgresio.OpInsert && evt.Operation != postgresio.OpUpdate {
		return
	}

	raw := evt.After
	if raw == nil {
		return
	}

	orderID, _ := raw["order_id"].(int64)
	if orderID == 0 {
		if f, ok := raw["order_id"].(float64); ok {
			orderID = int64(f)
		}
	}

	customerID, _ := raw["customer_id"].(string)
	if customerID == "" {
		customerID = "UNKNOWN"
	}

	amount, _ := raw["amount"].(float64)

	// Detect schema evolution: loyalty_tier introduced in v2
	loyaltyTier, hasLoyalty := raw["loyalty_tier"].(string)
	schemaVer := 1
	if hasLoyalty && loyaltyTier != "" {
		schemaVer = 2
	} else {
		loyaltyTier = "STANDARD" // Safe backward-compatible default
	}

	// Capture any unanticipated columns dynamically into extra_fields
	knownKeys := map[string]bool{
		"order_id":     true,
		"customer_id":  true,
		"amount":       true,
		"loyalty_tier": true,
	}
	extra := make(map[string]any)
	for k, v := range raw {
		if !knownKeys[k] {
			extra[k] = v
			fn.driftCounter.Inc(ctx, 1)
			schemaVer = 3
		}
	}
	extraJSON, _ := json.Marshal(extra)

	emit(EvolvedOrderRecord{
		OrderID:     orderID,
		CustomerID:  customerID,
		Amount:      amount,
		LoyaltyTier: loyaltyTier,
		ExtraFields: string(extraJSON),
		SchemaVer:   schemaVer,
		SyncedAt:    time.Now().UTC(),
	})
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// Ingest from CDC stream
	cdcOptions := []postgresio.CDCOption{
		postgresio.WithCDCHost(*host),
		postgresio.WithCDCPort(*port),
		postgresio.WithCDCDatabase(*database),
		postgresio.WithCDCUsername(*username),
		postgresio.WithCDCPassword(*password),
		postgresio.WithCDCSSLMode(*sslMode),
		postgresio.WithCDCSlotName("schema_evolution_slot"),
		postgresio.WithCDCPublication("orders_pub"),
	}
	stream := postgresio.ReadCDC(s, cdcOptions...)

	// Transform with schema adaptation and drift tracking
	evolved := beam.ParDo(s, newSchemaAdaptiveTransformFn(), stream)

	// Sink to target table
	writeOptions := postgresio.NewWriteOptions(
		postgresio.WithHost(*host),
		postgresio.WithPort(*port),
		postgresio.WithDatabase(*database),
		postgresio.WithUsername(*username),
		postgresio.WithPassword(*password),
		postgresio.WithSSLMode(*sslMode),
		postgresio.WithPrimaryKeyColumns("order_id"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
		postgresio.WithBatchSize(1000),
	)
	postgresio.Write(s, *table, writeOptions, evolved)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
}
