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
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

// TestMain runs beam.Init() via ptest.Main, which the Beam runners require
// before any pipeline is constructed or executed.
func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestParseCDCOrderFn(t *testing.T) {
	fn := &parseCDCOrderFn{}
	commitTime := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)

	event := postgresio.ChangeEvent{
		Operation:  postgresio.OpInsert,
		CommitTime: commitTime,
		After: map[string]any{
			"order_id":    "ORD-999",
			"customer_id": "CUST-10",
			"item":        "Spanner Database",
			"amount":      1500.50,
			"ordered_at":  commitTime.Format(time.RFC3339),
		},
	}

	var emitted []Order
	emit := func(o Order) {
		emitted = append(emitted, o)
	}

	fn.ProcessElement(event, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted order, got %d", len(emitted))
	}
	got := emitted[0]
	if got.OrderID != "ORD-999" || got.CustomerID != "CUST-10" || got.Amount != 1500.50 {
		t.Errorf("unexpected order content: %+v", got)
	}
}

func TestEnrichOrderWithPostgresFn_CacheHitAndRules(t *testing.T) {
	fn := &enrichOrderWithPostgresFn{
		CacheTTLSec: 60,
		PoolSize:    1,
		cache:       make(map[string]cachedProfile),
	}

	// Pre-populate cache to test cache hit pathway without requiring live database
	fn.putCache("CUST-1", CustomerProfile{
		CustomerID:    "CUST-1",
		CustomerName:  "Test User",
		CustomerEmail: "test@example.com",
		LoyaltyTier:   "GOLD",
		CreditLimit:   1000.00,
		RiskScore:     25.0,
		UpdatedAt:     time.Now().UTC(),
	})

	fn.putCache("CUST-HIGH-RISK", CustomerProfile{
		CustomerID:    "CUST-HIGH-RISK",
		CustomerName:  "Risky User",
		CustomerEmail: "risky@example.com",
		LoyaltyTier:   "STANDARD",
		CreditLimit:   5000.00,
		RiskScore:     88.0,
		UpdatedAt:     time.Now().UTC(),
	})

	ctx := context.Background()

	// 1. Test Approved Order (Amount <= CreditLimit, RiskScore < 75)
	var emitted []EnrichedOrder
	emit := func(eo EnrichedOrder) {
		emitted = append(emitted, eo)
	}

	orderApproved := Order{
		OrderID:    "ORD-001",
		CustomerID: "CUST-1",
		Item:       "Cloud Workstation",
		Amount:     500.00,
		OrderedAt:  time.Now().UTC(),
	}

	if err := fn.ProcessElement(ctx, orderApproved, emit); err != nil {
		t.Fatalf("ProcessElement failed: %v", err)
	}

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted enriched order, got %d", len(emitted))
	}
	if !emitted[0].IsApproved {
		t.Errorf("expected order to be approved, got reason: %s", emitted[0].ApprovalReason)
	}
	if emitted[0].LoyaltyTier != "GOLD" {
		t.Errorf("expected GOLD tier, got %s", emitted[0].LoyaltyTier)
	}

	// 2. Test Rejected Order (RiskScore >= 75)
	emitted = nil
	orderRejectedRisk := Order{
		OrderID:    "ORD-002",
		CustomerID: "CUST-HIGH-RISK",
		Item:       "GPU Instance",
		Amount:     200.00,
		OrderedAt:  time.Now().UTC(),
	}

	if err := fn.ProcessElement(ctx, orderRejectedRisk, emit); err != nil {
		t.Fatalf("ProcessElement failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted enriched order, got %d", len(emitted))
	}
	if emitted[0].IsApproved {
		t.Errorf("expected order to be rejected due to risk score")
	}

	// 3. Test Rejected Order (Amount > CreditLimit)
	emitted = nil
	orderRejectedLimit := Order{
		OrderID:    "ORD-003",
		CustomerID: "CUST-1",
		Item:       "Enterprise Cluster",
		Amount:     5000.00, // exceeds 1000.00
		OrderedAt:  time.Now().UTC(),
	}

	if err := fn.ProcessElement(ctx, orderRejectedLimit, emit); err != nil {
		t.Fatalf("ProcessElement failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted enriched order, got %d", len(emitted))
	}
	if emitted[0].IsApproved {
		t.Errorf("expected order to be rejected due to credit limit")
	}
}

func TestEnrichmentPipelineGraph(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	sampleOrders := []Order{
		{OrderID: "ORD-1", CustomerID: "CUST-1", Item: "RAM", Amount: 100.0, OrderedAt: time.Now().UTC()},
	}
	orders := beam.CreateList(s, sampleOrders)

	enrichFn := &enrichOrderWithPostgresFn{
		ConnStr:     "host=localhost port=5432 dbname=beammeup user=beam_navigator password=beam_navigator sslmode=disable",
		CacheTTLSec: 300,
		PoolSize:    2,
		cache: map[string]cachedProfile{
			"CUST-1": {
				profile: CustomerProfile{
					CustomerID:   "CUST-1",
					CustomerName: "Alice",
					LoyaltyTier:  "PLATINUM",
					CreditLimit:  10000.0,
					RiskScore:    10.0,
				},
				expiresAt: time.Now().Add(10 * time.Minute),
			},
		},
	}
	enriched := beam.ParDo(s, enrichFn, orders)

	passert.Count(s, enriched, "enriched count", 1)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}
}
