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
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestSchemaAdaptiveTransformFn(t *testing.T) {
	fn := newSchemaAdaptiveTransformFn()
	ctx := context.Background()

	t.Run("v1 record backward compatibility with defaults", func(t *testing.T) {
		var emitted []EvolvedOrderRecord
		emit := func(r EvolvedOrderRecord) {
			emitted = append(emitted, r)
		}

		evt := postgresio.ChangeEvent{
			Operation: postgresio.OpInsert,
			After: map[string]any{
				"order_id":    int64(1001),
				"customer_id": "CUST-A",
				"amount":      100.0,
			},
		}

		fn.ProcessElement(ctx, evt, emit)

		if len(emitted) != 1 {
			t.Fatalf("expected 1 record emitted, got %d", len(emitted))
		}
		r := emitted[0]
		if r.OrderID != 1001 {
			t.Errorf("expected OrderID 1001, got %d", r.OrderID)
		}
		if r.LoyaltyTier != "STANDARD" {
			t.Errorf("expected default LoyaltyTier STANDARD, got %s", r.LoyaltyTier)
		}
		if r.SchemaVer != 1 {
			t.Errorf("expected SchemaVer 1, got %d", r.SchemaVer)
		}
	})

	t.Run("v2 record with newly added column", func(t *testing.T) {
		var emitted []EvolvedOrderRecord
		emit := func(r EvolvedOrderRecord) {
			emitted = append(emitted, r)
		}

		evt := postgresio.ChangeEvent{
			Operation: postgresio.OpInsert,
			After: map[string]any{
				"order_id":     int64(1002),
				"customer_id":  "CUST-B",
				"amount":       250.0,
				"loyalty_tier": "PLATINUM",
			},
		}

		fn.ProcessElement(ctx, evt, emit)

		if len(emitted) != 1 {
			t.Fatalf("expected 1 record emitted, got %d", len(emitted))
		}
		r := emitted[0]
		if r.LoyaltyTier != "PLATINUM" {
			t.Errorf("expected LoyaltyTier PLATINUM, got %s", r.LoyaltyTier)
		}
		if r.SchemaVer != 2 {
			t.Errorf("expected SchemaVer 2, got %d", r.SchemaVer)
		}
	})

	t.Run("v3 record with dynamic drift extra fields", func(t *testing.T) {
		var emitted []EvolvedOrderRecord
		emit := func(r EvolvedOrderRecord) {
			emitted = append(emitted, r)
		}

		evt := postgresio.ChangeEvent{
			Operation: postgresio.OpInsert,
			After: map[string]any{
				"order_id":         int64(1003),
				"customer_id":      "CUST-C",
				"amount":           300.0,
				"loyalty_tier":     "GOLD",
				"reward_points":    float64(500),
				"referral_code_id": "REF-999",
			},
		}

		fn.ProcessElement(ctx, evt, emit)

		if len(emitted) != 1 {
			t.Fatalf("expected 1 record emitted, got %d", len(emitted))
		}
		r := emitted[0]
		if r.SchemaVer != 3 {
			t.Errorf("expected SchemaVer 3, got %d", r.SchemaVer)
		}
		if !strings.Contains(r.ExtraFields, "reward_points") || !strings.Contains(r.ExtraFields, "referral_code_id") {
			t.Errorf("expected extra fields to contain new columns, got %s", r.ExtraFields)
		}
	})
}
