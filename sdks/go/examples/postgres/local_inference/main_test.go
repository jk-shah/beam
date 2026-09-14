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

func TestParseCDCPaymentFn(t *testing.T) {
	fn := &parseCDCPaymentFn{}
	commitTime := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)

	event := postgresio.ChangeEvent{
		Operation:  postgresio.OpInsert,
		CommitTime: commitTime,
		After: map[string]any{
			"payment_id":            "PAY-TEST-1",
			"account_id":            "ACC-100",
			"amount":                500.00,
			"merchant_category":     "CRYPTO",
			"distance_from_home_km": 150.0,
			"is_international":      true,
			"previous_fraud_count":  int64(1),
			"created_at":            commitTime.Format(time.RFC3339),
		},
	}

	var emitted []PaymentEvent
	emit := func(p PaymentEvent) {
		emitted = append(emitted, p)
	}

	fn.ProcessElement(event, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted payment, got %d", len(emitted))
	}
	got := emitted[0]
	if got.PaymentID != "PAY-TEST-1" || got.Amount != 500.00 || got.MerchantCategory != "CRYPTO" || !got.IsInternational {
		t.Errorf("unexpected payment content: %+v", got)
	}
}

func TestLocalFraudInferenceFn_Scoring(t *testing.T) {
	fn := &localFraudInferenceFn{
		ModelVersion: "test-model-v1",
	}
	ctx := context.Background()
	if err := fn.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// 1. Low Risk Payment (Low amount, home proximity, domestic, grocery)
	lowRisk := PaymentEvent{
		PaymentID:          "PAY-LOW",
		AccountID:          "ACC-1",
		Amount:             25.00,
		MerchantCategory:   "GROCERY",
		DistanceFromHomeKm: 2.0,
		IsInternational:    false,
		PreviousFraudCount: 0,
		CreatedAt:          time.Now().UTC(),
	}

	var emitted []FraudPrediction
	emit := func(fp FraudPrediction) {
		emitted = append(emitted, fp)
	}

	fn.ProcessElement(ctx, lowRisk, emit)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 prediction, got %d", len(emitted))
	}
	if emitted[0].IsFlagged {
		t.Errorf("expected low risk payment not to be flagged, prob: %f", emitted[0].FraudProbability)
	}
	if emitted[0].RiskTier != "LOW" {
		t.Errorf("expected LOW risk tier, got: %s", emitted[0].RiskTier)
	}

	// 2. Critical Risk Payment (High amount, extreme distance, international, crypto, prior fraud)
	emitted = nil
	highRisk := PaymentEvent{
		PaymentID:          "PAY-HIGH",
		AccountID:          "ACC-2",
		Amount:             5000.00,
		MerchantCategory:   "CRYPTO",
		DistanceFromHomeKm: 2500.0,
		IsInternational:    true,
		PreviousFraudCount: 3,
		CreatedAt:          time.Now().UTC(),
	}

	fn.ProcessElement(ctx, highRisk, emit)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 prediction, got %d", len(emitted))
	}
	if !emitted[0].IsFlagged {
		t.Errorf("expected high risk payment to be flagged, prob: %f", emitted[0].FraudProbability)
	}
	if emitted[0].RiskTier != "CRITICAL" {
		t.Errorf("expected CRITICAL risk tier, got: %s", emitted[0].RiskTier)
	}
	if emitted[0].FraudProbability < 0.90 {
		t.Errorf("expected probability >= 0.90, got: %f", emitted[0].FraudProbability)
	}
}

func TestLocalInferencePipelineGraph(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	samplePayments := []PaymentEvent{
		{
			PaymentID:          "PAY-1",
			AccountID:          "ACC-1",
			Amount:             10.0,
			MerchantCategory:   "RESTAURANT",
			DistanceFromHomeKm: 1.0,
			IsInternational:    false,
			PreviousFraudCount: 0,
			CreatedAt:          time.Now().UTC(),
		},
	}
	payments := beam.CreateList(s, samplePayments)

	inferenceFn := &localFraudInferenceFn{ModelVersion: "unit-test-v1"}
	predictions := beam.ParDo(s, inferenceFn, payments)

	passert.Count(s, predictions, "prediction count", 1)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}
}
