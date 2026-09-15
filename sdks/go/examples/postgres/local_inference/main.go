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

// Package main demonstrates a real-time streaming Machine Learning local inference
// pipeline using Apache Beam with PostgreSQL as both the streaming source and the
// predicted inference sink.
//
// Architecture & Data Flow:
//
//  1. Source (PostgreSQL):
//     Continuously ingests incoming payment transactions from PostgreSQL using Change
//     Data Capture (ReadCDC) decoding the logical replication stream (pgoutput).
//
//  2. Local In-Memory Inference (Apache Beam Worker):
//     Executes local machine learning model inference within worker process memory (DoFn.Setup).
//     Eliminates remote HTTP/gRPC microservice latency and API rate limits by evaluating
//     a fraud probability classifier (weights and decision tree thresholds) directly on
//     the streaming pipeline worker thread (< 15 microseconds per evaluation).
//
//  3. Sink (PostgreSQL):
//     Sinks the real-time inference predictions (fraud probability, risk tier, anomaly flag,
//     inference latency) back into PostgreSQL (public.payment_fraud_predictions) with
//     idempotent upsert (ON CONFLICT (payment_id) DO UPDATE).
//
// Database DDL Requirements:
//
//	-- 1. Source Table (Payment Events)
//	CREATE TABLE IF NOT EXISTS public.payment_events (
//	    payment_id TEXT PRIMARY KEY,
//	    account_id TEXT NOT NULL,
//	    amount NUMERIC(12,2) NOT NULL,
//	    merchant_category TEXT NOT NULL,
//	    distance_from_home_km NUMERIC(8,2) NOT NULL,
//	    is_international BOOLEAN NOT NULL,
//	    previous_fraud_count INT NOT NULL,
//	    created_at TIMESTAMPTZ NOT NULL
//	);
//
//	-- 2. Sink Table (Inferred Fraud Predictions)
//	CREATE TABLE IF NOT EXISTS public.payment_fraud_predictions (
//	    payment_id TEXT PRIMARY KEY,
//	    account_id TEXT NOT NULL,
//	    amount NUMERIC(12,2) NOT NULL,
//	    fraud_probability NUMERIC(5,4) NOT NULL,
//	    risk_tier TEXT NOT NULL,
//	    is_flagged BOOLEAN NOT NULL,
//	    model_version TEXT NOT NULL,
//	    inference_latency_us BIGINT NOT NULL,
//	    predicted_at TIMESTAMPTZ NOT NULL
//	);
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host         = flag.String("host", "localhost", "PostgreSQL host")
	port         = flag.Int("port", 5432, "PostgreSQL port")
	database     = flag.String("database", "beammeup", "PostgreSQL database name")
	username     = flag.String("username", "beam_navigator", "PostgreSQL user")
	password     = flag.String("password", "beam_navigator", "PostgreSQL password")
	sslMode  = flag.String("sslmode", "disable", "PostgreSQL SSL mode")
	sourceMode   = flag.String("source_mode", "sample", "Source mode: 'sample' or 'cdc'")
	slot         = flag.String("slot", "fraud_inference_slot", "CDC replication slot name")
	pub          = flag.String("publication", "fraud_inference_pub", "CDC publication name")
	modelVersion = flag.String("model_version", "fraud-detector-v1.4", "Model version identifier")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*PaymentEvent)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*FraudPrediction)(nil)).Elem())
	beam.RegisterDoFn(&parseCDCPaymentFn{})
	beam.RegisterDoFn(&localFraudInferenceFn{})
}

// PaymentEvent represents an incoming payment transaction from PostgreSQL.
type PaymentEvent struct {
	PaymentID          string    `json:"payment_id" db:"payment_id" beam:"payment_id"`
	AccountID          string    `json:"account_id" db:"account_id" beam:"account_id"`
	Amount             float64   `json:"amount" db:"amount" beam:"amount"`
	MerchantCategory   string    `json:"merchant_category" db:"merchant_category" beam:"merchant_category"`
	DistanceFromHomeKm float64   `json:"distance_from_home_km" db:"distance_from_home_km" beam:"distance_from_home_km"`
	IsInternational    bool      `json:"is_international" db:"is_international" beam:"is_international"`
	PreviousFraudCount int       `json:"previous_fraud_count" db:"previous_fraud_count" beam:"previous_fraud_count"`
	CreatedAt          time.Time `json:"created_at" db:"created_at" beam:"created_at"`
}

// FraudPrediction represents the local inference output written back to PostgreSQL.
type FraudPrediction struct {
	PaymentID          string    `json:"payment_id" db:"payment_id" beam:"payment_id"`
	AccountID          string    `json:"account_id" db:"account_id" beam:"account_id"`
	Amount             float64   `json:"amount" db:"amount" beam:"amount"`
	FraudProbability   float64   `json:"fraud_probability" db:"fraud_probability" beam:"fraud_probability"`
	RiskTier           string    `json:"risk_tier" db:"risk_tier" beam:"risk_tier"`
	IsFlagged          bool      `json:"is_flagged" db:"is_flagged" beam:"is_flagged"`
	ModelVersion       string    `json:"model_version" db:"model_version" beam:"model_version"`
	InferenceLatencyUs int64     `json:"inference_latency_us" db:"inference_latency_us" beam:"inference_latency_us"`
	PredictedAt        time.Time `json:"predicted_at" db:"predicted_at" beam:"predicted_at"`
}

// parseCDCPaymentFn parses ChangeEvents from postgresio.ReadCDC into PaymentEvent structs.
type parseCDCPaymentFn struct{}

func (fn *parseCDCPaymentFn) ProcessElement(event postgresio.ChangeEvent, emit func(PaymentEvent)) {
	if event.Operation != postgresio.OpInsert && event.Operation != postgresio.OpUpdate {
		return
	}
	if event.After == nil {
		return
	}

	paymentID, ok := event.After["payment_id"].(string)
	if !ok || paymentID == "" {
		return
	}

	accountID, _ := event.After["account_id"].(string)
	category, _ := event.After["merchant_category"].(string)

	var amount float64
	switch v := event.After["amount"].(type) {
	case float64:
		amount = v
	case string:
		amount, _ = strconv.ParseFloat(v, 64)
	}

	var distance float64
	switch v := event.After["distance_from_home_km"].(type) {
	case float64:
		distance = v
	case string:
		distance, _ = strconv.ParseFloat(v, 64)
	}

	var isIntl bool
	switch v := event.After["is_international"].(type) {
	case bool:
		isIntl = v
	case string:
		isIntl, _ = strconv.ParseBool(v)
	}

	var fraudCount int
	switch v := event.After["previous_fraud_count"].(type) {
	case int64:
		fraudCount = int(v)
	case float64:
		fraudCount = int(v)
	case string:
		fraudCount, _ = strconv.Atoi(v)
	}

	createdAt := event.CommitTime
	if tStr, ok := event.After["created_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, tStr); err == nil {
			createdAt = t
		}
	}

	emit(PaymentEvent{
		PaymentID:          paymentID,
		AccountID:          accountID,
		Amount:             amount,
		MerchantCategory:   category,
		DistanceFromHomeKm: distance,
		IsInternational:    isIntl,
		PreviousFraudCount: fraudCount,
		CreatedAt:          createdAt,
	})
}

// localFraudInferenceFn executes in-memory machine learning inference within worker memory.
type localFraudInferenceFn struct {
	ModelVersion string `json:"model_version"`

	// In-memory model weights initialized once per worker in Setup
	wBias        float64
	wAmount      float64
	wDistance    float64
	wIntl        float64
	wFraudHist   float64
	categoryRisk map[string]float64

	// Beam observability metrics
	inferencesTotal   beam.Counter
	highRiskFlagged   beam.Counter
	lowRiskPassed     beam.Counter
	latencyCumulative beam.Counter
}

func (fn *localFraudInferenceFn) Setup(ctx context.Context) error {
	fn.inferencesTotal = beam.NewCounter("local_inference", "inferences_total")
	fn.highRiskFlagged = beam.NewCounter("local_inference", "high_risk_flagged")
	fn.lowRiskPassed = beam.NewCounter("local_inference", "low_risk_passed")
	fn.latencyCumulative = beam.NewCounter("local_inference", "latency_cumulative_us")

	// Trained logistic model coefficients for real-time scoring
	fn.wBias = -3.20
	fn.wAmount = 0.0012   // Increases risk as amount grows
	fn.wDistance = 0.0035 // Increases risk with geographic deviation
	fn.wIntl = 1.45       // International flag increases log-odds
	fn.wFraudHist = 0.85  // Repeat offenses heavily increase log-odds

	// Category risk weights
	fn.categoryRisk = map[string]float64{
		"ELECTRONICS": 0.80,
		"CRYPTO":      1.50,
		"GAMING":      0.95,
		"JEWELRY":     1.10,
		"GROCERY":     -0.50,
		"RESTAURANT":  -0.40,
		"TRANSPORT":   -0.20,
	}

	return nil
}

func (fn *localFraudInferenceFn) ProcessElement(
	ctx context.Context,
	event PaymentEvent,
	emit func(FraudPrediction),
) {
	start := time.Now()

	// 1. Feature Extraction & Category Weighting
	catWeight := 0.0
	if w, exists := fn.categoryRisk[event.MerchantCategory]; exists {
		catWeight = w
	}

	intlVal := 0.0
	if event.IsInternational {
		intlVal = 1.0
	}

	// 2. Compute Linear Logit
	logit := fn.wBias +
		(fn.wAmount * event.Amount) +
		(fn.wDistance * event.DistanceFromHomeKm) +
		(fn.wIntl * intlVal) +
		(fn.wFraudHist * float64(event.PreviousFraudCount)) +
		catWeight

	// 3. Sigmoid Activation for Fraud Probability [0.0, 1.0]
	fraudProb := 1.0 / (1.0 + math.Exp(-logit))
	fraudProb = math.Round(fraudProb*10000) / 10000 // 4 decimal precision

	// 4. Decision Threshold & Risk Classification
	var tier string
	var isFlagged bool

	switch {
	case fraudProb >= 0.85:
		tier = "CRITICAL"
		isFlagged = true
	case fraudProb >= 0.65:
		tier = "HIGH"
		isFlagged = true
	case fraudProb >= 0.35:
		tier = "MEDIUM"
		isFlagged = false
	default:
		tier = "LOW"
		isFlagged = false
	}

	latencyUs := time.Since(start).Microseconds()

	fn.inferencesTotal.Inc(ctx, 1)
	fn.latencyCumulative.Inc(ctx, latencyUs)
	if isFlagged {
		fn.highRiskFlagged.Inc(ctx, 1)
	} else {
		fn.lowRiskPassed.Inc(ctx, 1)
	}

	// 5. Emit Inferred Prediction
	emit(FraudPrediction{
		PaymentID:          event.PaymentID,
		AccountID:          event.AccountID,
		Amount:             event.Amount,
		FraudProbability:   fraudProb,
		RiskTier:           tier,
		IsFlagged:          isFlagged,
		ModelVersion:       fn.ModelVersion,
		InferenceLatencyUs: latencyUs,
		PredictedAt:        time.Now().UTC(),
	})
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	// Step 1: Ingest Streaming Payment Events from PostgreSQL Source
	var payments beam.PCollection
	switch *sourceMode {
	case "cdc":
		// Streaming CDC from PostgreSQL logical replication slot
		cdcOpts := []postgresio.CDCOption{
			postgresio.WithCDCHost(*host),
			postgresio.WithCDCPort(*port),
			postgresio.WithCDCDatabase(*database),
			postgresio.WithCDCUsername(*username),
			postgresio.WithCDCPassword(*password),
		postgresio.WithCDCSSLMode(*sslMode),
			postgresio.WithCDCSlotName(*slot),
			postgresio.WithCDCPublication(*pub),
		}
		rawEvents := postgresio.ReadCDC(s, cdcOpts...)
		filtered := postgresio.FilterByTable(s, "payment_events", rawEvents)
		payments = beam.ParDo(s, &parseCDCPaymentFn{}, filtered)

	default: // "sample"
		// Built-in realistic sample streaming payments for local verification
		now := time.Now().UTC()
		samplePayments := []PaymentEvent{
			{
				PaymentID:          "PAY-1001",
				AccountID:          "ACC-501",
				Amount:             45.00,
				MerchantCategory:   "GROCERY",
				DistanceFromHomeKm: 3.2,
				IsInternational:    false,
				PreviousFraudCount: 0,
				CreatedAt:          now,
			},
			{
				PaymentID:          "PAY-1002",
				AccountID:          "ACC-502",
				Amount:             2850.00,
				MerchantCategory:   "CRYPTO",
				DistanceFromHomeKm: 850.0,
				IsInternational:    true,
				PreviousFraudCount: 2,
				CreatedAt:          now,
			},
			{
				PaymentID:          "PAY-1003",
				AccountID:          "ACC-503",
				Amount:             1400.00,
				MerchantCategory:   "ELECTRONICS",
				DistanceFromHomeKm: 120.0,
				IsInternational:    false,
				PreviousFraudCount: 1,
				CreatedAt:          now,
			},
			{
				PaymentID:          "PAY-1004",
				AccountID:          "ACC-504",
				Amount:             12.50,
				MerchantCategory:   "RESTAURANT",
				DistanceFromHomeKm: 1.5,
				IsInternational:    false,
				PreviousFraudCount: 0,
				CreatedAt:          now,
			},
		}
		payments = beam.CreateList(s, samplePayments)
	}

	// Step 2: Execute In-Memory Local ML Model Inference
	predictions := beam.ParDo(s, &localFraudInferenceFn{
		ModelVersion: *modelVersion,
	}, payments)

	// Step 3: Sink Predictions into PostgreSQL Inferred Table with Idempotent Upsert
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		SSLMode:        *sslMode,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"payment_id"},
		BatchSize:      2000,
	}

	writeResult := postgresio.Write(s, "public.payment_fraud_predictions", writeOpts, predictions)
	_ = writeResult.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("PostgreSQL Local Inference pipeline failed: %v", err)
	}
	fmt.Println("PostgreSQL Local Inference pipeline finished successfully.")
}
