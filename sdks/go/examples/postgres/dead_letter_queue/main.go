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

// Package main demonstrates an enterprise Dead-Letter Queue (DLQ) and multi-output
// anomaly routing pipeline using Apache Beam with PostgreSQL sources and sinks.
//
// Use Case:
// Production Dataflow pipelines must handle dirty, malformed, or fraudulent data
// without halting execution. This pipeline ingests incoming payments, performs
// business validation (positive amounts, valid currency codes, account existence),
// routes clean transactions to the primary production table, and routes rejected
// poison records to a dedicated PostgreSQL dead-letter queue table with error metadata.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "beam_test", "PostgreSQL database name")
	username = flag.String("username", "beam_test", "PostgreSQL user")
	password = flag.String("password", "beam_test", "PostgreSQL password")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*RawPayment)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*CleanPayment)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*DeadLetterPayment)(nil)).Elem())
	beam.RegisterDoFn(&validateAndRoutePaymentFn{})
}

// RawPayment represents an incoming payment record.
type RawPayment struct {
	PaymentID string  `json:"payment_id" db:"payment_id" beam:"payment_id"`
	AccountID string  `json:"account_id" db:"account_id" beam:"account_id"`
	Amount    float64 `json:"amount" db:"amount" beam:"amount"`
	Currency  string  `json:"currency" db:"currency" beam:"currency"`
	Payload   string  `json:"payload" db:"payload" beam:"payload"`
}

// CleanPayment represents a sanitized, validated payment ready for settlement.
type CleanPayment struct {
	PaymentID string    `json:"payment_id" db:"payment_id" beam:"payment_id"`
	AccountID string    `json:"account_id" db:"account_id" beam:"account_id"`
	Amount    float64   `json:"amount" db:"amount" beam:"amount"`
	Currency  string    `json:"currency" db:"currency" beam:"currency"`
	Status    string    `json:"status" db:"status" beam:"status"`
	CleanedAt time.Time `json:"cleaned_at" db:"cleaned_at" beam:"cleaned_at"`
}

// DeadLetterPayment captures a rejected payment with failure diagnostics.
type DeadLetterPayment struct {
	PaymentID    string    `json:"payment_id" db:"payment_id" beam:"payment_id"`
	AccountID    string    `json:"account_id" db:"account_id" beam:"account_id"`
	Amount       float64   `json:"amount" db:"amount" beam:"amount"`
	ErrorCode    string    `json:"error_code" db:"error_code" beam:"error_code"`
	ErrorMessage string    `json:"error_message" db:"error_message" beam:"error_message"`
	RawPayload   string    `json:"raw_payload" db:"raw_payload" beam:"raw_payload"`
	RejectedAt   time.Time `json:"rejected_at" db:"rejected_at" beam:"rejected_at"`
}

// validateAndRoutePaymentFn inspects each payment and bifurcates valid records
// from invalid poison records using Beam multi-output.
type validateAndRoutePaymentFn struct{}

var supportedCurrencies = map[string]bool{
	"USD": true,
	"EUR": true,
	"GBP": true,
	"CAD": true,
	"JPY": true,
}

func (fn *validateAndRoutePaymentFn) ProcessElement(
	p RawPayment,
	emitValid func(CleanPayment),
	emitDLQ func(DeadLetterPayment),
) {
	now := time.Now().UTC()

	// Validation Rule 1: Account ID must not be empty
	if strings.TrimSpace(p.AccountID) == "" {
		emitDLQ(DeadLetterPayment{
			PaymentID:    p.PaymentID,
			AccountID:    p.AccountID,
			Amount:       p.Amount,
			ErrorCode:    "ERR_MISSING_ACCOUNT",
			ErrorMessage: "account_id cannot be blank or whitespace",
			RawPayload:   p.Payload,
			RejectedAt:   now,
		})
		return
	}

	// Validation Rule 2: Amount must be strictly positive
	if p.Amount <= 0.0 {
		emitDLQ(DeadLetterPayment{
			PaymentID:    p.PaymentID,
			AccountID:    p.AccountID,
			Amount:       p.Amount,
			ErrorCode:    "ERR_INVALID_AMOUNT",
			ErrorMessage: fmt.Sprintf("payment amount %f is not strictly positive", p.Amount),
			RawPayload:   p.Payload,
			RejectedAt:   now,
		})
		return
	}

	// Validation Rule 3: Currency must be in supported set
	curr := strings.ToUpper(strings.TrimSpace(p.Currency))
	if !supportedCurrencies[curr] {
		emitDLQ(DeadLetterPayment{
			PaymentID:    p.PaymentID,
			AccountID:    p.AccountID,
			Amount:       p.Amount,
			ErrorCode:    "ERR_UNSUPPORTED_CURRENCY",
			ErrorMessage: fmt.Sprintf("currency %q is not supported for settlement", p.Currency),
			RawPayload:   p.Payload,
			RejectedAt:   now,
		})
		return
	}

	// Emitting to clean output
	emitValid(CleanPayment{
		PaymentID: p.PaymentID,
		AccountID: strings.TrimSpace(p.AccountID),
		Amount:    p.Amount,
		Currency:  curr,
		Status:    "VALIDATED_READY_FOR_SETTLEMENT",
		CleanedAt: now,
	})
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	// Sample payments including valid, malformed, and invalid currency entries
	samplePayments := []RawPayment{
		{PaymentID: "PAY-001", AccountID: "ACC-100", Amount: 250.00, Currency: "USD", Payload: `{"method":"card"}`},
		{PaymentID: "PAY-002", AccountID: "   ", Amount: 75.50, Currency: "EUR", Payload: `{"method":"wire"}`},
		{PaymentID: "PAY-003", AccountID: "ACC-101", Amount: -40.00, Currency: "GBP", Payload: `{"method":"card"}`},
		{PaymentID: "PAY-004", AccountID: "ACC-102", Amount: 1200.00, Currency: "CAD", Payload: `{"method":"ach"}`},
		{PaymentID: "PAY-005", AccountID: "ACC-103", Amount: 500.00, Currency: "XYZ", Payload: `{"method":"crypto"}`},
		{PaymentID: "PAY-006", AccountID: "ACC-104", Amount: 89.90, Currency: "JPY", Payload: `{"method":"wallet"}`},
	}

	rawPCol := beam.CreateList(s, samplePayments)

	// Bifurcate into Valid Payments and Dead-Letter Queue records
	validPCol, dlqPCol := beam.ParDo2(s, &validateAndRoutePaymentFn{}, rawPCol)

	// Sink 1: Clean Payments committed to target PostgreSQL table
	cleanWriteOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"payment_id"},
		BatchSize:      1000,
	}
	cleanResult := postgresio.Write(s, "clean_payments", cleanWriteOpts, validPCol)
	_ = cleanResult.FailedRows

	// Sink 2: Rejected records routed to PostgreSQL Dead-Letter Queue table
	dlqWriteOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"payment_id"},
		BatchSize:      1000,
	}
	dlqResult := postgresio.Write(s, "dead_letter_payments", dlqWriteOpts, dlqPCol)
	_ = dlqResult.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Dead-letter queue multi-output pipeline completed successfully.")
}
