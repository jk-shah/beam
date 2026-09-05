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

package postgresio

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	_ "github.com/lib/pq"
)

// SourceOrder represents a row read from test_pipelines.source_orders.
type SourceOrder struct {
	OrderID       int64     `beam:"order_id" db:"order_id"`
	CustomerID    string    `beam:"customer_id" db:"customer_id"`
	CustomerEmail string    `beam:"customer_email" db:"customer_email"`
	Amount        float64   `beam:"amount" db:"amount"`
	Status        string    `beam:"status" db:"status"`
	CountryCode   string    `beam:"country_code" db:"country_code"`
	ItemsCount    int       `beam:"items_count" db:"items_count"`
	CreatedAt     time.Time `beam:"created_at" db:"created_at"`
}

// TransformedOrder represents the enriched and masked row written to test_pipelines.target_orders_transformed.
type TransformedOrder struct {
	OrderID        int64     `beam:"order_id" db:"order_id"`
	CustomerID     string    `beam:"customer_id" db:"customer_id"`
	MaskedEmail    string    `beam:"masked_email" db:"masked_email"`
	NetAmount      float64   `beam:"net_amount" db:"net_amount"`
	ProcessingFee  float64   `beam:"processing_fee" db:"processing_fee"`
	CustomerTier   string    `beam:"customer_tier" db:"customer_tier"`
	Status         string    `beam:"status" db:"status"`
	CountryCode    string    `beam:"country_code" db:"country_code"`
	ItemsCount     int       `beam:"items_count" db:"items_count"`
	ProcessedAt    time.Time `beam:"processed_at" db:"processed_at"`
}

// ReadOrdersFn reads orders from PostgreSQL table into PCollection<SourceOrder>.
type ReadOrdersFn struct {
	ConnStr string
	Table   string
}

func (fn *ReadOrdersFn) ProcessElement(ctx context.Context, _ []byte, emit func(SourceOrder)) error {
	db, err := sql.Open("postgres", fn.ConnStr)
	if err != nil {
		return err
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT order_id, customer_id, customer_email, amount, status, country_code, items_count, created_at FROM %s ORDER BY order_id", fn.Table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o SourceOrder
		if err := rows.Scan(&o.OrderID, &o.CustomerID, &o.CustomerEmail, &o.Amount, &o.Status, &o.CountryCode, &o.ItemsCount, &o.CreatedAt); err != nil {
			return err
		}
		emit(o)
	}
	return rows.Err()
}

// EnrichOrderFn masks PII, calculates fees, and assigns loyalty tiers.
type EnrichOrderFn struct{}

func (fn *EnrichOrderFn) ProcessElement(o SourceOrder, emit func(TransformedOrder)) {
	email := o.CustomerEmail
	maskedEmail := "***"
	if idx := strings.Index(email, "@"); idx != -1 {
		prefix := email[:idx]
		if len(prefix) > 3 {
			prefix = prefix[:3]
		}
		maskedEmail = prefix + "***@" + email[idx+1:]
	}

	fee := math.Round(o.Amount*0.025*100) / 100
	net := math.Round((o.Amount-fee)*100) / 100

	tier := "STANDARD"
	if o.Amount >= 1000.0 {
		tier = "VIP"
	}

	emit(TransformedOrder{
		OrderID:       o.OrderID,
		CustomerID:    o.CustomerID,
		MaskedEmail:   maskedEmail,
		NetAmount:     net,
		ProcessingFee: fee,
		CustomerTier:  tier,
		Status:        o.Status,
		CountryCode:   o.CountryCode,
		ItemsCount:    o.ItemsCount,
		ProcessedAt:   time.Now().UTC(),
	})
}

// FilterHighValueFn filters for completed orders with amount >= 100.
type FilterHighValueFn struct{}

func (fn *FilterHighValueFn) ProcessElement(o SourceOrder, emit func(SourceOrder)) {
	if o.Status == "COMPLETED" && o.Amount >= 100.0 {
		emit(o)
	}
}

func init() {
	beam.RegisterType(reflect.TypeOf((*SourceOrder)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*TransformedOrder)(nil)).Elem())
	beam.RegisterDoFn(&ReadOrdersFn{})
	beam.RegisterDoFn(&EnrichOrderFn{})
	beam.RegisterDoFn(&FilterHighValueFn{})
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestGoComplexPipeline_PostgresToPostgres(t *testing.T) {
	connStr := "host=localhost port=5432 user=beam_test dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Skipf("skipping: cannot connect to local postgres: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("skipping: postgres unreachable: %v", err)
	}

	// 1. Truncate target tables before test
	if _, err := db.Exec("TRUNCATE TABLE test_pipelines.target_orders_transformed; TRUNCATE TABLE test_pipelines.target_orders_filtered;"); err != nil {
		t.Fatalf("failed to truncate target tables: %v", err)
	}

	// 2. Build and run Beam Go Pipeline
	p, s := beam.NewPipelineWithRoot()

	initCol := beam.Create(s, []byte{})
	orders := beam.ParDo(s, &ReadOrdersFn{
		ConnStr: connStr,
		Table:   "test_pipelines.source_orders",
	}, initCol)

	// Branch A: Complex Transformation & PII Masking
	transformed := beam.ParDo(s, &EnrichOrderFn{}, orders)
	Write(s, "test_pipelines.target_orders_transformed", WriteOptions{
		Host:           "localhost",
		Port:           5432,
		Database:       "postgres",
		Username:       "beam_test",
		Password:       "beam_password",
		WriteMode:      WriteModeUpsert,
		PrimaryKeyCols: []string{"order_id"},
		BatchSize:      10,
	}, transformed)

	// Branch B: Filtering (completed >= 100.0)
	filtered := beam.ParDo(s, &FilterHighValueFn{}, orders)
	Write(s, "test_pipelines.target_orders_filtered", WriteOptions{
		Host:           "localhost",
		Port:           5432,
		Database:       "postgres",
		Username:       "beam_test",
		Password:       "beam_password",
		WriteMode:      WriteModeUpsert,
		PrimaryKeyCols: []string{"order_id"},
		BatchSize:      10,
	}, filtered)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}

	// 3. Verify target_orders_transformed
	var transformedCount, vipCount int
	if err := db.QueryRow("SELECT count(*) FROM test_pipelines.target_orders_transformed").Scan(&transformedCount); err != nil {
		t.Fatalf("failed to query transformed count: %v", err)
	}
	if transformedCount != 20 {
		t.Errorf("expected 20 transformed rows, got %d", transformedCount)
	}

	if err := db.QueryRow("SELECT count(*) FROM test_pipelines.target_orders_transformed WHERE customer_tier = 'VIP'").Scan(&vipCount); err != nil {
		t.Fatalf("failed to query vip count: %v", err)
	}
	if vipCount != 7 {
		t.Errorf("expected 7 VIP rows, got %d", vipCount)
	}

	// 4. Verify target_orders_filtered
	var filteredCount int
	if err := db.QueryRow("SELECT count(*) FROM test_pipelines.target_orders_filtered").Scan(&filteredCount); err != nil {
		t.Fatalf("failed to query filtered count: %v", err)
	}
	if filteredCount != 13 {
		t.Errorf("expected 13 filtered rows, got %d", filteredCount)
	}

	t.Logf("Go Pipeline Verification Passed: %d transformed rows (%d VIP), %d filtered rows", transformedCount, vipCount, filteredCount)
}
