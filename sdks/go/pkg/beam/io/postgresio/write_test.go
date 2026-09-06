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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func TestBuildUnnestQueryUpsert(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeUpsert), WithPrimaryKeyColumns("id")),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region", "amount"},
	}

	batch := []any{
		TestOrder{ID: 1, Region: "US", Amount: 100.0},
		TestOrder{ID: 2, Region: "EU", Amount: 200.0},
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error building unnest query: %v", err)
	}

	expectedPrefix := `INSERT INTO "public"."orders" ("id", "region", "amount") SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::double precision[])`
	if !strings.HasPrefix(query, expectedPrefix) {
		t.Errorf("expected query to start with %q, got %q", expectedPrefix, query)
	}

	expectedConflict := `ON CONFLICT ("id") DO UPDATE SET "region" = EXCLUDED."region", "amount" = EXCLUDED."amount"`
	if !strings.Contains(query, expectedConflict) {
		t.Errorf("expected conflict clause %q in query: %q", expectedConflict, query)
	}

	if len(args) != 3 {
		t.Errorf("expected 3 array arguments for 3 columns, got %d", len(args))
	}
}

func TestBuildUnnestQueryInsertOnly(t *testing.T) {
	fn := &writeFn{
		Table:          `"orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeInsert)),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: nil,
		columns:        []string{"id", "region", "amount"},
	}

	batch := []any{
		TestOrder{ID: 1, Region: "US", Amount: 50.0},
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Contains(query, "ON CONFLICT") {
		t.Errorf("expected no ON CONFLICT clause for WriteModeInsert, got %q", query)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestWriteTransformPipelineConstruction(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	orders := []TestOrder{
		{ID: 1, Region: "US", Amount: 10.5},
		{ID: 2, Region: "EU", Amount: 25.0},
	}

	col := beam.CreateList(s, orders)
	opts := NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
		WithPrimaryKeyColumns("id"),
	)

	result := Write(s, "public.orders", opts, col)

	if !result.SuccessfulRows.IsValid() {
		t.Errorf("expected valid SuccessfulRows PCollection")
	}
	if !result.FailedRows.IsValid() {
		t.Errorf("expected valid FailedRows PCollection")
	}

	// Verify pipeline graph builds
	if _, _, err := p.Build(); err != nil {
		t.Fatalf("pipeline build failed: %v", err)
	}
}

func TestWriteRejectsInvalidTableNameAtGraphConstruction(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on invalid table name, but none occurred")
		}
	}()

	_, s := beam.NewPipelineWithRoot()
	col := beam.CreateList(s, []TestOrder{{ID: 1}})
	opts := NewWriteOptions()

	// Malicious table name should panic at graph construction time
	Write(s, `orders"; DROP TABLE users; --`, opts, col)
}

func TestStagedCopyExecutionDirect(t *testing.T) {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=beam_test password=beam_test dbname=postgres sslmode=disable search_path=pg_catalog,pg_temp")
	if err != nil {
		t.Skipf("skipping db integration test: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("skipping db integration test: %v", err)
	}

	type StagedRecord struct {
		ID       int64     `db:"id"`
		Code     string    `db:"code"`
		Amount   float64   `db:"amount"`
		Region   string    `db:"region"`
		Tier     string    `db:"tier"`
		LoadedAt time.Time `db:"loaded_at"`
	}

	fn := &writeFn{
		Table:          `"public"."migrated_transactions"`,
		Options:        NewWriteOptions(WithWriteMethod(WriteMethodStagedCopy), WithWriteMode(WriteModeUpsert)),
		Type:           beam.EncodedType{T: reflect.TypeOf(StagedRecord{})},
		PrimaryKeyCols: []string{"id"},
		db:             db,
	}
	fn.inspectColumns(reflect.TypeOf(StagedRecord{}))

	batch := make([]any, 25000)
	now := time.Now().UTC()
	for i := 0; i < 25000; i++ {
		batch[i] = StagedRecord{
			ID:       int64(900000 + i),
			Code:     fmt.Sprintf("STAGED-%d", i),
			Amount:   123.45,
			Region:   "us-west1",
			Tier:     "ENTERPRISE",
			LoadedAt: now,
		}
	}

	ctx := context.Background()
	start := time.Now()
	if err := fn.executeStagedCopy(ctx, batch); err != nil {
		t.Fatalf("executeStagedCopy failed: %v", err)
	}
	dur := time.Since(start)
	t.Logf("executeStagedCopy inserted %d rows in %v (Rate: %.2f rows/sec)", len(batch), dur, float64(len(batch))/dur.Seconds())
}

