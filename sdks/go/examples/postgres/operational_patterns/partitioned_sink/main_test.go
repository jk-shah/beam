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
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestGeneratePartitionedOrdersFn(t *testing.T) {
	fn := &generatePartitionedOrdersFn{}
	var emitted []PartitionedOrder
	emit := func(o PartitionedOrder) {
		emitted = append(emitted, o)
	}

	fn.ProcessElement(1001, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted order, got %d", len(emitted))
	}
	o := emitted[0]
	if o.OrderID != 1001 {
		t.Errorf("expected OrderID 1001, got %d", o.OrderID)
	}
	if o.OrderDate == "" {
		t.Errorf("expected non-empty OrderDate")
	}
	if o.TotalAmount <= 0.0 {
		t.Errorf("expected positive TotalAmount, got %f", o.TotalAmount)
	}
}

func TestKeyByPartitionDateFn(t *testing.T) {
	fn := &keyByPartitionDateFn{}
	order := PartitionedOrder{
		OrderID:   55,
		OrderDate: "2026-09-14",
	}

	var emittedKey string
	var emittedOrder PartitionedOrder
	emit := func(key string, val PartitionedOrder) {
		emittedKey = key
		emittedOrder = val
	}

	fn.ProcessElement(order, emit)

	if emittedKey != "2026-09-14" {
		t.Errorf("expected key 2026-09-14, got %s", emittedKey)
	}
	if emittedOrder.OrderID != 55 {
		t.Errorf("expected OrderID 55, got %d", emittedOrder.OrderID)
	}
}

func TestFlattenPartitionGroupFn(t *testing.T) {
	fn := &flattenPartitionGroupFn{}
	items := []PartitionedOrder{
		{OrderID: 1, OrderDate: "2026-09-01", UpdatedAt: time.Now()},
		{OrderID: 2, OrderDate: "2026-09-01", UpdatedAt: time.Now()},
	}

	idx := 0
	ordersIter := func(out *PartitionedOrder) bool {
		if idx < len(items) {
			*out = items[idx]
			idx++
			return true
		}
		return false
	}

	var emitted []PartitionedOrder
	emit := func(o PartitionedOrder) {
		emitted = append(emitted, o)
	}

	fn.ProcessElement("2026-09-01", ordersIter, emit)

	if len(emitted) != 2 {
		t.Fatalf("expected 2 orders emitted, got %d", len(emitted))
	}
}

func TestPartitionedWriteOptions_CompositeConflictTarget(t *testing.T) {
	opts := postgresio.NewWriteOptions(
		postgresio.WithHost("localhost"),
		postgresio.WithPort(5432),
		postgresio.WithDatabase("analytics"),
		postgresio.WithPrimaryKeyColumns("order_id", "order_date"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
	)

	if len(opts.PrimaryKeyCols) != 2 {
		t.Fatalf("expected 2 primary key columns, got %d", len(opts.PrimaryKeyCols))
	}
	if opts.PrimaryKeyCols[0] != "order_id" || opts.PrimaryKeyCols[1] != "order_date" {
		t.Errorf("expected [order_id, order_date], got %v", opts.PrimaryKeyCols)
	}
}
