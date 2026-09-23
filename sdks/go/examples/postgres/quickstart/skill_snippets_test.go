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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
)

type SkillOrder struct {
	OrderID     int64   `beam:"order_id"`
	CustomerID  int32   `beam:"customer_id"`
	TotalAmount float64 `beam:"total_amount"`
	Status      string  `beam:"status"`
}

func buildReadPipeline(s beam.Scope) beam.PCollection {
	ro := postgresio.NewReadOptions(
		postgresio.WithReadHost("localhost"),
		postgresio.WithReadPort(5432),
		postgresio.WithReadDatabase("beammeup"),
		postgresio.WithReadUsername("beam_navigator"),
		postgresio.WithReadPassword("beam_navigator"),
		postgresio.WithReadSSLMode("disable"),
	)
	return postgresio.Read(s, "public.orders", reflect.TypeOf(SkillOrder{}), ro)
}

func buildWritePipeline(s beam.Scope, orders beam.PCollection) postgresio.WriteResult {
	wo := postgresio.NewWriteOptions(
		postgresio.WithHost("localhost"),
		postgresio.WithPort(5432),
		postgresio.WithDatabase("beammeup"),
		postgresio.WithUsername("scotty"),
		postgresio.WithPassword("scotty_secret"),
		postgresio.WithSSLMode("disable"),
		postgresio.WithPrimaryKeyColumns("order_id"),
		postgresio.WithBatchSize(1000),
		postgresio.WithMaxBatchBytes(4*1024*1024),
	)
	return postgresio.Write(s, "public.orders", wo, orders)
}

func buildPgBouncerPipeline(s beam.Scope, events beam.PCollection) postgresio.WriteResult {
	wo := postgresio.NewWriteOptions(
		postgresio.WithHost("localhost"),
		postgresio.WithPort(6432),
		postgresio.WithDatabase("beammeup"),
		postgresio.WithUsername("beam_navigator"),
		postgresio.WithPassword("beam_navigator"),
		postgresio.WithSSLMode("disable"),
		postgresio.WithPgBouncer(true),
		postgresio.WithPrimaryKeyColumns("event_id"),
	)
	return postgresio.Write(s, "public.events", wo, events)
}

func buildCDCPipeline(s beam.Scope) beam.PCollection {
	return postgresio.ReadCDC(s,
		postgresio.WithCDCHost("localhost"),
		postgresio.WithCDCPort(5432),
		postgresio.WithCDCDatabase("beammeup"),
		postgresio.WithCDCUsername("scotty"),
		postgresio.WithCDCPassword("scotty_secret"),
		postgresio.WithCDCSSLMode("disable"),
		postgresio.WithCDCSlotName("beam_cdc_slot"),
		postgresio.WithCDCPublication("pub_beam_cdc"),
		postgresio.WithCDCFailoverSlot(true),
	)
}

func TestSkillSnippetsCompile(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	orders := buildReadPipeline(s)
	writeRes := buildWritePipeline(s, orders)
	if writeRes.SuccessfulRows == (beam.PCollection{}) {
		t.Fatal("expected non-empty SuccessfulRows PCollection")
	}
	_ = buildPgBouncerPipeline(s, orders)
	cdcStream := buildCDCPipeline(s)
	if cdcStream == (beam.PCollection{}) {
		t.Fatal("expected non-empty cdcStream PCollection")
	}
	_ = p
}
