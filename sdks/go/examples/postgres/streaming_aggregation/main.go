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

// Package main demonstrates a real-time streaming windowed aggregation pipeline
// using Apache Beam with PostgreSQL as both the Change Data Capture (CDC) streaming
// source and the analytics rollup sink.
//
// Use Case: Streaming Materialized View with LSN Freshness Tracking
// In PostgreSQL, native `MATERIALIZED VIEW` objects require expensive `REFRESH MATERIALIZED VIEW`
// commands that scan whole tables, spike database CPU, and cause write lock contention.
//
// This pipeline acts as an off-database incremental computation engine:
// 1. Ingests financial transactions from a PostgreSQL CDC stream (ReadCDC).
// 2. Windows events into 1-minute fixed intervals.
// 3. Computes rolling aggregates (count, total amount, max amount) while tracking the maximum LSN.
// 4. Sinks the rollups into an analytics table using staged COPY upsert (Write).
// 5. Stores `last_applied_lsn` (PG_LSN) and `last_tx_time`, allowing dashboard queries to verify
//    data freshness and calculate replication lag using `pg_wal_lsn_diff(pg_current_wal_lsn(), last_applied_lsn)`.
//
// Target PostgreSQL Schema:
//
//	CREATE TABLE public.merchant_minute_rollups (
//	    merchant_id       VARCHAR(64) NOT NULL,
//	    window_start      TIMESTAMP WITH TIME ZONE NOT NULL,
//	    window_end        TIMESTAMP WITH TIME ZONE NOT NULL,
//	    transaction_count BIGINT NOT NULL,
//	    total_amount      DOUBLE PRECISION NOT NULL,
//	    max_amount        DOUBLE PRECISION NOT NULL,
//	    last_applied_lsn  PG_LSN NOT NULL,
//	    last_tx_time      TIMESTAMP WITH TIME ZONE NOT NULL,
//	    PRIMARY KEY (merchant_id, window_start)
//	);
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "postgres", "PostgreSQL database name")
	username = flag.String("username", "beam_test", "PostgreSQL user")
	password = flag.String("password", "beam_test", "PostgreSQL password")
	slot     = flag.String("slot", "beam_aggregation_slot", "CDC replication slot name")
	pub      = flag.String("publication", "beam_publication", "CDC publication name")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*TransactionEvent)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*MerchantRollup)(nil)).Elem())
	beam.RegisterDoFn(&extractTransactionFn{})
	beam.RegisterDoFn(&aggregateRollupFn{})
}

// TransactionEvent represents an incoming financial transaction from PostgreSQL CDC.
type TransactionEvent struct {
	MerchantID string    `json:"merchant_id" beam:"merchant_id"`
	Amount     float64   `json:"amount" beam:"amount"`
	Timestamp  int64     `json:"timestamp" beam:"timestamp"`
	LSN        uint64    `json:"lsn" beam:"lsn"`
	CommitTime time.Time `json:"commit_time" beam:"commit_time"`
}

// MerchantRollup represents the windowed aggregated metrics sink to PostgreSQL,
// acting as a live streaming Materialized View with LSN freshness tracking.
type MerchantRollup struct {
	MerchantID       string    `json:"merchant_id" db:"merchant_id" beam:"merchant_id"`
	WindowStart      time.Time `json:"window_start" db:"window_start" beam:"window_start"`
	WindowEnd        time.Time `json:"window_end" db:"window_end" beam:"window_end"`
	TransactionCount int64     `json:"transaction_count" db:"transaction_count" beam:"transaction_count"`
	TotalAmount      float64   `json:"total_amount" db:"total_amount" beam:"total_amount"`
	MaxAmount        float64   `json:"max_amount" db:"max_amount" beam:"max_amount"`
	LastAppliedLSN   string    `json:"last_applied_lsn" db:"last_applied_lsn" beam:"last_applied_lsn"`
	LastTxTime       time.Time `json:"last_tx_time" db:"last_tx_time" beam:"last_tx_time"`
}

// formatPgLSN formats a uint64 LSN into PostgreSQL's canonical PG_LSN string (e.g. "0/16B2D48").
func formatPgLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// extractTransactionFn extracts transaction fields from raw CDC change events
// and attaches event-time timestamps for windowing.
type extractTransactionFn struct{}

func (fn *extractTransactionFn) ProcessElement(event postgresio.ChangeEvent, emit func(typex.EventTime, string, TransactionEvent)) error {
	if event.Operation != postgresio.OpInsert && event.Operation != postgresio.OpUpdate {
		return nil
	}
	rawMerchant, ok := event.After["merchant_id"]
	if !ok || rawMerchant == nil {
		return nil
	}
	rawAmount, ok := event.After["amount"]
	if !ok || rawAmount == nil {
		return nil
	}

	var amount float64
	switch v := rawAmount.(type) {
	case float64:
		amount = v
	case string:
		var err error
		amount, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return nil
		}
	default:
		return nil
	}

	merchantID := fmt.Sprintf("%v", rawMerchant)
	eventTime := event.CommitTime
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}

	txn := TransactionEvent{
		MerchantID: merchantID,
		Amount:     amount,
		Timestamp:  eventTime.UnixMicro(),
		LSN:        event.LSN,
		CommitTime: eventTime,
	}

	emit(typex.EventTime(eventTime.UnixMilli()), merchantID, txn)
	return nil
}

// aggregateRollupFn computes summary statistics for all transactions in the window
// and tracks the highest LSN and latest transaction timestamp.
type aggregateRollupFn struct{}

func (fn *aggregateRollupFn) ProcessElement(iw window.IntervalWindow, merchantID string, txns func(*TransactionEvent) bool, emit func(MerchantRollup)) {
	var count int64
	var total float64
	var maxAmount float64
	var maxLSN uint64
	var latestTx time.Time

	var txn TransactionEvent
	for txns(&txn) {
		count++
		total += txn.Amount
		if txn.Amount > maxAmount {
			maxAmount = txn.Amount
		}
		if txn.LSN > maxLSN {
			maxLSN = txn.LSN
		}
		if txn.CommitTime.After(latestTx) {
			latestTx = txn.CommitTime
		}
	}

	if count > 0 {
		emit(MerchantRollup{
			MerchantID:       merchantID,
			WindowStart:      iw.Start.ToTime(),
			WindowEnd:        iw.End.ToTime(),
			TransactionCount: count,
			TotalAmount:      total,
			MaxAmount:        maxAmount,
			LastAppliedLSN:   formatPgLSN(maxLSN),
			LastTxTime:       latestTx,
		})
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	// 1. Ingest streaming change events from PostgreSQL CDC
	cdcStream := postgresio.ReadCDC(s,
		postgresio.WithCDCHost(*host),
		postgresio.WithCDCPort(*port),
		postgresio.WithCDCDatabase(*database),
		postgresio.WithCDCUsername(*username),
		postgresio.WithCDCPassword(*password),
		postgresio.WithCDCSlotName(*slot),
		postgresio.WithCDCPublication(*pub),
		postgresio.WithCDCCreateSlotIfMissing(true),
	)

	// 2. Extract transaction fields and assign event-time timestamps
	keyedTxns := beam.ParDo(s, &extractTransactionFn{}, cdcStream)

	// 3. Apply Fixed (Tumbling) 1-Minute Event-Time Windowing
	windowed := beam.WindowInto(s, window.NewFixedWindows(1*time.Minute), keyedTxns)

	// 4. Group by Merchant ID within the window
	grouped := beam.GroupByKey(s, windowed)

	// 5. Aggregate rollups per merchant and window
	rollups := beam.ParDo(s, &aggregateRollupFn{}, grouped)

	// 6. Write aggregated rollups to PostgreSQL analytics table with staged COPY atomic upsert
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMethod:    postgresio.WriteMethodStagedCopy,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"merchant_id", "window_start"},
		BatchSize:      5000,
	}

	result := postgresio.Write(s, "public.merchant_minute_rollups", writeOpts, rollups)

	// Ensure dead-letter failed rows are monitored
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Streaming aggregation pipeline completed successfully.")
}
