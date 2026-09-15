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

// Package main demonstrates an end-to-end data enrichment pipeline using PostgreSQL
// as the ingestion source, the dynamic enrichment lookup database, and the target analytics sink.
//
// Architecture & Data Flow:
//
//  1. Source (PostgreSQL):
//     Ingests purchase transactions from PostgreSQL (either streaming CDC via ReadCDC or table scans).
//
//  2. Mid-Pipeline Enrichment (PostgreSQL):
//     For each transaction, enriches the order with customer dimensions (credit limit, risk score,
//     loyalty tier) queried directly from a PostgreSQL reference table (public.customer_profiles).
//     To avoid exhausting database connections and saturating query bandwidth, the DoFn leverages:
//     - Worker-level connection pooling (database/sql with configurable max connections)
//     - Concurrency-safe in-memory TTL caching (absorbing repetitive customer lookups)
//     - Graceful fallback defaults for unregistered or missing customer profiles
//
//  3. Sink (PostgreSQL):
//     Sinks the enriched order records into public.enriched_orders using postgresio.Write
//     configured with atomic upsert (ON CONFLICT (order_id) DO UPDATE) or staged COPY.
//
// Database DDL Requirements:
//
//	-- 1. Source Table (Orders)
//	CREATE TABLE IF NOT EXISTS public.orders (
//	    order_id TEXT PRIMARY KEY,
//	    customer_id TEXT NOT NULL,
//	    item TEXT NOT NULL,
//	    amount NUMERIC(12,2) NOT NULL,
//	    ordered_at TIMESTAMPTZ NOT NULL
//	);
//
//	-- 2. Reference / Dimension Table (Customer Profiles)
//	CREATE TABLE IF NOT EXISTS public.customer_profiles (
//	    customer_id TEXT PRIMARY KEY,
//	    customer_name TEXT NOT NULL,
//	    customer_email TEXT NOT NULL,
//	    loyalty_tier TEXT NOT NULL,
//	    credit_limit NUMERIC(12,2) NOT NULL,
//	    risk_score NUMERIC(5,2) NOT NULL,
//	    updated_at TIMESTAMPTZ NOT NULL
//	);
//
//	-- 3. Sink Table (Enriched Orders)
//	CREATE TABLE IF NOT EXISTS public.enriched_orders (
//	    order_id TEXT PRIMARY KEY,
//	    customer_id TEXT NOT NULL,
//	    customer_name TEXT NOT NULL,
//	    customer_email TEXT NOT NULL,
//	    loyalty_tier TEXT NOT NULL,
//	    credit_limit NUMERIC(12,2) NOT NULL,
//	    risk_score NUMERIC(5,2) NOT NULL,
//	    item TEXT NOT NULL,
//	    amount NUMERIC(12,2) NOT NULL,
//	    is_approved BOOLEAN NOT NULL,
//	    approval_reason TEXT NOT NULL,
//	    enriched_at TIMESTAMPTZ NOT NULL
//	);
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	_ "github.com/lib/pq"
)

var (
	host        = flag.String("host", "localhost", "PostgreSQL host")
	port        = flag.Int("port", 5432, "PostgreSQL port")
	database    = flag.String("database", "beammeup", "PostgreSQL database name")
	username    = flag.String("username", "beam_navigator", "PostgreSQL user")
	password    = flag.String("password", "beam_navigator", "PostgreSQL password")
	sslMode     = flag.String("sslmode", "disable", "PostgreSQL SSL mode")
	sourceMode  = flag.String("source_mode", "sample", "Source mode: 'sample', 'table', or 'cdc'")
	cacheTTLSec = flag.Int("cache_ttl_sec", 300, "In-memory cache TTL in seconds for dimension lookups")
	poolSize    = flag.Int("pool_size", 10, "Maximum open database connections per worker for enrichment")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Order)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*CustomerProfile)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*EnrichedOrder)(nil)).Elem())
	beam.RegisterDoFn(&readOrdersTableFn{})
	beam.RegisterDoFn(&parseCDCOrderFn{})
	beam.RegisterDoFn(&enrichOrderWithPostgresFn{})
}

// Order represents an incoming purchase order from PostgreSQL.
type Order struct {
	OrderID    string    `json:"order_id" db:"order_id" beam:"order_id"`
	CustomerID string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	Item       string    `json:"item" db:"item" beam:"item"`
	Amount     float64   `json:"amount" db:"amount" beam:"amount"`
	OrderedAt  time.Time `json:"ordered_at" db:"ordered_at" beam:"ordered_at"`
}

// CustomerProfile represents the customer dimension record retrieved from PostgreSQL.
type CustomerProfile struct {
	CustomerID    string    `json:"customer_id" db:"customer_id"`
	CustomerName  string    `json:"customer_name" db:"customer_name"`
	CustomerEmail string    `json:"customer_email" db:"customer_email"`
	LoyaltyTier   string    `json:"loyalty_tier" db:"loyalty_tier"`
	CreditLimit   float64   `json:"credit_limit" db:"credit_limit"`
	RiskScore     float64   `json:"risk_score" db:"risk_score"`
	UpdatedAt     time.Time `json:"updated_at" db:"updated_at"`
}

// EnrichedOrder represents the joined transaction record written to the PostgreSQL sink.
type EnrichedOrder struct {
	OrderID        string    `json:"order_id" db:"order_id" beam:"order_id"`
	CustomerID     string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	CustomerName   string    `json:"customer_name" db:"customer_name" beam:"customer_name"`
	CustomerEmail  string    `json:"customer_email" db:"customer_email" beam:"customer_email"`
	LoyaltyTier    string    `json:"loyalty_tier" db:"loyalty_tier" beam:"loyalty_tier"`
	CreditLimit    float64   `json:"credit_limit" db:"credit_limit" beam:"credit_limit"`
	RiskScore      float64   `json:"risk_score" db:"risk_score" beam:"risk_score"`
	Item           string    `json:"item" db:"item" beam:"item"`
	Amount         float64   `json:"amount" db:"amount" beam:"amount"`
	IsApproved     bool      `json:"is_approved" db:"is_approved" beam:"is_approved"`
	ApprovalReason string    `json:"approval_reason" db:"approval_reason" beam:"approval_reason"`
	EnrichedAt     time.Time `json:"enriched_at" db:"enriched_at" beam:"enriched_at"`
}

// readOrdersTableFn reads source orders from PostgreSQL table public.orders.
type readOrdersTableFn struct {
	ConnStr string `json:"conn_str"`
}

func (fn *readOrdersTableFn) ProcessElement(ctx context.Context, _ []byte, emit func(Order)) error {
	db, err := sql.Open("postgres", fn.ConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}
	defer db.Close()

	query := "SELECT order_id, customer_id, item, amount, ordered_at FROM public.orders ORDER BY ordered_at"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query source orders: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.OrderID, &o.CustomerID, &o.Item, &o.Amount, &o.OrderedAt); err != nil {
			return fmt.Errorf("failed to scan source order: %w", err)
		}
		emit(o)
	}
	return rows.Err()
}

// parseCDCOrderFn parses ChangeEvents from postgresio.ReadCDC into Order structs.
type parseCDCOrderFn struct{}

func (fn *parseCDCOrderFn) ProcessElement(event postgresio.ChangeEvent, emit func(Order)) {
	if event.Operation != postgresio.OpInsert && event.Operation != postgresio.OpUpdate {
		return
	}
	if event.After == nil {
		return
	}
	orderID, ok := event.After["order_id"].(string)
	if !ok || orderID == "" {
		return
	}
	custID, _ := event.After["customer_id"].(string)
	item, _ := event.After["item"].(string)
	var amount float64
	switch v := event.After["amount"].(type) {
	case float64:
		amount = v
	case string:
		amount, _ = strconv.ParseFloat(v, 64)
	}

	orderedAt := event.CommitTime
	if tStr, ok := event.After["ordered_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, tStr); err == nil {
			orderedAt = t
		}
	}

	emit(Order{
		OrderID:    orderID,
		CustomerID: custID,
		Item:       item,
		Amount:     amount,
		OrderedAt:  orderedAt,
	})
}

type cachedProfile struct {
	profile   CustomerProfile
	expiresAt time.Time
}

// enrichOrderWithPostgresFn enriches transactions by querying PostgreSQL customer_profiles.
// It manages a worker-local connection pool and in-memory TTL cache.
type enrichOrderWithPostgresFn struct {
	ConnStr     string `json:"conn_str"`
	CacheTTLSec int    `json:"cache_ttl_sec"`
	PoolSize    int    `json:"pool_size"`

	// Runtime worker state (unexported, initialized in Setup)
	db          *sql.DB
	cache       map[string]cachedProfile
	cacheMu     sync.RWMutex
	cacheHits   beam.Counter
	cacheMisses beam.Counter
	dbErrors    beam.Counter
	approved    beam.Counter
	rejected    beam.Counter
}

func (fn *enrichOrderWithPostgresFn) Setup(ctx context.Context) error {
	fn.cacheHits = beam.NewCounter("postgres_enrichment", "cache_hits")
	fn.cacheMisses = beam.NewCounter("postgres_enrichment", "cache_misses")
	fn.dbErrors = beam.NewCounter("postgres_enrichment", "db_errors")
	fn.approved = beam.NewCounter("postgres_enrichment", "orders_approved")
	fn.rejected = beam.NewCounter("postgres_enrichment", "orders_rejected")

	fn.cache = make(map[string]cachedProfile)

	db, err := sql.Open("postgres", fn.ConnStr)
	if err != nil {
		return fmt.Errorf("failed to open enrichment database connection pool: %w", err)
	}

	maxConns := fn.PoolSize
	if maxConns <= 0 {
		maxConns = 10
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2)
	db.SetConnMaxLifetime(30 * time.Minute)

	fn.db = db
	return nil
}

func (fn *enrichOrderWithPostgresFn) Teardown() {
	if fn.db != nil {
		_ = fn.db.Close()
	}
}

func (fn *enrichOrderWithPostgresFn) getCached(custID string) (CustomerProfile, bool) {
	fn.cacheMu.RLock()
	entry, found := fn.cache[custID]
	fn.cacheMu.RUnlock()

	if !found || time.Now().After(entry.expiresAt) {
		return CustomerProfile{}, false
	}
	return entry.profile, true
}

func (fn *enrichOrderWithPostgresFn) putCache(custID string, profile CustomerProfile) {
	ttl := time.Duration(fn.CacheTTLSec) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	fn.cacheMu.Lock()
	fn.cache[custID] = cachedProfile{
		profile:   profile,
		expiresAt: time.Now().Add(ttl),
	}
	fn.cacheMu.Unlock()
}

func (fn *enrichOrderWithPostgresFn) ProcessElement(
	ctx context.Context,
	o Order,
	emit func(EnrichedOrder),
) error {
	// 1. Check local cache first
	profile, hit := fn.getCached(o.CustomerID)
	if hit {
		fn.cacheHits.Inc(ctx, 1)
	} else {
		fn.cacheMisses.Inc(ctx, 1)
		// 2. Query PostgreSQL dimension table
		query := `
			SELECT customer_id, customer_name, customer_email, loyalty_tier, credit_limit, risk_score
			FROM public.customer_profiles
			WHERE customer_id = $1
		`
		var p CustomerProfile
		err := fn.db.QueryRowContext(ctx, query, o.CustomerID).Scan(
			&p.CustomerID,
			&p.CustomerName,
			&p.CustomerEmail,
			&p.LoyaltyTier,
			&p.CreditLimit,
			&p.RiskScore,
		)

		if err == sql.ErrNoRows {
			// Apply default profile for unregistered or new customers
			p = CustomerProfile{
				CustomerID:    o.CustomerID,
				CustomerName:  "Unregistered Customer",
				CustomerEmail: "guest@example.com",
				LoyaltyTier:   "STANDARD",
				CreditLimit:   500.00,
				RiskScore:     50.00,
			}
		} else if err != nil {
			fn.dbErrors.Inc(ctx, 1)
			// Return default fallback profile to prevent halting the streaming pipeline
			p = CustomerProfile{
				CustomerID:    o.CustomerID,
				CustomerName:  "Lookup Fallback",
				CustomerEmail: "fallback@example.com",
				LoyaltyTier:   "STANDARD",
				CreditLimit:   250.00,
				RiskScore:     65.00,
			}
		}

		fn.putCache(o.CustomerID, p)
		profile = p
	}

	// 3. Evaluate real-time business logic
	isApproved := true
	reason := "APPROVED"

	if o.Amount > profile.CreditLimit {
		isApproved = false
		reason = fmt.Sprintf("EXCEEDS_CREDIT_LIMIT: order amount $%.2f exceeds limit $%.2f", o.Amount, profile.CreditLimit)
	} else if profile.RiskScore >= 75.0 {
		isApproved = false
		reason = fmt.Sprintf("HIGH_RISK_SCORE: customer risk score %.1f exceeds threshold 75.0", profile.RiskScore)
	}

	if isApproved {
		fn.approved.Inc(ctx, 1)
	} else {
		fn.rejected.Inc(ctx, 1)
	}

	// 4. Emit enriched record
	emit(EnrichedOrder{
		OrderID:        o.OrderID,
		CustomerID:     o.CustomerID,
		CustomerName:   profile.CustomerName,
		CustomerEmail:  profile.CustomerEmail,
		LoyaltyTier:    profile.LoyaltyTier,
		CreditLimit:    profile.CreditLimit,
		RiskScore:      profile.RiskScore,
		Item:           o.Item,
		Amount:         o.Amount,
		IsApproved:     isApproved,
		ApprovalReason: reason,
		EnrichedAt:     time.Now().UTC(),
	})

	return nil
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		*host, *port, *database, *username, *password, *sslMode,
	)

	// Step 1: Ingest Orders from PostgreSQL Source
	var orders beam.PCollection
	switch *sourceMode {
	case "cdc":
		// Read from PostgreSQL logical replication CDC stream
		cdcOpts := []postgresio.CDCOption{
			postgresio.WithCDCHost(*host),
			postgresio.WithCDCPort(*port),
			postgresio.WithCDCDatabase(*database),
			postgresio.WithCDCUsername(*username),
			postgresio.WithCDCPassword(*password),
		postgresio.WithCDCSSLMode(*sslMode),
			postgresio.WithCDCSlotName("orders_enrichment_slot"),
			postgresio.WithCDCPublication("orders_pub"),
		}
		rawEvents := postgresio.ReadCDC(s, cdcOpts...)
		filteredOrders := postgresio.FilterByTable(s, "orders", rawEvents)
		orders = beam.ParDo(s, &parseCDCOrderFn{}, filteredOrders)

	case "table":
		// Query bounded orders table from PostgreSQL
		seed := beam.CreateList(s, [][]byte{[]byte("init")})
		orders = beam.ParDo(s, &readOrdersTableFn{ConnStr: connStr}, seed)

	default: // "sample"
		// Built-in sample orders for local execution and validation
		now := time.Now().UTC()
		sampleOrders := []Order{
			{OrderID: "ORD-1001", CustomerID: "CUST-1", Item: "Compute Engine Server", Amount: 450.00, OrderedAt: now},
			{OrderID: "ORD-1002", CustomerID: "CUST-2", Item: "Cloud Storage Bucket", Amount: 80.00, OrderedAt: now},
			{OrderID: "ORD-1003", CustomerID: "CUST-1", Item: "BigQuery High-Memory Slot", Amount: 1200.00, OrderedAt: now},
			{OrderID: "ORD-1004", CustomerID: "CUST-3", Item: "Cloud Spanner Enterprise", Amount: 3200.00, OrderedAt: now},
			{OrderID: "ORD-1005", CustomerID: "CUST-999", Item: "Cloud DNS Record", Amount: 15.00, OrderedAt: now},
		}
		orders = beam.CreateList(s, sampleOrders)
	}

	// Step 2: Enrich Orders using PostgreSQL Dimension Lookups
	enrichFn := &enrichOrderWithPostgresFn{
		ConnStr:     connStr,
		CacheTTLSec: *cacheTTLSec,
		PoolSize:    *poolSize,
	}
	enriched := beam.ParDo(s, enrichFn, orders)

	// Step 3: Sink Enriched Orders back to PostgreSQL with Idempotent Upsert
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		SSLMode:        *sslMode,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"order_id"},
		BatchSize:      2000,
	}

	writeResult := postgresio.Write(s, "public.enriched_orders", writeOpts, enriched)
	_ = writeResult.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("PostgreSQL enrichment pipeline failed: %v", err)
	}
	fmt.Println("PostgreSQL Source -> Enrichment -> Sink pipeline finished successfully.")
}
