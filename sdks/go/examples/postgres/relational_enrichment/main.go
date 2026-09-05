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

// Package main demonstrates a relational stream join and Customer 360 enrichment
// pipeline using Apache Beam CoGroupByKey with PostgreSQL.
//
// Use Case:
// E-commerce orders arrive continuously with only a customer_id foreign key. To provide
// real-time customer analytics, this pipeline joins incoming orders with customer
// profile dimensions (name, VIP tier, loyalty status) using CoGroupByKey, and sinks the
// enriched customer 360 transaction view into PostgreSQL.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "postgres", "PostgreSQL database name")
	username = flag.String("username", "beam_test", "PostgreSQL user")
	password = flag.String("password", "beam_test", "PostgreSQL password")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Order)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*CustomerProfile)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*EnrichedOrder)(nil)).Elem())
	beam.RegisterDoFn(&keyOrderByCustFn{})
	beam.RegisterDoFn(&keyCustByIDFn{})
	beam.RegisterDoFn(&enrichOrderWithCustomerFn{})
}

// Order represents an incoming purchase transaction.
type Order struct {
	OrderID    string    `json:"order_id" db:"order_id" beam:"order_id"`
	CustomerID string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	Item       string    `json:"item" db:"item" beam:"item"`
	Amount     float64   `json:"amount" db:"amount" beam:"amount"`
	OrderedAt  time.Time `json:"ordered_at" db:"ordered_at" beam:"ordered_at"`
}

// CustomerProfile represents dimension metadata for a user.
type CustomerProfile struct {
	CustomerID  string `json:"customer_id" db:"customer_id" beam:"customer_id"`
	Name        string `json:"name" db:"name" beam:"name"`
	Email       string `json:"email" db:"email" beam:"email"`
	LoyaltyTier string `json:"loyalty_tier" db:"loyalty_tier" beam:"loyalty_tier"`
}

// EnrichedOrder represents the joined 360-degree analytics record.
type EnrichedOrder struct {
	OrderID       string    `json:"order_id" db:"order_id" beam:"order_id"`
	CustomerID    string    `json:"customer_id" db:"customer_id" beam:"customer_id"`
	CustomerName  string    `json:"customer_name" db:"customer_name" beam:"customer_name"`
	CustomerEmail string    `json:"customer_email" db:"customer_email" beam:"customer_email"`
	LoyaltyTier   string    `json:"loyalty_tier" db:"loyalty_tier" beam:"loyalty_tier"`
	Item          string    `json:"item" db:"item" beam:"item"`
	Amount        float64   `json:"amount" db:"amount" beam:"amount"`
	EnrichedAt    time.Time `json:"enriched_at" db:"enriched_at" beam:"enriched_at"`
}

type keyOrderByCustFn struct{}

func (fn *keyOrderByCustFn) ProcessElement(o Order, emit func(string, Order)) {
	emit(o.CustomerID, o)
}

type keyCustByIDFn struct{}

func (fn *keyCustByIDFn) ProcessElement(c CustomerProfile, emit func(string, CustomerProfile)) {
	emit(c.CustomerID, c)
}

// enrichOrderWithCustomerFn performs an inner/left join across orders and customer profiles.
type enrichOrderWithCustomerFn struct{}

func (fn *enrichOrderWithCustomerFn) ProcessElement(
	customerID string,
	orders func(*Order) bool,
	customers func(*CustomerProfile) bool,
	emit func(EnrichedOrder),
) {
	// Retrieve customer profile
	var cust CustomerProfile
	hasCust := customers(&cust)
	if !hasCust {
		// Default profile if customer record not yet arrived
		cust = CustomerProfile{
			CustomerID:  customerID,
			Name:        "Unknown Customer",
			Email:       "unregistered@domain.com",
			LoyaltyTier: "STANDARD",
		}
	}

	now := time.Now().UTC()
	var o Order
	for orders(&o) {
		emit(EnrichedOrder{
			OrderID:       o.OrderID,
			CustomerID:    customerID,
			CustomerName:  cust.Name,
			CustomerEmail: cust.Email,
			LoyaltyTier:   cust.LoyaltyTier,
			Item:          o.Item,
			Amount:        o.Amount,
			EnrichedAt:    now,
		})
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	now := time.Now().UTC()

	// 1. Ingest Orders
	sampleOrders := []Order{
		{OrderID: "ORD-1001", CustomerID: "CUST-1", Item: "Compute Engine Server", Amount: 450.00, OrderedAt: now},
		{OrderID: "ORD-1002", CustomerID: "CUST-2", Item: "Cloud Storage Bucket", Amount: 80.00, OrderedAt: now},
		{OrderID: "ORD-1003", CustomerID: "CUST-1", Item: "BigQuery Slots", Amount: 1200.00, OrderedAt: now},
		{OrderID: "ORD-1004", CustomerID: "CUST-3", Item: "Cloud Spanner Node", Amount: 950.00, OrderedAt: now},
	}
	ordersCol := beam.CreateList(s, sampleOrders)

	// 2. Ingest Customers
	sampleCustomers := []CustomerProfile{
		{CustomerID: "CUST-1", Name: "Alice Enterprise", Email: "alice@enterprise.org", LoyaltyTier: "PLATINUM"},
		{CustomerID: "CUST-2", Name: "Bob Startup", Email: "bob@startup.io", LoyaltyTier: "SILVER"},
		{CustomerID: "CUST-3", Name: "Charlie AI", Email: "charlie@ai-labs.com", LoyaltyTier: "GOLD"},
	}
	customersCol := beam.CreateList(s, sampleCustomers)

	// 3. Key both collections by CustomerID
	keyedOrders := beam.ParDo(s, &keyOrderByCustFn{}, ordersCol)
	keyedCusts := beam.ParDo(s, &keyCustByIDFn{}, customersCol)

	// 4. Relational Join via CoGroupByKey
	joined := beam.CoGroupByKey(s, keyedOrders, keyedCusts)

	// 5. Transform joined streams into unified EnrichedOrder records
	enriched := beam.ParDo(s, &enrichOrderWithCustomerFn{}, joined)

	// 6. Sink to PostgreSQL enriched_orders table with atomic upsert
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"order_id"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.enriched_orders", writeOpts, enriched)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Relational enrichment CoGroupByKey pipeline completed successfully.")
}
