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

// Package main runs a continuous CDC synchronization pipeline from PostgreSQL source to target.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "postgres", "PostgreSQL database host.")
	port     = flag.Int("port", 5432, "PostgreSQL database port.")
	database = flag.String("database", "quickstart", "Database name.")
	username = flag.String("username", "beam_cdc", "Replication username.")
	password = flag.String("password", "secret", "Replication password.")
	slotName = flag.String("slot_name", "quickstart_slot", "Logical replication slot name.")
	pubName  = flag.String("publication", "beam_pub", "Publication name.")
)

type targetOrderRow struct {
	ID         int       `beam:"id"`
	CustomerID int       `beam:"customer_id"`
	Amount     float64   `beam:"amount"`
	Status     string    `beam:"status"`
	SyncedAt   time.Time `beam:"synced_at"`
}

func transformCDCToTarget(record postgresio.ChangeEvent, emit func(targetOrderRow)) error {
	if record.Operation == postgresio.OpDelete {
		return nil
	}

	data := record.After
	if len(data) == 0 {
		return nil
	}

	idVal, ok := data["id"].(int64)
	if !ok {
		if f, ok := data["id"].(float64); ok {
			idVal = int64(f)
		}
	}
	custVal, _ := data["customer_id"].(int64)
	amountVal, _ := data["amount"].(float64)
	statusVal, _ := data["status"].(string)

	emit(targetOrderRow{
		ID:         int(idVal),
		CustomerID: int(custVal),
		Amount:     amountVal,
		Status:     statusVal,
		SyncedAt:   time.Now().UTC(),
	})
	return nil
}

func main() {
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	cdcOpts := []postgresio.CDCOption{
		postgresio.WithCDCHost(*host),
		postgresio.WithCDCPort(*port),
		postgresio.WithCDCDatabase(*database),
		postgresio.WithCDCUsername(*username),
		postgresio.WithCDCPassword(*password),
		postgresio.WithCDCSlotName(*slotName),
		postgresio.WithCDCPublication(*pubName),
		postgresio.WithCDCSSLMode("disable"),
		postgresio.WithCDCHeartbeatInterval(10 * time.Second),
	}

	cdcStream := postgresio.ReadCDC(s.Scope("ReadCDC"), cdcOpts...)

	targetRows := beam.ParDo(s.Scope("Transform"), transformCDCToTarget, cdcStream)

	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		SSLMode:        "disable",
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"id"},
		BatchSize:      100,
	}

	postgresio.Write(s.Scope("WriteTarget"), "public.orders_target", writeOpts, targetRows)

	ctx := context.Background()
	fmt.Printf("Starting PostgreSQL CDC quickstart pipeline connecting to %s:%d/%s...\n", *host, *port, *database)
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("pipeline failed: %v", err)
	}
}
