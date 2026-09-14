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
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*targetOrderRow)(nil)).Elem())
	beam.RegisterFunction(transformCDCToTarget)
}

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
	ID         int       `beam:"id" db:"id"`
	CustomerID int       `beam:"customer_id" db:"customer_id"`
	Amount     float64   `beam:"amount" db:"amount"`
	Status     string    `beam:"status" db:"status"`
	SyncedAt   time.Time `beam:"synced_at" db:"synced_at"`
}

func toInt(v any) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	case string:
		var i int
		fmt.Sscanf(val, "%d", &i)
		return i
	default:
		return 0
	}
}

func toFloat(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case int:
		return float64(val)
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	default:
		return 0
	}
}

func transformCDCToTarget(record postgresio.ChangeEvent, emit func(targetOrderRow)) error {
	log.Printf("[TRANSFORM] received change event: op=%v table=%s.%s lsn=%d", record.Operation, record.Schema, record.Table, record.LSN)
	if record.Operation == postgresio.OpDelete {
		return nil
	}

	data := record.After
	if len(data) == 0 {
		return nil
	}

	idVal := toInt(data["id"])
	custVal := toInt(data["customer_id"])
	amountVal := toFloat(data["amount"])
	statusVal, _ := data["status"].(string)

	emit(targetOrderRow{
		ID:         idVal,
		CustomerID: custVal,
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
		postgresio.WithCDCCreateSlotIfMissing(true),
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
		BatchSize:      1,
	}

	postgresio.Write(s.Scope("WriteTarget"), "public.orders_target", writeOpts, targetRows)

	ctx := context.Background()
	fmt.Printf("Starting PostgreSQL CDC quickstart pipeline connecting to %s:%d/%s...\n", *host, *port, *database)
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("pipeline failed: %v", err)
	}
}
