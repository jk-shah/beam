<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam Go SDK: PostgreSQLIO (`postgresio`)

`postgresio` is a native Apache Beam Go SDK I/O connector providing high-throughput writing, upserts, and Change Data Capture (CDC) streaming for PostgreSQL without Java Virtual Machine (JVM) dependencies or cross-language expansion overhead.

---

## 1. Architectural Highlights

### High-Throughput Write Engine (`postgresio.Write`)
* **Parameterized `UNNEST` Array Upsert**: Executes batch inserts and upserts (`INSERT INTO ... ON CONFLICT DO UPDATE`) via vectorized array parameters (`pq.Array`). This avoids `CREATE TEMP TABLE ... ON COMMIT DROP` statements that induce system catalog bloat and locks on `pg_class` and `pg_attribute`.
* **In-Memory Batch Compaction & Deadlock Prevention**: The `BatchCompactor` applies Last-Write-Wins (LWW) deduplication within micro-batches and sorts records canonically by composite primary key prior to database execution. This guarantees uniform row-lock acquisition order across distributed parallel workers, eliminating `SQLState 40P01` deadlocks.
* **Connection Pool Management & PgBouncer Safety**: Clamps worker connection pools based on CPU availability (`runtime.NumCPU() / 2`). Setting `WithPgBouncer(true)` enforces simple query protocol execution, avoiding prepared statement collisions (`SQLState 42P05`) in transaction pooling mode.
* **Dead-Letter Queue (DLQ)**: Separates successfully committed rows from rejected records, appending sanitized error messages and PostgreSQL SQL states without credential leakage.

### Change Data Capture Streaming Source (`postgresio.ReadCDC`)
* **Pure Go `pgoutput` Binary Decoder**: Zero-dependency binary decoder implementing PostgreSQL's logical streaming replication protocol (`pgoutput`). Parses `Begin`, `Commit`, `Relation`, `Insert`, `Update`, `Delete`, `Truncate`, and `Keepalive` messages directly into typed `ChangeEvent` structs.
* **Decoupled Keepalive Heartbeat & Checkpoint Semantics**: A dedicated background goroutine sends periodic `StandbyStatusUpdate` ('r') messages to prevent PostgreSQL's `wal_sender_timeout` (60s) drops during downstream backpressure. Crucially, the heartbeat advances the client timestamp while strictly keeping `FlushLSN` locked to the last confirmed bundle checkpoint (coordinated via Beam's `BundleFinalizer`), eliminating silent data loss hazards on worker crashes.
* **Single-Consumer Slot Invariant with Auto-Partitioned Fanout**: Strictly maintains a single connection (`Parallelism = 1`) at the replication slot boundary (`active_pid` exclusivity), feeding downstream parallel worker clusters via `postgresio.PartitionByPrimaryKey` and `beam.Reshuffle`.
* **Stateful Out-of-Line TOAST Reassembly**: Under `REPLICA IDENTITY DEFAULT`, unmodified large columns are omitted by PostgreSQL as `'u'`. `postgresio.ReassembleToast` uses Beam runner state (`state.Value[ChangeEvent]`) to cache baseline tuples and patch unmodified TOAST fields on `UPDATE` events.
* **Dynamic Cloud IAM Token Renewal**: Integrates the `TokenProvider` interface to automatically refresh credentials across worker reconnects for AWS RDS IAM (15-min expiry) and Google Cloud SQL / AlloyDB (60-min expiry).

---

## 2. Usage Examples

### High-Throughput Write & Upsert Pipeline
```go
package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

type Order struct {
	ID     int64   `db:"id"`
	Region string  `db:"region"`
	Amount float64 `db:"amount"`
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	orders := []Order{
		{ID: 101, Region: "US-EAST", Amount: 250.50},
		{ID: 102, Region: "EU-WEST", Amount: 120.00},
	}
	input := beam.CreateList(s, orders)

	opts := postgresio.NewWriteOptions(
		postgresio.WithHost("10.0.1.50"),
		postgresio.WithPort(5432),
		postgresio.WithDatabase("production"),
		postgresio.WithUsername("beam_writer"),
		postgresio.WithPassword("secret123"),
		postgresio.WithPrimaryKeyColumns("id"),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
		postgresio.WithBatchSize(5000),
	)

	result := postgresio.Write(s, "public.orders", opts, input)

	if err := beamx.Run(context.Background(), p); err != nil {
		panic(err)
	}
}
```

### Continuous CDC Streaming Pipeline
```go
package main

import (
	"context"
	"flag"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func logChangeFn(ctx context.Context, evt postgresio.ChangeEvent) {
	log.Infof(ctx, "CDC Event: op=%s table=%s lsn=%d after=%v",
		evt.Operation, evt.FullTableName(), evt.LSN, evt.After)
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	// 1. Single-consumer logical replication stream
	changes := postgresio.ReadCDC(s,
		postgresio.WithCDCHost("10.0.1.50"),
		postgresio.WithCDCPort(5432),
		postgresio.WithCDCDatabase("production"),
		postgresio.WithCDCUsername("repl_user"),
		postgresio.WithCDCPassword("repl_secret"),
		postgresio.WithCDCSlotName("beam_streaming_slot"),
		postgresio.WithCDCPublication("sales_pub"),
		postgresio.WithCDCHeartbeatInterval(10*time.Second),
	)

	// 2. Partition and fanout downstream across cluster workers
	partitioned := postgresio.PartitionByPrimaryKey(s, changes)

	// 3. Reassemble out-of-line TOAST values from state cache
	hydrated := postgresio.ReassembleToast(s, partitioned)

	// 4. Downstream processing
	beam.ParDo0(s, logChangeFn, hydrated)

	if err := beamx.Run(context.Background(), p); err != nil {
		panic(err)
	}
}
```

---

## 3. Configuration Reference

### Write Options (`WriteOptions`)
| Option | Default | Purpose |
| :--- | :--- | :--- |
| `WithHost(string)` | `""` | Target PostgreSQL host or IP |
| `WithPort(int)` | `5432` | Target PostgreSQL port |
| `WithDatabase(string)` | `""` | Target database name |
| `WithUsername(string)` | `""` | Database user |
| `WithPassword(string)` | `""` | Database password |
| `WithWriteMode(WriteMode)` | `WriteModeUpsert` | Mutation mode (`WriteModeInsert`, `WriteModeUpsert`, `WriteModeUpdate`) |
| `WithPrimaryKeyColumns(...string)` | `nil` | Primary key columns used for `ON CONFLICT` resolution |
| `WithBatchSize(int)` | `5000` | Maximum rows per micro-batch flush |
| `WithMaxBatchBytes(int)` | `8388608` (8 MB) | Maximum bytes per micro-batch flush |
| `WithFlushInterval(Duration)` | `1s` | Maximum time between micro-batch flushes |
| `WithPgBouncer(bool)` | `false` | Disables prepared statement caching for PgBouncer transaction pooling |
| `WithDialFunc(DialFunc)` | `nil` | Custom dialer for Cloud SQL / AlloyDB / AWS RDS IAM sockets |

### CDC Options (`CDCOptions`)
| Option | Default | Purpose |
| :--- | :--- | :--- |
| `WithCDCHost(string)` | `""` | PostgreSQL host or IP |
| `WithCDCPort(int)` | `5432` | PostgreSQL port |
| `WithCDCDatabase(string)` | `""` | Database name |
| `WithCDCUsername(string)` | `""` | Replication username |
| `WithCDCPassword(string)` | `""` | Replication password |
| `WithCDCSlotName(string)` | `""` | Replication slot name (`^[a-z0-9_]{1,63}$`) |
| `WithCDCPublication(string)` | `""` | Publication name |
| `WithCDCStartLSN(uint64)` | `0` | Starting Log Sequence Number (LSN) |
| `WithCDCHeartbeatInterval(Duration)` | `10s` | Frequency of StandbyStatusUpdate keepalive transmissions (<60s) |
| `WithCDCReplicaIdentityFull(bool)` | `false` | Signals source tables use `REPLICA IDENTITY FULL` |
| `WithCDCTokenProvider(TokenProvider)` | `nil` | Dynamic credential refresh provider for IAM / OAuth2 |
| `WithCDCDialFunc(DialFunc)` | `nil` | Custom network dialer |
