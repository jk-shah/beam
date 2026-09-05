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

`postgresio` is a native Apache Beam Go SDK I/O connector providing high-throughput writing and upserts to PostgreSQL without Java Virtual Machine (JVM) dependencies or cross-language expansion overhead.

---

## 1. Architectural Highlights

* **Parameterized `UNNEST` Array Upsert**: Executes batch inserts and upserts (`INSERT INTO ... ON CONFLICT DO UPDATE`) via vectorized array parameters (`pq.Array`). This avoids `CREATE TEMP TABLE ... ON COMMIT DROP` statements that induce system catalog bloat and locks on `pg_class` and `pg_attribute`.
* **In-Memory Batch Compaction & Deadlock Prevention**: The `BatchCompactor` applies Last-Write-Wins (LWW) deduplication within micro-batches and sorts records canonically by composite primary key prior to database execution. This guarantees uniform row-lock acquisition order across distributed parallel workers, eliminating `SQLState 40P01` deadlocks.
* **Connection Pool Management & PgBouncer Safety**: Clamps worker connection pools based on CPU availability (`runtime.NumCPU() / 2`). Setting `WithPgBouncer(true)` enforces simple query protocol execution, avoiding prepared statement collisions (`SQLState 42P05`) in transaction pooling mode.
* **Dead-Letter Queue (DLQ)**: Separates successfully committed rows from rejected records, appending sanitized error messages and PostgreSQL SQL states without credential leakage.
* **Decoupled Cloud IAM Dialers**: Provides a pluggable `DialFunc` interface (`type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)`), allowing zero-dependency integration with Google Cloud SQL, AlloyDB, and AWS RDS IAM.

---

## 2. Usage Examples

### High-Throughput Upsert Pipeline
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

	// Process or log rejected records routed to Dead-Letter Queue
	// beam.ParDo0(s, logErrorsFn, result.FailedRows)

	if err := beamx.Run(context.Background(), p); err != nil {
		panic(err)
	}
}
```

---

## 3. Configuration Reference

| Option | Default | Purpose |
| :--- | :--- | :--- |
| `WithHost(string)` | `""` | Target PostgreSQL host or IP |
| `WithPort(int)` | `5432` | Target PostgreSQL port |
| `WithDatabase(string)` | `""` | Target database name |
| `WithUsername(string)` | `""` | Database user |
| `WithPassword(string)` | `""` | Database password |
| `WithWriteMode(WriteMode)` | `WriteModeUpsert` | `WriteModeInsert`, `WriteModeUpsert`, `WriteModeUpdate` |
| `WithPrimaryKeyColumns(...string)`| `nil` | Primary key columns for conflict resolution and sorting |
| `WithBatchSize(int)` | `5000` | Maximum rows buffered before issuing a batch write |
| `WithMaxBatchBytes(int)` | `8388608` (8MB) | Maximum payload byte size before flushing |
| `WithFlushInterval(time.Duration)`| `1s` | Maximum latency before flushing in-flight rows |
| `WithMaxConnections(int)` | `NumCPU()/2` | Maximum concurrent database connections per worker |
| `WithPgBouncer(bool)` | `false` | Bypasses prepared statement caching for transaction poolers |
| `WithDialFunc(DialFunc)` | `nil` | Custom network dialer for Cloud SQL / AlloyDB socket tunnels |

---

## 4. Verification and Testing

Execute the unit test suite:
```bash
go test -v -race ./sdks/go/pkg/beam/io/postgresio/...
```
