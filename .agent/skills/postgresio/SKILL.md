---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: postgresio
description: Guides using the Apache Beam PostgreSQL connector (postgresio) across Go, Python, and Beam YAML. Use when reading from PostgreSQL (batch parallel reads, CDC replication streams), writing to PostgreSQL (vectorized COPY, staged MERGE/upsert, PgBouncer transaction pooling), or building pipelines with postgresio.
---

# PostgreSQL Connector (`postgresio`) in Apache Beam

The Apache Beam PostgreSQL connector (`postgresio`) provides high-throughput, zero-lock reading, continuous Change Data Capture (CDC) streaming, and high-performance vectorized writing for PostgreSQL databases.

## 1. Connector Capabilities & Architecture

- **Bounded Batch Reads (`postgresio.Read`)**:
  - Parallel partition splitting across primary keys or indexed ranges.
  - Automatic row unmarshaling into Go structs, Python Row dictionaries, or Beam Schemas.
  - Read queries execute without holding long-lived transaction locks.
- **Vectorized Staged Writes (`postgresio.Write`)**:
  - In-memory chunking and streaming via PostgreSQL's binary `COPY FROM STDIN` protocol.
  - Automatic fallback or staged `MERGE` / `ON CONFLICT (...) DO UPDATE` for idempotent upsert semantics.
  - Throughput exceeding **80,000 to 100,000 rows/second**.
  - Built-in dead-letter queue (DLQ) support for unroutable or malformed records.
- **Continuous CDC Streaming (`postgresio.ReadCDC`)**:
  - Zero-contention reading directly from the write-ahead log (WAL) using the native `pgoutput` logical decoding plugin.
  - Implemented as a **Splittable DoFn (SDF)** with dynamic restriction tracking across Log Sequence Numbers (LSN).
  - Event-time watermark extraction based on transaction commit timestamps (`track_commit_timestamp=on`).
  - Native support for PostgreSQL 17 failover-synchronized replication slots (`sync_replication_slots=on`).
- **Connection Pooler Compatibility**:
  - Native `WithPgBouncer(true)` mode for transaction-pooling mode (`pool_mode = transaction`).
  - Prevents session state leaks, avoids prepared statement collision, and enforces transaction-scoped staging tables.
- **Cross-Language Expansion**:
  - Available across Go, Python (via gRPC Expansion Service), and declarative Beam YAML.
  - Registered SchemaTransform URNs:
    - `beam:schematransform:org.apache.beam:postgres_read:v1`
    - `beam:schematransform:org.apache.beam:postgres_write:v1`
    - `beam:schematransform:org.apache.beam:postgres_read_cdc:v1`

---

## 2. Go SDK Usage & Code Snippets

Package import:
```go
import (
    "reflect"

    "github.com/apache/beam/sdks/v2/go/pkg/beam"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
)
```

### 2.1 Bounded Batch Read
Reads records in parallel using indexed column splitting or custom queries:

```go
type Order struct {
    OrderID     int64   `beam:"order_id"`
    CustomerID  int32   `beam:"customer_id"`
    TotalAmount float64 `beam:"total_amount"`
    Status      string  `beam:"status"`
}

func BuildPipeline(s beam.Scope) beam.PCollection {
    ro := postgresio.NewReadOptions(
        postgresio.WithReadHost("localhost"),
        postgresio.WithReadPort(5432),
        postgresio.WithReadDatabase("beammeup"),
        postgresio.WithReadUsername("beam_navigator"),
        postgresio.WithReadPassword("beam_navigator"),
        postgresio.WithReadSSLMode("disable"),
    )

    // Returns a PCollection of Order structs from schema-qualified table "public.orders"
    orders := postgresio.Read(s, "public.orders", reflect.TypeOf(Order{}), ro)
    return orders
}
```

### 2.2 High-Throughput Vectorized Write with Upsert
Batches records in worker memory and streams them via binary `COPY` into staging, followed by set-based atomic `MERGE`:

```go
func WriteOrders(s beam.Scope, orders beam.PCollection) postgresio.WriteResult {
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
```

### 2.3 PgBouncer Transaction Pooling Mode
When connecting to PgBouncer on port 6432 with `pool_mode = transaction`:

```go
func WriteToPgBouncer(s beam.Scope, events beam.PCollection) postgresio.WriteResult {
    wo := postgresio.NewWriteOptions(
        postgresio.WithHost("localhost"),
        postgresio.WithPort(6432),
        postgresio.WithDatabase("beammeup"),
        postgresio.WithUsername("beam_navigator"),
        postgresio.WithPassword("beam_navigator"),
        postgresio.WithSSLMode("disable"),
        postgresio.WithPgBouncer(true), // Enforces single-transaction staging lifecycle
        postgresio.WithPrimaryKeyColumns("event_id"),
    )

    return postgresio.Write(s, "public.events", wo, events)
}
```

### 2.4 Streaming Change Data Capture (CDC)
Streams row-level insert, update, and delete mutations directly from the PostgreSQL write-ahead log:

```go
func StreamCDC(s beam.Scope) beam.PCollection {
    // Reads from publication pub_beam_cdc using replication slot beam_cdc_slot
    cdcStream := postgresio.ReadCDC(s,
        postgresio.WithCDCHost("localhost"),
        postgresio.WithCDCPort(5432),
        postgresio.WithCDCDatabase("beammeup"),
        postgresio.WithCDCUsername("scotty"),
        postgresio.WithCDCPassword("scotty_secret"),
        postgresio.WithCDCSSLMode("disable"),
        postgresio.WithCDCSlotName("beam_cdc_slot"),
        postgresio.WithCDCPublication("pub_beam_cdc"),
        postgresio.WithCDCFailoverSlot(true), // Failover sync ready (PostgreSQL 17)
    )
    return cdcStream
}
```

---

## 3. Python SDK Usage (Cross-Language)

Import the Python wrapper:
```python
import apache_beam as beam
from apache_beam.io.postgres import ReadFromPostgresIO, WriteToPostgresIO, ReadFromPostgresCDC
```

### 3.1 Python Batch Read
```python
with beam.Pipeline() as p:
  orders = p | "ReadOrders" >> ReadFromPostgresIO(
      host="localhost",
      port=5432,
      database="beammeup",
      table="public.orders",
      username="beam_navigator",
      password="beam_navigator",
      sslmode="disable",
      query="SELECT order_id, customer_id, total_amount, status FROM orders",
      expansion_service="localhost:45690" # Optional: defaults to auto-expansion
  )
```

### 3.2 Python Vectorized Write with Dead-Letter Handling
```python
with beam.Pipeline() as p:
  orders = p | beam.Create([
      {"order_id": 1, "customer_id": 101, "total_amount": 99.50, "status": "COMPLETED"},
      {"order_id": 2, "customer_id": 102, "total_amount": 45.00, "status": "PENDING"},
  ])

  result = orders | "WriteOrders" >> WriteToPostgresIO(
      host="localhost",
      port=5432,
      database="beammeup",
      table="public.target_orders",
      username="scotty",
      password="scotty_secret",
      sslmode="disable",
      conflict_keys=["order_id"],
      update_fields=["customer_id", "total_amount", "status"],
      max_batch_rows=1000,
      use_pgbouncer=False,
      expansion_service="localhost:45690"
  )

  # Access successful and failed row outputs
  successful = result.successful_rows
  dead_letter = result.failed_rows
```

### 3.3 Python Streaming CDC with MERGE Replication
`ReadFromPostgresCDC` emits `CDCRecord` rows carrying an operation type. To
replicate deletes as well as inserts and updates, the sink must run in `MERGE`
mode and be told which column holds the operation and which value denotes a
delete. Omitting them degrades the stream to upsert-only and the replica
diverges from the source.

```python
with beam.Pipeline() as p:
  mutations = p | "StreamPostgresCDC" >> ReadFromPostgresCDC(
      host="localhost",
      port=5432,
      database="beammeup",
      slot_name="beam_cdc_slot",
      publication="pub_beam_cdc",
      username="scotty",
      password="scotty_secret",
      sslmode="disable",
      failover_slot=True,
      expansion_service="localhost:45690"
  )

  _ = mutations | "ReplicateToTarget" >> WriteToPostgresIO(
      host="replica.example.com",
      port=5432,
      database="beammeup",
      table="public.orders",
      username="scotty",
      password="scotty_secret",
      sslmode="disable",
      conflict_keys=["order_id"],
      write_mode="MERGE",
      op_column="_op_type",
      delete_op_value="d",
      replication_origin="beam_replica_origin",
      expansion_service="localhost:45690"
  )
```

`failover_slot` requires PostgreSQL 17 or newer. `replication_origin` tags the
sink transactions so a bidirectional topology does not re-capture its own
writes.

`op_column` names a property of the change, not of the stored row. It is read
only to decide which `MERGE` branch applies, and is never inserted or updated,
so the target table does not need the column and gains nothing from having it.
The sink adds it to its own staging table.

`MERGE` mode also constrains two things. It requires the staged `COPY` write
method, which is the default; the parameterized `UNNEST` method builds
`INSERT ... ON CONFLICT`, a statement with no `DELETE` branch. And it cannot
write partial rows, because the staging table carries every column. Both cases
fail the bundle and route the rows to the failed-mutations output rather than
writing a result that is missing the deletes.

---

## 4. Declarative Beam YAML Usage

Beam YAML pipelines can invoke the Go-backed PostgreSQL connector SchemaTransforms:

### 4.1 YAML Batch Read (`ReadFromPostgresIO`)
```yaml
pipeline:
  transforms:
    - type: ReadFromPostgresIO
      name: ExtractActiveOrders
      config:
        host: "localhost"
        port: 5432
        database: "beammeup"
        table: "public.orders"
        username: "beam_navigator"
        password: "beam_navigator"
        sslmode: "disable"
        query: "SELECT order_id, customer_id, total_amount, status FROM orders WHERE status = 'ACTIVE'"
```

### 4.2 YAML Vectorized Staged Sink (`WriteToPostgresIO`)
```yaml
pipeline:
  transforms:
    - type: ReadFromPostgresIO
      name: IngestSource
      config:
        host: "localhost"
        port: 5432
        database: "beammeup"
        table: "public.orders"
        username: "beam_navigator"
        password: "beam_navigator"
        sslmode: "disable"
        query: "SELECT * FROM public.orders"

    - type: WriteToPostgresIO
      name: UpsertTarget
      input: IngestSource
      config:
        host: "localhost"
        port: 5432
        database: "beammeup"
        table: "public.target_orders"
        username: "scotty"
        password: "scotty_secret"
        sslmode: "disable"
        conflict_keys: ["order_id"]
        update_fields: ["customer_id", "total_amount", "status"]
        max_batch_rows: 1000
```

### 4.3 YAML Streaming CDC (`ReadFromPostgresCDC`)
```yaml
pipeline:
  transforms:
    - type: ReadFromPostgresCDC
      name: IngestWALStream
      config:
        host: "localhost"
        port: 5432
        database: "beammeup"
        slot_name: "beam_cdc_slot"
        publication: "pub_beam_cdc"
        username: "scotty"
        password: "scotty_secret"
        sslmode: "disable"
```

### 4.4 YAML CDC Replication with MERGE (`ReadFromPostgresCDC` to `WriteToPostgresIO`)
`publication_tables` applies a column list and row filter at the publication,
so the WAL decoder emits only the requested columns and rows instead of
filtering them downstream. It requires PostgreSQL 15 or newer.

```yaml
pipeline:
  transforms:
    - type: ReadFromPostgresCDC
      name: IngestWALStream
      config:
        host: "localhost"
        port: 5432
        database: "beammeup"
        slot_name: "beam_cdc_slot"
        publication: "pub_beam_cdc"
        username: "scotty"
        password: "scotty_secret"
        sslmode: "disable"
        failover_slot: true
        publication_tables:
          - table_name: "public.orders"
            columns: ["order_id", "customer_id", "status"]
            row_filter: "status <> 'DRAFT'"

    - type: WriteToPostgresIO
      name: ReplicateToTarget
      input: IngestWALStream
      config:
        host: "replica.example.com"
        port: 5432
        database: "beammeup"
        table: "public.orders"
        username: "scotty"
        password: "scotty_secret"
        sslmode: "disable"
        conflict_keys: ["order_id"]
        write_mode: "MERGE"
        op_column: "_op_type"
        delete_op_value: "d"
        replication_origin: "beam_replica_origin"
```

---

## 5. PostgreSQL Database Engine Prerequisites

To use `postgresio` in production, configure the following database settings:

### 5.1 `postgresql.conf` Settings
```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_slot_wal_keep_size = 2048MB   # Critical: prevents lagging slots from filling primary disk
track_commit_timestamp = on       # Required for Beam event-time watermark extraction
password_encryption = scram-sha-256
```

### 5.2 Least-Privilege Role Provisioning (Zero Superuser)
```sql
-- Pipeline user for CDC reading and replication
CREATE ROLE scotty WITH LOGIN REPLICATION PASSWORD 'scotty_secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO scotty;

-- Create publication for CDC
CREATE PUBLICATION pub_beam_cdc FOR ALL TABLES;

-- Pipeline user for unprivileged batch reads/writes
CREATE ROLE beam_navigator WITH LOGIN PASSWORD 'beam_navigator';
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO beam_navigator;
```

---

## 6. Testing & Verifying Pipelines

### 6.1 Executing Go Unit & Integration Tests
Run the connector's Go tests from the `sdks` directory of a Beam checkout:
```bash
cd sdks
go test -count=1 ./go/pkg/beam/io/postgresio/
```
Tests that need a live server skip themselves when one is not reachable, so the
command above is safe without any setup. To include them, start the lab in
section 6.2 first.

### 6.2 Running the Quickstart Container Lab
A self-contained 10-minute containerized testbed is located in `sdks/go/examples/postgres/quickstart/`:
```bash
cd sdks/go/examples/postgres/quickstart
docker compose up -d
go run main.go --runner=prism
```

### 6.3 Verifying Cross-Language Expansion
Run Python unit tests:
```bash
python3 -m unittest apache_beam.io.postgres_cdc_test
```

Start the Go expansion service for live cross-language execution:
```bash
go run sdks/go/cmd/beam-go-expansion-service/main.go --port=45690
```

---

## 7. Common Operational Pitfalls & Best Practices

1. **PgBouncer Session Error**:
   - *Problem*: `ERROR: prepared statement does not exist` or staging tables disappear.
   - *Solution*: Use `postgresio.WithPgBouncer(true)`. In YAML/Python set `use_pgbouncer: true`.
2. **Partitioned Table Conflict Constraint**:
   - *Problem*: `ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification (SQLSTATE 42P10)`.
   - *Solution*: PostgreSQL partitioned tables require the partition key to be part of the unique constraint. Specify all composite primary key columns in `conflict_keys` (e.g. `["order_id", "order_date"]`).
3. **Replication Slot Lag & Disk Growth**:
   - *Problem*: Retained WAL size grows uncontrollably if the Beam pipeline stops.
   - *Solution*: Always configure `max_slot_wal_keep_size` on PostgreSQL. Monitor `pg_replication_slots` using the provided Airflow slot hygiene audit DAG in `sdks/go/examples/postgres/beam-postgres-lab/dags/postgres_slot_hygiene_audit.py`.
4. **Sequence Drift on Cutover**:
   - *Problem*: Target table `INSERT` queries fail with duplicate key violation (`23505`) after cutover.
   - *Solution*: Run `SELECT setval(pg_get_serial_sequence('table', 'id'), MAX(id) + 1000) FROM table;` post-cutover before shifting write traffic.
