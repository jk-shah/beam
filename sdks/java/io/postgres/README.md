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

# Apache Beam PostgreSQL I/O Connector (`PostgreSQLIO`)

`PostgreSQLIO` is a high-throughput, cross-language Apache Beam I/O connector designed for real-time Change Data Capture (CDC) streaming ingestion and high-performance bulk/streaming writes for PostgreSQL, Google Cloud SQL for PostgreSQL, and AlloyDB for PostgreSQL.

---

## 1. Architecture Overview

`PostgreSQLIO` provides unified pipeline components for reading and writing PostgreSQL streams and tables:

```
                                  SOURCE PIPELINE (CDC)
 ┌──────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐
 │ PostgreSQL Primary   │ ───▶ │  PostgreSqlWalReader    │ ───▶ │ PostgreSqlDemuxRouter   │
 │ (pgoutput stream)    │      │  (Watermark Snapshots)  │      │ (64 Parallel Channels)  │
 └──────────────────────┘      └─────────────────────────┘      └────────────┬────────────┘
                                                                             │
                                                                             ▼
 ┌──────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐
 │ PCollection<Row> /   │ ◀─── │ WatermarkDeduplication  │ ◀─── │ ToastReconstructionDoFn │
 │ ChangeEvent<Row>     │      │ (LSN Deduplication)     │      │ (Stateful Row Merging)  │
 └──────────────────────┘      └─────────────────────────┘      └─────────────────────────┘

                                   SINK PIPELINE (WRITE)
 ┌──────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐
 │ PCollection<Row>     │ ───▶ │  BatchCompactor         │ ───▶ │ PostgreSqlWrite         │
 │ (Input Records)      │      │  (Intra-Batch PK Dedup) │      │ (UNNEST / Staged COPY)  │
 └──────────────────────┘      └─────────────────────────┘      └────────────┬────────────┘
                                                                             │
                                                                             ▼
                                                                ┌─────────────────────────┐
                                                                │  PostgreSQL Destination │
                                                                │  (Atomic ON CONFLICT)   │
                                                                └─────────────────────────┘
```

### Core Capabilities
* **Source (`PostgreSQLIO.readCDC()`)**:
  * **Lock-Free Watermark Snapshotting**: Dynamic splitting of initial table data interleaved with streaming logical replication without table read locks.
  * **64-Channel Demux Router**: Fanning out replication events across deterministic primary key hash buckets to eliminate single-thread bottlenecks.
  * **Dual-Engine Vectorized Parser**: Zero-allocation binary `pgoutput` unpacking using SIMD memory buffers and direct byte extraction.
  * **Stateful TOAST Column Reconstruction**: Reconstructs unchanged out-of-line TOAST values (`TEXT`, `JSONB`, `BYTEA` $>2\text{ KB}$) in Beam runner-managed state (`ValueState<Row>`) without issuing point queries to the live database.
  * **Dynamic Schema Evolution**: Tracks in-flight DDL changes (`ALTER TABLE`) and projects older tuples onto updated target schemas.
  * **High-Availability Slot Repair**: Automatic slot repair and tombstone anti-join ($\mathcal{K}_{\text{target}} \setminus \mathcal{K}_{\text{source}}$) maintaining exactly-once replication invariants after failovers.
* **Sink (`PostgreSQLIO.write()`)**:
  * **Parameterized `UNNEST` Array Upsert (`STREAMING_UPSERT_UNNEST`)**: Low-latency upserting ($60\text{k}\text{--}80\text{k}$ rows/sec, $\sim 40\text{ms}$ p99) using vectorized SQL arrays.
  * **Staged Binary COPY Upsert (`STAGED_COPY_UPSERT`)**: Bulk ingestion ($100\text{k}\text{--}125\text{k}$ rows/sec) streaming binary rows directly into unlogged temporary tables followed by single-query atomic partition merges.
  * **Connection Pooling**: Thread-safe HikariCP management with PgBouncer compatibility (`prepareThreshold=0`, non-blocking initialization).
  * **Replication Origin Stamping**: Automatic `SELECT pg_replication_origin_session_setup('...')` on connection checkout to prevent multi-master replication loops.
  * **Dead-Letter Queue (DLQ)**: Automatic error routing for unparseable or constraint-violating rows with structured error metadata (`PostgreSqlWriteError`).

---

## 2. Directory & Package Structure

```
sdks/java/io/postgres/
├── build.gradle                                               # Module dependencies & build config
├── README.md                                                  # SDE Onboarding & Architecture Guide
└── src/
    ├── main/java/org/apache/beam/sdk/io/postgres/
    │   ├── PostgreSQLIO.java                                  # Primary entry-point transform API
    │   ├── PostgreSqlDataSourceConfiguration.java             # Connection pool & cloud auth settings
    │   ├── PostgreSqlRowMappers.java                          # Beam Row <-> JDBC/Binary mappings
    │   ├── PostgreSqlTypeUtils.java                           # PostgreSQL type OIDs and conversions
    │   ├── PostgreSqlUtils.java                               # SQL identifier escaping & sanitization
    │   ├── auth/                                              # Dynamic Cloud IAM & Secret Manager auth
    │   │   ├── AlloyDbIamPasswordProvider.java
    │   │   ├── AwsRdsIamPasswordProvider.java
    │   │   ├── GoogleCloudSqlIamPasswordProvider.java
    │   │   └── SecretManagerDynamicPasswordProvider.java
    │   ├── cdc/                                               # Source CDC logical replication engine
    │   │   ├── ChangeEvent.java                               # Strongly-typed CDC change tuple
    │   │   ├── HeartbeatManager.java                          # pg_logical_emit_message keepalives
    │   │   ├── LsnRange.java                                  # Log Sequence Number ranges
    │   │   ├── PgOutputParser.java                            # Protocol parser (M, P, K, r, B, C, I, U, D, T)
    │   │   ├── PostgreSqlArrayDecoder.java                    # Multi-dimensional array decoders
    │   │   ├── PostgreSqlDemuxChannelRouter.java              # 64-channel parallel demux router
    │   │   ├── PostgreSqlDynamicSchemaTracker.java            # In-flight DDL schema evolution
    │   │   ├── PostgreSqlGeospatialCodec.java                 # PostGIS geometry/geography codecs
    │   │   ├── PostgreSqlHaSlotReaderDoFn.java                # Resilient replication slot reader
    │   │   ├── PostgreSqlInFlightTransactionSpooler.java      # Spooling uncommitted transactions
    │   │   ├── PostgreSqlRange.java                           # Range type codecs (int4range, daterange)
    │   │   ├── PostgreSqlReadCDC.java                         # CDC root transform builder
    │   │   ├── PostgreSqlSlotRepairManager.java               # Disaster recovery slot re-creation
    │   │   ├── PostgreSqlSnapshotSourceDoFn.java              # Lock-free initial snapshot DoFn
    │   │   ├── PostgreSqlToastReconstructionDoFn.java         # Stateful out-of-line TOAST cache
    │   │   ├── PostgreSqlVectorizedDecoder.java               # SIMD-aligned binary frame decoder
    │   │   ├── SlotRepairPolicy.java                          # Slot failover recovery policies
    │   │   ├── WatermarkDeduplicationDoFn.java                # LSN deduplication & watermark advancement
    │   │   └── WatermarkSnapshotSourceDoFn.java               # Integrated snapshot + CDC DoFn
    │   ├── provider/                                          # Cross-Language SchemaTransform providers
    │   │   ├── PostgreSqlReadSchemaTransformProvider.java     # beam:schematransform:org.apache.beam:postgres_read:v1
    │   │   └── PostgreSqlWriteSchemaTransformProvider.java    # beam:schematransform:org.apache.beam:postgres_write:v1
    │   ├── sink/                                              # High-throughput sink engine
    │   │   ├── BatchCompactor.java                            # Intra-bundle primary key compactor
    │   │   ├── ConnectionPoolManager.java                     # Shared HikariDataSource pool cache
    │   │   ├── CopyManagerBinaryWriterDoFn.java               # Binary COPY protocol writer
    │   │   ├── PostgreSqlSinkReconciler.java                  # Tombstone anti-join reconciler
    │   │   ├── PostgreSqlWrite.java                           # Sink root transform builder
    │   │   ├── SanitizingExceptionTransformer.java            # Safe error message scrubber
    │   │   ├── StagedCopyUpsertWriterDoFn.java                # Staged temporary table COPY writer
    │   │   └── UnnestArrayUpsertWriterDoFn.java               # Parameterized UNNEST array upsert writer
    │   └── templates/                                         # Production Dataflow templates
    │       ├── PostgreSqlStreamingReplicationTemplate.java
    │       ├── PostgreSqlTemplateOptions.java
    │       ├── PostgreSqlToBigQueryStreamingTemplate.java
    │       └── PostgreSqlToIcebergStreamingTemplate.java
    └── test/java/org/apache/beam/sdk/io/postgres/             # Comprehensive unit & integration test suites
```

---

## 3. Engineering Invariants & Coding Guidelines

When adding features or modifying code in `PostgreSQLIO`, you must adhere to the following invariants:

### A. Non-Blocking Connection Pool Initialization
* **Invariant**: Never let `HikariDataSource` eagerly block during pipeline construction.
* **Implementation**: Always specify `config.setInitializationFailTimeout(-1)` in `PostgreSqlDataSourceConfiguration`. This ensures that worker nodes can assemble execution graphs before external database network routes become reachable.

### B. Output Schema & Coder Propagation
* **Invariant**: In Beam's Java SDK, whenever emitting `Row` elements through `TupleTag<Row>` or `PCollectionRowTuple`, you must explicitly register the Schema or RowCoder on the output `PCollection`:
  ```java
  pcollection.setRowSchema(schema);
  // or
  pcollection.setCoder(RowCoder.of(schema));
  ```
  Failing to set the schema will cause pipeline construction errors on runners enforcing type inference.

### C. Nullable Schema Fields for TOAST Tables
* **Invariant**: Under PostgreSQL default replica identity (`REPLICA IDENTITY DEFAULT`), `UPDATE` events omit unchanged out-of-line TOAST columns on the wire.
* **Implementation**: Tables with TOAST attributes must define columns as nullable (`addNullableField(...)`) so that parser null insertions do not violate non-null schema assertions before stateful reconstruction.

### D. Multi-Master CDC Loop Prevention
* **Invariant**: Writes committed by Beam sink pipelines must be tagged with a replication origin to avoid infinite replication reflection in bidirectional setups.
* **Implementation**: Use `.withReplicationOriginName("beam_origin")` which issues `SELECT pg_replication_origin_session_setup('...')` on connection checkout.

### E. IAM Token Lifetime & Pool Recycling
* **Invariant**: Google Cloud SQL and AlloyDB OAuth2/IAM tokens expire after 60 minutes.
* **Implementation**: Default `maxLifetimeMs` is set to **30 minutes ($1,800,000\text{ ms}$)** in `PostgreSqlDataSourceConfiguration` to ensure physical connections are recycled before credential expiration.

---

## 4. Cross-Language & Multi-SDK Usage

`PostgreSQLIO` exposes standard SchemaTransform providers with full parameter parity across all SDKs:

### Python SDK (`apache_beam.transforms.managed`)
```python
import apache_beam as beam
from apache_beam.transforms.managed import POSTGRES, Read, Write

# Read CDC Stream
with beam.Pipeline() as p:
    events = p | "ReadCDC" >> Read(
        POSTGRES,
        config={
            "url": "jdbc:postgresql://host:5432/mydb",
            "table": "public.orders",
            "username": "postgres",
            "password": "password",
            "slot_name": "beam_slot",
            "publication_name": "beam_pub",
        },
    )

# Write Stream
with beam.Pipeline() as p:
    _ = (
        p
        | "CreateRows" >> beam.Create(rows)
        | "WritePostgres" >> Write(
            POSTGRES,
            config={
                "url": "jdbc:postgresql://host:5432/mydb",
                "table": "public.orders_sink",
                "primary_key_columns": ["order_id"],
                "write_mode": "STREAMING_UPSERT_UNNEST",
            },
        )
    )
```

### Beam YAML
```yaml
pipeline:
  transforms:
    - type: ReadFromPostgres
      name: ReadOrdersCDC
      config:
        url: "jdbc:postgresql://localhost:5432/orders_db"
        table: "public.orders"
        slotName: "orders_cdc_slot"
        publicationName: "orders_pub"

    - type: WriteToPostgres
      name: WriteUpsertOrders
      config:
        url: "jdbc:postgresql://localhost:5432/analytics_db"
        table: "public.orders_summary"
        primaryKeyColumns: ["order_id"]
        writeMode: "STREAMING_UPSERT_UNNEST"
```

### Go SDK (`sdks/go/pkg/beam/io/xlang/postgresio`)
```go
import "github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/postgresio"

// Read CDC
events := postgresio.Read(s, postgresio.ReadConfig{
    Url:             "jdbc:postgresql://localhost:5432/mydb",
    Table:           "public.users",
    SlotName:        "go_cdc_slot",
    PublicationName: "go_pub",
})

// Write
postgresio.Write(s, rows, postgresio.WriteConfig{
    Url:               "jdbc:postgresql://localhost:5432/mydb",
    Table:             "public.users_sink",
    PrimaryKeyColumns: []string{"user_id"},
    WriteMode:         "STREAMING_UPSERT_UNNEST",
})
```

---

## 5. Development & Testing Workflow

### Building and Running Tests
All tests can be executed via Gradle on a Java 21 environment:

```bash
# Run all unit and integration test suites in sdks/java/io/postgres
./gradlew :sdks:java:io:postgres:test -PenableCheckerFramework=false

# Run a specific test suite
./gradlew :sdks:java:io:postgres:test --tests "org.apache.beam.sdk.io.postgres.cdc.PostgreSqlUpstreamSemanticsConformanceTest" -PenableCheckerFramework=false

# Run Sink integration tests
./gradlew :sdks:java:io:postgres:test --tests "org.apache.beam.sdk.io.postgres.sink.UnnestArrayUpsertWriterTest" -PenableCheckerFramework=false
```

---

## 6. Upstream PostgreSQL Conformance Testing

`PostgreSQLIO` maintains a dedicated semantic test suite replicating the official upstream PostgreSQL core tests from [`src/test/modules/test_decoding/sql/`](https://github.com/postgres/postgres/tree/master/src/test/modules/test_decoding/sql):

* **`PostgreSqlUpstreamSemanticsConformanceTest`**:
  * `toast.sql`: Insertion of large out-of-line values ($>2\text{ KB}$), unchanged attribute updates (`'u'`), and toast modifications.
  * `truncate.sql`: Atomic multi-table `TRUNCATE` messages with `CASCADE` (0x01) and `RESTART IDENTITY` (0x02) option flags.
  * `stream.sql` / `spill.sql`: In-flight transaction streaming (`STREAM START`, `STREAM STOP`, `STREAM COMMIT`, `STREAM ABORT`) and buffer discard on rollback.
  * `prepared.sql`: Two-phase commit transactions (`PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`).
  * `messages.sql`: Generic logical messages emitted via `pg_logical_emit_message` (transactional and non-transactional).

When modifying low-level protocol parsing in `PgOutputParser.java`, always ensure that this conformance test suite passes 100%.
