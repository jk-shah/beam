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

# PostgreSQLIO Pull Request Reviewer's Guide

This guide provides Apache Beam committers and reviewers with an architectural map, file-by-file review sequence, design rationale, and test verification procedures for reviewing `PostgreSQLIO`.

---

## 1. Suggested Review Order (File-by-File)

To facilitate a structured review, review the codebase in the following five layers:

```
 LAYER 1: Core API & Configuration ─────────▶ LAYER 2: High-Throughput Write Sink
 (PostgreSQLIO, PostgreSqlDataSourceConfig)   (PostgreSqlWrite, UNNEST, Staged COPY)
                     │                                         │
                     ▼                                         ▼
 LAYER 3: CDC Ingestion & Protocol Engine ──▶ LAYER 4: Cross-Language & Multi-SDK
 (PgOutputParser, TOAST, VectorizedDecoders)  (SchemaTransforms, Python, Go, YAML)
                     │
                     ▼
 LAYER 5: Conformance & Unit Test Suites
 (PostgreSqlUpstreamSemanticsConformanceTest, 28 Test Suites)
```

### Step 1: Core API & Configuration
Start here to understand the public entry point and connection lifecycle:
1. `src/main/java/org/apache/beam/sdk/io/postgres/PostgreSQLIO.java`: Top-level user-facing API (`readCDC()` and `write()`).
2. `src/main/java/org/apache/beam/sdk/io/postgres/PostgreSqlDataSourceConfiguration.java`: HikariCP connection pooling, PgBouncer compatibility (`prepareThreshold=0`), dynamic cloud IAM (Cloud SQL, AlloyDB, AWS RDS), and Secret Manager resolution.

### Step 2: High-Throughput Write Sink
Review how records are batched, compacted, and written:
1. `src/main/java/org/apache/beam/sdk/io/postgres/sink/PostgreSqlWrite.java`: Root sink transform orchestrating batching and error routing.
2. `src/main/java/org/apache/beam/sdk/io/postgres/sink/BatchCompactor.java`: Intra-bundle primary key deduplication and canonical sorting to prevent PostgreSQL `SQLState 40P01` deadlock errors.
3. `src/main/java/org/apache/beam/sdk/io/postgres/sink/UnnestArrayUpsertWriterDoFn.java`: Parameterized `UNNEST(ARRAY[...])` array upsert writer ($60\text{k}\text{--}80\text{k}$ rows/sec).
4. `src/main/java/org/apache/beam/sdk/io/postgres/sink/StagedCopyUpsertWriterDoFn.java`: Binary COPY temp-table partition merger ($100\text{k}\text{--}125\text{k}$ rows/sec).
5. `src/main/java/org/apache/beam/sdk/io/postgres/sink/ConnectionPoolManager.java`: Thread-safe worker datasource cache.

### Step 3: CDC Ingestion & Binary Protocol Engine
Review how logical replication events are parsed, reconstructed, and deduplicated:
1. `src/main/java/org/apache/beam/sdk/io/postgres/cdc/PgOutputParser.java`: Zero-allocation binary parser for PostgreSQL `pgoutput` wire protocol ('B', 'C', 'R', 'I', 'U', 'D', 'T', 'S', 'E', 'c', 'A', 'M', 'P', 'K', 'r').
2. `src/main/java/org/apache/beam/sdk/io/postgres/cdc/PostgreSqlToastReconstructionDoFn.java`: Stateful out-of-line TOAST column reconstruction in runner-managed state (`ValueState<Row>`).
3. `src/main/java/org/apache/beam/sdk/io/postgres/cdc/PostgreSqlDemuxChannelRouter.java`: 64-channel parallel stream demux router.
4. `src/main/java/org/apache/beam/sdk/io/postgres/cdc/PostgreSqlDynamicSchemaTracker.java`: Dynamic schema evolution tracker for in-flight DDL changes.
5. `src/main/java/org/apache/beam/sdk/io/postgres/cdc/WatermarkDeduplicationDoFn.java`: Log Sequence Number (LSN) deduplication and watermark advancement.

### Step 4: Cross-Language SchemaTransform Providers & Multi-SDK Parity
Review how the connector is exposed to Python, Go, and Beam YAML:
1. `src/main/java/org/apache/beam/sdk/io/postgres/provider/PostgreSqlReadSchemaTransformProvider.java`: `beam:schematransform:org.apache.beam:postgres_read:v1`.
2. `src/main/java/org/apache/beam/sdk/io/postgres/provider/PostgreSqlWriteSchemaTransformProvider.java`: `beam:schematransform:org.apache.beam:postgres_write:v1`.
3. `sdks/python/apache_beam/transforms/managed.py`: Python Managed transform bindings (`ReadFromPostgres`, `WriteToPostgres`).
4. `sdks/go/pkg/beam/io/xlang/postgresio/postgres.go`: Go cross-language package.

### Step 5: Test Suites & Semantic Conformance
Review the test suites:
1. `src/test/java/org/apache/beam/sdk/io/postgres/cdc/PostgreSqlUpstreamSemanticsConformanceTest.java`: Direct replication of upstream PostgreSQL core tests (`toast.sql`, `truncate.sql`, `stream.sql`, `spill.sql`, `prepared.sql`, `messages.sql`).
2. `src/test/java/org/apache/beam/sdk/io/postgres/sink/UnnestArrayUpsertWriterTest.java`: Unit tests for parameterized SQL generation across single PK, composite PK, and all-PK tables.
3. `src/test/java/org/apache/beam/sdk/io/postgres/CloudManagedDataSourceConfigurationTest.java`: IAM auth and Secret Manager resolution tests.

---

## 2. Reviewer FAQ & Architectural Rationale

### Q1: Why not reuse `JdbcIO` for PostgreSQL CDC?
* **Rationale**: `JdbcIO` is designed for batch query partitioning (`SELECT ... WHERE split_col BETWEEN ? AND ?`) and JDBC batch inserts. Logical decoding requires streaming binary socket replication (`pgoutput`), LSN progression, LSN-based stateful deduplication, dynamic schema evolution tracking, and stateful TOAST reconstruction, which cannot be expressed within `JdbcIO`'s execution model.

### Q2: How are unchanged TOAST columns handled without MVCC dirty reads?
* **Rationale**: Under PostgreSQL `REPLICA IDENTITY DEFAULT`, `UPDATE` events omit unchanged out-of-line TOAST columns (`TEXT`, `JSONB`, `BYTEA` $>2\text{ KB}$). Issuing point queries back to the database introduces transaction locks and MVCC dirty read hazards. `PostgreSqlToastReconstructionDoFn` caches the last known baseline row in runner-managed persistent state (`ValueState<Row>`) with a 24-hour sliding TTL, merging attributes locally without database queries.

### Q3: How is SQL injection prevented in dynamic query generation?
* **Rationale**: Table names, schema names, and column identifiers are validated and escaped using standard PostgreSQL double-quoting (`PostgreSqlUtils.escapeIdentifier`), and replication origin names are validated with strict alphanumeric regex matching (`^[a-zA-Z0-9_]{1,64}$`). All row values in `UNNEST` writers are passed as parameterized JDBC arrays (`?`), never concatenated as raw literals.

### Q4: How does connection pooling behave during pipeline construction?
* **Rationale**: `HikariConfig.setInitializationFailTimeout(-1)` is explicitly configured so that connection pools do not eagerly attempt network handshakes during DAG assembly on the job submission client before runner workers launch.

### Q5: What third-party dependencies are required?
* **Rationale**: All runtime dependencies are standard permissive open-source libraries (**ASF Category A**: PostgreSQL JDBC driver, HikariCP, Jackson, Guava, SLF4J, Joda-Time). Optional Cloud IAM / AWS providers use reflection or `compileOnly` scopes to avoid mandatory heavy transitive dependencies.

---

## 3. Fast Verification Commands

Reviewers can verify formatting, licensing, and all 28 test suites locally in under 60 seconds:

```bash
# 1. Verify code formatting and spotless standards
./gradlew :sdks:java:io:postgres:spotlessCheck

# 2. Run all unit and integration test suites (85 tests, ~27s)
./gradlew :sdks:java:io:postgres:test -PenableCheckerFramework=false

# 3. Run upstream PostgreSQL semantic conformance suite specifically
./gradlew :sdks:java:io:postgres:test --tests "org.apache.beam.sdk.io.postgres.cdc.PostgreSqlUpstreamSemanticsConformanceTest" -PenableCheckerFramework=false

# 4. Verify Go SDK cross-language tests
cd sdks/go/pkg/beam/io/xlang/postgresio && go test -v .
```

---

## 4. Proposed PR Staging Strategy

For committers preferring staged, bite-sized pull requests rather than a single large merge:

| PR Stage | PR Title | Scope | Lines of Code |
| :---: | :--- | :--- | :---: |
| **PR 1** | **PostgreSQLIO: Core Configuration & High-Throughput Write Sinks** | `PostgreSQLIO.write()`, `UnnestArrayUpsertWriter`, `StagedCopyUpsertWriter`, `BatchCompactor`, `ConnectionPoolManager`, and 9 sink test suites. | ~1,800 |
| **PR 2** | **PostgreSQLIO: High-Performance CDC Ingestion & Logical Replication Engine** | `PostgreSQLIO.readCDC()`, `PgOutputParser`, `ToastReconstructionDoFn`, `DynamicSchemaTracker`, `DemuxRouter`, and upstream conformance tests. | ~2,400 |
| **PR 3** | **PostgreSQLIO: Cross-Language SchemaTransforms & Multi-SDK Parity** | `PostgreSqlReadSchemaTransformProvider`, `PostgreSqlWriteSchemaTransformProvider`, Python Managed integration, Go package, Beam YAML tests. | ~1,200 |
| **PR 4** | **PostgreSQLIO: Cloud IAM Auth Providers & Dataflow Streaming Templates** | Cloud SQL IAM, AlloyDB IAM, AWS RDS IAM, Secret Manager provider, and BigQuery/Iceberg replication templates. | ~1,100 |
