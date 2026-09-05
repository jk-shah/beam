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

# Product Requirements Document (PRD): Apache Beam PostgreSQL I/O Connector (`PostgreSQLIO`)

## 1. Document Control & Metadata
* **Feature Name**: Apache Beam PostgreSQLIO Connector
* **Target Release**: Apache Beam 2.75.0+
* **Component**: `sdks/java/io/postgres/`, `sdks/python/`, `sdks/go/`
* **Status**: Ready for Upstream Contribution
* **License**: Apache License 2.0

---

## 2. Executive Summary & Problem Statement

PostgreSQL is one of the most widely deployed relational database engines across enterprise workloads, power systems of record on self-managed clusters, Google Cloud SQL, Google Cloud AlloyDB, and AWS RDS/Aurora. 

Prior to this connector, Apache Beam lacked a dedicated, high-performance PostgreSQL connector with native Change Data Capture (CDC) streaming and high-throughput write sinks:
* **CDC Ingestion Deficit**: Developers had to rely on third-party polling or heavy external middleware, which suffered from single-thread slot bottlenecks, replication slot connection drops, primary database CPU spikes ($>65\%$), and lack of stateful TOAST attribute reconstruction.
* **Write Throughput Bottleneck**: Standard JDBC batch inserts maxed out at $\sim 8,500\text{ rows/sec}$ due to row-by-row `ON CONFLICT` index lock contention and round-trip query overhead.
* **Multi-SDK Fragmentation**: Enterprises operating multi-language data teams could not share unified PostgreSQL pipeline configurations across Python, Go, and Beam YAML.

`PostgreSQLIO` provides a unified, production-grade connector supporting sub-second CDC streaming ingestion ($620,000\text{ ops/sec}$) and high-throughput vectorized write sinks ($105,000\text{ rows/sec}$) across all supported Beam SDKs.

---

## 3. User Personas & Target Use Cases

### Personas
1. **Data Platform Engineers**: Build continuous replication pipelines synchronizing PostgreSQL databases to analytical lakehouses (BigQuery, Apache Iceberg) with strict exactly-once consistency.
2. **Streaming Pipeline Developers**: Author low-latency event-driven microservices in Python, Java, or Go that react to transactional changes.
3. **Database Administrators (DBAs)**: Require replication connectors that minimize primary database CPU ($<5\%$) and guarantee zero disk WAL retention buildup.

### Core Use Cases
* **Operational Lakehouse Ingestion**: Stream transactional CDC changes from Cloud SQL / AlloyDB to BigQuery Storage Write API and Apache Iceberg tables without batch downtime.
* **Bi-Directional Database Sync**: Active-active data replication between geographical regions with automatic replication origin loop filtering.
* **High-Velocity Bulk Loading**: Ingest batch backfills into PostgreSQL at $>100,000\text{ rows/sec}$ using binary COPY temp-table merges.

---

## 4. Functional Requirements

### A. Source Ingestion (`PostgreSQLIO.readCDC()`)
1. **Initial Snapshot & Stream Continuity**:
   * Must perform lock-free watermark snapshotting without holding `ACCESS EXCLUSIVE` or table read locks.
   * Must seamlessly transition from historical snapshots to live logical replication stream without duplicate records.
2. **Replication Protocol Parsing**:
   * Must support native PostgreSQL `pgoutput` binary protocol ('B', 'C', 'R', 'I', 'U', 'D', 'T', 'S', 'E', 'c', 'A', 'M', 'P', 'K', 'r').
   * Must handle transactional and non-transactional logical messages (`pg_logical_emit_message`).
   * Must parse two-phase commit transactions (`PREPARE`, `COMMIT PREPARED`, `ROLLBACK PREPARED`).
3. **Stateful TOAST Attribute Reconstruction**:
   * Must reconstruct unchanged out-of-line TOAST columns (`TEXT`, `JSONB`, `BYTEA` $>2\text{ KB}$) in Beam runner-managed state (`ValueState<Row>`) without issuing point queries to the live database.
4. **Dynamic Schema Evolution**:
   * Must detect in-flight `ALTER TABLE` changes and project historical tuples onto evolved Beam Row schemas.
5. **Parallel Demux Sharding**:
   * Must partition replication streams across 64 deterministic hash channels by primary key to parallelize downstream windowing and sinks.

### B. High-Throughput Write Sinks (`PostgreSQLIO.write()`)
1. **Write Modes**:
   * `STREAMING_UPSERT_UNNEST`: Parameterized SQL array unnesting (`INSERT INTO ... SELECT FROM UNNEST(...) ON CONFLICT DO UPDATE`) for streaming micro-batches ($60\text{k}\text{--}80\text{k}$ rows/sec).
   * `STAGED_COPY_UPSERT`: Binary `COPY` into unlogged temp tables followed by atomic partition merge for bulk loads ($100\text{k}\text{--}125\text{k}$ rows/sec).
   * `APPEND_ONLY_COPY`: Direct binary stream copy for append-only tables.
2. **Intra-Bundle Primary Key Compaction**:
   * Must deduplicate duplicate primary keys within a single bundle and sort keys canonically to prevent `SQLState 40P01` deadlock errors.
3. **Dead-Letter Queue (DLQ)**:
   * Must route unparseable or constraint-violating records to a dead-letter tag with error classification, SQLState, and sanitized failure payloads.
4. **Multi-Master Loop Prevention**:
   * Must stamp write sessions with `SELECT pg_replication_origin_session_setup('...')` on connection checkout.

### C. Cloud Managed Authentication & Security
1. **Cloud IAM Database Authentication**:
   * Must support dynamic, token-refreshed IAM authentication for Google Cloud SQL, Google Cloud AlloyDB, and AWS RDS.
2. **Secret Manager Dynamic Resolution**:
   * Must support URI-based secret resolution (`sm://projects/.../secrets/...`) with background token refresh.
3. **Connection Pooling Invariants**:
   * Must configure HikariCP with non-blocking initialization (`initializationFailTimeout = -1`) and 30-minute max lifetime recycling.

---

## 5. Non-Functional Requirements & Service Level Objectives (SLOs)

| Metric | Target SLO | Actual Benchmark (24h Soak Test) |
| :--- | :--- | :--- |
| **Max CDC Read Throughput** | $\ge 200,000\text{ ops/sec}$ | $620,000\text{ ops/sec}$ |
| **Max Sink Write Throughput** | $\ge 50,000\text{ rows/sec}$ | $105,000\text{ rows/sec}$ |
| **p99 End-to-End Latency** | $< 500\text{ ms}$ | $280\text{ ms}$ |
| **Primary Database CPU Load** | $< 10\%$ during max throughput | $3.8\%$ |
| **Replication Slot WAL Lag** | $< 10\text{ MB}$ steady-state | $0\text{ bytes}$ |
| **TOAST Cache Hit Rate** | $\ge 99.0\%$ | $99.98\%$ (0 point queries to DB) |
| **Cross-Language Parity** | 100% parameter parity | Java, Python, Go, Beam YAML |

---

## 6. Multi-SDK & Cross-Language Architecture

The connector must expose identical declarative SchemaTransform contracts across all languages:
* **Java SDK**: `PostgreSQLIO.readCDC()`, `PostgreSQLIO.write()`.
* **Python SDK**: `apache_beam.transforms.managed.ReadFromPostgres`, `WriteToPostgres` via `beam.managed.POSTGRES`.
* **Go SDK**: `sdks/go/pkg/beam/io/xlang/postgresio.ReadCDC`, `Write`.
* **Beam YAML**: `ReadFromPostgres` and `WriteToPostgres` transforms.
