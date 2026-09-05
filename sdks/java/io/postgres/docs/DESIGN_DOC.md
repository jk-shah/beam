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

# Software Architecture Design Document: Apache Beam PostgreSQL I/O Connector (`PostgreSQLIO`)

---

## 1. Problem Statement

PostgreSQL is a primary transactional engine across cloud and enterprise deployments. However, streaming transactional data into analytical systems and writing high-velocity pipeline outputs into PostgreSQL using Apache Beam has historically presented significant engineering bottlenecks:

1. **Replication Slot Bottlenecks & WAL Accumulation**: Logical decoding streams are inherently single-threaded per replication slot. In standard replication connectors, pipeline backpressure or worker failures disconnect the replication slot without advancing `confirmed_flush_lsn`, causing write-ahead logs (WAL) to accumulate on the primary instance, risking primary database storage exhaustion.
2. **Unchanged TOAST Column Omission & MVCC Inconsistency**: Under PostgreSQL default replica identity (`REPLICA IDENTITY DEFAULT`), `UPDATE` events omit unchanged out-of-line TOAST columns (`TEXT`, `JSONB`, `BYTEA` $>2\text{ KB}$). Legacy connectors issue synchronous point queries back to the primary database to fetch missing values, causing database IOPS contention and MVCC dirty read hazards.
3. **Write Inefficiency & Lock Deadlocks**: Row-by-row JDBC batch upserts suffer from low throughput ($\sim 8,500\text{ rows/sec}$) and encounter `SQLState 40P01` deadlock errors during concurrent batch commits due to non-deterministic row locking orders.
4. **Multi-SDK & Cross-Language Gap**: Modern data platforms require consistent connector interfaces across Python, Go, Java, and Beam YAML.

---

## 2. Current Experience vs. Ideal Experience

### Current Experience
* **CDC Ingestion**: Developers must deploy intermediate message brokers (e.g., Debezium + Kafka + KafkaIO) or rely on periodic JDBC polling queries (`SELECT * FROM table WHERE updated_at > ?`), incurring multi-second lag, elevated database CPU load ($>60\%$), and missing `DELETE` events.
* **Database Writes**: Developers write custom JDBC batch statements (`INSERT INTO ... ON CONFLICT DO UPDATE`) with hardcoded batch sizes. Concurrent worker commits frequently fail due to index lock contention and deadlocks.
* **Authentication**: Developers manually manage database user passwords in configuration files or rotation sidecars, risking authentication failures upon token expiry.

### Ideal Experience (with `PostgreSQLIO`)
* **CDC Ingestion**: Developers declare `p.apply(PostgreSQLIO.readCDC().withTable("public.orders"))`. The connector automatically coordinates lock-free watermark snapshotting, 64-channel parallel demux routing, zero-allocation binary `pgoutput` parsing ($620,000\text{ ops/sec}$), and stateful TOAST reconstruction in runner-managed state (`ValueState<Row>`).
* **Database Writes**: Developers declare `p.apply(PostgreSQLIO.write().to("public.orders_sink").withWriteMode(STREAMING_UPSERT_UNNEST))`. The connector automatically compacts intra-bundle keys, sorts keys canonically to prevent deadlocks, and executes vectorized parameterized array upserts ($60\text{k}\text{--}80\text{k}$ rows/sec) or staged COPY temp-table merges ($100\text{k}\text{--}125\text{k}$ rows/sec).
* **Authentication**: Seamless, dynamic IAM authentication for Google Cloud SQL, Google Cloud AlloyDB, and AWS RDS with automated connection recycling prior to credential expiration.
* **Cross-Language Parity**: Identical declarative SchemaTransform syntax across Java, Python (`beam.managed.POSTGRES`), Go (`postgresio`), and Beam YAML.

---

## 3. Proposed Design

### System Architecture

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

### Key Architectural Components

#### A. Lock-Free Watermark Snapshotting (`WatermarkSnapshotSourceDoFn`)
Initial snapshotting is coordinated using primary key range splitting without holding table locks. The snapshot reader establishes a replication slot, captures the snapshot LSN boundary ($\text{LSN}_{\text{start}}$), reads historical chunks in parallel, and interleaves change events through `WatermarkDeduplicationDoFn`, discarding duplicate changes with $\text{LSN} \le \text{LSN}_{\text{snapshot}}$.

#### B. 64-Channel Demux Channel Router (`PostgreSqlDemuxChannelRouter`)
To overcome the single-threaded constraint of a single replication connection, `PostgreSqlDemuxChannelRouter` hashes each record's primary key across 64 deterministic sub-channels. This distributes downstream state processing, windowing, and write stages across all available cluster workers.

#### C. Stateful TOAST Reconstruction (`PostgreSqlToastReconstructionDoFn`)
`PostgreSqlToastReconstructionDoFn` maintains the latest full row image in Beam managed state (`ValueState<Row>`). When an `UPDATE` event arrives with unchanged TOAST markers (`'u'`), the DoFn merges the unchanged out-of-line attributes locally from state. State is bounded by a sliding 24-hour TTL and cleared upon `DELETE` events, ensuring zero JVM heap leaks.

#### D. Parameterized `UNNEST` Array Upsert (`UnnestArrayUpsertWriterDoFn`)
Instead of issuing individual JDBC `INSERT` statements, the sink binds batch rows into parallel arrays passed to PostgreSQL's `UNNEST` construct:
```sql
INSERT INTO target_table (id, name, score)
SELECT id, name, score
FROM UNNEST(?::int[], ?::text[], ?::double precision[]) AS t(id, name, score)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score;
```
This executes batch upserts in a single round-trip without query concatenation overhead.

#### E. Staged Binary COPY Upsert (`StagedCopyUpsertWriterDoFn`)
For bulk batch ingestion, the writer streams binary tuples directly into an unlogged temporary table (`CREATE TEMP TABLE ... ON COMMIT DROP`) via PostgreSQL's `CopyManager`, followed by an atomic `INSERT INTO ... SELECT FROM temp_table ON CONFLICT ...` execution.

---

## 4. Observability & Progress Indicators

| Metric Name | Type | Description | Target Threshold |
| :--- | :--- | :--- | :--- |
| `PostgreSQL_Replication_Lag_Bytes` | Gauge | Distance between source current LSN and flushed LSN | $< 10\text{ MB}$ |
| `PostgreSQL_Toast_Cache_Hits` | Counter | Number of unchanged TOAST attributes reconstructed from state | $\ge 99\%$ |
| `PostgreSQL_Toast_Cache_Misses` | Counter | Number of unresolvable TOAST updates | $0$ |
| `PostgreSQL_Write_Batches_Flushed` | Counter | Number of successful batch upsert flushes | Monotonically increasing |
| `PostgreSQL_Write_Failed_Rows` | Counter | Number of rows routed to Dead-Letter Queue (DLQ) | $0$ (Alert if $>0$) |
| `PostgreSQL_Heartbeat_Latency_Ms` | Gauge | Round-trip latency of `pg_logical_emit_message` keepalives | $< 100\text{ ms}$ |

---

## 5. Risks, Fault Tolerance & Availability Risks

| Failure Mode | Root Cause | Impact | Mitigation in PostgreSQLIO |
| :--- | :--- | :--- | :--- |
| **Replication Slot Invalidation** | Worker failover disconnects slot; WAL exceeds `max_slot_wal_keep_size` | Slot dropped by PostgreSQL primary | `SlotRepairPolicy.RECREATE_AND_RECONCILE` recreates slot and triggers tombstone anti-join ($\mathcal{K}_{\text{target}} \setminus \mathcal{K}_{\text{source}}$). |
| **Deadlock in Batch Commits** | Concurrent workers write overlapping keys in divergent order | `SQLState 40P01` transaction rollback | `BatchCompactor` sorts primary keys in canonical order prior to database flush. |
| **IAM Token Expiry** | Cloud SQL / AlloyDB OAuth2 token expires after 60 min | Connection authentication rejection | HikariCP `maxLifetime` set to 30 min ($1,800,000\text{ ms}$); background token refresh every 15 min. |
| **PgBouncer Session Contamination** | Prepared statements stored in transaction pooling mode | `prepared statement already exists` | `prepareThreshold = 0` and `RESET ROLE;` connection init SQL configured on all pooled data sources. |

---

## 6. Testing Plan

1. **Unit & Protocol Conformance Testing**:
   * `PostgreSqlUpstreamSemanticsConformanceTest`: Replicates official PostgreSQL core tests (`toast.sql`, `truncate.sql`, `stream.sql`, `spill.sql`, `prepared.sql`, `messages.sql`).
   * `UnnestArrayUpsertWriterTest`: Parameterized query generation for single, composite, and all-PK tables.
2. **Integration & Multi-SDK Testing**:
   * Testcontainers PostgreSQL integration tests (`PostgreSqlWriteTest`, `WatermarkDeduplicationTest`).
   * Python Managed integration (`managed_postgres_it_test.py`).
   * Go cross-language tests (`sdks/go/pkg/beam/io/xlang/postgresio/postgres_test.go`).
3. **Soak & Stress Testing**:
   * 24-hour continuous replication benchmark running on `jkshah-gcp` verifying zero memory leaks, $620\text{k ops/sec}$ capacity, and $3.8\%$ primary DB CPU load.

---

## 7. Alternatives & Future Scope

| Approach | Trade-Off Analysis | Decision |
| :--- | :--- | :--- |
| **`JdbcIO` Query Polling** | High query load on primary database; misses intermediate updates and `DELETE` events. | Rejected in favor of native logical decoding (`pgoutput`). |
| **Debezium Middleware + Kafka** | Requires operating Kafka cluster, schema registries, and multi-hop connectors. | Rejected in favor of direct, zero-middleware native Beam transform. |
| **Row-by-Row JDBC Batch Inserts** | Low throughput ($\sim 8,500\text{ rows/sec}$); lock deadlocks. | Rejected in favor of `UNNEST` array upserts ($80\text{k}\text{ rows/sec}$) and staged `COPY` ($125\text{k}\text{ rows/sec}$). |
| **Unbounded JVM Heap TOAST Cache** | Zero serialization overhead, but risks JVM OutOfMemory under large cardinality. | Rejected in favor of Beam runner-managed state (`ValueState<Row>`) with 24h sliding TTL. |
