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

# PostgreSQL Operational Reality Patterns

This directory provides production reference implementations for common operational scenarios encountered when deploying Apache Beam pipelines with PostgreSQL. Each pattern includes runnable code in Go, Declarative Beam YAML, and Python.

---

## Pattern Matrix

| Pattern Directory | Operational Challenge | PostgreSQL Architectural Constraint | Technical Remediation | Languages |
| :--- | :--- | :--- | :--- | :--- |
| [`backfill_cutover`](file:///Users/jkshah/Projects/Apache%20Beam/sdks/go/examples/postgres/operational_patterns/backfill_cutover) | Cold-start table migration | Logical slots only capture changes committed after slot creation | Consistent snapshot read (`EXPORT_SNAPSHOT`) combined with slot LSN cutover and key deduplication | Go, YAML, Python |
| [`pgbouncer_compat`](file:///Users/jkshah/Projects/Apache%20Beam/sdks/go/examples/postgres/operational_patterns/pgbouncer_compat) | Connection pooler errors | Transaction pooling reassigns backend connections after every commit, breaking prepared statement caches | `WithPgBouncer(true)` disables statement caches and scopes staging tables to `ON COMMIT DROP` | Go, YAML, Python |
| [`partitioned_sink`](file:///Users/jkshah/Projects/Apache%20Beam/sdks/go/examples/postgres/operational_patterns/partitioned_sink) | Partitioned table upsert failures | Unique constraints and primary keys on partitioned tables must include all partition key columns | Composite conflict target (`order_id`, `order_date`) with upstream key sharding to minimize lock contention | Go, YAML, Python |
| [`schema_evolution`](file:///Users/jkshah/Projects/Apache%20Beam/sdks/go/examples/postgres/operational_patterns/schema_evolution) | Mid-stream DDL crashes | Relation message schema changes cause crashes if unmapped columns are encountered | Resilient adaptive projection with defaults, dynamic column bags, and drift telemetry metrics | Go, YAML, Python |
| [`failover_recovery`](file:///Users/jkshah/Projects/Apache%20Beam/sdks/go/examples/postgres/operational_patterns/failover_recovery) | Standby promotion slot loss | Pre-PG17 logical slots exist in memory/disk on the primary only and vanish upon failover | PostgreSQL 17 `WithCDCFailoverSlot(true)` synchronized standby slots with automatic reconnect | Go, YAML, Python |
| [`sequence_migration`](file:///Users/jkshah/Projects/Apache%20Beam/sdks/go/examples/postgres/operational_patterns/sequence_migration) | Primary key collisions | Logical replication streams data tuples but does not advance `nextval` sequence counters | Pipeline computes maximum observed ID and issues `SELECT setval(...)` prior to application cutover | Go, YAML, Python |

---

## 1. Backfill and Cutover (`backfill_cutover`)

### Operational Problem
When bootstrapping an existing high-volume table into a CDC pipeline, creating a logical replication slot only captures transactions committed *after* the slot's consistent point. Reading the table via an uncoordinated batch job while streaming CDC creates race conditions:
- Updates occurring during the batch read may be overwritten by stale batch rows.
- Duplicate writes and out-of-order mutations corrupt target tables.

### Solution
1. **Phase 1: Consistent Snapshot Export**: The replication slot is created using `CREATE_REPLICATION_SLOT slot_name LOGICAL pgoutput EXPORT_SNAPSHOT`. This returns an exported snapshot identifier and confirmed consistent LSN.
2. **Phase 2: Historical Batch Read**: The table is read under `SET TRANSACTION SNAPSHOT '<snapshot_id>'`, providing an exact baseline.
3. **Phase 3: Stream Cutover & Deduplication**: The streaming CDC reader resumes from the confirmed LSN. Stream mutations supersede snapshot rows through primary-key grouping.
4. **Phase 4: Sink**: Target writes use idempotent `ON CONFLICT DO UPDATE`.

---

## 2. PgBouncer Transaction Pooling Compatibility (`pgbouncer_compat`)

### Operational Problem
Enterprise PostgreSQL deployments frequently route application connections through PgBouncer operating in `pool_mode = transaction`. In this mode:
- Backend server connections are reassigned to different clients after each `COMMIT` or `ROLLBACK`.
- Session-level prepared statements (`PREPARE stmt_name`) fail with `ERROR: prepared statement "stmt_name" already exists` or `does not exist`.
- Session-level temporary tables leak or persist across transactions, consuming temp space.

### Solution
1. **Prepared Statement Suppression**: Specifying `postgresio.WithPgBouncer(true)` (or `use_pgbouncer: true` in YAML) disables prepared statement caching and switches driver query dispatch to direct execution.
2. **Transaction-Scoped Staging**: Staging tables created during bulk loading or upsert operations use `CREATE TEMP TABLE ... ON COMMIT DROP`. When the micro-batch transaction commits, PostgreSQL automatically drops the table and cleans up memory buffers.

---

## 3. Partitioned Table Sink (`partitioned_sink`)

### Operational Problem
When sinking high-throughput data to a partitioned table (`PARTITION BY RANGE (order_date)`):
- PostgreSQL requires all unique constraints and primary keys to include the partitioning key.
- Executing an upsert with `ON CONFLICT (order_id) DO UPDATE` raises:
  ```
  ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification
  ```
- Randomly distributed writes across partition boundaries cause lock contention and buffer cache thrashing across physical partition tables.

### Solution
1. **Composite Conflict Target**: Configure `postgresio.WithPrimaryKeyColumns("order_id", "order_date")` to match the partition primary key.
2. **Upstream Partition Localization**: The pipeline groups records by `order_date` (`beam.KeyBy + beam.Reshuffle`) before sinking. Each worker batch writes to a single physical partition relation, optimizing tuple routing and write throughput.

---

## 4. Schema Evolution Handling (`schema_evolution`)

### Operational Problem
Production schemas evolve continuously (e.g., adding `loyalty_tier` or dropping obsolete attributes). In PostgreSQL logical replication:
- DDL statements (`ALTER TABLE`) generate `RelationMessage` protocol frames that update the relation descriptor.
- Rigid pipeline code that expects a fixed tuple layout fails with unmarshaling errors or crashes on missing columns.

### Solution
1. **Adaptive Attribute Extraction**: The transformation inspects `ChangeEvent.After` dynamically.
2. **Backward-Compatible Defaults**: Missing columns in older records default to safe values (e.g., `"STANDARD"` for `loyalty_tier`).
3. **Dynamic Extension Attribute**: Unanticipated columns are serialized into an `extra_fields` JSON container.
4. **Operational Observability**: Increments the `schema_drift_detected_total` metric to alert operational monitors of upstream DDL migrations.

---

## 5. High Availability Failover Recovery (`failover_recovery`)

### Operational Problem
In PostgreSQL primary-standby clusters (managed by Patroni, Cloud SQL HA, or repmgr), standard logical replication slots exist only on the primary instance. If the primary crashes or planned maintenance promotes a standby:
- The logical replication slot does not exist on the promoted standby.
- The pipeline aborts with `replication slot "ha_slot" does not exist`.
- Re-creating the slot loses all changes accumulated between the primary crash and slot recreation.

### Solution
1. **Failover Slots (PostgreSQL 17+)**: Slots are created with `FAILOVER` enabled:
   ```sql
   CREATE_REPLICATION_SLOT ha_failover_slot LOGICAL pgoutput (FAILOVER);
   ```
2. **Standby Slot Synchronization**: The primary specifies `synchronized_standby_slots = 'standby_1'`, ensuring physical standbys track and synchronize the logical slot position.
3. **Automatic Reconnection**: When failover occurs, the pipeline reconnects to the cluster virtual IP (VIP) or endpoint, identifies the synchronized slot, and resumes streaming from the exact confirmed LSN without message loss.

---

## 6. Sequence and Identity Reconciliation (`sequence_migration`)

### Operational Problem
In PostgreSQL, `SERIAL`, `BIGSERIAL`, and `GENERATED ALWAYS AS IDENTITY` columns rely on sequence generators (`pg_class relkind='S'`). When migrating tables using bulk `COPY` or logical replication:
- Tuples are inserted with explicit primary key values.
- PostgreSQL **does not** advance the sequence generator counter (`nextval`).
- When client application writes cut over to the new database, the first `INSERT` without an explicit ID calls `nextval()`, generating an already-existing ID (e.g., `1`), causing:
  ```
  ERROR: duplicate key value violates unique constraint "orders_pkey"
  Detail: Key (order_id)=(1) already exists.
  ```

### Solution
1. **Global Maximum Calculation**: The pipeline computes the global maximum primary key across all processed historical and streaming records using Beam's `stats.Max`.
2. **Reconciliation Statement Generation**: Emits an idempotent sequence update query:
   ```sql
   SELECT setval(pg_get_serial_sequence('public.orders', 'order_id'), <max_id>, true);
   ```
3. **Verification**: When application writes activate, `nextval()` immediately produces `<max_id> + 1`, eliminating primary key collision errors.
