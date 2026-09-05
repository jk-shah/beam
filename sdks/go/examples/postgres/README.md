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

# Apache Beam PostgreSQL Dataflow Pattern Library

This directory provides production-ready, verified reference implementations for the most popular Google Cloud Dataflow use cases using PostgreSQL as the source and sink.

Each example demonstrates idiomatic Apache Beam pipeline design, error-handling guarantees, and high-throughput database interactions without external JVM dependencies or third-party wrappers.

---

## Pattern Catalog

| Pattern / Use Case | Directory | Primary Beam Concepts | PostgreSQL Source/Sink Characteristics |
| :--- | :--- | :--- | :--- |
| **1. Dead-Letter Queue (DLQ) & Anomaly Routing** | [`dead_letter_queue/`](./dead_letter_queue/) | `beam.ParDo2` (Multi-Output), Data Sanitization, Error Tagging | Dual sink: clean production table + quarantined `dead_letter_payments` table with error codes. |
| **2. Stream Deduplication & Idempotent Upsert** | [`deduplication/`](./deduplication/) | `beam.GroupByKey`, Event-Time Windowing, Discarding Duplicates | Elimination of at-least-once retries; atomic `ON CONFLICT (event_id) DO UPDATE` sink. |
| **3. Customer 360 Relational Stream Join** | [`relational_enrichment/`](./relational_enrichment/) | `beam.CoGroupByKey`, Stream-to-Dimension Enrichment | Joining transaction stream with customer dimension tables; atomic upsert to `enriched_orders`. |
| **4. Slowly Changing Dimensions (SCD Type 2)** | [`scd_type2/`](./scd_type2/) | Temporal Sorting, Version Generation, Window State | Non-destructive audit history with `valid_from`, `valid_to`, `is_current` flags in `customer_dim_history`. |
| **5. High-Throughput Vectorized Batch ETL** | [`vectorized_batch_etl/`](./vectorized_batch_etl/) | Parameterized `UNNEST` Vectorization, Micro-Batching | High-speed database migration loading >33,000 rows/sec with zero per-row heap allocations. |
| **6. Real-Time Streaming Windowed Aggregation** | [`streaming_aggregation/`](./streaming_aggregation/) | `postgresio.ReadCDC`, `window.NewFixedWindows`, Rollup Grouping | Decoupled logical replication stream source + continuous window rollup table sink. |

---

## Database Prerequisites & DDL

Create the required target tables in PostgreSQL before running the examples:

```sql
-- 1. Dead-Letter Queue (DLQ) & Clean Payments
CREATE TABLE IF NOT EXISTS public.clean_payments (
    payment_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status TEXT NOT NULL,
    cleaned_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dead_letter_payments (
    payment_id TEXT PRIMARY KEY,
    account_id TEXT,
    amount NUMERIC(12,2),
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload TEXT,
    rejected_at TIMESTAMPTZ NOT NULL
);

-- 2. Canonical Events (Deduplication)
CREATE TABLE IF NOT EXISTS public.canonical_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- 3. Customer 360 Enriched Orders (Relational Stream Join)
CREATE TABLE IF NOT EXISTS public.enriched_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_email TEXT NOT NULL,
    loyalty_tier TEXT NOT NULL,
    item TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    enriched_at TIMESTAMPTZ NOT NULL
);

-- 4. Slowly Changing Dimensions Type 2 (Customer Dimension History)
CREATE TABLE IF NOT EXISTS public.customer_dim_history (
    customer_id TEXT NOT NULL,
    version INT NOT NULL,
    full_name TEXT NOT NULL,
    tier TEXT NOT NULL,
    address TEXT NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ NOT NULL,
    is_current BOOLEAN NOT NULL,
    PRIMARY KEY (customer_id, version)
);

-- 5. Migrated Transactions (Vectorized Batch ETL)
CREATE TABLE IF NOT EXISTS public.migrated_transactions (
    id BIGINT PRIMARY KEY,
    code TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    region TEXT NOT NULL,
    tier TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL
);

-- 6. Merchant Minute Rollups (Streaming Windowed Aggregation)
CREATE TABLE IF NOT EXISTS public.merchant_minute_rollups (
    merchant_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL,
    max_amount NUMERIC(14,2) NOT NULL,
    PRIMARY KEY (merchant_id, window_start)
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO beam_test;
```

---

## Detailed Pattern Walkthroughs

### 1. Dead-Letter Queue (DLQ) & Anomaly Routing

**Challenge**: In real-time data ingestion pipelines, dirty, malformed, or fraudulent data must not crash the streaming engine or block the pipeline. Dropping records silently violates financial audit compliance.

**Architecture**:
```
[ Incoming Payments ] ──> [ Validate & Route DoFn (ParDo2) ]
                                  │
         ┌────────────────────────┴────────────────────────┐
         ▼ (Valid)                                         ▼ (Corrupted / Poison)
[ public.clean_payments ]                      [ public.dead_letter_payments ]
  - payment_id                                   - payment_id
  - account_id                                   - error_code (ERR_MISSING_ACCOUNT)
  - amount                                       - error_message
  - status = 'VALIDATED'                         - raw_payload
```

**Run Locally**:
```bash
go run sdks/go/examples/postgres/dead_letter_queue/main.go \
    --host=localhost \
    --port=5432 \
    --database=postgres \
    --username=beam_test \
    --password=beam_test
```

---

### 2. Stream Deduplication & Idempotent Upsert

**Challenge**: At-least-once distributed messaging systems (such as Cloud Pub/Sub, Kafka, or transient network retries) emit duplicate event payloads. Re-processing payment or checkout events causes duplicate charges and distorted analytics.

**Architecture**:
```
[ Raw Event Stream (With Duplicates) ]
                 │
                 ▼
[ Key by event_id + Assign Timestamp ]
                 │
                 ▼
[ WindowInto 1-Minute Fixed Windows ]
                 │
                 ▼
[ GroupByKey (Collects Duplicate Keys) ]
                 │
                 ▼
[ Deduplicate DoFn (Selects Canonical Event) ]
                 │
                 ▼
[ postgresio.Write (ON CONFLICT DO UPDATE) ]
                 │
                 ▼
[ public.canonical_events ]
```

**Run Locally**:
```bash
go run sdks/go/examples/postgres/deduplication/main.go \
    --host=localhost \
    --port=5432 \
    --database=postgres \
    --username=beam_test \
    --password=beam_test
```

---

### 3. Customer 360 Relational Stream Join (`CoGroupByKey`)

**Challenge**: High-throughput transactional data (e.g. orders, credit authorizations) contains only foreign keys (`customer_id`). Downstream analytics and fraud detection require denormalized, enriched records with real-time customer tier, email, and name.

**Architecture**:
```
[ Orders Stream ]     ──> [ KeyBy customer_id ] ──┐
                                                  ▼
                                         [ CoGroupByKey ]
                                                  ▲
[ Customer Profiles ] ──> [ KeyBy customer_id ] ──┘
                                                  │
                                                  ▼
                                    [ EnrichOrderWithCustomer DoFn ]
                                                  │
                                                  ▼
                                       [ public.enriched_orders ]
```

**Run Locally**:
```bash
go run sdks/go/examples/postgres/relational_enrichment/main.go \
    --host=localhost \
    --port=5432 \
    --database=postgres \
    --username=beam_test \
    --password=beam_test
```

---

### 4. Slowly Changing Dimensions (SCD Type 2) Historical Auditing

**Challenge**: When dimension attributes change (e.g. customer address change, tier upgrade from STANDARD to GOLD), overwriting records destroys point-in-time historical reporting accuracy. SCD Type 2 preserves the lineage of every change.

**Architecture**:
```
[ Customer Profile Modifications ]
                 │
                 ▼
[ KeyBy customer_id -> GroupByKey ]
                 │
                 ▼
[ GenerateSCD2History DoFn ]
  - Chronological sorting
  - Generates version (1, 2, 3...)
  - Sets valid_from, valid_to ('9999-12-31' for active)
  - Sets is_current boolean flag
                 │
                 ▼
[ postgresio.Write (ON CONFLICT (customer_id, version) DO UPDATE) ]
                 │
                 ▼
[ public.customer_dim_history ]
```

**Run Locally**:
```bash
go run sdks/go/examples/postgres/scd_type2/main.go \
    --host=localhost \
    --port=5432 \
    --database=postgres \
    --username=beam_test \
    --password=beam_test
```

---

### 5. High-Throughput Vectorized Batch ETL

**Challenge**: Bulk loading millions of records row-by-row saturates database CPU and triggers query lock contention.

**Solution**: `postgresio.Write` flushes records using parameterized PostgreSQL `UNNEST` array casts (`INSERT INTO ... SELECT * FROM UNNEST($1::bigint[], $2::text[])`), clamping connection pool concurrency to `runtime.NumCPU() / 2` to eliminate CPU thrashing.

**Run Locally**:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --host=localhost \
    --port=5432 \
    --database=postgres \
    --username=beam_test \
    --password=beam_test \
    --rows=100000
```

---

### 6. Real-Time Streaming Windowed Aggregation

**Challenge**: Processing continuous transaction streams into rolling analytics metrics (per-minute volume, maximum transaction amount) without stalling worker memory or missing late-arriving events.

**Architecture**:
```
[ postgresio.ReadCDC (Single-Slot pgoutput Stream) ]
                 │
                 ▼
[ Extract & Assign Event-Time Timestamps ]
                 │
                 ▼
[ WindowInto 1-Minute Tumbling Windows ]
                 │
                 ▼
[ GroupByKey (merchant_id) ]
                 │
                 ▼
[ AggregateRollup DoFn ]
  - transaction_count = count(txns)
  - total_amount = sum(amount)
  - max_amount = max(amount)
                 │
                 ▼
[ postgresio.Write (ON CONFLICT (merchant_id, window_start) DO UPDATE) ]
                 │
                 ▼
[ public.merchant_minute_rollups ]
```

---

## Production Execution on Google Cloud Dataflow

To execute any of these pipelines on managed Google Cloud Dataflow, supply standard Dataflow runner flags:

```bash
# Set environment variables
export PROJECT="my-gcp-project"
export REGION="us-central1"
export BUCKET="gs://my-dataflow-staging-bucket"
export NETWORK="default"
export SUBNETWORK="regions/us-central1/subnetworks/default"

# Submit Dead-Letter Queue pipeline to Dataflow
go run sdks/go/examples/postgres/dead_letter_queue/main.go \
    --runner=dataflow \
    --project=${PROJECT} \
    --region=${REGION} \
    --temp_location=${BUCKET}/temp \
    --staging_location=${BUCKET}/staging \
    --network=${NETWORK} \
    --subnetwork=${SUBNETWORK} \
    --host="10.0.0.15" \
    --port=5432 \
    --database="production_db" \
    --username="beam_test" \
    --password="secret_password"
```

For Cloud SQL or AlloyDB private IP deployments, ensure the Dataflow worker subnet has Private Google Access or VPC peering enabled to reach the PostgreSQL private IP address.

---

## Advanced Distributed Analytical Use Cases

For advanced analytical workloads (multi-dimensional OLAP cubes, Top-N partition ranking, graph degree centrality, statistical feature scaling, sessionization, and data reconciliation diffs), see the dedicated reference implementations in [`advanced_use_cases/`](./advanced_use_cases/).
