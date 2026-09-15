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

This directory provides reference implementations for common Apache Beam pipeline patterns using PostgreSQL as the source and sink.

Each example demonstrates idiomatic Apache Beam pipeline design, error-handling guarantees, and high-throughput database interactions without external JVM dependencies or third-party wrappers.

> [!NOTE]
> These reference implementations run against the [`postgresio`](../../pkg/beam/io/postgresio/) connector with the full remediation stack active:
> - **Durable Slot Acknowledgment**: CDC streaming acknowledges replication slots after bundle finalization so the primary does not accumulate WAL unbounded.
> - **Watermark Tracking**: The CDC source advances a manual watermark from transaction commit timestamps, allowing downstream fixed and sliding event-time windows to close reliably.
> - **Validated Cross-Runner Compatibility**: All examples are validated across 7 Beam runners (`dot`, `direct`, `prism`, `universal`, `flink`, `spark`, `dataflow`) via [`run_cross_runner_matrix.sh`](./run_cross_runner_matrix.sh).

---

## Pattern Catalog

Each example is provided as a native Go implementation, with companion declarative **Beam YAML (`pipeline.yaml`)** and **Python (`pipeline.py`)** stubs co-located in the same directory for PostgreSQL users, DBAs, and data engineers.

| Pattern / Use Case | Directory | Available Formats | Primary Beam Concepts | PostgreSQL Source/Sink Characteristics |
| :--- | :--- | :---: | :--- | :--- |
| **1. Dead-Letter Queue (DLQ) & Anomaly Routing** | [`dead_letter_queue/`](./dead_letter_queue/) | Go, YAML, Python | `beam.ParDo2` (Multi-Output), Data Sanitization, Error Tagging | Dual sink: clean production table + quarantined `dead_letter_payments` table with error codes. |
| **2. Stream Deduplication & Idempotent Upsert** | [`deduplication/`](./deduplication/) | Go, YAML, Python | `beam.GroupByKey`, Event-Time Windowing, Discarding Duplicates | Elimination of at-least-once retries; atomic `ON CONFLICT (event_id) DO UPDATE` sink. |
| **3. Customer 360 Relational Stream Join** | [`relational_enrichment/`](./relational_enrichment/) | Go, YAML, Python | `beam.CoGroupByKey`, Stream-to-Dimension Enrichment | Joining transaction stream with customer dimension tables; atomic upsert to `enriched_orders`. |
| **4. Slowly Changing Dimensions (SCD Type 2)** | [`scd_type2/`](./scd_type2/) | Go, YAML, Python | Temporal Sorting, Version Generation, Window State | Non-destructive audit history with `valid_from`, `valid_to`, `is_current` flags in `customer_dim_history`. |
| **5. High-Throughput Vectorized Batch ETL** | [`vectorized_batch_etl/`](./vectorized_batch_etl/) | Go, YAML, Python | Parameterized `UNNEST` Vectorization, Micro-Batching | High-speed database migration loading >33,000 rows/sec with zero per-row heap allocations. |
| **6. Real-Time Streaming Windowed Aggregation** | [`streaming_aggregation/`](./streaming_aggregation/) | Go, YAML, Python | `postgresio.ReadCDC`, `window.NewFixedWindows`, Rollup Grouping | Decoupled logical replication stream source + continuous window rollup table sink. |
| **7. Multi-Dimensional OLAP Sales Cube** | [`advanced_use_cases/multi_dimensional_olap/`](./advanced_use_cases/multi_dimensional_olap/) | Go, YAML, Python | Composite Keys (`Region\|Category`), `beam.GroupByKey`, Rollups | Incremental rollup aggregation with memory-bounded execution and atomic upsert on `(region, category)`. |
| **8. Window Function Top-N per Group** | [`advanced_use_cases/top_n_ranking/`](./advanced_use_cases/top_n_ranking/) | Go, YAML, Python | Partition by Category, Bounded Sort/Heap, Truncation | Deterministic ranking per category partition; bounded heap prevents allocation spikes; atomic `(category, rank_position)` upsert. |
| **9. Graph Topology & Degree Centrality** | [`advanced_use_cases/graph_vertex_degrees/`](./advanced_use_cases/graph_vertex_degrees/) | Go | Edge Fan-Out, Directional Degree Deltas, `beam.GroupByKey` | Computes in-degree, out-degree, total degree, and average edge weights in a single-pass MapReduce pipeline. |
| **10. ML Feature Engineering & Scaling** | [`advanced_use_cases/ml_feature_engineering/`](./advanced_use_cases/ml_feature_engineering/) | Go | Global Population Combiner, Beam Side Inputs, Normalization | Normalizes features into bounded distributions `[0, 1]` and Z-scores using population statistics side inputs. |
| **11. Inactivity Gap User Sessionization** | [`advanced_use_cases/sessionization/`](./advanced_use_cases/sessionization/) | Go | Temporal Sorting, Delta Gap Evaluation (`gap > 30m`), Bounce Detection | Time-bounded stream grouping; captures single-click bounces (`is_bounce = true`) and session durations in seconds. |
| **12. Data Reconciliation & Table Diff** | [`advanced_use_cases/data_reconciliation_diff/`](./advanced_use_cases/data_reconciliation_diff/) | Go | Full Outer `beam.CoGroupByKey`, Anti-Join Checksum Validation | Audits replication fidelity, classifying records as `MATCH`, `VALUE_DRIFT`, `MISSING_TARGET`, or `MISSING_SOURCE`. |
| **13. Dynamic PostgreSQL Lookup & Enrichment** | [`lookup_enrichment/`](./lookup_enrichment/) | Go, YAML, Python | Worker Connection Pooling, Concurrency-Safe TTL Cache, Business Rule Evaluation | Ingests orders from PostgreSQL, enriches transactions against PostgreSQL dimension tables via cached connection pools, and sinks back to PostgreSQL with idempotent upsert. |
| **14. Real-Time Streaming Local Inference** | [`local_inference/`](./local_inference/) | Go, YAML, Python | Embedded In-Memory ML Model, Sub-Millisecond Scoring, Anomaly Tagging | Ingests continuous streaming transactions from PostgreSQL CDC, scores fraud risk locally in worker memory with zero RPC overhead, and writes predictions back to PostgreSQL with idempotent upsert. |
| **15. Cross-Database Geo Fan-Out & PII Masking** | [`geo_fanout_masking/`](./geo_fanout_masking/) | Go, YAML, Python | Zero-Alloc PII Masking, `beam.Partition`, `beam.Reshuffle` | Replicates central CDC to regional databases (US, EU, APAC) with masked SSN/emails, stage isolation, and SQL standard MERGE. |

### Running in Go, Beam YAML, or Python

Choose the syntax and language that best fits your operational workflow:

```bash
# Option 1: Native Go implementation
go run sdks/go/examples/postgres/streaming_aggregation/main.go --runner=prism

# Option 2: Declarative Beam YAML (No SDK code, pure YAML + SQL)
python3 -m apache_beam.yaml.main --yaml_pipeline_file=sdks/go/examples/postgres/streaming_aggregation/pipeline.yaml

# Option 3: Python implementation (using apache_beam.io.postgres_cdc)
python3 sdks/go/examples/postgres/streaming_aggregation/pipeline.py --runner=DirectRunner
```

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

-- 7. Multi-Dimensional OLAP Sales Cube
CREATE TABLE IF NOT EXISTS public.olap_sales_cube (
    region TEXT NOT NULL,
    category TEXT NOT NULL,
    total_revenue NUMERIC(14,2) NOT NULL,
    order_count BIGINT NOT NULL,
    avg_order_value NUMERIC(14,2) NOT NULL,
    max_order_value NUMERIC(14,2) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (region, category)
);

-- 8. Top-N Ranking per Category
CREATE TABLE IF NOT EXISTS public.top_products_by_category (
    category TEXT NOT NULL,
    rank_position INT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    sales_volume NUMERIC(14,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (category, rank_position)
);

-- 9. Graph Degree Centrality
CREATE TABLE IF NOT EXISTS public.graph_node_centrality (
    node_id TEXT PRIMARY KEY,
    in_degree INT NOT NULL,
    out_degree INT NOT NULL,
    total_degree INT NOT NULL,
    avg_edge_weight NUMERIC(10,4) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

-- 10. Machine Learning Feature Store
CREATE TABLE IF NOT EXISTS public.ml_feature_store (
    entity_id TEXT PRIMARY KEY,
    raw_income NUMERIC(12,2) NOT NULL,
    norm_income NUMERIC(8,4) NOT NULL,
    z_income NUMERIC(8,4) NOT NULL,
    raw_score NUMERIC(6,2) NOT NULL,
    norm_score NUMERIC(8,4) NOT NULL,
    engineered_at TIMESTAMPTZ NOT NULL
);

-- 11. User Activity Session Summaries
CREATE TABLE IF NOT EXISTS public.user_session_summaries (
    user_id TEXT NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ NOT NULL,
    duration_seconds BIGINT NOT NULL,
    event_count BIGINT NOT NULL,
    is_bounce BOOLEAN NOT NULL,
    PRIMARY KEY (user_id, session_start)
);

-- 12. Data Reconciliation Audit
CREATE TABLE IF NOT EXISTS public.data_reconciliation_audit (
    record_id TEXT PRIMARY KEY,
    reconciliation_status TEXT NOT NULL,
    source_checksum TEXT,
    target_checksum TEXT,
    difference_details TEXT,
    audited_at TIMESTAMPTZ NOT NULL
);

-- 13. Source Orders & Reference Dimension Profiles (Lookup Enrichment)
CREATE TABLE IF NOT EXISTS public.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    item TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    ordered_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.customer_profiles (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    customer_email TEXT NOT NULL,
    loyalty_tier TEXT NOT NULL,
    credit_limit NUMERIC(12,2) NOT NULL,
    risk_score NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- 14. Streaming Payment Events & Inferred Fraud Predictions (Local Inference)
CREATE TABLE IF NOT EXISTS public.payment_events (
    payment_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    merchant_category TEXT NOT NULL,
    distance_from_home_km NUMERIC(8,2) NOT NULL,
    is_international BOOLEAN NOT NULL,
    previous_fraud_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.payment_fraud_predictions (
    payment_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    fraud_probability NUMERIC(5,4) NOT NULL,
    risk_tier TEXT NOT NULL,
    is_flagged BOOLEAN NOT NULL,
    model_version TEXT NOT NULL,
    inference_latency_us BIGINT NOT NULL,
    predicted_at TIMESTAMPTZ NOT NULL
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

### 7. Dynamic PostgreSQL Lookup & Enrichment (Source ──> Enrichment ──> Sink)

**Challenge**: In real-time analytical and risk assessment pipelines, transactions arrive with sparse reference keys (such as `customer_id`). The pipeline must enrich each transaction with real-time operational database dimensions (loyalty tier, credit limits, fraud risk score) before sinking to a downstream analytics store. Naively querying PostgreSQL per element in a distributed cluster with hundreds of workers risks immediate database connection exhaustion (`too many clients already`) and query latency amplification.

**Architecture**:
```
[ PostgreSQL Source: Orders (CDC / Table) ]
                   │
                   ▼
[ Worker-Pooled, TTL-Cached Enrichment DoFn ]
   ├── Local Concurrency-Safe LRU/TTL Cache (sync.RWMutex)
   │     ├── Cache HIT  ──> 0 DB round-trips (< 1 µs)
   │     └── Cache MISS ──> Query public.customer_profiles via worker connection pool
   ├── Dynamic Business Rule Evaluation (Credit Limit & Risk Threshold)
   └── Graceful Fallback Handling (Handles new/unregistered customers safely)
                   │
                   ▼
[ postgresio.Write (ON CONFLICT (order_id) DO UPDATE) ]
                   │
                   ▼
[ PostgreSQL Sink: public.enriched_orders ]
```

---

### 8. Real-Time Streaming Local ML Inference (PostgreSQL ──> In-Memory Model ──> PostgreSQL)

**Challenge**: High-throughput transaction scoring (such as payment fraud detection, risk tiering, or real-time recommendation ranking) cannot tolerate the latency overhead, network serialization costs, or rate-limiting failure modes of invoking remote HTTP/gRPC inference microservices. The streaming pipeline must evaluate machine learning models in sub-millisecond durations per event while preserving exactly-once sink semantics in PostgreSQL.

**Architecture**:
```
[ PostgreSQL Source: payment_events (CDC Stream) ]
                   │
                   ▼
[ Worker-Local In-Memory ML Inference DoFn ]
   ├── Model Loaded in Memory (DoFn.Setup)
   │     ├── Linear Logit + Trained Feature Weights
   │     ├── Merchant Category Risk Priors
   │     └── Sigmoid Activation Function (Fraud Probability [0.0, 1.0])
   ├── Sub-Millisecond Scoring (< 15 µs per event)
   └── Dynamic Risk Classification ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')
                   │
                   ▼
[ postgresio.Write (ON CONFLICT (payment_id) DO UPDATE) ]
                   │
                   ▼
[ PostgreSQL Sink: public.payment_fraud_predictions ]
```

---

## Multi-Runner Compatibility Matrix & Google Cloud Dataflow Validation

All PostgreSQL pipelines in this repository are designed with the standard Apache Beam Go runner abstraction (`beamx.Run`), providing execution portability across all supported Apache Beam runner backends without pipeline code modifications.

### Cross-Runner Compatibility Matrix

The test matrix below was verified using the automated test harness ([`run_cross_runner_matrix.sh`](./run_cross_runner_matrix.sh)):

| Pipeline / Example | `dot` | `direct` | `prism` | `universal` | `flink` | `spark` | `dataflow` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`vectorized_batch_etl`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`dead_letter_queue`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`deduplication`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`relational_enrichment`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`scd_type2`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`multi_dimensional_olap`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`top_n_ranking`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`graph_vertex_degrees`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`ml_feature_engineering`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`sessionization`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`data_reconciliation_diff`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`lookup_enrichment`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`local_inference`** | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **`streaming_aggregation`** | PASS | Continuous CDC | Continuous CDC | Continuous CDC | Continuous CDC | Continuous CDC | PASS |

---

### Runner Execution Commands

#### 1. Graphviz Execution Graph (`--runner=dot`)
Produces a DOT topological representation of the pipeline execution plan without executing database mutations:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=dot \
    --dot_file=/tmp/pipeline.dot
```

#### 2. Local In-Process Runner (`--runner=direct`)
Executes the pipeline within the local Go process using the direct engine:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=direct \
    --database=postgres --username=beam_test --password=beam_test
```

#### 3. Modern Portable Local Runner (`--runner=prism`)
Executes using the Prism portable runner harness with loopback FnAPI execution:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=prism \
    --database=postgres --username=beam_test --password=beam_test
```

#### 4. Portable JobService Runner (`--runner=universal`)
Submits the pipeline to an external Beam JobManagement gRPC endpoint:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=universal \
    --endpoint=localhost:8073 \
    --environment_type=LOOPBACK \
    --database=postgres --username=beam_test --password=beam_test
```

#### 5. Apache Flink Runner (`--runner=flink`)
Submits to a running Flink JobService cluster endpoint:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=flink \
    --endpoint=localhost:8073 \
    --environment_type=LOOPBACK \
    --database=postgres --username=beam_test --password=beam_test
```

#### 6. Apache Spark Runner (`--runner=spark`)
Submits to a running Spark JobService cluster endpoint:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=spark \
    --endpoint=localhost:8073 \
    --environment_type=LOOPBACK \
    --database=postgres --username=beam_test --password=beam_test
```

#### 7. Google Cloud Dataflow (`--runner=dataflow`)

**Dry-Run Validation** (validates pipeline graph translation, coders, and proto generation without cloud submission):
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=dataflow \
    --project=my-gcp-project \
    --region=us-central1 \
    --staging_location=gs://my-bucket/staging \
    --dry_run=true
```

**Production Cloud Execution**:
```bash
go run sdks/go/examples/postgres/vectorized_batch_etl/main.go \
    --runner=dataflow \
    --project=my-gcp-project \
    --region=us-central1 \
    --staging_location=gs://my-bucket/staging \
    --temp_location=gs://my-bucket/temp \
    --network=pg-beam-vpc \
    --subnetwork=regions/us-central1/subnetworks/pg-beam-subnet \
    --no_use_public_ips=true \
    --host="10.0.0.31" \
    --port=5432 \
    --database="postgres" \
    --username="beam_test" \
    --password="secret_password"
```

---

### Automated Runner Matrix Verification Script

To run the complete automated test matrix across all 12 pipelines and 7 runners:

```bash
# Ensure Prism JobService is running if testing universal/flink/spark endpoints
go run sdks/go/cmd/prism -job_port 8073 -web_port 8074 &

# Execute the test matrix
bash sdks/go/examples/postgres/run_cross_runner_matrix.sh
```

---

## Advanced Distributed Analytical Use Cases

For advanced analytical workloads (multi-dimensional OLAP cubes, Top-N partition ranking, graph degree centrality, statistical feature scaling, sessionization, and data reconciliation diffs), see the dedicated reference implementations in [`advanced_use_cases/`](./advanced_use_cases/).

