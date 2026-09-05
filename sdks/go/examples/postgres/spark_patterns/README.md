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

# Apache Beam Go: Apache Spark Workload Migration Patterns for PostgreSQL

This library provides production reference implementations for translating the six most prevalent Apache Spark data processing workloads into native Apache Beam Go pipelines using PostgreSQL as the source and sink via `postgresio`.

---

## Spark vs. Beam Architectural Mapping Matrix

| Spark Architecture / API | Beam Go Implementation | Target PostgreSQL Table | Engineering Trade-offs & Benefits |
| :--- | :--- | :--- | :--- |
| **1. Multi-Dimensional OLAP Cube**<br>`df.groupBy("region", "category").agg(...)` | Composite Keying (`Region\|Category`) + `beam.GroupByKey` + `aggregateOlapCubeFn` | [`public.olap_sales_cube`](#database-prerequisites--ddl) | Eliminates Spark driver shuffle spill; executes memory-bounded incremental rollup and atomic upsert. |
| **2. Window Function Top-N per Group**<br>`dense_rank().over(partitionBy("cat").orderBy($"sales".desc))` | Partition by Category + `beam.GroupByKey` + bounded memory sort/heap ranking | [`public.top_products_by_category`](#database-prerequisites--ddl) | Deterministic ranking per partition; bounded heap prevents JVM GC pauses; atomic `(category, rank)` upsert. |
| **3. Graph Topology & Centrality (GraphX)**<br>`graph.inDegrees.join(graph.outDegrees)` | Edge fan-out to directional degree deltas + `beam.GroupByKey` + `aggregateNodeCentralityFn` | [`public.graph_node_centrality`](#database-prerequisites--ddl) | Replaces heavy GraphX Pregel iterations with single-pass distributed MapReduce vertex combining. |
| **4. Feature Engineering & Scaling (MLlib)**<br>`StandardScaler` + `MinMaxScaler` | Global population combiner + Beam Side Input + `normalizeFeaturesFn` | [`public.ml_feature_store`](#database-prerequisites--ddl) | Scales features into bounded distributions `[0, 1]` and Z-scores without multi-pass Spark DataFrame caching. |
| **5. Inactivity Gap Sessionization**<br>`groupBy(session_window($"time", "30 mins"), $"user")` | User event stream sorting + delta time gap evaluation (`timeSinceLast > 30m`) | [`public.user_session_summaries`](#database-prerequisites--ddl) | Direct stream-to-session bounding; captures bounce visits (`event_count == 1`) and session durations in seconds. |
| **6. Data Reconciliation & Table Diff**<br>`sourceDf.join(targetDf, Seq("id"), "full_outer")` | Outer join via `beam.CoGroupByKey` + anti-join checksum validation DoFn | [`public.data_reconciliation_audit`](#database-prerequisites--ddl) | Audits replication fidelity, pinpointing `MISSING_TARGET`, `MISSING_SOURCE`, and `VALUE_DRIFT` records. |

---

## Database Prerequisites & DDL

Execute the following DDL in PostgreSQL prior to running the pipelines:

```sql
-- 1. Multi-Dimensional OLAP Sales Cube
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

-- 2. Top-N Ranking per Category
CREATE TABLE IF NOT EXISTS public.top_products_by_category (
    category TEXT NOT NULL,
    rank_position INT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    sales_volume NUMERIC(14,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (category, rank_position)
);

-- 3. Graph Degree Centrality
CREATE TABLE IF NOT EXISTS public.graph_node_centrality (
    node_id TEXT PRIMARY KEY,
    in_degree INT NOT NULL,
    out_degree INT NOT NULL,
    total_degree INT NOT NULL,
    avg_edge_weight NUMERIC(10,4) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

-- 4. Machine Learning Feature Store
CREATE TABLE IF NOT EXISTS public.ml_feature_store (
    entity_id TEXT PRIMARY KEY,
    raw_income NUMERIC(12,2) NOT NULL,
    norm_income NUMERIC(8,4) NOT NULL,
    z_income NUMERIC(8,4) NOT NULL,
    raw_score NUMERIC(6,2) NOT NULL,
    norm_score NUMERIC(8,4) NOT NULL,
    engineered_at TIMESTAMPTZ NOT NULL
);

-- 5. User Activity Session Summaries
CREATE TABLE IF NOT EXISTS public.user_session_summaries (
    user_id TEXT NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ NOT NULL,
    duration_seconds BIGINT NOT NULL,
    event_count BIGINT NOT NULL,
    is_bounce BOOLEAN NOT NULL,
    PRIMARY KEY (user_id, session_start)
);

-- 6. Data Reconciliation Audit
CREATE TABLE IF NOT EXISTS public.data_reconciliation_audit (
    record_id TEXT PRIMARY KEY,
    reconciliation_status TEXT NOT NULL,
    source_checksum TEXT,
    target_checksum TEXT,
    difference_details TEXT,
    audited_at TIMESTAMPTZ NOT NULL
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO beam_test;
```

---

## Pattern Inventory & Local Execution Runbook

### 1. Multi-Dimensional OLAP Cube
- **File**: [`multi_dimensional_olap/main.go`](./multi_dimensional_olap/main.go)
- **Execution Command**:
  ```bash
  go run sdks/go/examples/postgres/spark_patterns/multi_dimensional_olap/main.go \
      --database=postgres --username=beam_test --password=beam_test
  ```

### 2. Window Function Top-N Ranking per Category
- **File**: [`top_n_ranking/main.go`](./top_n_ranking/main.go)
- **Execution Command**:
  ```bash
  go run sdks/go/examples/postgres/spark_patterns/top_n_ranking/main.go \
      --top_k=3 --database=postgres --username=beam_test --password=beam_test
  ```

### 3. Graph Degree Centrality & Topological Summary
- **File**: [`graph_vertex_degrees/main.go`](./graph_vertex_degrees/main.go)
- **Execution Command**:
  ```bash
  go run sdks/go/examples/postgres/spark_patterns/graph_vertex_degrees/main.go \
      --database=postgres --username=beam_test --password=beam_test
  ```

### 4. Machine Learning Feature Engineering (StandardScaler & MinMaxScaler)
- **File**: [`ml_feature_engineering/main.go`](./ml_feature_engineering/main.go)
- **Execution Command**:
  ```bash
  go run sdks/go/examples/postgres/spark_patterns/ml_feature_engineering/main.go \
      --database=postgres --username=beam_test --password=beam_test
  ```

### 5. Inactivity Gap User Sessionization
- **File**: [`sessionization/main.go`](./sessionization/main.go)
- **Execution Command**:
  ```bash
  go run sdks/go/examples/postgres/spark_patterns/sessionization/main.go \
      --gap_minutes=30 --database=postgres --username=beam_test --password=beam_test
  ```

### 6. Data Reconciliation & Table Anti-Join Diff
- **File**: [`data_reconciliation_diff/main.go`](./data_reconciliation_diff/main.go)
- **Execution Command**:
  ```bash
  go run sdks/go/examples/postgres/spark_patterns/data_reconciliation_diff/main.go \
      --database=postgres --username=beam_test --password=beam_test
  ```
