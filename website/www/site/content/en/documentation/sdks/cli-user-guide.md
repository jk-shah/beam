---
type: languages
title: "Apache Beam CLI User Guide"
---
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

# Apache Beam CLI User Guide: By Data Processing Task

This guide is organized around **what you want to accomplish**—from inspecting an
unknown dataset in `<90 ms` on your laptop to running failure-isolated streaming
joins, online enrichment, ML inference, Lakehouse registration, and verified
cross-store migrations on production clusters.

For a compact syntax lookup of all subcommands, URI schemes, and exit codes, see
the [Apache Beam CLI Reference](../cli/) or run `beam --llms-txt`.

---

## What Do You Want to Do?

Jump directly to the workflow that matches your task:

| Your Goal | Command Pattern | Typical Latency / Scale | Section |
| :--- | :--- | :--- | :--- |
| **Inspect an unknown file, Lakehouse table, or warehouse view** | `beam inspect <URI> --stats -n 5` | `<90 ms` local / `$0` metadata | [Task 1](#task-1-explore-unknown-files-tables-and-live-streams-safely) |
| **Peek or live-follow Kafka, Pub/Sub, or Lakehouse commits** | `beam tail -f -i <URI>` | `<65 ms` non-destructive | [Task 1.4](#14-peek-and-live-follow-streams-without-perturbing-production) |
| **Convert & filter CSV, JSONL, Parquet, or Avro** | `beam pipe raw.csv clean.parquet --where "..." --select "..."` | `<150 ms` local or Cloud | [Task 2.1](#21-convert-formats-filter-rows-and-project-columns) |
| **Quarantine dirty rows & scrub PII into a Dead-Letter Queue** | `beam pipe ... --expect "..." --dlq bad.jsonl --dlq-redact-fields ssn` | Local & Distributed | [Task 2.2](#22-enforce-data-quality-rules-and-quarantine-dirty-records) |
| **Block accidental multi-TB scans or breaking schema drift** | `beam pipe ... --max-bytes 10GB --check-schema --assert-counts` | `<100 ms` pre-flight | [Task 2.3](#23-prevent-runaway-scans-schema-breakage-and-race-overwrites) |
| **Join Parquet + CSV + Iceberg with SQL & quantiles** | `beam pipe --from o=... --join c=... --sql "SELECT ..."` | `<80 ms` local or Runner | [Task 3.1](#31-join-multiple-formats-and-compute-quantiles-in-sql) |
| **Run `TUMBLE` / `HOP` / `SESSION` SQL windows without Java** | `beam pipe ... --sql "SELECT ... FROM TABLE(TUMBLE(...))" --sql-engine python` | Zero-JVM Beam YAML | [Task 3.2](#32-run-windowed-sql-without-a-jvm) |
| **Launch an interactive or batch Apache Calcite SQL shell** | `beam sql --io kafka -e "SELECT ..."` | Cached JVM `BeamSqlLine` | [Task 3.3](#33-run-interactive-or-batch-apache-calcite-sql-beam-sql) |
| **Migrate a database table & prove zero row or decimal drift** | `beam pipe postgres://... iceberg://... --verify --key id` + `beam diff` | Distributed $O(1)$ RAM | [Task 4](#task-4-migrate-databases-and-mathematically-prove-zero-drift) |
| **Replicate CDC (`UPSERT` + `DELETE`) across files & tables** | `beam pipe <CDC_SRC> <DEST> --cdc --primary-key id` | Local / BQ Write API / Iceberg MoR | [Task 5](#task-5-replicate-change-data-capture-cdc-across-engines) |
| **Expose Parquet or Delta files as Iceberg with `0` bytes copied** | `beam pipe --zero-copy` / `beam catalog sync <SRC> <DEST>` | `<200 ms` metadata-only | [Task 6](#task-6-register-lakehouse-tables-with-zero-bytes-copied) |
| **Deduplicate streams, enrich from online stores, & score ML models** | `beam pipe ... --dedupe-by ... --enrich ... --infer ...` | Stateful / Batched Workers | [Task 7](#task-7-deduplicate-enrich-and-run-ml-inference-on-streams) |
| **Correlate event-time streams (`ASOF`, Interval, Late DLQ, CEP)** | `beam pipe ... --asof-join` / `--interval-join` / `--pattern` | Watermark Stateful DoFns | [Task 8](#task-8-correlate-event-time-streams-joins-late-data-and-patterns) |
| **Graduate to Flink, Spark, Dataflow, Flex Templates & Airflow** | `--emit-yaml` -> `beam check --plan` -> `beam package` -> `beam export-schedule` | `<2 s` Zero-Docker | [Task 9](#task-9-graduate-from-laptop-prototype-to-production-clusters) |
| **Automate pipelines safely with AI agents or CI/CD** | `beam --llms-txt` + `--json` | `fd 1` isolated JSON | [Task 10](#task-10-automate-pipelines-with-ai-agents-and-cicd) |

---

## Core Mental Model: One Command Across Four Execution Tiers

Every `beam` command shares the same URI grammar, SQL three-valued `NULL` logic,
dead-letter quarantine schema, and row-conservation accounting—while
automatically routing execution to the lightest tier capable of running your job:

1. **Tier 1 — Zero-SDK Metadata & Control (`<60–90 ms`)**: `beam inspect`,
   `beam tail`, `beam schema diff`, `beam catalog sync`, `beam check`,
   `beam package`, `beam config`, `beam io`, and `beam doctor` run without
   importing the heavy SDK runtime or spinning up workers.
2. **Tier 2 — In-Process Local Fast Path (`<80–250 ms`, `<= 64 MB`)**: Bounded
   local file pipelines (`CSV`, `JSONL`, `Parquet`, `Avro`, `Text`) and local
   `--sql` queries (`sqlite` or `duckdb`) execute directly in-process with zero
   cluster startup overhead (`BEAM_CLI_NO_FAST_PATH=1` forces a Beam runner).
3. **Tier 3 — Zero-JVM Local Portable Runner (`PrismRunner` / `FnApiRunner`)**:
   Multi-branch DAGs, stateful streaming simulations, and `--sql-engine python`
   windowed aggregations execute locally across Python workers without requiring
   Java.
4. **Tier 4 — Distributed Production Runners (`FlinkRunner`, `SparkRunner`, `DataflowRunner`)**:
   Adding `--runner=<Runner>` (or ejecting portable YAML via `--emit-yaml`)
   scales the exact same pipeline across multi-terabyte batch datasets and
   24/7 unbounded event streams.

### One-Time Setup & 3-Minute End-to-End Lab

```sh
python3 -m venv .venv-beam && source .venv-beam/bin/activate
pip install 'apache-beam[yaml]' pyarrow fastavro

# Optional: add cloud connectors or DuckDB analytical SQL
# pip install 'apache-beam[yaml,gcp,aws]' duckdb

beam doctor
```

*(Both `beam` and `apache-beam` entrypoints are installed; use `apache-beam` if
another binary on `PATH` shadows `beam`.)*

Try the complete local workflow—inspection, PII-redacted DLQ quarantine,
row-conservation proof, multi-format SQL join, zero-copy Iceberg v2
registration, and `beam diff` verification—in an empty directory:

```sh
mkdir beam-lab && cd beam-lab

cat << 'EOF' > orders.csv
order_id,zip,customer_id,email,status,amount,created_at,metadata_json
1001,00501,c1,ada@example.com,PAID,149.50,2026-11-15T10:00:00Z,"{""channel"":""web""}"
1002,94043,c2,linus@example.org,CANCELLED,25.00,2026-11-15T10:05:00Z,"{""channel"":""mobile""}"
1003,02139,c1,ADA@example.com,SHIPPED,299.00,2026-11-15T10:12:00-05:00,"{""channel"":""partner""}"
1004,10001,c3,bad@example.com,PAID,-15.00,2026-11-15T10:20:00Z,"{""channel"":""web""}"
corrupt_line_missing_columns
EOF

cat << 'EOF' > customers.csv
customer_id,tier,country
c1,ENTERPRISE,US
c2,STARTER,FI
EOF

# 1. Inspect schema & sanitized sample (<90 ms; 'zip' stays 'str' because of '00501')
beam inspect orders.csv -n 3

# 2. Clean, normalize UTC timestamps, redact PII in DLQ, and prove row conservation
beam pipe orders.csv curated/orders.parquet \
  --where "status IN ('PAID', 'SHIPPED')" \
  --select "CAST(order_id AS INT) AS order_id, zip, customer_id, lower(email) AS email, parse_timestamp(created_at) AS created_utc, json_get(metadata_json, '$.channel') AS channel, round(amount, 2) AS amount" \
  --expect "positive_amount: amount > 0 -> dlq" \
  --dlq quarantine/orders_dlq.jsonl \
  --dlq-redact-fields email \
  --assert-counts --json

# 3. Join curated Parquet + customer CSV in SQL -> single-document keyed JSON
beam pipe \
  --from o=curated/orders.parquet \
  --join c=customers.csv \
  --sql "SELECT COALESCE(c.tier, 'UNMAPPED') AS tier, COUNT(*) AS orders, ROUND(SUM(o.amount), 2) AS revenue, ROUND(quantile(o.amount, 0.95), 2) AS p95_amount FROM o LEFT JOIN c ON o.customer_id = c.customer_id GROUP BY 1" \
  --to reports/tier_summary.json --to-opt key=tier

# 4. Register curated Parquet as an Apache Iceberg v2 table (0 bytes copied) & verify zero drift
beam pipe curated/orders.parquet "iceberg://$PWD/iceberg_wh/lake.orders" --zero-copy
beam diff curated/orders.parquet "iceberg://$PWD/iceberg_wh/lake.orders" --key order_id
```

---

## Task 1: Explore Unknown Files, Tables, and Live Streams Safely

Before writing any transformation, you usually want to answer three questions:
*What columns and types are in this dataset? Are there nulls or skew? What do live
events look like right now?*

### 1.1 Inspect Schemas, Column Statistics, and Snapshots

`beam inspect` reads file footers, Lakehouse transaction logs, or `$0` warehouse
metadata (`tables.get` / `dryRun` for views) without scanning full tables:

```sh
# Local or cloud files (glob shards are resolved automatically)
beam inspect "s3://analytics-lake/events/2026-11-*.parquet" --stats -n 5

# Search a wide 400-column table for timestamp or revenue columns
beam inspect bigquery://my-proj.sales.fact_orders --search "amount|created"

# Bound Parquet footer scanning across 500+ shards (sets truncated_scan: true when capped)
beam inspect "s3://analytics-lake/archive/*.parquet" --max-scanned-bytes 4MB --json

# Inspect a specific Delta Lake version or Apache Iceberg v2 snapshot
beam inspect "delta://gs://my-lake/orders?version=12" --stats
beam inspect "iceberg://sales.orders?warehouse=s3://my-lake/warehouse" -n 3
```

### 1.2 Discover Connectors and Transforms Offline (`beam io`)

```sh
beam io list
beam io describe bigquery
beam io describe WindowInto
```

### 1.3 Preview Transformations Without Writing Output Files

Omit the destination URI (and cap rows with `-n`) to preview filtered or
computed rows directly in your terminal before materializing any files:

```sh
beam pipe orders.csv \
  --drop-bad \
  --where "amount >= 100 AND status = 'PAID'" \
  --select "order_id, lower(email) AS email, round(amount * 1.08, 2) AS total_with_tax" \
  -n 5
```

When writing an output file, pass `--preview` alongside `--to` to materialize
the destination *and* return a sanitized sample in one pass.

### 1.4 Peek and Live-Follow Streams Without Perturbing Production

`beam tail` inspects live streams **non-destructively**:
- On **Kafka** (`kafka://`), it uses an ephemeral consumer group
  (`beam-tail-<uuid>`) with `enable.auto.commit=false`, never advancing
  committed production offsets.
- On **Pub/Sub** (`pubsub://`), peeking a **topic** creates an ephemeral
  subscription protected by 5-minute `beam_lease_until` label heartbeats and
  local PID leases (so concurrent `>15m` tail sessions never delete each other,
  while crashed sessions expire cleanly). Peeking an existing **subscription**
  pulls messages without `ACK`ing them so downstream consumers lose zero events.
- On **large log files and shards**, `--max-scanned-bytes` (default `64MB`)
  bounds memory below `16 MiB` even on multi-gigabyte logs.

```sh
# Bounded 5-second peek of 10 events
beam tail kafka://broker:9092/payments -n 10 --duration 5s

# Unix-style live follow with interactive rolling msg/s TUI (-i)
beam tail -f -i pubsub://projects/my-proj/topics/clickstream

# Watch newly committed snapshots arriving on a Delta or Iceberg table
beam tail -f "iceberg://lake.orders?warehouse=gs://my-lake/wh"
```

---

## Task 2: Clean, Transform, and Quarantine Messy Batch Data

Real-world batch files rarely arrive clean: CSVs contain truncated quotes, ZIP
codes with leading zeros (`00501`), `"N/A"` placeholders, negative amounts, and
PII fields that must never leak into error logs.

### 2.1 Convert Formats, Filter Rows, and Project Columns

`beam pipe` accepts positional `<SOURCE> <SINK>` URIs (or explicit `--from` /
`--to` flags) and infers formats from file extensions:

```sh
beam pipe "raw/orders_*.csv" curated/orders.parquet \
  --null-values "NA,N/A,-" \
  --where "status IN ('PAID', 'SHIPPED') AND email ILIKE '%@example.com' AND amount BETWEEN 10 AND 50000" \
  --select "CAST(order_id AS INT) AS order_id, strip(customer_name) AS customer_name, parse_timestamp(created_at) AS created_utc, json_get(metadata_json, '$.channel') AS channel, round(amount, 2) AS amount"
```

What `beam pipe` handles automatically here:
- **Leading-zero & header safety**: Columns containing values like `00501` stay
  `string` rather than silently truncating to integer `501` (override explicitly
  anytime with `--from-opt fields="zip:str,amount:float"`), and CSV shards in a
  glob align by column name even when column orders differ across files.
- **Sparse JSONL schema safety**: Local JSONL inputs (`<= 64 MB`) scan every
  record so sparse keys appearing after row 100 are preserved automatically; on
  larger remote streams, unseen keys raise `SchemaDriftError` (or route cleanly
  to `--dlq` rather than silently dropping fields; customize via
  `--from-opt sample_rows=all` or `on_unknown_fields=ignore`).
- **SQL-natural expressions**: `--where` and `--select` compile AST-sandboxed
  expressions supporting `=` / `<>`, `[NOT] LIKE` / `ILIKE`, `[NOT] BETWEEN`,
  `IN (...)`, `IS [NOT] NULL`, `CAST(x AS INT|FLOAT|STRING|BOOL)`, backticked
  reserved-word columns (`` `class` ``, `` `from` ``), dot-access on nested
  structs (`user.address.country`), UTC timestamp helpers (`parse_timestamp` /
  `to_utc`, `epoch_ms`, `year`, `month`, `day`, `hour`, `date_part`), and JSON
  extraction (`json_get` / `json_extract_scalar` / `json_value`)—with strict SQL
  three-valued `NULL` semantics across both local and distributed runners
  *(note: as in SQL, `state != 'CA'` drops `NULL` rows; write
  `state IS NULL OR state != 'CA'` to retain `NULL`s)*.

### 2.2 Enforce Data Quality Rules and Quarantine Dirty Records

Instead of aborting a 500-million-row ingest on three corrupt lines—or writing
ad-hoc `try/except` wrappers—combine `--expect` rules with `--dlq` and
`--dlq-redact-fields`:

```sh
beam pipe raw/events.jsonl clean/events.parquet \
  --select "event_id, user_id, email, ssn, amount, env" \
  --expect "valid_amount: amount > 0 -> dlq" \
  --expect "has_user: user_id IS NOT NULL -> dlq" \
  --expect "non_test_env: env != 'sandbox' -> drop" \
  --dlq quarantine/events_dlq.jsonl \
  --dlq-redact-fields "email,ssn,payment.card_last4" \
  --assert-counts
```

- **Malformed syntax + rule violations in one quarantine**: Corrupt CSV/JSON
  parse errors, schema drift, sink rejections, and `--expect ... -> dlq`
  violations all route to `quarantine/events_dlq.jsonl` with structured
  (`step`, `error_type`, `error`, `raw_payload`) metadata. Referencing a column
  in `--expect` that was dropped by `--select` fails fast pre-flight (`exit 2`).
- **Recursive PII scrubbing (`--dlq-redact-fields`)**: Redacts sensitive keys
  inside both structured rows and nested JSON strings in `raw_payload` before
  dead-letter records touch disk or object storage.
- **Drop-only shorthand (`--drop-bad`)**: When you simply want invalid records
  discarded without materializing a DLQ dataset, pass `--drop-bad` (alias
  `--skip-errors`).

### 2.3 Prevent Runaway Scans, Schema Breakage, and Race Overwrites

Add pre- and post-flight guardrails when promoting batch jobs to shared
warehouses:

```sh
beam pipe "gs://raw-landing/orders/*.parquet" bigquery://prod.analytics.orders \
  --check-schema \
  --max-bytes 25GB \
  --dlq "gs://raw-landing/dlq/orders_bad.jsonl" \
  --assert-counts \
  --if-exists replace
```

1. **`--check-schema`** (or standalone `beam schema diff <SRC> <DST> --mode BACKWARD`):
   Compares source and target schemas in `<100 ms` *before* launching workers,
   exiting `2` with copy-pasteable `--select "CAST(...)"` remediation hints if a
   required column was dropped or narrowed (`FLOAT64 -> INT32`).
2. **`--max-bytes 25GB`**: Evaluates scan size pre-flight (`$0` metadata /
   `dryRun` on BigQuery, `pg_partition_tree` on PostgreSQL, sibling-safe object
   listings on S3/GCS) and aborts with exit code `8` (`EXIT_COST_LIMIT`) before
   spending compute.
3. **Atomic `--if-exists` & multi-job concurrency**: Default `--if-exists fail`
   uses kernel-atomic hardlinks (`os.link`) and exclusive Iceberg locks so
   concurrent jobs racing on the same path never overwrite the winner's output
   or DLQ; `--if-exists replace` stages new local outputs in private directories
   and swaps primary sinks ahead of DLQ only after execution,
   `--assert-counts`, and `--verify` all pass.
4. **`--assert-counts`**: Verifies mathematically that
   `rows_read == rows_written + rows_filtered_out + dlq_rows + stage_dropped_rows`
   (exiting `1` if a single record goes missing).

---

## Task 3: Query, Join, and Window Multi-Format Data with SQL

When your task involves multi-table `JOIN`s, `GROUP BY` rollups, percentiles, or
window functions across heterogeneous files and Lakehouse tables, use `--sql`
(or `--sql-file query.sql`).

### 3.1 Join Multiple Formats and Compute Quantiles in SQL

Alias each input with `alias=URI` via `--from` / `--join`. Locally (`<= 64 MB`),
`beam pipe --sql` runs in `<80 ms` inside a read-only `:memory:` SQLite sandbox
augmented with `median(x)`, `quantile(x, 0.95)` (`0.0..1.0` fraction scale),
`percentile(x, 95)` (`0..100` percentage scale), `stddev(x)`, `corr(x, y)`,
`regr_slope(y, x)`, `sha256(x)`, `mask_email(x)`, `mask_last4(x)`, and
`to_utc(ts)`—or in an in-memory DuckDB sandbox via `--sql-engine duckdb`:

```sh
beam pipe \
  --from o=data/orders_*.parquet \
  --join c=data/customers.csv \
  --join fx=data/fx_rates.json \
  --sql "
    SELECT
      COALESCE(c.tier, 'UNMAPPED') AS tier,
      COUNT(*) AS orders,
      ROUND(SUM(o.amount * fx.usd_rate), 2) AS revenue_usd,
      ROUND(quantile(o.amount * fx.usd_rate, 0.95), 2) AS p95_order_usd,
      MIN(to_utc(o.created_at)) AS first_order_utc
    FROM o
    LEFT JOIN c ON o.customer_id = c.customer_id
    JOIN fx ON o.currency = fx.currency
    WHERE o.status = 'PAID'
    GROUP BY 1
    ORDER BY revenue_usd DESC
  " \
  --expect "positive_rev: revenue_usd > 0 -> dlq" \
  --dlq reports/quality_dlq.jsonl \
  --to reports/revenue_by_tier.json \
  --to-opt key=tier
```

- **Single-document keyed JSON (`--to-opt key=tier`)**: Emits a clean lookup
  object `{"ENTERPRISE": {"orders": ...}, "PRO": {...}}` instead of JSONL when
  generating configuration artifacts or API payloads.
- **Composable pipeline stages**: Combine `--where` (pre-filter before SQL),
  `--sql`, `--select` (post-SQL projection), `--expect` (per-rule quality gates),
  and `--dlq` / `--drop-bad` in a single command across `.parquet`, `.avro`,
  `.jsonl`, and `.csv` sinks.
- **ANSI OLAP extensions (`--sql-engine duckdb`)**: Pass `--sql-engine duckdb`
  whenever your query uses `PERCENTILE_CONT(...) WITHIN GROUP`, `QUALIFY`, or
  `GROUPING SETS`.

### 3.2 Run Windowed SQL Without a JVM

Standard distributed Beam Calcite SQL uses a Java expansion service. When
running in Python-only containers, CI runners, or airgapped environments without
a JVM, `--sql-engine python` (automatically selected for SQL TVF window queries
when no JVM is present) lowers `TUMBLE`, `HOP`, and `SESSION` table-valued
functions directly into pure-Python Beam YAML (`AssignTimestamps` + `Filter` +
`WindowInto` + `Combine` + `MapToFields`):

```sh
beam pipe events.jsonl windowed_counts.parquet \
  --sql-engine python \
  --sql "
    SELECT
      service,
      window_start,
      window_end,
      COUNT(*) AS requests,
      COUNT(DISTINCT user_id) AS unique_users,
      ROUND(AVG(latency_ms), 2) AS avg_latency_ms
    FROM TABLE(TUMBLE(TABLE source, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE))
    WHERE status_code >= 500
    GROUP BY service, window_start, window_end
    HAVING COUNT(*) >= 10
  "
```

> **Tip**: Pass `--portable` to `beam pipe` or `beam check` before deploying a
> local SQL prototype to a remote cluster—`beam` flags any SQLite-only scalar
> functions pre-flight with exit code `2` so remote jobs never fail mid-launch.

### 3.3 Run Interactive or Batch Apache Calcite SQL (`beam sql`)

For interactive `CREATE EXTERNAL TABLE` DDL exploration or batch Calcite SQL
scripts (`BeamSqlLine`), use `beam sql` (requires Java 11+ and shares shaded
JAR caches in `~/.beam/cache/` with `scripts/beam-sql.sh`):

```sh
beam sql --list-ios
beam sql -e "SELECT 1 + 1 AS two;"
beam sql --io kafka --io bigquery-storage --runner dataflow
```

---

## Task 4: Migrate Databases and Mathematically Prove Zero Drift

Database and warehouse migrations (`PostgreSQL -> Iceberg`, `MySQL -> BigQuery`,
`Delta Lake -> Iceberg`) traditionally rely on spot-checking `COUNT(*)`—which
misses silent float/decimal rounding, timezone shifts, and NULL-coercion bugs.

### 4.1 Extract from Relational Databases Without Inline Secrets

`beam` rejects inline `user:password@` credentials in URIs and flags with exit
code `2` so secrets never leak into shell history, process tables, or agent
transcripts. Provide credentials via environment variables, ADC, or a `0600`
options file (symbolic links are refused):

```sh
export BEAM_POSTGRES_USER="readonly_migrator"
export BEAM_POSTGRES_PASSWORD="***"

# 1. Pre-flight schema compatibility check (<100 ms)
beam schema diff \
  "postgres://db.internal:5432/commerce?table=public.ledger_entries" \
  "iceberg://finance.ledger_entries?warehouse=s3://corp-lake/wh" \
  --mode BACKWARD

# 2. Migrate and automatically reconcile source vs. destination (--verify)
beam pipe \
  --from "postgres://db.internal:5432/commerce?table=public.ledger_entries" \
  --to "iceberg://finance.ledger_entries?warehouse=s3://corp-lake/wh" \
  --verify --key entry_id \
  --assert-counts
```

### 4.2 Reconcile Any Two Datasets in $O(1)$ Worker Memory (`beam diff`)

Run `beam diff` standalone in CI/CD or cutover scripts across files, `delta://`,
`iceberg://`, and `bigquery://`:

```sh
beam diff \
  "delta://s3://legacy-lake/ledger" \
  "iceberg://finance.ledger_entries?warehouse=s3://corp-lake/wh" \
  --key entry_id \
  --columns shared \
  --atol 1e-6 \
  --to migration_audit.json
```

- **Exact 38-digit `Decimal` fidelity**: Financial `DECIMAL(38, s)` /
  `BIGNUMERIC` columns are canonicalized via exact decimal tuple digits without
  28-digit thread-context rounding or 64-bit float truncation.
- **Deterministic CI exit code**: Exits `0` when datasets match and **`6`
  (`EXIT_DIFF_MISMATCH`)** when any schema drift, missing key, extra key,
  duplicate key, or value mismatch is found—writing bounded diagnostics to
  `migration_audit.json`.

---

## Task 5: Replicate Change Data Capture (CDC) Across Engines

When upstream databases emit Debezium envelopes (`before` / `after` / `op` /
`source.lsn`), Cloud Spanner Change Streams, or Lakehouse change feeds, `--cdc
--primary-key <COLS>` applies ordered `UPSERT` and `DELETE` mutations across
files, warehouses, and Lakehouses.

### 5.1 Compact CDC Logs into Latest-State Parquet Snapshots

```sh
beam pipe cdc/orders_debezium.jsonl snapshots/orders_latest.parquet \
  --cdc --primary-key order_id \
  --dlq dlq/cdc_corrupt.jsonl \
  --assert-counts
```

Locally (`<= 64 MB`), `local_fast` compacts mutations in `<250 ms` ordered by
`lsn` / `ts_ms` / `seq_ts` / `updated_at` (or explicit `--cdc-seq-col`),
dropping deleted keys cleanly.

### 5.2 Apply Live or Batch `UPSERT` + `DELETE` to Iceberg v2 and BigQuery

```sh
# Apache Iceberg v2 Merge-on-Read (writes equality-delete manifests)
beam pipe kafka://broker:9092/dbserver1.public.orders \
  "iceberg://lake.orders?warehouse=s3://corp-lake/wh" \
  --cdc --primary-key order_id \
  --window fixed:60s \
  --dlq s3://corp-lake/dlq/orders_cdc/ \
  --runner=FlinkRunner

# BigQuery Storage Write API _default stream (_CHANGE_TYPE + hex sequence number)
beam pipe pubsub://projects/my-proj/subscriptions/orders-cdc \
  bigquery://my-proj.analytics.orders_live \
  --cdc --primary-key order_id \
  --dlq gs://my-bucket/dlq/orders_cdc/ \
  --runner=DataflowRunner \
  --project=my-proj --region=us-central1 --temp_location=gs://my-bucket/tmp
```

### 5.3 Zero-Gap Lakehouse Migration (Pinned Snapshot + Incremental CDC)

To migrate an active Delta Lake table to Apache Iceberg without halting writers
or missing commits that land during the initial sync:

```sh
# 1. Pin current snapshot version V0 and register existing Parquet files with 0 bytes copied
beam inspect delta://s3://my-lake/orders --sample 0 --json
# Suppose snapshot_version is 42:
beam pipe "delta://s3://my-lake/orders?version=42" \
  "iceberg://lake.orders?warehouse=s3://my-lake/iceberg_wh" \
  --zero-copy --verify --key order_id

# 2. Replay commits 43..45 via incremental CDC Merge-on-Read
beam pipe "delta://s3://my-lake/orders?start_version=43&end_version=45" \
  "iceberg://lake.orders?warehouse=s3://my-lake/iceberg_wh" \
  --cdc --primary-key order_id --if-exists append
```

---

## Task 6: Register Lakehouse Tables with Zero Bytes Copied

Copying terabytes of Parquet files just to switch catalog engines wastes hours
and storage egress. `beam` synthesizes Apache Iceberg v2 metadata manifests
directly over your existing Parquet, partitioned Delta Lake, or Iceberg files in
`<200 ms` with **`bytes_copied = 0`**.

### 6.1 Convert Partitioned Parquet or Delta Tables In-Place (`--zero-copy`)

```sh
beam pipe \
  --from delta://gs://my-lake/delta/events \
  --to "iceberg://analytics.events?warehouse=gs://my-lake/iceberg_wh" \
  --zero-copy
```

Hive-style directory partitions (`date=2026-11-15/region=US/*.parquet`) and
schema evolution across historical Delta commits are mapped automatically to
Iceberg `PartitionSpec` records and monotonic column IDs, with atomic snapshot
locks and 16-attempt CAS retries protecting concurrent writers.

### 6.2 Synchronize Partitions into Enterprise REST Catalogs (`beam catalog sync`)

Register partitioned Parquet directories, Delta logs, or Iceberg metadata into
standard Iceberg REST catalogs, BigLake REST, or BigQuery Managed Iceberg:

```sh
# Preview manifest & partition discovery without mutating the catalog
beam catalog sync \
  "s3://my-lake/parquet/clickstream/" \
  "iceberg://analytics.clickstream?catalog=rest&uri=https://catalog.internal/v1" \
  --dry-run

# Register in BigLake / BigQuery Managed Iceberg in <200 ms
beam catalog sync \
  "gs://my-lake/delta/orders" \
  "iceberg://bigquery.analytics.orders?warehouse=gs://my-lake/wh"
```

---

## Task 7: Deduplicate, Enrich, and Run ML Inference on Streams

Production streaming pipelines often fail in three predictable ways: duplicate
at-least-once retries double-count revenue, external lookup misses crash
workers, and a single malformed input tensor drops an entire 64-element ML
inference batch (or triggers repeated multi-GB model reloads). `beam pipe`
isolates every stage at the **individual record** level.

Pipeline stages compose in a deterministic order:
`--where` -> `--pre-select` -> `--enrich` -> `--infer` -> `--post-where` -> `--window` -> `--select` -> `--expect`.

### 7.1 Deduplicate At-Least-Once Streams (`--dedupe-by`)

Keep either the `first` arrival or the `last` state per key within a bounded
time horizon, optionally auditing suppressed duplicates in `--dedupe-dlq`:

```sh
beam pipe kafka://broker:9092/payments clean/payments.parquet \
  --timestamp-col event_ts \
  --dedupe-by payment_id \
  --dedupe-window 15m \
  --dedupe-keep last \
  --dedupe-order sequence_num \
  --dedupe-dlq audit/duplicate_payments.jsonl
```

*(Note: When your pipeline also specifies `--dlq <URI>`, pass `--dedupe-dlq` as
a boolean flag without a path so suppressed duplicates route into the shared
DLQ.)*

### 7.2 Hydrate Records from Online Stores with Read-Through Caching (`--enrich`)

Batch worker-side lookups against `file:`, parameterized BigQuery
(`UNNEST(@keys)`), Cloud Bigtable, Feast, or Vertex AI Feature Store—protected
by `AdaptiveThrottler` and optional TLS `rediss://` caching
(`export BEAM_REDIS_PASSWORD=...`):

```sh
beam pipe events.jsonl enriched.parquet \
  --pre-select "*, lower(strip(raw_customer_id)) AS customer_id" \
  --enrich "bigtable:project=my-proj,instance=prod-bt,table=profiles,key=customer_id,columns=[tier:str,country:str],cache=rediss://cache.internal:6379/0,ttl=300s,on_miss=dlq" \
  --dlq dlq/missing_profiles.jsonl
```

### 7.3 Run Batch & Streaming ML Inference Without Poison-Pill Batch Drops (`--infer`)

Score records against commit-pinned HuggingFace `safetensors` models
(`trust_remote_code=False` enforced) or remote Vertex AI Endpoints (`<1.4 MB`
payload auto-splitting + rate limiting):

```sh
beam pipe reviews.jsonl flagged_reviews.parquet \
  --infer "hf:distilbert/distilbert-base-uncased-finetuned-sst-2-english,task=text-classification,revision=714eb0f,fields=[body],flatten=true" \
  --post-where "prediction_label = 'NEGATIVE' AND prediction_score >= 0.90" \
  --dlq dlq/bad_reviews.jsonl \
  --dlq-redact-fields author_email \
  --assert-counts
```

- **$O(\log N)$ recursive binary batch bisection**: If 1 corrupt record inside a
  64-row batch raises a tokenization or tensor error, `CliInfer` recursively
  bisects the batch in-memory (`<= 13` calls instead of `65` linear retries) on
  the existing loaded model instance—emitting all `63` healthy predictions with
  **zero model weight reloads** and routing only the single bad row to `--dlq`.
- **Stage-scoped options**: When chaining multiple `--enrich` or `--infer`
  stages, target stage `N` cleanly with `<1-based-stage>:key=value` (for
  example, `--infer-opt 2:max_batch_size=16`).

---

## Task 8: Correlate Event-Time Streams (Joins, Late Data, and Patterns)

### 8.1 Point-in-Time `ASOF JOIN` (Zero Lookahead Leakage)

Match each left event (such as a trade or impression) with the most recent right
reference event (such as a quote or feature snapshot) as of that exact event
timestamp:

```sh
beam pipe \
  --from t=trades.jsonl \
  --join q=quotes.jsonl \
  --asof-join "on=symbol,left_time=trade_ts,right_time=quote_ts,direction=backward,within=5s" \
  --to matched_trades.parquet
```

*(You can also write `SELECT ... FROM t ASOF LEFT JOIN q ON t.symbol = q.symbol AND t.trade_ts >= q.quote_ts` directly inside `--sql`.)*

### 8.2 Event-Time Interval Joins with Watermark-Skew Circuit Breakers

When joining two unbounded streams within a time window (for example, orders
matched to shipments within `15m`), a stalled upstream partition on one stream
can otherwise buffer millions of fast-stream records in worker state.
`--max-watermark-skew` prevents worker OOMs by shedding or quarantining
excessively skewed records:

```sh
beam pipe \
  --from o=kafka://broker:9092/orders \
  --join s=kafka://broker:9092/shipments \
  --interval-join "on=order_id,left_time=order_ts,right_time=ship_ts,between=0s..15m,type=left" \
  --max-watermark-skew 30m \
  --skew-action dlq \
  --dlq s3://my-lake/dlq/skewed_events/ \
  --to "iceberg://ops.order_fulfillment?warehouse=s3://my-lake/wh" \
  --runner=FlinkRunner
```

### 8.3 Speculative Early/Late Window Triggers and Late-Data Quarantine

Emit low-latency speculative panes every 10 seconds, refine results when late
events arrive within `--allowed-lateness`, and route droppably-late events past
the horizon into `--late-data-dlq` instead of silently discarding them:

```sh
beam pipe pubsub://projects/my-proj/subscriptions/ad-clicks \
  bigquery://my-proj.realtime.click_rollups \
  --timestamp-col click_ts \
  --window "fixed:1m" \
  --select "campaign_id, COUNT(*) AS clicks, SUM(cost) AS spend" \
  --allowed-lateness 10m \
  --trigger "watermark,early=every:10s,late=count:1" \
  --accumulation accumulating \
  --emit-pane-info \
  --late-data-dlq gs://my-bucket/dlq/late_clicks/ \
  --runner=DataflowRunner \
  --project=my-proj --region=us-central1 --temp_location=gs://my-bucket/tmp
```

*(Tip: When testing triggers locally against bounded JSONL/CSV files without
`--runner=PrismRunner`, use a count-based early trigger such as
`early=count:1`.)*

### 8.4 Detect Ordered Event Sequences and Missing Follow-Ups (`--pattern`)

Use bounded non-deterministic finite automata (`CliPatternMatch`) with
zero-`eval` predicates to detect multi-step user funnels or timeout absences
(matching `event_type` by default, or any column via `event_col=<col>`):

```sh
# Abandonment / SLA timeout alert (!payment absence match)
beam pipe checkout_events.jsonl abandoned_checkouts.jsonl \
  --timestamp-col event_ts \
  --pattern "checkout -> !payment within 15m" \
  --pattern-opt key=order_id

# Multi-step funnel with per-step filter predicates
beam pipe auth_events.jsonl suspicious_logins.jsonl \
  --timestamp-col event_ts \
  --pattern "on=user_id,seq=login -> mfa{2,3} -> purchase within 10m,where=[purchase: amount >= 1000]"
```

---

## Task 9: Graduate from Laptop Prototype to Production Clusters

You never have to rewrite a working `beam pipe` command into Java or Python
boilerplate to go to production.

### 9.1 Save Named Runner Profiles (`beam config`)

Store reusable runner options in `~/.beam/config.yaml` (`0600` permissions
enforced; override path via `BEAM_CONFIG`, or bypass in CI via `--no-config` /
`BEAM_NO_CONFIG=1`; dangerous code-loading options are blocked):

```sh
beam config set --profile flink-prod runner FlinkRunner
beam config set --profile flink-prod flink_master "flink-jobmanager.internal:8081"

beam config set --profile df-prod runner DataflowRunner
beam config set --profile df-prod project my-gcp-project
beam config set --profile df-prod region us-central1
beam config set --profile df-prod temp_location gs://my-prod-bucket/tmp
```

### 9.2 Eject, Validate, and Run Portable Beam YAML (`--emit-yaml` & `beam check`)

Every `beam pipe` pipeline compiles to human-readable [Beam YAML](../yaml/) that
can be checked into Git, parameterized with Jinja2 `{{ vars }}`, statically
validated in `<90 ms`, and executed on any Beam runner:

```sh
# 1. Export portable Beam YAML
beam pipe "gs://{{ bucket }}/raw/{{ dt }}/*.parquet" \
  bigquery://my-proj.analytics.daily_orders \
  --where "amount > 0" \
  --dlq "gs://{{ bucket }}/dlq/{{ dt }}/bad.jsonl" \
  --emit-yaml > pipelines/daily_orders.yaml

# 2. Validate YAML schema, sandboxed expressions, and deterministic plan_hash
beam check pipelines/daily_orders.yaml -p bucket=prod-landing -p dt=2026-11-15 --plan

# 3. Execute on Flink, Spark, or Dataflow using a saved profile
beam run pipelines/daily_orders.yaml -p bucket=prod-landing -p dt=2026-11-15 --profile df-prod
```

### 9.3 Submit Detached Cloud Jobs Safely (`--wait-timeout`, `beam wait`, `beam cancel`)

Long-running cloud jobs do not require keeping a terminal or CI runner open:

```sh
# Detach cleanly after 15s if still running (exits 7 with job_id recorded in ~/.beam/jobs/)
beam run pipelines/daily_orders.yaml -p bucket=prod-landing -p dt=2026-11-15 \
  --profile df-prod --wait-timeout 15s --json

# Poll later from any step and run deferred --assert-counts / --verify checks on completion
beam wait <JOB_ID> --timeout 30m --json

# Or drain a streaming job gracefully before deploying a new version
beam cancel <JOB_ID> --drain
```

A built-in 30-minute local idempotency ledger prevents accidental duplicate
`--if-exists=append` submissions if a network timeout retries the same command,
while automatically allowing immediate retries if post-job `--assert-counts` or
`--verify` validation failed.

### 9.4 Package Zero-Docker Flex Templates (`beam package`) & Export Schedules

Package parameterized YAML pipelines into Dataflow Flex Templates in `<2 s`
**without Docker or Cloud Build** (or generate single-image `Dockerfile` +
`metadata.json` bundles from custom Python `.py` pipelines via AST
introspection), then export native Airflow / Cloud Composer DAGs:

```sh
# 1. Package parameterized Beam YAML as a Flex Template Spec (<2 s, zero Docker)
beam package pipelines/daily_orders.yaml \
  --to gs://my-prod-bucket/templates/daily_orders_flex.json \
  --project=my-gcp-project --region=us-central1 \
  --no-use-public-ips

# 2. Generate a production Apache Airflow / Cloud Composer DAG
beam export-schedule pipelines/daily_orders.yaml \
  --orchestrator composer \
  --project=my-gcp-project \
  --schedule "0 2 * * *" \
  --to dags/daily_orders_dag.py
```

---

## Task 10: Automate Pipelines with AI Agents and CI/CD

`beam` is engineered to be invoked directly by autonomous coding/data agents and
strict CI/CD pipelines without brittle log-scraping.

### 10.1 Bootstrap Any Agent in One Command (`beam --llms-txt`)

```sh
beam --llms-txt
```

Emits a token-efficient, version-pinned Markdown specification of all
subcommands, URI schemes, SQL dialects, guardrails, and exit codes in `<50 ms`.

### 10.2 Enforce Pure JSON Output and Prompt-Injection Quarantine (`--json`)

Passing `--json` activates three hard runtime guarantees:

1. **OS File-Descriptor Isolation (`os.dup2`)**: Redirects raw `stdout` (`fd 1`)
   to `stderr` (`fd 2`) before importing SDKs or C++/JVM libraries, guaranteeing
   that `stdout` contains **exactly one** valid RFC-8259 JSON object
   (`schema_version: "1.0"`).
2. **Untrusted Data Quarantine (`untrusted_data.rows`)**: Sampled dataset values
   from `inspect`, `tail`, `diff`, and sinkless `pipe` are isolated inside
   `untrusted_data.rows` with ANSI/OSC terminal escapes, C0/C1 control codes,
   Unicode BiDi overrides, and Unicode Tag characters stripped so adversarial
   dataset strings cannot hijack terminals or agent prompts.
3. **Self-Healing `next_actions`**: Every non-zero error envelope includes
   concrete `next_actions` commands (for example, the exact `pip install` extra
   on exit `4` or the exact `--select "CAST(...)"` fix on schema drift).

### 10.3 Branch Deterministically on Exit Codes

```sh
beam pipe raw/orders.csv warehouse/orders.parquet \
  --check-schema --max-bytes 10GB --dlq dlq/bad.jsonl --assert-counts --verify --key order_id --json
rc=$?

case $rc in
  0) echo "Verified migration succeeded" ;;
  2) echo "Usage or schema contract violation (inspect error.message & next_actions)" ;;
  4) echo "Missing optional extra or runtime (run suggested pip install)" ;;
  6) echo "Data reconciliation mismatch detected by --verify / beam diff" ;;
  7) echo "Remote job still running cleanly in background; poll with beam wait" ;;
  8) echo "Pre-flight --max-bytes scan budget exceeded; refine partition filter" ;;
  *) echo "Pipeline or conservation failure (exit $rc)" ;;
esac
```

---

## Troubleshooting & Common Error Playbook

| Symptom / Exit Code | Root Cause | Fix |
| :--- | :--- | :--- |
| `exit 2`: `cannot pass both --dlq and --dedupe-dlq '<path>'` | Pipeline already sets `--dlq <URI>` | Pass boolean `--dedupe-dlq` (no path) alongside `--dlq <URI>` |
| `--where "state != 'CA'"` dropped empty `state` rows | SQL 3-valued logic treats `NULL != 'CA'` as `UNKNOWN` | Write `--where "state IS NULL OR state != 'CA'"` |
| `exit 2` pre-flight error on `--expect` after `--select` | `--expect` referenced a column dropped by `--select` | Keep the column in `--select` or filter earlier via `--where` |
| `exit 2`: inline credentials rejected in `postgres://user:pass@...` | Secrets blocked in URIs and flags | Set `BEAM_POSTGRES_USER` / `BEAM_POSTGRES_PASSWORD` or `0600` `--from-opt-file` |
| `exit 2`: output path already exists | Default `--if-exists fail` prevents overwrites | Pass `--if-exists replace` (atomic swap) or `--if-exists append` |
| `exit 2`: `--to -` rejected | Raw rows on `stdout` collide with logs/JSON | Omit `--to` for sinkless local preview, or write a file and `beam inspect` |
| `AttributeError: 'NoneType' ... 'time'` on local bounded `--trigger` | `early=every:10s` needs a streaming processing-time clock | Use `early=count:1` on local bounded files, or `--runner=PrismRunner` / `DataflowRunner` |
| `SchemaDriftError` on large/remote JSONL past row 100 | Unseen JSON key appeared after initial sample | Pass `--dlq`, `--from-opt sample_rows=all`, or `--from-opt on_unknown_fields=ignore` |
| `exit 4` on `--sql-engine duckdb` or `beam sql` | Missing optional `duckdb` or Java 11+ | Run `pip install duckdb`, or use `--sql-engine sqlite` / `--sql-engine python` |
| `exit 6` on `beam diff` | Schema, key, or 1-ULP value drift detected | Inspect `outputs.differing_keys`; add `--atol 1e-6` only if float noise is expected |
| `exit 8`: `CostLimitExceededError` | Source estimate exceeded `--max-bytes` | Narrow partition/glob filter or raise `--max-bytes` |
| Private-IP Dataflow Flex job hangs on PyPI | Workers lack internet (`--no-use-public-ips`) | Package with `beam package ... --no-use-public-ips` |

---

## Choosing the Right Tool: Operational Boundaries

`beam` is designed to excel at **guardrailed local-to-distributed data movement,
multi-source SQL/streaming transformation, online enrichment/ML inference,
zero-copy Lakehouse registration, and cross-store verification**. Know when to
pair or prefer complementary tools:

- **50+ Table Warehouse Modeling DAGs**: Use **dbt**, **Dataform**, or **Apache
  Airflow** to manage multi-hop SQL model lineage and scheduling, invoking
  `beam` for external ingestion, CDC replication, ML enrichment, or `beam diff`
  deployment gates.
- **Ultra-Low-Latency (`<50 ms` p99) Custom Stateful Microservices**: Author
  custom **Apache Beam SDK** (`Java` / `Python` / `Go`) or dedicated stateful
  streaming applications when you need custom binary coders or sub-50ms RPC
  loops.
- **Interactive Charting & Ad-Hoc Notebook Exploration on Clean Local Files**:
  Use **DuckDB**, **Polars**, or **Jupyter notebooks** directly for visual
  exploration; reach for `beam` when datasets require dirty-row DLQs, PII
  redaction, cross-engine reconciliation, streaming state, or seamless
  graduation to `FlinkRunner`, `SparkRunner`, and `DataflowRunner`.
