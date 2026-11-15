---
type: languages
title: "Apache Beam CLI"
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

# Apache Beam CLI (`beam`)

The Apache Beam CLI (`beam`, also installed as `apache-beam`) provides a single,
fast (`<90 ms` cold-start) command-line interface for running
[Beam YAML](../yaml/) pipelines, moving and transforming datasets without
writing boilerplate code, querying multi-format data with SQL (locally or via
zero-JVM / [Beam SQL](/documentation/dsls/sql/overview/) lowering), reconciling
migrations across storage engines, inspecting schemas and live streams, and
packaging pipelines for production runners—designed equally for interactive
human developers and autonomous AI coding/data agents.

> **Starting from a concrete data task?** Jump straight to the
> **[Apache Beam CLI User Guide](../cli-user-guide/)**, which maps common goals—
> cleaning dirty files, multi-source SQL joins, database migrations, CDC
> replication, zero-copy Lakehouse sync, streaming enrichment/ML inference,
> event-time joins, and production cluster graduation—to runnable recipes.

## Prerequisites & Installation

Install the Apache Beam Python SDK with the `[yaml]` extra inside a Python 3.10+
virtual environment:

```sh
python3 -m venv .venv-beam
source .venv-beam/bin/activate
pip install 'apache-beam[yaml]' pyarrow fastavro
```

For Google Cloud connectors (`gs://`, `bigquery://`, `pubsub://`, `spanner://`,
and `DataflowRunner`), include the `[gcp]` extra (and optional `duckdb` for
`--sql-engine duckdb`):

```sh
pip install 'apache-beam[yaml,gcp]' duckdb
```

Verify your installation, optional extras, SQL engines, Java runtime (needed
only for Calcite `beam sql` and Java-backed cross-language connectors), and
cloud credentials:

```sh
beam --version
beam doctor
```

---

## Five-Minute Quickstart

### 1. Preview, filter, and convert files without writing code (`beam pipe`)

```sh
printf 'id,name,age,amount\n1,Ada,36,149.50\n2,Linus,17,25.00\n3,Grace,45,299.00\n' > orders.csv

# Sinkless local preview on stdout (no output file written):
beam pipe orders.csv --where "age >= 21 AND amount >= 100" -n 5

# Materialize clean Parquet using positional <SOURCE> <SINK> (or --from / --to):
beam pipe orders.csv clean/orders.parquet \
  --where "age >= 21 AND amount >= 100" \
  --select "id, lower(name) AS name, round(amount, 2) AS amount" \
  --assert-counts
```

### 2. Inspect schemas, statistics, and sanitized samples (`beam inspect`)

```sh
beam inspect clean/orders.parquet --stats -n 5
```

### 3. Reconcile datasets across formats or engines (`beam diff`)

```sh
beam pipe clean/orders.parquet mirror/orders.jsonl
beam diff clean/orders.parquet mirror/orders.jsonl --key id
```

### 4. Graduate to portable Beam YAML (`--emit-yaml`, `beam check`, `beam run`)

Every `beam pipe` command compiles directly to standard Beam YAML:

```sh
beam pipe orders.csv clean/orders.parquet \
  --where "age >= 21" --emit-yaml > pipeline.yaml

beam check pipeline.yaml
beam run pipeline.yaml
```

---

## Subcommand Reference

| Subcommand | Description | Cold Start |
| :--- | :--- | :--- |
| `beam run <FILE.yaml \| ->` | Execute a Beam YAML pipeline from a file or `stdin` (`-p K=V` Jinja params; `--no-jinja` default on `-`; `--dry-run` validates without running). | Runner |
| `beam pipe <SRC> [<DST>]` | Compile and run a declarative data pipeline (`--from`/`--to`, `--where`, `--select`, `--sql`, `--expect`, `--dlq`, `--max-bytes`, `--check-schema`, `--assert-counts`, `--verify`, `--zero-copy`, `--cdc`, `--emit-yaml`). | `<80 ms` (local) / Runner |
| `beam inspect <URI>` | Sub-second schema, row count, byte estimate, column `--stats`, `--search` column filter, Lakehouse snapshot metadata, and sanitized sample rows. | `<90 ms` (local) |
| `beam tail [-f] [-i] [-n N] <URI>` | Non-destructive bounded stream peek or live follow (`-f`) with optional interactive throughput TUI (`-i`) across Pub/Sub, Kafka, Delta/Iceberg commits, and shards. | `<65 ms` |
| `beam diff <LEFT> <RIGHT>` | In-process (`<= 64 MB`) or distributed $O(1)$-memory keyed (`--key`) / multiset reconciliation with exact 38-digit `Decimal` fidelity & float tolerances (`--rtol`, `--atol`). Exits `0` on match, `6` on drift. | `<90 ms` (local) / Runner |
| `beam schema [diff\|check] <SRC> <DST>` | Zero-compute pre-flight schema compatibility guard (`BACKWARD`, `FORWARD`, `FULL`); detects dropped columns & type narrowing with `CAST(...)` remediation hints. | `<90 ms` |
| `beam catalog sync <SRC> <DST>` | Zero-copy Lakehouse catalog registration (`0` bytes copied) from partitioned Parquet, Delta Lake, or Iceberg metadata into Iceberg REST, BigLake REST, or BigQuery Managed Iceberg. | `<200 ms` |
| `beam package <FILE.yaml \| FILE.py \| ->` | Zero-Docker `<2 s` Dataflow Flex Template packager for Beam YAML (`{{ var }}` Jinja AST extraction) and zero-boilerplate Python AST single-image Flex bundle generator. | `<65 ms` |
| `beam export-schedule <FILE.yaml>` | Compile a parameterized pipeline into a production Cloud Composer (Apache Airflow) DAG or serverless Cloud Scheduler trigger (`--orchestrator composer\|cloud-scheduler`). | `<80 ms` |
| `beam check <FILE.yaml \| ->` | Offline `SafeLoader` schema, recursive `Cli*` security/AST sandbox, and provider trust validator + deterministic `plan_hash` (`--plan` expands full runner graph; `--portable` checks SQL portability). | `<90 ms` |
| `beam io <list \| describe>` | Offline catalog of supported URI schemes, Beam YAML connectors/transforms, required extras, and execution tiers. | `<60 ms` |
| `beam doctor` | Pre-flight diagnostics for Python extras, SQL engines (`sqlite`, `duckdb`), JDK 11–25 compatibility, expansion JAR / Prism caches, and Application Default Credentials. | `<90 ms` |
| `beam wait <JOB_ID>` / `cancel <JOB_ID>` | Poll detached remote jobs (`~/.beam/jobs/<id>.json`), run deferred `--assert-counts` / `--verify` post-checks, or cancel / `--drain` active jobs. | `<90 ms` |
| `beam sql [-e SQL \| -f FILE]` | Launch the interactive Beam Calcite SQL shell or execute batch SQL queries (`--io`, `--list-ios`, `--version`), sharing `~/.beam/cache/`. | JVM |
| `beam config <set\|get\|unset\|list\|use>` | Manage named `PipelineOptions` profiles in `~/.beam/config.yaml` (`0600` permissions; dangerous code-loading keys rejected). | `<60 ms` |
| `beam --llms-txt` | Emit a compact, version-pinned Markdown manual, recipe index, and exit-code contract for AI coding and data agents. | `<50 ms` |

---

## Universal URI Grammar

`beam pipe`, `beam inspect`, `beam diff`, `beam schema`, `beam catalog sync`,
and `beam tail` accept consistent URIs across local files, object stores,
Lakehouse table formats, databases, data warehouses, and event streams:

| Storage / System | URI Syntax | Notes |
| :--- | :--- | :--- |
| **Local / Cloud Files** | `./orders.csv`, `gs://bucket/path/*.parquet`, `s3://...`, `azfs://...`, `hdfs://...` | Auto-infers `csv`, `json`/`jsonl`, `parquet`, `avro`, `txt`; resolves `-00000-of-NNNNN` shards and preserves CSV leading zeros (`00501`). |
| **Delta Lake** | `delta://gs://bucket/tables/orders` or `delta:///local/orders?version=3` | Pure-Python `_delta_log` inspection, time travel (`?version=N`), incremental ranges (`?start_version=A&end_version=B`), `--zero-copy`, and CDC. |
| **Apache Iceberg v2** | `iceberg://ns.orders?warehouse=gs://bucket/wh` | Supports `warehouse` (Hadoop/S3/GCS/local), `catalog=rest`, `catalog=biglake`, and `bigquery.` managed tables, including Merge-on-Read (`equality-deletes`). |
| **BigQuery** | `bigquery://PROJECT.DATASET.TABLE` | `$0` `tables.get` / `dryRun` inspection, Storage Read API, `FILE_LOADS` / `STREAMING_INSERTS`, and Storage Write API `_default` CDC (`--cdc`). |
| **PostgreSQL / MySQL** | `postgres://HOST:5432/DB?table=public.orders`, `mysql://HOST:3306/DB?table=orders` | Inline `user:pass@` credentials rejected (`exit 2`); use `BEAM_POSTGRES_USER` / `BEAM_POSTGRES_PASSWORD` or `0600` `--from-opt-file`. |
| **Cloud Spanner** | `spanner://PROJECT/INSTANCE/DB?table=Orders` or `?change_stream=OrdersStream` | Supports batch reads and Spanner Change Streams (`--cdc`). |
| **Pub/Sub & Kafka** | `pubsub://projects/PROJ/topics/T`, `pubsub://projects/PROJ/subscriptions/S`, `kafka://HOST:9092/TOPIC` | Non-destructive bounded peek & live `tail -f -i` follow + streaming `beam pipe` (with optional Schema Registry parameters). |

---

## Key Capabilities

### Safe `--where` Filtering, `--select` Projections, & `--expect` Rules

`--where`, `--select`, and `--expect` compile tokenized, AST-allowlisted
expressions (blocking arbitrary code execution and exponential AST/CPU DoS
operators) with SQL three-valued `NULL` logic across local and distributed
runners—working safely even when column names collide with reserved words
(`` `class` ``, `` `from` ``, `` `in` ``) or traverse nested structs
(`user.tier`):

- **Boolean, pattern & range operators**: `AND`, `OR`, `NOT`, `=` / `==`,
  `<>` / `!=`, `<`, `<=`, `>`, `>=`, `[NOT] IN (...)`, `[NOT] BETWEEN a AND b`,
  `[NOT] LIKE`, `ILIKE`, `IS NULL`, `IS NOT NULL`, `CAST(col AS INT|FLOAT|STRING|BOOL)`
- **Arithmetic**: `+`, `-`, `*`, `/`, `//`, `%`
- **Scalar & string helpers**: `lower`, `upper`, `strip`/`trim`, `len`/`length`,
  `coalesce`/`ifnull`, `abs`, `round`, `min`, `max`, `int`, `float`, `str`, `bool`
- **UTC timestamp & JSON helpers**: `parse_timestamp` / `to_utc` (normalizes ISO
  offsets and epoch sec/ms/us to UTC `YYYY-MM-DDTHH:MM:SSZ`), `epoch_ms` /
  `to_epoch_ms`, `year`, `month`, `day`, `hour`, `minute`, `second`,
  `date_part`, and `json_get` / `json_extract_scalar` / `json_value`
- **Data quality expectations (`--expect`)**: Enforce named rules such as
  `--expect "positive_amt: amount > 0 -> dlq"` (`dlq`, `drop`, or `fail`) with
  live `beam_cli` monitoring counters.

### Analytical SQL, Multi-Source Joins & Zero-JVM SQL Lowering (`--sql`)

`beam pipe --sql "<QUERY>"` (or `--sql-file query.sql`) unifies fast local
analytics with distributed Beam pipelines:

- **Sub-100ms Local SQL (`sqlite` default & `--sql-engine duckdb`)**: Query and
  `--join` across CSV, Parquet, JSONL, Avro, `delta://`, and `iceberg://` in a
  read-only `:memory:` SQLite sandbox (with built-in `median`, `quantile`,
  `percentile`, `stddev`, `corr`, `regr_slope`, and `to_utc`) or an in-memory
  DuckDB sandbox (`PERCENTILE_CONT`, `QUALIFY`, `GROUPING SETS`), optionally
  writing keyed JSON objects (`--to-opt key=region`).
- **Zero-JVM Pure-Python SQL Lowering (`--sql-engine python`)**: Lowers
  `SELECT ... FROM TABLE(TUMBLE|HOP|SESSION(TABLE src, DESCRIPTOR(ts), INTERVAL '5' MINUTE)) WHERE ... GROUP BY ... HAVING ...`
  (including `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV_POP`,
  `VAR_POP`, `window_start`, `window_end`) directly into pure-Python Beam YAML
  transforms—requiring **no Java runtime or expansion service**.
- **Distributed Beam Calcite SQL & `--portable`**: Exports standard Beam YAML
  `Sql` transforms via `--emit-yaml` or `--runner=DataflowRunner` /
  `FlinkRunner`; pass `--portable` to fail closed pre-flight on local-only SQL
  functions.

### Governance Guardrails: `--check-schema`, `--dlq`, `--max-bytes`, & `--assert-counts`

```sh
beam pipe "gs://my-bucket/raw/orders_*.csv" \
  bigquery://my-proj.analytics.orders_clean \
  --check-schema \
  --where "amount > 0 AND status <> 'CANCELLED'" \
  --dlq "gs://my-bucket/dlq/orders_bad.jsonl" \
  --dlq-redact-fields "customer_email,ssn" \
  --max-bytes 10GB \
  --assert-counts \
  --json
```

- `--check-schema` (and `beam schema diff`) detects dropped required columns,
  type narrowing, and nullability mismatches in `<100 ms` before launching
  workers.
- `--max-bytes` evaluates source byte size pre-flight (`$0` metadata / `dryRun`
  on BigQuery `TABLE`, `VIEW`, and `EXTERNAL` tables; `pg_partition_tree` on
  PostgreSQL; paged listings on object stores) and exits `8` before compute
  burn.
- `--dlq` routes malformed CSV/JSON/CDC source rows, schema-drift records,
  expectation failures, and rejected sink rows into a structured quarantine
  while `--dlq-redact-fields` scrubs PII recursively (or pass `--drop-bad` to
  discard invalid rows without writing a DLQ).
- `--assert-counts` verifies
  `rows_read == rows_written + rows_filtered_out + dlq_rows + stage_dropped_rows`
  and exits `1` if any record is unaccounted for; local `--if-exists replace`
  stages outputs atomically so failed runs never overwrite prior valid files.

### Zero-Copy Lakehouse (`--zero-copy`, `beam catalog sync`) & Cross-Engine CDC (`--cdc`)

- **Zero-Copy Lakehouse Bridges (`--zero-copy` & `beam catalog sync`)**:
  Registers existing partitioned Parquet directories or active Delta Lake
  (`delta://`) files into Apache Iceberg v2 (`format-version: 2`, partition
  specs, monotonic column IDs) or enterprise catalogs (Iceberg REST, BigLake
  REST, BigQuery Managed Iceberg) in `<200 ms` with **`bytes_copied = 0`**:
  ```sh
  beam pipe delta://gs://my-bucket/delta/orders \
    "iceberg://lake.orders?warehouse=gs://my-bucket/iceberg_wh" \
    --zero-copy --verify --key id
  ```
- **Cross-Engine CDC (`--cdc --primary-key <PK>`)**: Compacts CDC logs into
  latest-row Parquet/JSON files in `<250 ms` locally, or applies ordered
  `UPSERT` + `DELETE` mutations to **BigQuery Storage Write API** (`_CHANGE_TYPE`
  + 16-digit hex `_CHANGE_SEQUENCE_NUMBER`, auto-DDL
  `PRIMARY KEY (...) NOT ENFORCED`) and **Apache Iceberg v2 Merge-on-Read**
  (`equality-deletes`):
  ```sh
  beam pipe gs://my-bucket/cdc/debezium_orders.jsonl \
    bigquery://my-proj.analytics.orders_cdc \
    --cdc --primary-key id \
    --dlq gs://my-bucket/dlq/cdc_bad.jsonl \
    --assert-counts
  ```

### Zero-Docker Flex Packaging (`beam package`), Schedule Export, & Live Tail (`beam tail -f`)

- **Zero-Docker YAML & Python AST Flex Templates (`beam package`)**: Extracts
  Jinja2 `{{ var }}` parameters via AST without executing templates, writes a
  ready-to-launch `ContainerSpec` JSON targeting Google's regional
  `Yaml_Template` launcher image in `<2 s` (with airgapped
  `--no-use-public-ips` protection), or introspects custom Python `.py`
  `PipelineOptions` via Python `ast` to generate single-image Flex bundles:
  ```sh
  beam package orders_flex.yaml \
    --to gs://my-bucket/templates/orders_flex.json \
    --project=my-proj --region=us-central1
  ```
- **Enterprise Orchestrator Export (`beam export-schedule`)**: Generates native
  Cloud Composer / Apache Airflow DAGs (`DataflowStartFlexTemplateOperator`) or
  serverless Cloud Scheduler HTTP triggers directly from validated pipelines.
- **Non-Destructive Stream & Commit Tailing (`beam tail -f -i`)**: Streams live
  records as flushed NDJSON (`--json`), interactive TUI throughput cards (`-i`),
  or sanitized terminal lines across Pub/Sub, Kafka (ephemeral consumer group,
  `enable.auto.commit=false`), newly landing shards, and live Delta/Iceberg
  commits.

### Stateful Streaming, Online Enrichment & ML Inference

`beam pipe` provides record-level failure isolation across stateful streaming,
external enrichment, and ML inference (detailed with end-to-end recipes in
[Tasks 7 & 8 of the User Guide](../cli-user-guide/#task-7-deduplicate-enrich-and-run-ml-inference-on-streams)):

- **Deduplication (`--dedupe-by`)**: Event-time or processing-time deduplication
  (`--dedupe-window 15m --dedupe-keep last --dedupe-order seq_no`), optionally
  auditing suppressed duplicates via `--dedupe-dlq`.
- **Online Enrichment (`--enrich`)**: Batched worker-side lookups across
  parameterized BigQuery (`UNNEST(@keys)`), Cloud Bigtable, Vertex AI Feature
  Store, Feast, and `file:` with optional TLS `rediss://` read-through caching
  (`BEAM_REDIS_PASSWORD`), `AdaptiveThrottler`, and `on_miss=null|drop|dlq|fail`.
- **ML Inference (`--infer`)**: `RunInference` integration for Vertex AI
  Endpoints (`<1.4 MB` payload auto-split + `rate_limit`) and commit-pinned
  HuggingFace `safetensors` models (`trust_remote_code=False`) with in-worker
  singleton bisection so 1 malformed record routes to `--dlq` without dropping
  healthy batch peers or reloading model weights.
- **Temporal Joins, Bounded CEP & Window Triggers**: Point-in-time `ASOF JOIN`
  (`--asof-join` or SQL `ASOF LEFT JOIN`), event-time `--interval-join` with
  `--max-watermark-skew` OOM circuit breakers, bounded sequence/absence matching
  (`--pattern`), and speculative/late window triggers with droppably-late
  quarantine (`--trigger` + `--late-data-dlq` + `--emit-pane-info`).

---

## Deterministic Agent Mode (`--json`, `untrusted_data`, & Exit Codes)

Passing `--json` (or `--output=json`) switches any `beam` command into a strict
machine-readable contract (`schema_version: "1.0"`):

1. **OS File-Descriptor Isolation**: Redirects OS `stdout` (`fd 1`) to `stderr`
   (`fd 2`) through process exit so C++, gRPC, and JVM logs never corrupt JSON
   output on `stdout`.
2. **Prompt-Injection Quarantine (`untrusted_data`)**: Dataset rows returned by
   `inspect`, `tail`, `diff`, and sinkless `pipe` are isolated inside
   `untrusted_data.rows` with ANSI/OSC terminal escapes, control characters,
   Unicode BiDi overrides, and Unicode Tag characters (`U+E0000..U+E007F`)
   stripped.
3. **Timeout Continuity (`--wait-timeout` -> `beam wait`)**: On remote runners,
   if `--wait-timeout` expires while a job is still running, `beam` detaches
   cleanly (exit `7`, `"detached": true`), persists `~/.beam/jobs/<job_id>.json`
   (including a validation-aware 30-minute idempotency ledger that prevents
   duplicate `--if-exists=append` resubmissions), and resumes via
   `beam wait <job_id> --json`.
4. **Right-Sized Routing**: Keep `<100 MB` local file inspection, filtering,
   SQL, and reconciliation on the default local fast path / local runner; pass
   `--runner=DataflowRunner`, `FlinkRunner`, or `SparkRunner` for
   multi-gigabyte, cross-network, or unbounded streaming workloads.

### Exit Code Contract

| Exit Code | Constant | Meaning |
| :--- | :--- | :--- |
| `0` | `EXIT_OK` | Command succeeded (`beam diff` datasets matched; pipeline completed). |
| `1` | `EXIT_PIPELINE_FAILED` | Pipeline execution failed or `--assert-counts` row conservation failed. |
| `2` | `EXIT_USAGE` | CLI usage, YAML/schema contract, AST expression, existing-output, or inline-secret error. |
| `3` | `EXIT_CAPABILITY` | Requested feature or flag combination is unsupported on the selected runner. |
| `4` | `EXIT_MISSING_PACKAGE` | Missing optional Python extra (`[yaml]`, `[gcp]`, `pyarrow`, `duckdb`) or Java 11–25 runtime (`next_actions` gives exact install command). |
| `6` | `EXIT_DIFF_MISMATCH` | `beam diff` or `beam pipe --verify` found schema, key, or value drift. |
| `7` | `EXIT_TIMEOUT` | `--wait-timeout` or `beam wait --timeout` expired (`detached: true` for active remote jobs). |
| `8` | `EXIT_COST_LIMIT` | Pre-flight `--max-bytes` scan guardrail exceeded (`CostLimitExceededError`). |
| `130` | `EXIT_SIGINT` | Interrupted by `SIGINT` (`Ctrl-C`); local pipelines stop cleanly. |
