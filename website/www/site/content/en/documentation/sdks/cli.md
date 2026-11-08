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
fast (`<90 ms` startup) command-line interface for running [Beam YAML](../yaml/)
pipelines, moving and transforming datasets without writing boilerplate code,
querying data with [Beam SQL](/documentation/dsls/sql/overview/), reconciling
migrations across storage engines, inspecting schemas and live streams, and
packaging pipelines for production runners—designed equally for interactive
human developers and autonomous AI coding/data agents.

## Prerequisites & Installation

Install the Apache Beam Python SDK with the `[yaml]` extra inside a Python 3.10+
virtual environment:

```sh
python3 -m venv .venv-beam
source .venv-beam/bin/activate
pip install 'apache-beam[yaml]'
```

For Google Cloud connectors (`gs://`, `bigquery://`, `pubsub://`, `spanner://`,
and `DataflowRunner`), include the `[gcp]` extra:

```sh
pip install 'apache-beam[yaml,gcp]'
```

Verify your installation, optional extras, Java runtime (required only for
Beam SQL and Java-backed cross-language connectors), and cloud credentials:

```sh
beam --version
beam doctor
```

---

## Five-Minute Quickstart

### 1. Move & filter files without writing a pipeline file (`beam pipe`)

```sh
printf 'id,name,age,amount\n1,Ada,36,149.50\n2,Linus,17,25.00\n3,Grace,45,299.00\n' > orders.csv

beam pipe --from orders.csv --to clean/orders.parquet \
  --where "age >= 21 AND amount >= 100" \
  --select "id, lower(name) AS name, round(amount, 2) AS amount"
```

### 2. Inspect schemas, statistics, and sanitized samples (`beam inspect`)

```sh
beam inspect clean/orders.parquet --stats --sample 5
```

### 3. Reconcile datasets across formats or engines (`beam diff`)

```sh
beam pipe --from clean/orders.parquet --to mirror/orders.json
beam diff clean/orders.parquet mirror/orders.json --key id
```

### 4. Graduate to portable Beam YAML (`--emit-yaml` & `beam run`)

Every `beam pipe` command compiles directly to standard Beam YAML:

```sh
beam pipe --from orders.csv --to clean/orders.parquet \
  --where "age >= 21" --emit-yaml > pipeline.yaml

beam check pipeline.yaml
beam run pipeline.yaml
```

---

## Subcommand Reference

| Subcommand | Description | Cold Start |
| :--- | :--- | :--- |
| `beam run <FILE.yaml \| ->` | Execute a Beam YAML pipeline from a file or `stdin` (`--no-jinja` default on `-`; `--dry-run` validates without running). | Runner |
| `beam pipe --from <URI> --to <URI>` | Compile and run a declarative data pipeline (`--where`, `--select`, `--dlq`, `--max-bytes`, `--assert-counts`, `--verify`, `--zero-copy`, `--cdc`, `--emit-yaml`). | Local / Runner |
| `beam inspect <URI>` | Sub-second schema, row count, byte estimate, column `--stats`, `--search` column filter, Lakehouse snapshot metadata, and sanitized sample rows. | `<90 ms` (local) |
| `beam tail [-f] [-n N] <URI>` | Non-destructive bounded stream peek or Unix-style live follow (`-f` / `--follow`) across Pub/Sub, Kafka, Delta/Iceberg commits, and local/cloud shards. | `<65 ms` |
| `beam diff <LEFT> <RIGHT>` | Distributed $O(1)$-memory keyed (`--key`) or multiset reconciliation with float tolerances (`--rtol`, `--atol`). Exits `0` on match, `6` on mismatch. | Runner |
| `beam package <FILE \| ->` | Zero-Docker `<2 s` Dataflow Flex Template packager for Beam YAML (`{{ var }}` Jinja AST extraction) and zero-boilerplate Python AST Flex bundle generator. | `<65 ms` |
| `beam check <FILE.yaml \| ->` | Offline `SafeLoader` schema & provider trust validator + deterministic `plan_hash` (`--plan` expands full runner graph). | `<80 ms` |
| `beam io <list \| describe>` | Offline catalog of supported URI schemes, Beam YAML connectors, required extras, and execution tiers. | `<60 ms` |
| `beam doctor` | Pre-flight diagnostics for Python extras, JDK 11–25 compatibility, expansion JAR / Prism caches, and Application Default Credentials. | `<90 ms` |
| `beam wait <JOB_ID>` / `cancel <JOB_ID>` | Poll detached remote jobs (`~/.beam/jobs/<id>.json`), run deferred `--assert-counts` / `--verify` post-checks, or cancel / `--drain` active jobs. | `<90 ms` |
| `beam sql [-e SQL \| -f FILE]` | Launch the interactive Beam Calcite SQL shell or execute batch SQL queries (`--io`, `--list-ios`, `--version`), sharing `~/.beam/cache/`. | JVM |
| `beam config <set\|get\|unset\|list\|use>` | Manage named `PipelineOptions` profiles in `~/.beam/config.yaml` (`0600` permissions; announced on `stderr` when applied). | `<60 ms` |
| `beam --llms-txt` | Emit a compact, version-pinned Markdown manual and exit-code contract for AI coding and data agents. | `<50 ms` |

---

## Universal URI Grammar

`beam pipe`, `beam inspect`, `beam diff`, and `beam tail` accept consistent URIs
across local files, object stores, Lakehouse table formats, databases, data
warehouses, and event streams:

| Storage / System | URI Syntax | Notes |
| :--- | :--- | :--- |
| **Local / Cloud Files** | `./orders.csv`, `gs://bucket/path/*.parquet`, `s3://...`, `azfs://...`, `hdfs://...` | Auto-infers `csv`, `json`/`jsonl`, `parquet`, `avro`; resolves `-00000-of-NNNNN` shards automatically. |
| **Delta Lake** | `delta://gs://bucket/tables/orders` or `delta:///local/orders?version=3` | Pure-Python `_delta_log` transaction log inspection, reading, time travel (`?version=N`), `--zero-copy`, and Change Data Feed. |
| **Apache Iceberg v2** | `iceberg://ns.orders?warehouse=gs://bucket/wh` | Supports `warehouse` (Hadoop/GCS/local), `catalog=biglake`, and `catalog=rest`, including Merge-on-Read (`equality-deletes`). |
| **BigQuery** | `bigquery://PROJECT.DATASET.TABLE` | `$0` `tables.get` / `dryRun` inspection, Storage Read API, `FILE_LOADS` / `STREAMING_INSERTS`, and Storage Write API `_default` CDC (`--cdc`). |
| **PostgreSQL / MySQL** | `postgres://HOST:5432/DB?table=public.orders`, `mysql://HOST:3306/DB?table=orders` | Inline `user:pass@` credentials rejected (`exit 2`); use `BEAM_POSTGRES_USER` / `BEAM_POSTGRES_PASSWORD` or `0600` `--from-opt-file`. |
| **Cloud Spanner** | `spanner://PROJECT/INSTANCE/DB?table=Orders` or `?change_stream=OrdersStream` | Supports batch reads and Spanner Change Streams (`--cdc`). |
| **Pub/Sub & Kafka** | `pubsub://projects/PROJ/topics/T`, `pubsub://projects/PROJ/subscriptions/S`, `kafka://HOST:9092/TOPIC` | Non-destructive bounded peek & live `tail -f` follow + streaming `beam pipe`. |

---

## Key Capabilities

### Safe `--where` Filtering & `--select` Projections

`--where` and `--select` compile tokenized, AST-allowlisted expressions (blocking
arbitrary code execution and exponential AST/CPU DoS operators) into Beam YAML
`Filter` and `MapToFields` callables that work safely even when column names
collide with Python keywords (`in`, `from`, `is`, `class`):

- **Boolean & comparison operators**: `AND`, `OR`, `NOT`, `==`, `!=`, `<`, `<=`,
  `>`, `>=`, `IN (...)`, `IS NULL`, `IS NOT NULL`
- **Arithmetic**: `+`, `-`, `*`, `/`
- **Scalar functions**: `lower`, `upper`, `strip`, `len`, `coalesce`, `abs`,
  `round`, `int`, `float`, `str`, `bool`

### Dead-Letter Quarantine (`--dlq`), Scan Caps (`--max-bytes`), & Row Conservation (`--assert-counts`)

```sh
beam pipe \
  --from "gs://my-bucket/raw/orders_*.csv" \
  --to bigquery://my-proj.analytics.orders_clean \
  --where "amount > 0 AND status != 'CANCELLED'" \
  --dlq "gs://my-bucket/dlq/orders_bad.json" \
  --max-bytes 10GB \
  --assert-counts \
  --json
```

- `--max-bytes` evaluates source byte size pre-flight (`$0` metadata / `dryRun`
  on BigQuery `TABLE`, `VIEW`, and `EXTERNAL` tables; `pg_partition_tree` on
  PostgreSQL; lazy paged listings on object stores) and fails closed with exit
  code `8` before launching compute.
- `--dlq` routes malformed CSV/JSON/CDC source rows and rejected sink rows into
  a flat primitive schema (`step`, `error_type`, `error`, `raw_payload`).
- `--assert-counts` verifies `rows_read == rows_written + rows_filtered_out + dlq_rows`
  and exits `1` if any record is unaccounted for.

### Zero-Copy Lakehouse Registration (`--zero-copy`) & Cross-Engine CDC (`--cdc`)

- **Zero-Copy Lakehouse Bridges (`--zero-copy`)**: Registers existing Parquet
  shards or active Delta Lake (`delta://`) Parquet files into an Apache Iceberg
  v2 table (`format-version: 2`, `schema.name-mapping.default`) in `<150 ms`
  with **`bytes_copied = 0`**:
  ```sh
  beam pipe \
    --from delta://gs://my-bucket/delta/orders \
    --to "iceberg://lake.orders?warehouse=gs://my-bucket/iceberg_wh" \
    --zero-copy --verify --verify-key id
  ```
- **Cross-Engine CDC (`--cdc --primary-key <PK>`)**: Normalizes Debezium
  envelopes (`before`/`after`/`op`/`source.lsn`), Spanner Change Streams, Delta
  Change Data Feed, Iceberg incremental snapshots, or flat CDC JSONL streams
  into ordered `UPSERT` + `DELETE` mutations on **BigQuery Storage Write API**
  (`_CHANGE_TYPE` + 16-digit hex `_CHANGE_SEQUENCE_NUMBER`, auto-DDL
  `PRIMARY KEY (...) NOT ENFORCED`) or **Apache Iceberg v2 Merge-on-Read**
  (`equality-deletes`):
  ```sh
  beam pipe \
    --from gs://my-bucket/cdc/debezium_orders.jsonl \
    --to bigquery://my-proj.analytics.orders_cdc \
    --cdc --primary-key id \
    --dlq gs://my-bucket/dlq/cdc_bad.json \
    --assert-counts
  ```

### Zero-Docker Flex Template Packaging (`beam package`) & Live Streaming (`beam tail -f`)

- **Zero-Docker YAML Flex Templates**: Extracts Jinja2 `{{ var }}` and
  `{{ var | default(...) }}` parameters via AST without executing templates,
  writes a ready-to-launch `ContainerSpec` JSON targeting Google's regional
  `Yaml_Template` launcher image in `<2 s` (with automatic `requirements_file: null`
  protection for airgapped `--no-use-public-ips` VPCs), and emits ready-to-run
  `gcloud dataflow flex-template run` and Apache Airflow
  (`DataflowStartFlexTemplateOperator`) snippets:
  ```sh
  beam package orders_flex.yaml \
    --to gs://my-bucket/templates/orders_flex.json \
    --project=my-proj --region=us-central1
  ```
  For custom Python `.py` pipelines, `beam package pipeline.py --output-dir ./bundle --image <AR_IMAGE>`
  introspects `PipelineOptions.add_argument` via Python `ast` and generates the
  single-image `Dockerfile` and `metadata.json` automatically.
- **Live Stream & Commit Follow (`beam tail -f`)**: Streams live records as
  flushed NDJSON (`--json`) or formatted lines across Pub/Sub topics (via
  auto-cleaned ephemeral subscriptions), shared subscriptions (never ACKing
  messages), Kafka topics, newly landing GCS/local shards, and live Delta Lake
  or Apache Iceberg snapshot commits.

---

## Deterministic Agent Mode (`--json`, `untrusted_data`, & Exit Codes)

Passing `--json` (or `--output=json`) switches any `beam` command into a strict
machine-readable contract (`schema_version: "1.0"`):

1. **OS File-Descriptor Isolation**: Redirects OS `stdout` (`fd 1`) to `stderr`
   (`fd 2`) through process exit so C++, gRPC, and JVM logs never corrupt JSON
   output on `stdout`.
2. **Prompt-Injection Quarantine (`untrusted_data`)**: Dataset rows returned by
   `inspect`, `tail`, and `diff` are isolated inside `untrusted_data.rows` with
   ANSI/OSC terminal escapes, control characters, Unicode BiDi overrides, and
   Unicode Tag characters (`U+E0000..U+E007F`) stripped.
3. **Timeout Continuity (`--wait-timeout` -> `beam wait`)**: On remote runners,
   if `--wait-timeout` expires while a job is still running, `beam` detaches
   cleanly (exit `7`, `"detached": true`), persists `~/.beam/jobs/<job_id>.json`
   (including a 30-minute idempotency ledger that prevents duplicate
   `--if-exists=append` resubmissions), and resumes via `beam wait <job_id> --json`.
4. **Right-Sized Routing**: Keep `<100 MB` local file inspection, filtering, and
   reconciliation on the default local runner (`DirectRunner` / `PrismRunner`);
   pass `--runner=DataflowRunner`, `FlinkRunner`, or `SparkRunner` for
   multi-gigabyte, cross-network, or unbounded streaming workloads.

### Exit Code Contract

| Exit Code | Constant | Meaning |
| :--- | :--- | :--- |
| `0` | `EXIT_OK` | Command succeeded (`beam diff` datasets matched; pipeline completed). |
| `1` | `EXIT_PIPELINE_FAILED` | Pipeline execution failed or `--assert-counts` row conservation failed. |
| `2` | `EXIT_USAGE` | CLI usage, YAML schema, AST expression, existing-output, or inline-secret error. |
| `3` | `EXIT_CAPABILITY` | Requested feature or flag combination is unsupported on the selected runner. |
| `4` | `EXIT_MISSING_PACKAGE` | Missing optional Python extra (`[yaml]`, `[gcp]`, `pyarrow`) or Java 11–25 runtime (`next_actions` gives exact install command). |
| `6` | `EXIT_DIFF_MISMATCH` | `beam diff` or `beam pipe --verify` found schema, key, or value drift. |
| `7` | `EXIT_TIMEOUT` | `--wait-timeout` or `beam wait --timeout` expired (`detached: true` for active remote jobs). |
| `8` | `EXIT_COST_LIMIT` | Pre-flight `--max-bytes` scan guardrail exceeded (`CostLimitExceededError`). |
| `130` | `EXIT_SIGINT` | Interrupted by `SIGINT` (`Ctrl-C`); local pipelines stop cleanly. |
