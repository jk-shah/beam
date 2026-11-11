# Apache Beam CLI Preview (`beam` & `apache-beam`)

> **Branch**: [`jk-shah/beam@beam-cli-preview`](https://github.com/jk-shah/beam/tree/beam-cli-preview/sdks/python/cli-preview) · **Version**: `0.2.0` · **Upstream Tracking Issue**: [apache/beam#40891](https://github.com/apache/beam/issues/40891)
>
> Installs a `<90 ms` cold-start `beam` (and `apache-beam`) CLI binary on top of released PyPI `apache-beam>=2.71.0` wheels—**no repo clone, no C++ compiler, no Go toolchain, and no Gradle build required**.

---

## 1. Try It in 10 Seconds (Fresh Venv, Zero Repo Clone)

Install regular released `apache-beam` from PyPI **first**, then install the `cli-preview` overlay on top of it (this resolves prebuilt PyPI binary wheels first and ensures the preview `beam` / `apache-beam` entrypoints take precedence):

### Option A: `uv` (Recommended — ~3–5 seconds)
```bash
# 1. Create & activate a clean Python 3.10+ virtual environment
uv venv .venv-beam-cli && source .venv-beam-cli/bin/activate

# 2. Install regular released Apache Beam + format extras from PyPI first
uv pip install "apache-beam[yaml,gcp]>=2.71.0" pyarrow fastavro

# 3. Install (or upgrade) the CLI preview overlay from GitHub on top of Apache Beam
uv pip install --upgrade "git+https://github.com/jk-shah/beam.git@beam-cli-preview#subdirectory=sdks/python/cli-preview"
```

*(Local-only without GCP connectors: drop `,gcp` in step 2 -> `uv pip install "apache-beam[yaml]>=2.71.0" pyarrow fastavro`; optional ANSI SQL engine for `--sql-engine duckdb`: also install `duckdb`)*

### Option B: Standard `pip`
```bash
# 1. Create & activate a clean Python 3.10+ virtual environment
python3 -m venv .venv-beam-cli && source .venv-beam-cli/bin/activate
pip install --upgrade pip

# 2. Install regular released Apache Beam + format extras from PyPI first
pip install "apache-beam[yaml,gcp]>=2.71.0" pyarrow fastavro

# 3. Install (or upgrade) the CLI preview overlay from GitHub on top of Apache Beam
pip install --upgrade --force-reinstall --no-deps "git+https://github.com/jk-shah/beam.git@beam-cli-preview#subdirectory=sdks/python/cli-preview"
```

### Run the 12-Step Interactive Showcase Immediately (`~8s`)
```bash
beam-demo
# (or: python -m apache_beam_cli_preview.demo)
```

Or explore interactively:
```bash
beam --version --json
beam doctor --json
beam io list --json
beam --llms-txt | head -n 45
```

---

## 2. Test It with Your AI Coding Agent (Claude Code / Gemini / Cursor / Codex)

Give your agent either:
1. **Zero-Config Self-Discovery**: Tell the agent:
   > *"Use `/path/to/.venv-beam-cli/bin/beam` (run `beam --llms-txt` first to learn its contract, exit codes, and URI schemes) to inspect, clean, and reconcile `<dataset>`."*
2. **Agent Skill (`SKILL.md`)**: Copy [`.agents/skills/apache-beam-cli/SKILL.md`](../../../.agents/skills/apache-beam-cli/SKILL.md) into your workspace's `.agents/skills/apache-beam-cli/SKILL.md`.

---

## 3. Complete Command & Connector Surface

| Command | Purpose | Cold Start |
| :--- | :--- | :--- |
| `beam --llms-txt` | Self-documenting Markdown manual + fast recipe index + exit codes + URI syntax for AI agents | `<60 ms` |
| `beam doctor` | Checks Python extras (`yaml`, `pyarrow`, `duckdb`, `deltalake`, `pyiceberg`), `PATH` binary collisions, Java runtime, and GCP ADC | `<90 ms` |
| `beam io list` / `describe` | Lists & describes Universal URI connectors (`csv`, `json`, `text`, `parquet`, `avro`, `delta`, `iceberg`, `bigquery`, `postgres`, `mysql`, `spanner`, `pubsub`, `kafka`) | `<60 ms` |
| `beam inspect <URI>` | Schema, size, row count, column `--stats`, `--search`, `delta://`/`iceberg://` snapshots, and sanitized samples | `<90 ms` (local) |
| `beam check <FILE\|->` | Fast static YAML, sandboxed `--jinja-variables`, and provider trust check (`--plan` for full Beam graph expansion) | `<80 ms` (static) |
| `beam pipe --from <A> [--to <B>]` | Zero-code ETL, `--where`, `--select`, `--sql` (repeatable `--from`), `--portable`, `--preview`, `--dlq`, `--max-bytes`, `--assert-counts`, `--verify`, `--zero-copy`, and `--cdc` | `<80 ms` (local) or Cloud |
| `beam diff <LEFT> <RIGHT>` | Distributed $O(1)$-memory key/hash reconciliation (`--columns shared\|all\|c1,c2`, `--to FILE`, `--exit-zero`, `EXIT_DIFF_MISMATCH = 6`) | Local or Cloud |
| `beam tail [-f] <URI>` | Bounded stream peek OR live `tail -f` follow (`pubsub://`, `kafka://`, `delta://`, `iceberg://`, `gs://`/local shards) | `<65 ms` |
| `beam package <FILE\|->` | Zero-Docker YAML Flex Templates (`<2 s`) & zero-boilerplate Python AST Flex bundles | `<65 ms` |
| `beam run <FILE\|->` | Runs Beam YAML pipelines (`--no-jinja` default on stdin; sandboxed Jinja2 rendering) | Local or Cloud |
| `beam wait <JOB_ID>` | Polls detached remote jobs via cached REST v1b3 with Cloud Console URLs and deferred post-checks | `<90 ms` |
| `beam cancel <JOB_ID>` | Cancels or `--drain`s running remote pipelines | `<90 ms` |
| `beam sql` | Beam Calcite SQL shell & one-shot queries (`-e` / `-f`, `--io delta,iceberg,...`) | JVM |
| `beam config` | Persistent `PipelineOptions` profiles (`~/.beam/config.yaml`) | `<60 ms` |

---

## 4. Agentic Safety, Schema & Determinism Guarantees

1. **Pure JSON Envelope (`--json` / `--output=json`)**:
   - Permanently redirects OS file descriptor `stdout` (`fd 1`) to `stderr` (`fd 2`) before importing `apache_beam`, keeping C++/gRPC/JVM logs out of machine-parsed JSON stdout.
   - Every response includes `schema_version: "1.0"`, `ok`, `exit_code`, `duration_ms`, `metrics`, `outputs`, `warnings`, `error`, and `next_actions` (and `beam tail -f --json` emits flushed NDJSON `{"event":"record", ...}` lines followed by the final summary envelope).
2. **Atomic `--if-exists replace` (Never Loses Previous Output on Failure)**:
   - Default `--if-exists=fail` refuses to overwrite existing files or non-empty BigQuery tables.
   - When `--if-exists=replace` is passed across local fast-path, `--sql`, or Beam runner paths, outputs are staged and committed atomically **only after** execution, `--dlq` writes, `--assert-counts`, and `--verify` all succeed—a failed run always leaves the previous output intact on disk.
3. **Zero Silent Schema or Leading-Zero Loss**:
   - **JSONL Schema-Drift Protection**: Local JSONL inputs `<= 64 MB` are automatically scanned end-to-end so sparse fields appearing after row 100 are included automatically. On larger or remote JSONL inputs, any unseen field appearing after schema sampling triggers `SchemaDriftError` (or quarantines the full record to `--dlq` with an explicit warning in `warnings`—never silently dropping fields). Override anytime with `--from-opt sample_rows=all|<N>`, `--from-opt fields="col:type,..."`, or `--from-opt on_unknown_fields=ignore`.
   - **Leading-Zero & Explicit Column Typing**: Local CSVs automatically scan all rows so late leading-zero strings (`00123` at row 120) stay `str` instead of silently truncating to `int`; declare explicit types anytime via `--from-opt fields="zip:str,amount:float"` and custom null sentinels via `--null-values "NA,N/A,-"`.
4. **Prompt-Injection Quarantine (`untrusted_data`) & Cost Guardrails**:
   - Sampled rows from `inspect`, `tail`, `--sql` stdout (or `--preview` when `--to` is passed), and `diff` live strictly inside `untrusted_data.rows` with ANSI/OSC escapes, C0/C1 control codes, Unicode BiDi overrides, and Unicode Tag characters stripped.
   - `--max-bytes 10GB` fails closed with exit code `8` (`EXIT_COST_LIMIT`) *before* launching any job (`$0` `tables.get` metadata fast path + `$0` `dryRun` fallback for BigQuery `VIEW`s and `EXTERNAL` tables).
5. **Row Conservation, Runner Graduation (`--portable`) & Distributed Verification**:
   - `--assert-counts` proves `rows_read == rows_written + rows_filtered_out + dlq_rows + cdc_superseded_rows`.
   - `--portable` (`--strict-beam`) and `outputs.portable_to_runner` verify before cloud deployment that local SQL queries and options graduate cleanly to Beam Calcite SQL on Dataflow/Flink/Spark.
   - `beam diff` reconciles arbitrarily large sharded CSV, JSON, Parquet, Avro, Delta Lake, Apache Iceberg v2, and BigQuery datasets in $O(1)$ worker memory (`--columns shared|all|col1,col2`, `--rtol`/`--atol`, `--to report.json`, `--exit-zero`).

---

## 5. Local Analytical SQL (`--sql`), Repeatable `--from` Joins & Keyed JSON

Run single-table aggregations, window functions, CTEs, or multi-format joins (`CSV` + `Parquet` + `JSONL` + `Avro`) in `<80 ms` locally via sandboxed `:memory:` SQLite (built-in default) or `--sql-engine duckdb` (optional `duckdb` extra for full ANSI `FILTER (WHERE ...)`, `QUALIFY`, `GROUPING SETS`), with built-in `median(x)`, `quantile(x, p)`, `regr_slope(y, x)`, `corr(x, y)`, and `to_utc(ts)` ISO-8601 UTC normalization:

```bash
# Multi-source join (repeatable --from; --join also works) + quantiles + UTC normalization -> keyed JSON object:
beam pipe \
  --from orders=data/orders_*.parquet \
  --from fx=data/fx_rates.csv \
  --sql "SELECT o.region, COUNT(*) AS n, ROUND(SUM(o.amount * fx.rate), 2) AS usd_total, quantile(o.amount, 0.95) AS p95, MIN(to_utc(o.created_at)) AS first_utc FROM orders o JOIN fx ON o.currency = fx.currency GROUP BY o.region" \
  --to out/by_region.json \
  --to-opt key=region \
  --json
```

---

## 6. Running on Google Cloud Dataflow (`DataflowRunner`)

`apache-beam-cli-preview` automatically synthesizes and stages its pure-Python worker wheel (`<5 ms`) + `--beam_plugin` runtime hooks onto remote Dataflow SDK workers whenever `--runner=DataflowRunner` is passed (automatically skipped when a custom `--sdk_container_image` or `--no-stage-cli-wheel` / `BEAM_NO_STAGE_CLI_WHEEL=1` is supplied):

```bash
beam pipe \
  --from "gs://<BUCKET>/raw/*.csv" \
  --to "bigquery://<PROJECT>.<DATASET>.<TABLE>" \
  --where "country == 'US' and amount >= 100" \
  --select "id,country,amount" \
  --dlq "gs://<BUCKET>/dlq/bad.json" \
  --assert-counts \
  --wait-timeout 10s \
  --runner=DataflowRunner \
  --project=<PROJECT> \
  --region=us-central1 \
  --temp_location="gs://<BUCKET>/tmp" \
  --json

# Poll detached job (zero-fork REST v1b3 poller with Cloud Console URL & stderr progress):
beam wait <JOB_ID> --json
```

*(Enterprise GCP org tip: if your VPC uses custom subnet mode or blocks external worker IPs, append `--subnetwork=regions/<REGION>/subnetworks/default --no_use_public_ips`.)*

---

## 7. Simplified Flex Templates (`beam package`) & Live Streaming (`beam tail -f`)

### Zero-Docker YAML Flex Templates (`<2s`, No `Dockerfile` / `metadata.json` / Cloud Build)
`beam package` inspects Jinja2 `{{ var }}` and `{{ var | default(...) }}` parameters via AST (zero template execution), resolves Google's regional `Yaml_Template` launcher image, writes the ready-to-launch `ContainerSpec` directly to GCS, and outputs ready-to-paste `gcloud dataflow flex-template run` + Cloud Composer (`DataflowStartFlexTemplateOperator`) snippets:

```bash
beam package orders_flex.yaml \
  --to "gs://<BUCKET>/templates/orders_flex.json" \
  --project=<PROJECT> --region=us-central1 \
  --subnetwork=regions/us-central1/subnetworks/default --no-use-public-ips \
  --temp-location="gs://<BUCKET>/tmp" \
  --json
```

For custom Python pipelines (`.py`) or YAML pipelines with custom Python providers, `beam package pipeline.py --output-dir ./flex_bundle --image <AR_IMAGE> [--build-image] --json` extracts `PipelineOptions.add_argument` / `add_value_provider_argument` via Python `ast` and generates the single-image `Dockerfile` + `metadata.json` + `ContainerSpec` automatically.

### Linux-Style Live Stream & Shard Follow (`beam tail -f`)
```bash
# Follow live Pub/Sub topic events (creates & auto-deletes an isolated ephemeral subscription):
beam tail -f -n 20 pubsub://projects/<PROJECT>/topics/<TOPIC> --json

# Follow a shared subscription non-destructively (never ACKs; deduplicates message_ids):
beam tail -f --duration 30s pubsub://projects/<PROJECT>/subscriptions/<SUB>

# Watch newly landing GCS / local DLQ or output shards in real time:
beam tail -f "gs://<BUCKET>/dlq/*.json" --json

# Follow live Delta Lake or Apache Iceberg snapshot commits in real time:
beam tail -f -n 10 "iceberg://lake.orders?warehouse=gs://<BUCKET>/wh" --json
```

---

## 8. Cross-Engine CDC (`--cdc`) & Zero-Copy Lakehouse Bridges (`--zero-copy`)

### Zero-Copy Lakehouse Registration (`delta://` / Parquet -> `iceberg://`)
Register existing Parquet shards or active Delta Lake Parquet files into an Apache Iceberg v2 table (`format-version: 2`, `schema.name-mapping.default` for field-ID-free Parquet compatibility with BigQuery/BigLake/Spark/Trino) in `<150 ms` locally or on GCS with **`bytes_copied = 0`**:

```bash
# Inspect a Delta Lake table (with optional ?version=N time travel) with zero JVM startup:
beam inspect "delta://gs://<BUCKET>/delta/orders" --json

# Zero-copy register Delta Lake active files into Apache Iceberg v2 + reconcile via --verify:
beam pipe \
  --from "delta://gs://<BUCKET>/delta/orders" \
  --to "iceberg://lake.orders?warehouse=gs://<BUCKET>/iceberg_wh" \
  --zero-copy \
  --verify --verify-key id \
  --project=<PROJECT> \
  --json
```

### Cross-Engine Change Data Capture (`--cdc --primary-key <COL>`)
Normalize Debezium (`before`/`after`/`op`/`source.lsn`), Spanner Change Streams, Delta Change Data Feed (`_change_type`/`_commit_version`), Iceberg incremental snapshots, or flat CDC streams (`--cdc-op-col`, `--cdc-seq-col`) into idempotent ordered **`UPSERT` + `DELETE`** mutations across:
- **Plain File Sinks (`.parquet`, `.jsonl`, `.csv`, `.avro`)**: Materializes the latest live row per primary key (dropping deleted keys and collapsing exact duplicate change events) in `<80 ms` locally or distributed on Beam runners.
- **`bigquery://`**: BigQuery Storage Write API `_default` stream (`_CHANGE_TYPE` + 16-digit `/`-segmented hex `_CHANGE_SEQUENCE_NUMBER`, auto-DDL `PRIMARY KEY (...) NOT ENFORCED`, and row-level `--dlq` routing).
- **`iceberg://`**: Apache Iceberg v2 Merge-on-Read (`equality_ids` delete files + `identifier-field-ids` + `PARQUET:field_id`).

```bash
# Materialize CDC changelog directly to a clean Parquet snapshot in <80ms locally:
beam pipe \
  --from "cdc/orders_changelog.jsonl" \
  --to "clean/orders_live.parquet" \
  --cdc --primary-key id --cdc-seq-col updated_at \
  --assert-counts --json

# Replicate Debezium / Pub/Sub / Delta CDC into BigQuery Storage Write API CDC with DLQ & conservation:
beam pipe \
  --from "gs://<BUCKET>/cdc/debezium_orders.jsonl" \
  --to "bigquery://<PROJECT>.<DATASET>.orders_cdc" \
  --cdc --primary-key id \
  --dlq "gs://<BUCKET>/dlq/orders_cdc_dlq.json" \
  --assert-counts \
  --project=<PROJECT> \
  --json
```

### Zero-Gap Full Sync + Incremental CDC Runbooks (Never Miss Intermediate Data)

When writers are actively committing to a source table during your initial backfill, reading an **unpinned** snapshot and starting CDC "from latest" afterwards drops every row committed during the backfill window. Use one of the two zero-gap handoff patterns below depending on your source engine and Apache Beam version:

#### Pattern A: Delta Lake -> Apache Iceberg (Version-Pinned Snapshot + Bounded CDF Windows)
> **Core Beam compatibility**: Native Java Managed I/O (`ReadFromDelta`, `ReadFromDeltaCDC`, and `WriteToIceberg(mode="merge-on-read")`) lands in **Apache Beam 2.77+** (exported via `--emit-yaml`). On released PyPI Beam `2.71–2.76`, `apache-beam-cli-preview` executes the exact same URI workflow via its built-in Python Iceberg v2 / Delta bridge (`deltalake` extra required only if Delta v3+ Deletion Vectors or Column Mapping are enabled).
>
> **Prerequisite on Delta writer**: Enable Change Data Feed **before** pinning `V0` (`ALTER TABLE orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`) so future `UPDATE`/`DELETE` commits emit `_change_data` files.

```bash
# Step 1: Pin exact snapshot watermark V0 BEFORE running the initial load
#         (commits V0+1, V0+2, ... may land while Step 2 runs — none will be lost!)
V0=$(beam inspect "delta://gs://<BUCKET>/delta/orders" --sample 0 --json \
     | python3 -c "import json,sys; print(json.load(sys.stdin)['outputs']['snapshot_version'])")

# Step 2: Initial full sync pinned strictly at ?version=${V0} into Iceberg v2
#         (Use --zero-copy if unpartitioned on the same bucket, or omit --zero-copy to rewrite Parquet)
beam pipe \
  --from "delta://gs://<BUCKET>/delta/orders?version=${V0}" \
  --to "iceberg://lake.orders?warehouse=gs://<BUCKET>/iceberg_wh" \
  --assert-counts --json

# Step 3: Pin upper bound V1 for your incremental CDC run so the watermark advances deterministically
V1=$(beam inspect "delta://gs://<BUCKET>/delta/orders" --sample 0 --json \
     | python3 -c "import json,sys; print(json.load(sys.stdin)['outputs']['snapshot_version'])")

# Step 4: Apply every intermediate change in [V0 + 1 .. V1] via Iceberg v2 Merge-on-Read equality deletes
if [ "${V1}" -gt "${V0}" ]; then
  beam pipe \
    --from "delta://gs://<BUCKET>/delta/orders?start_version=$((V0 + 1))&end_version=${V1}" \
    --to "iceberg://lake.orders?warehouse=gs://<BUCKET>/iceberg_wh" \
    --cdc --primary-key id --if-exists append --json
fi

# Step 5: Prove 0-row drift at watermark V1, then advance V0=${V1} for the next scheduled run!
beam diff \
  "delta://gs://<BUCKET>/delta/orders?version=${V1}" \
  "iceberg://lake.orders?warehouse=gs://<BUCKET>/iceberg_wh" \
  --key id --json
```

#### Pattern B: PostgreSQL / MySQL + Debezium -> BigQuery or Iceberg (Works on All Core Beam `2.71+`)
When replicating an OLTP database where writers never pause, core Apache Beam (`2.71+`) supports two zero-gap Debezium architectures:
1. **Direct Core Beam `ReadFromDebezium` -> `WriteToBigQueryCDC` / `WriteToIceberg`**:
   `ReadFromDebezium` (`snapshot.mode=initial`, the default) acquires a repeatable-read snapshot lock, records exact WAL LSN $L_0$, emits existing rows as `op="r"`, and transitions seamlessly to PostgreSQL `pgoutput` WAL streaming from $L_0$:
   ```bash
   # Credentials stay in a 0600 file or env vars — never on the command line:
   beam pipe \
     --from "postgres://db.internal:5432/shop?table=public.orders" \
     --from-opt-file ./pg_creds.yaml \
     --to "bigquery://<PROJECT>.<DATASET>.orders_live" \
     --cdc --primary-key id \
     --runner=DataflowRunner --project=<PROJECT> --region=us-central1 \
     --temp_location="gs://<BUCKET>/tmp" --json
   ```
2. **Decoupled Debezium -> Pub/Sub or Kafka (Overlapping LSN Watermark — Zero Lock Window)**:
   - **Step 1**: Create your Pub/Sub subscription / Kafka consumer group and start Debezium WAL capture **first** so changes begin buffering immediately with `source.lsn`.
   - **Step 2**: Run the initial batch table load (`beam pipe --from postgres://... --to bigquery://...`).
   - **Step 3**: Start the streaming CDC pipeline (`beam pipe --from pubsub://... --to bigquery://... --cdc --primary-key id`). Because `beam --cdc` maps `source.lsn` / `source.ts_us` to BigQuery's monotonic `_CHANGE_SEQUENCE_NUMBER` (and Iceberg's `sequence_number_column`), any overlapping events buffered during Step 2 apply **idempotently** without overwriting newer updates or reviving deleted rows.

---

## 9. Universal URIs, Safe Expressions & Exit Code Contract

### Universal URI Grammar

| System | URI Syntax | Notes |
| :--- | :--- | :--- |
| **Local / Cloud Files** | `./orders.csv`, `gs://bucket/path/*.parquet`, `s3://...`, `azfs://...`, `hdfs://...` | Auto-infers `csv`, `json`/`jsonl`, `parquet`, `avro`; resolves `-00000-of-NNNNN` shards & mixed-format globs |
| **Delta Lake** | `delta://gs://bucket/tables/orders` or `delta:///local/orders?version=3` | Pure-Python `_delta_log` reader, time travel (`?version=N`), `--zero-copy`, and Change Data Feed |
| **Apache Iceberg v2** | `iceberg://ns.orders?warehouse=gs://bucket/wh` | Supports `warehouse`, `catalog=biglake`, and `catalog=rest`, plus Merge-on-Read `equality-deletes` |
| **BigQuery** | `bigquery://PROJECT.DATASET.TABLE` | `$0` metadata/`dryRun` inspection, Storage Read API, `FILE_LOADS`/`STREAMING_INSERTS`, and Storage Write API `_default` CDC |
| **PostgreSQL / MySQL** | `postgres://HOST:5432/DB?table=public.orders`, `mysql://HOST:3306/DB?table=orders` | Inline `user:pass@` rejected (`exit 2`); use `BEAM_POSTGRES_USER` / `BEAM_POSTGRES_PASSWORD` or `0600` `--from-opt-file` |
| **Cloud Spanner** | `spanner://PROJECT/INSTANCE/DB?table=Orders` or `?change_stream=OrdersStream` | Batch reads and Spanner Change Streams (`--cdc`) |
| **Pub/Sub & Kafka** | `pubsub://projects/PROJ/topics/T`, `pubsub://projects/PROJ/subscriptions/S`, `kafka://HOST:9092/TOPIC` | Non-destructive peek, live `tail -f` follow, and streaming `beam pipe` |

### Safe `--where` & `--select` Expressions
* **Operators**: `AND`, `OR`, `NOT`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `IN (...)`, `IS NULL`, `IS NOT NULL`, `+`, `-`, `*`, `/` (blocks `**`, `<<`, sequence repetition `*`, and `_`-prefixed attributes).
* **Scalar Helpers**: `lower`, `upper`, `strip`, `len`, `coalesce`, `abs`, `round`, `int`, `float`, `str`, `bool`.

### Deterministic Exit Codes

| Exit Code | Constant | Meaning |
| :--- | :--- | :--- |
| `0` | `EXIT_OK` | Success (`beam diff` datasets match or `--exit-zero` passed; pipeline completed) |
| `1` | `EXIT_PIPELINE_FAILED` | Pipeline execution, schema-drift fail-fast, or `--assert-counts` conservation failed |
| `2` | `EXIT_USAGE` | CLI usage, YAML schema, AST expression, existing-output, or inline-secret error |
| `3` | `EXIT_CAPABILITY` | Feature or flag combination unsupported on selected runner |
| `4` | `EXIT_MISSING_PACKAGE` | Missing optional Python extra or Java 11–25 runtime (`next_actions` gives exact fix) |
| `6` | `EXIT_DIFF_MISMATCH` | `beam diff` or `beam pipe --verify` detected schema, key, or value drift |
| `7` | `EXIT_TIMEOUT` | `--wait-timeout` / `beam wait --timeout` expired (`detached: true` on remote) |
| `8` | `EXIT_COST_LIMIT` | Pre-flight `--max-bytes` scan guardrail exceeded |
| `130` | `EXIT_SIGINT` | Interrupted (`SIGINT` / `Ctrl-C`) |

---

## 10. Architecture & Documentation Links

* **Official Beam Website CLI Reference**: [`website/www/site/content/en/documentation/sdks/cli.md`](../../../website/www/site/content/en/documentation/sdks/cli.md)
* **CLI Module Map & Test Runner (`209` tests)**: [`sdks/python/apache_beam/cli/README.md`](../apache_beam/cli/README.md)
* **AI Coding Agent Skill**: [`.agents/skills/apache-beam-cli/SKILL.md`](../../../.agents/skills/apache-beam-cli/SKILL.md)

All CLI source modules live directly in their upstream target path [`sdks/python/apache_beam/cli/`](../apache_beam/cli/README.md) so individual PR slices cherry-pick cleanly onto `apache/beam:master` ([apache/beam#40891](https://github.com/apache/beam/issues/40891)). This `sdks/python/cli-preview/` package is a lightweight pure-Python overlay that lets anyone install and test the full future CLI experience against released PyPI `apache-beam` wheels in seconds.
