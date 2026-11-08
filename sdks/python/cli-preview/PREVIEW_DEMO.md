# Apache Beam CLI Preview (`beam` & `apache-beam`)

> **Branch**: [`jk-shah/beam@beam-cli-preview`](https://github.com/jk-shah/beam/tree/beam-cli-preview/sdks/python/cli-preview) · **Upstream Tracking Issue**: [apache/beam#40891](https://github.com/apache/beam/issues/40891)
>
> Installs a `<90 ms` cold-start `beam` (and `apache-beam`) CLI binary on top of released PyPI `apache-beam>=2.75.0` wheels—**no repo clone, no C++ compiler, no Go toolchain, and no Gradle build required**.

---

## 1. Try It in 10 Seconds (Fresh Venv, Zero Repo Clone)

### Option A: `uv` (Recommended — ~3 seconds)
```bash
uv venv .venv-beam
source .venv-beam/bin/activate
uv pip install "apache-beam-cli-preview[gcp] @ git+https://github.com/jk-shah/beam.git@beam-cli-preview#subdirectory=sdks/python/cli-preview"
```

*(Local-only without GCP connectors: drop `[gcp]` -> `uv pip install "apache-beam-cli-preview @ git+https://github.com/jk-shah/beam.git@beam-cli-preview#subdirectory=sdks/python/cli-preview"`)*

### Option B: Standard `pip`
```bash
python3 -m venv .venv-beam
source .venv-beam/bin/activate
pip install --upgrade pip
pip install "apache-beam-cli-preview[gcp] @ git+https://github.com/jk-shah/beam.git@beam-cli-preview#subdirectory=sdks/python/cli-preview"
```

### Run the 10-Step Interactive Showcase Immediately (`~10s`)
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
   > *"Use `/path/to/.venv-beam/bin/beam` (run `beam --llms-txt` first to learn its contract, exit codes, and URI schemes) to inspect, clean, and reconcile `<dataset>`."*
2. **Agent Skill (`SKILL.md`)**: Copy [`.agents/skills/apache-beam-cli/SKILL.md`](../../.agents/skills/apache-beam-cli/SKILL.md) into your workspace's `.agents/skills/apache-beam-cli/SKILL.md`.

---

## 3. Complete Command & Connector Surface

| Command | Purpose | Cold Start |
| :--- | :--- | :--- |
| `beam --llms-txt` | Self-documenting Markdown manual + exit codes + URI syntax for AI agents | `<60 ms` |
| `beam doctor` | Checks Python extras, Java runtime, JAR/Prism cache, and GCP ADC credentials | `<90 ms` |
| `beam io list` / `describe` | Lists & describes Universal URI connectors (`csv`, `json`, `parquet`, `avro`, `delta`, `iceberg`, `bigquery`, `postgres`, `mysql`, `spanner`, `pubsub`, `kafka`) | `<60 ms` |
| `beam inspect <URI>` | Schema, size, row count, column `--stats`, `--search`, `delta://`/`iceberg://` snapshots, and sanitized samples | `<90 ms` (local) |
| `beam check <FILE\|->` | Fast static YAML & provider trust check (`--plan` for full Beam graph expansion) | `<80 ms` (static) |
| `beam pipe --from <A> --to <B>` | Zero-code ETL, `--where`, `--select`, `--dlq`, `--max-bytes`, `--assert-counts`, `--verify`, `--zero-copy`, and `--cdc --primary-key` | Local or Cloud |
| `beam diff <LEFT> <RIGHT>` | Distributed $O(1)$-memory key/hash reconciliation across files, `delta://`, `iceberg://`, and `bigquery://` (`EXIT_DIFF_MISMATCH = 6`) | Local or Cloud |
| `beam tail [-f] <URI>` | Bounded stream peek OR live `tail -f` follow (`pubsub://`, `kafka://`, `delta://`, `iceberg://`, `gs://`/local shards) | `<65 ms` |
| `beam package <FILE\|->` | Zero-Docker YAML Flex Templates (`<2 s`) & zero-boilerplate Python AST Flex bundles | `<65 ms` |
| `beam run <FILE\|->` | Runs Beam YAML pipelines (`--no-jinja` default on stdin to block SSTI) | Local or Cloud |
| `beam wait <JOB_ID>` | Polls detached remote jobs and runs deferred `--assert-counts` / `--verify` checks | `<90 ms` |
| `beam cancel <JOB_ID>` | Cancels or `--drain`s running remote pipelines | `<90 ms` |
| `beam sql` | Beam Calcite SQL shell & one-shot queries (`-e` / `-f`) | JVM |
| `beam config` | Persistent `PipelineOptions` profiles (`~/.beam/config.yaml`) | `<60 ms` |

---

## 4. Agentic Safety & Determinism Guarantees

1. **Pure JSON Envelope (`--json` / `--output=json`)**:
   - Permanently redirects OS file descriptor `stdout` (`fd 1`) to `stderr` (`fd 2`) before importing `apache_beam`, keeping C++/gRPC/JVM logs out of machine-parsed JSON stdout.
   - Every response includes `schema_version: "1.0"`, `ok`, `exit_code`, `duration_ms`, `metrics`, `outputs`, `warnings`, `error`, and `next_actions` (and `beam tail -f --json` emits flushed NDJSON `{"event":"record", ...}` lines followed by the final summary envelope).
2. **Prompt-Injection Quarantine (`untrusted_data`)**:
   - Sampled rows from `inspect`, `tail`, and `diff` live strictly inside `untrusted_data.rows` (or `record` with `untrusted_data: true` in `tail -f` NDJSON) with ANSI/OSC escapes, C0/C1 control codes, Unicode BiDi overrides, and Unicode Tag characters (`U+E0000..U+E007F`) stripped.
3. **Cost & Mutation Guardrails**:
   - `--max-bytes 10GB` fails closed with exit code `8` (`EXIT_COST_LIMIT`) *before* launching any job (`$0` `tables.get` metadata fast path + `$0` `dryRun` fallback for BigQuery `VIEW`s and `EXTERNAL` tables).
   - Never overwrites existing files or non-empty BigQuery tables by default (`--if-exists={fail,replace,append}`).
4. **Row Conservation & Distributed Verification**:
   - `--assert-counts` proves `rows_read == rows_written + rows_filtered_out + dlq_rows` across stage fusion boundaries.
   - `beam diff` reconciles arbitrarily large sharded CSV, JSON, Parquet, Avro, Delta Lake, Apache Iceberg v2, and BigQuery datasets in $O(1)$ worker memory (`NaN == NaN`, normalized `Decimal`, configurable `--rtol`/`--atol`), automatically resolving sharded `-00000-of-NNNNN` output prefixes.

---

## 5. Running on Google Cloud Dataflow (`DataflowRunner`)

`apache-beam-cli-preview` automatically synthesizes and stages its pure-Python worker wheel (`<5 ms`) + `--beam_plugin` runtime hooks onto remote Dataflow SDK workers whenever `--runner=DataflowRunner` is passed—no manual `--extra_package` flags needed:

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

# Poll detached job until JOB_STATE_DONE and run deferred post-checks:
beam wait <JOB_ID> --json
```

*(Enterprise GCP org tip: if your VPC uses custom subnet mode or blocks external worker IPs, append `--subnetwork=regions/<REGION>/subnetworks/default --no_use_public_ips`.)*

---

## 6. Simplified Flex Templates (`beam package`) & Live Streaming (`beam tail -f`)

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

## 7. Cross-Engine CDC (`--cdc`) & Zero-Copy Lakehouse Bridges (`--zero-copy`)

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
Normalize Debezium (`before`/`after`/`op`/`source.lsn`), Spanner Change Streams, Delta Change Data Feed (`_change_type`/`_commit_version`), Iceberg incremental snapshots, or flat CDC streams into idempotent ordered **`UPSERT` + `DELETE`** mutations on:
- **`bigquery://`**: BigQuery Storage Write API `_default` stream (`_CHANGE_TYPE` + 16-digit `/`-segmented hex `_CHANGE_SEQUENCE_NUMBER`, auto-DDL `PRIMARY KEY (...) NOT ENFORCED`, and row-level `--dlq` routing).
- **`iceberg://`**: Apache Iceberg v2 Merge-on-Read (`equality_ids` delete files + `identifier-field-ids` + `PARQUET:field_id`).

```bash
# Replicate Debezium / Pub/Sub / Delta CDC into BigQuery Storage Write API CDC with DLQ & conservation:
beam pipe \
  --from "gs://<BUCKET>/cdc/debezium_orders.jsonl" \
  --to "bigquery://<PROJECT>.<DATASET>.orders_cdc" \
  --cdc --primary-key id \
  --dlq "gs://<BUCKET>/dlq/orders_cdc_dlq.json" \
  --assert-counts \
  --project=<PROJECT> \
  --json

# Export portable Beam YAML (ReadFromDebezium -> WriteToBigQueryCDC) & package as a Zero-Docker Flex Template:
beam pipe \
  --from "postgres://db.internal:5432/shop?table=public.orders" \
  --to "bigquery://<PROJECT>.<DATASET>.orders_cdc" \
  --cdc --primary-key id \
  --emit-yaml | beam package - --to "gs://<BUCKET>/templates/pg_to_bq_cdc.json" --json
```

---

## 8. Architecture & Contributing Upstream

All CLI source modules live directly in their upstream target path [`sdks/python/apache_beam/cli/`](../apache_beam/cli/README.md) so individual PR slices cherry-pick cleanly onto `apache/beam:master` ([apache/beam#40891](https://github.com/apache/beam/issues/40891)). This `sdks/python/cli-preview/` package is a lightweight pure-Python overlay that lets anyone install and test the full future CLI experience against released PyPI `apache-beam` wheels in seconds.
