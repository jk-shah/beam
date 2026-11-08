# Apache Beam CLI (`apache_beam.cli`) — Preview Source Overview

> **Quick Try (No C++ / Go / Gradle Build Required)**: If you want to install and try `beam` right away in `<10s`, see [`sdks/python/cli-preview/README.md`](../../cli-preview/README.md):
> ```bash
> uv venv .venv-beam && source .venv-beam/bin/activate
> uv pip install "apache-beam-cli-preview[gcp] @ git+https://github.com/jk-shah/beam.git@beam-cli-preview#subdirectory=sdks/python/cli-preview"
> beam-demo
> ```

---

## 1. Purpose & Upstream Rollout

This package implements the `beam` (and `apache-beam`) command-line interface tracked under **[apache/beam#40891](https://github.com/apache/beam/issues/40891)**.

* **Merged Upstream (`apache/beam:master`)**:
  * [#40968](https://github.com/apache/beam/pull/40968) — Initial experimental `beam` console script + `beam run`
  * [#41044](https://github.com/apache/beam/pull/41044) & [#41045](https://github.com/apache/beam/pull/41045) — Beam YAML generic comparison & Arrow/Parquet batch fixes
* **In Upstream Review**:
  * [#41040](https://github.com/apache/beam/pull/41040) — `beam pipe` v1 (CSV, JSON, Parquet, Avro across local & remote filesystems)
* **Preview Branch (`beam-cli-preview`)**:
  * Stacks the complete CLI roadmap in `sdks/python/apache_beam/cli/` so co-workers, Beam community members, and AI coding agents can evaluate the full end-to-end UX ahead of individual ~400-line upstream PRs.

---

## 2. Two-Tier `<90 ms` Architecture

1. **Zero-SDK Fast Path (`<60–90 ms` cold start)**:
   - Top-level dispatcher parses CLI flags (`_option_arity.py`), configures the OS-level `stdout -> stderr` JSON envelope guard (`output.py`), and executes metadata/local commands (`--help`, `--version`, `--llms-txt`, `config`, `doctor`, `io`, `check`, local `inspect`, bounded/follow file & Pub/Sub `tail`, `package`, `--zero-copy`, `wait`, `cancel`) **without importing `apache_beam`**.
2. **Lazy Beam YAML & Distributed Runner Path**:
   - Imports `apache_beam` lazily only when constructing or executing distributed pipelines (`run`, `pipe`, `diff`, `sql`, `check --plan`) across `DirectRunner` (`PrismRunner` / `FnApiRunner`), `DataflowRunner`, `FlinkRunner`, and `SparkRunner`.

---

## 3. Module Map

| Module | Responsibility |
| :--- | :--- |
| `main.py` | Argument parser, subcommand router, JSON envelope lifecycle, and runner execution |
| `_option_arity.py` | `<0.5 ms` static `PipelineOptions` flag-arity splitter |
| `output.py` | Schema v1.0 `--json` envelope, `os.dup2` stdout isolation, NDJSON streaming, and `untrusted_data` sanitizer |
| `uris.py` | Universal URI parser (`csv`, `json`, `parquet`, `avro`, `delta://`, `iceberg://`, `bigquery://`, `postgres://`, `mysql://`, `spanner://`, `pubsub://`, `kafka://`) |
| `pipe_spec.py` | Declarative `beam pipe` -> Beam YAML compiler (`--where`, `--select`, `--dlq`, `--assert-counts`, `--cdc`, `--zero-copy`) |
| `expressions.py` | Tokenized, AST-allowlisted, DoS-bounded `--where` / `--select` Python callable compiler |
| `dlq.py` | RFC-4180 multi-shard `ReadCsvWithDlq`, `ReadJsonWithDlq`, and flat primitive DLQ normalizers |
| `cost_guard.py` | Pre-flight `--max-bytes` cost estimator (`EXIT_COST_LIMIT = 8`) across files, Lakehouse logs, Postgres, and `$0` BigQuery metadata/`dryRun` |
| `diff.py` | Distributed $O(1)$-RAM `CombinePerKey` multiset hash & float-tolerance dataset reconciliation (`EXIT_DIFF_MISMATCH = 6`) |
| `inspect_cmd.py` | Sub-second PyArrow, Lakehouse snapshot, and `$0` BigQuery schema/stats/sample inspector |
| `tail.py` | Non-destructive bounded stream peek & live `tail -f` follow (files, GCS shards, Pub/Sub, Kafka, Delta/Iceberg commits) |
| `package.py` | `<2 s` Zero-Docker YAML Flex Templates (`jinja2` AST parameter extraction) & Python `ast` single-image Flex bundles |
| `lakehouse.py` | Pure-Python Delta Lake `_delta_log` & Apache Iceberg v2 reader, Merge-on-Read writer, and `--zero-copy` Avro manifest synthesizer |
| `cdc.py` | Cross-engine Debezium / Spanner / Lakehouse CDC normalizer & BigQuery Storage Write API `_default` (`UPSERT`/`DELETE`) sink |
| `jobs.py` | Detached job state (`~/.beam/jobs/`), 30m idempotency ledger, `beam wait` deferred post-checks, and `beam cancel` |
| `check_cmd.py` | Static YAML & provider trust validator + optional `--plan` graph expansion |
| `io_cmd.py` / `doctor.py` | Connector reference cards (`beam io`) and environment/ADC pre-flight diagnostics (`beam doctor`) |
| `sql.py` / `config.py` | Beam Calcite SQL runner (`beam sql`) and persistent `PipelineOptions` profiles (`beam config`) |
| `llms_txt.py` | Self-contained Markdown agent reference (`beam --llms-txt`) |

---

## 4. Running the Automated Test Suite (`100` Tests)

After installing the preview overlay (`sdks/python/cli-preview`) into a virtual environment, run the full CLI unit, `<90 ms` startup-isolation, pipeline integration, Lakehouse/CDC, and adversarial security test suite from the repository root:

```bash
python -m unittest discover -s sdks/python/apache_beam/cli -p '*_test.py'
```
