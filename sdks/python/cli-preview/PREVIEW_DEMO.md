# Apache Beam CLI v3.1 Preview (`beam` & `apache-beam`)

> **Status**: Complete v3.1 preview on the `beam-cli-preview` development branch.
> Installs a `<90 ms` cold-start `beam` (and `apache-beam`) CLI binary on top of released `apache-beam>=2.75.0` wheels in `<3 seconds`—no Cython or Gradle build required.

---

## 1. Quick Install (`<3s`)

```bash
uv venv .venv-beam
source .venv-beam/bin/activate
uv pip install "apache-beam[yaml,gcp]>=2.75.0" pyarrow fastavro
uv pip install ./sdks/python/cli-preview --no-deps
```

Verify installation and environment health:

```bash
beam --version --json
beam doctor --json
beam --llms-txt | head -n 40
```

---

## 2. Complete Command Surface (v3.1)

| Command | Purpose | Cold Start |
| :--- | :--- | :--- |
| `beam --llms-txt` | Self-documenting Markdown manual + exit codes + URI syntax for AI agents | `<60 ms` |
| `beam doctor` | Checks Python extras, Java runtime,JAR cache, and GCP ADC credentials | `<90 ms` |
| `beam io list` / `describe` | Lists & describes all Universal URI connectors and required options | `<60 ms` |
| `beam inspect <URI>` | Schema, size, row count, column `--stats`, `--search`, and sanitized samples | `<90 ms` (local) |
| `beam check <FILE\|->` | Fast static YAML & provider trust check (`--plan` for full Beam graph expansion) | `<80 ms` (static) |
| `beam pipe --from <A> --to <B>` | Zero-code ETL with `--where`, `--select`, `--dlq`, `--max-bytes`, `--assert-counts`, `--verify` | Local or Cloud |
| `beam diff <LEFT> <RIGHT>` | Distributed $O(1)$-memory key/hash dataset reconciliation (`EXIT_DIFF_MISMATCH = 6`) | Local or Cloud |
| `beam tail <URI>` | Bounded, non-destructive stream/file peek (`ack_deadline_seconds=0` on Pub/Sub) | Bounded |
| `beam run <FILE\|->` | Runs Beam YAML pipelines (`--no-jinja` default on stdin to block SSTI) | Local or Cloud |
| `beam wait <JOB_ID>` | Polls detached remote jobs and runs deferred `--assert-counts` / `--verify` checks | `<90 ms` |
| `beam cancel <JOB_ID>` | Cancels or `--drain`s running remote pipelines | `<90 ms` |
| `beam sql` | Beam Calcite SQL shell & one-shot queries | JVM |
| `beam config` | Persistent profiles (`~/.beam/config.yaml`) | `<60 ms` |

---

## 3. Agentic Safety & Determinism Guarantees

1. **Pure JSON Envelope (`--json` / `--output=json`)**:
   - Redirects OS file descriptor `stdout` (`fd 1`) to `stderr` (`fd 2`) before importing `apache_beam`, keeping C++/gRPC/JVM logs out of machine-parsed JSON stdout.
   - Every response includes `schema_version: "1.0"`, `ok`, `exit_code`, `duration_ms`, `metrics`, `outputs`, `warnings`, `error`, and `next_actions`.
2. **Prompt-Injection Quarantine (`untrusted_data`)**:
   - Sampled rows from `inspect`, `tail`, and `diff` live strictly inside `untrusted_data.rows` with ANSI/OSC escapes, C0/C1 control codes, Unicode BiDi overrides, and Unicode Tag characters (`U+E0000..U+E007F`) stripped.
3. **Cost & Mutation Guardrails**:
   - `--max-bytes 10GB` fails closed with exit code `8` (`EXIT_COST_LIMIT`) *before* launching any job (`$0` `tables.get` metadata fast path + `dryRun` fallback for BigQuery `VIEW`s).
   - Never overwrites existing files or non-empty BigQuery tables by default (`--if-exists={fail,replace,append}`).
4. **Row Conservation & Distributed Verification**:
   - `--assert-counts` proves `rows_read == rows_written + rows_filtered_out + dlq_rows` across stage fusion boundaries.
   - `beam diff` reconciles arbitrarily large sharded CSV, JSON, Parquet, Avro, and BigQuery datasets in $O(1)$ worker memory (`NaN == NaN`, normalized `Decimal`, configurable `--rtol`/`--atol`).

---

## 4. Run the End-to-End Local Showcase

```bash
bash sdks/python/cli-preview/demo_showcase.sh
```
