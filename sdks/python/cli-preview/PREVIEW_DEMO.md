# Apache Beam CLI Preview (`beam` & `apache-beam`)

> **Status**: Experimental full-roadmap preview on [`jk-shah/beam@beam-cli-preview`](https://github.com/jk-shah/beam/tree/beam-cli-preview/sdks/python/cli-preview).
> Installs a `<90 ms` cold-start `beam` (and `apache-beam`) CLI binary on top of released PyPI `apache-beam>=2.75.0` wheels—**no repo clone, no C++ compiler, and no Gradle build required**.

---

## 1. Try It Now (Fresh Venv, Zero Repo Clone)

### Option A: `uv` (Fastest)
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

### Run the 8-Step Interactive Showcase Immediately (`~8s`)
```bash
python -m apache_beam_cli_preview.demo
```

Or explore interactively:
```bash
beam --version --json
beam doctor --json
beam --llms-txt | head -n 45
```

---

## 2. Test It with Your AI Coding Agent (Claude Code / Gemini / Cursor / Codex)

Give your agent either:
1. **Zero-Config Discovery**: Tell the agent *"Use `/path/to/.venv-beam/bin/beam` (run `beam --llms-txt` first to learn its contract) to inspect, clean, and reconcile `<dataset>`."*
2. **Agent Skill (`SKILL.md`)**: Copy [`.agents/skills/apache-beam-cli/SKILL.md`](https://github.com/jk-shah/beam/blob/beam-cli-preview/.agents/skills/apache-beam-cli/SKILL.md) into your workspace's `.agents/skills/apache-beam-cli/SKILL.md`.

---

## 3. Complete Command Surface

| Command | Purpose | Cold Start |
| :--- | :--- | :--- |
| `beam --llms-txt` | Self-documenting Markdown manual + exit codes + URI syntax for AI agents | `<60 ms` |
| `beam doctor` | Checks Python extras, Java runtime, JAR/Prism cache, and GCP ADC credentials | `<90 ms` |
| `beam io list` / `describe` | Lists & describes all Universal URI connectors and required options | `<60 ms` |
| `beam inspect <URI>` | Schema, size, row count, column `--stats`, `--search`, and sanitized samples | `<90 ms` (local) |
| `beam check <FILE\|->` | Fast static YAML & provider trust check (`--plan` for full Beam graph expansion) | `<80 ms` (static) |
| `beam pipe --from <A> --to <B>` | Zero-code ETL with `--where`, `--select`, `--dlq`, `--max-bytes`, `--assert-counts`, `--verify` | Local or Cloud |
| `beam diff <LEFT> <RIGHT>` | Distributed $O(1)$-memory key/hash dataset reconciliation (`EXIT_DIFF_MISMATCH = 6`) | Local or Cloud |
| `beam tail <URI>` | Bounded, non-destructive stream/file peek (`ack_deadline` skip-ack on Pub/Sub) | Bounded |
| `beam run <FILE\|->` | Runs Beam YAML pipelines (`--no-jinja` default on stdin to block SSTI) | Local or Cloud |
| `beam wait <JOB_ID>` | Polls detached remote jobs and runs deferred `--assert-counts` / `--verify` checks | `<90 ms` |
| `beam cancel <JOB_ID>` | Cancels or `--drain`s running remote pipelines | `<90 ms` |
| `beam sql` | Beam Calcite SQL shell & one-shot queries (`-e` / `-f`) | JVM |
| `beam config` | Persistent `PipelineOptions` profiles (`~/.beam/config.yaml`) | `<60 ms` |

---

## 4. Agentic Safety & Determinism Guarantees

1. **Pure JSON Envelope (`--json` / `--output=json`)**:
   - Permanently redirects OS file descriptor `stdout` (`fd 1`) to `stderr` (`fd 2`) before importing `apache_beam`, keeping C++/gRPC/JVM logs out of machine-parsed JSON stdout.
   - Every response includes `schema_version: "1.0"`, `ok`, `exit_code`, `duration_ms`, `metrics`, `outputs`, `warnings`, `error`, and `next_actions`.
2. **Prompt-Injection Quarantine (`untrusted_data`)**:
   - Sampled rows from `inspect`, `tail`, and `diff` live strictly inside `untrusted_data.rows` with ANSI/OSC escapes, C0/C1 control codes, Unicode BiDi overrides, and Unicode Tag characters (`U+E0000..U+E007F`) stripped.
3. **Cost & Mutation Guardrails**:
   - `--max-bytes 10GB` fails closed with exit code `8` (`EXIT_COST_LIMIT`) *before* launching any job (`$0` `tables.get` metadata fast path + `$0` `dryRun` fallback for BigQuery `VIEW`s).
   - Never overwrites existing files or non-empty BigQuery tables by default (`--if-exists={fail,replace,append}`).
4. **Row Conservation & Distributed Verification**:
   - `--assert-counts` proves `rows_read == rows_written + rows_filtered_out + dlq_rows` across stage fusion boundaries.
   - `beam diff` reconciles arbitrarily large sharded CSV, JSON, Parquet, Avro, and BigQuery datasets in $O(1)$ worker memory (`NaN == NaN`, normalized `Decimal`, configurable `--rtol`/`--atol`), automatically resolving sharded `-00000-of-NNNNN` output prefixes.
