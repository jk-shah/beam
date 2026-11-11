# Apache Beam CLI — Cloud, Flex Templates, Streams, and Production Boundaries

This guide covers Zero-Docker Dataflow Flex Template packaging, offline validation, cloud execution, bounded stream tailing, and production scheduling boundaries with `beam`.

---

## 1. Zero-Docker Dataflow Flex Template Packaging (`beam package`)

`beam package` bundles Beam YAML or Python pipelines into Dataflow Flex Template specifications without requiring manual Dockerfile authoring or local container builds.

### Canonical Flag Usage: `--to`
Always specify the destination template spec with `--to`:
```bash
# Package Beam YAML pipeline into a Flex Template spec
beam package pipeline.yaml \
             --to gs://my-bucket/templates/orders.json \
             --project=my-project \
             --region=us-central1 \
             --json
```
*(Note: `-o` and `--output` are accepted as aliases for `--to`, and `--sdk` is case-insensitive, e.g. `--sdk yaml` or `--sdk YAML`.)*

### Jinja Parameter Extraction
If your Beam YAML pipeline contains Jinja2 expressions (e.g. `{{ input_topic }}` or `{{ threshold | default(100) }}`), `beam package` automatically extracts them into the template's `metadata.parameters` schema with inferred types and defaults:
```bash
beam package streaming.yaml \
             --to gs://my-bucket/templates/streaming.json \
             --project=my-project \
             --region=us-central1 \
             --json
```

### Packaging Python Pipelines
For Python pipelines, `beam package` introspects your pipeline's `PipelineOptions` via AST parsing, eliminating the need to maintain a separate `metadata.json`:
```bash
beam package main.py \
             --to gs://my-bucket/templates/python_pipe.json \
             --image gcr.io/my-project/beam-runner:latest \
             --json
```

---

## 2. Offline Pipeline Validation (`beam check`)

Validate Beam YAML pipeline structure, transform types, schemas, and provider dependencies offline before submission:
```bash
# Validate YAML syntax and transform schema offline
beam check pipeline.yaml --portable --json
```

### Static Warning for Untyped Python Expressions
When using `language: python` inside `MapToFields` followed downstream by `WriteToParquet`, untyped string expressions risk runtime PyArrow schema conversion failures (`LogicalType` errors). `beam check` detects this pattern statically and outputs a warning:
```
warning: MapToFields has untyped Python expressions preceding WriteToParquet; consider adding output_type (or using language: sql / typed callables) to prevent Arrow schema conversion errors.
```

### Plan Graph Generation (`--plan`)
Generate an execution plan graph and deterministic SHA-256 plan hash:
```bash
beam check pipeline.yaml --plan --json
```

---

## 3. Cloud Execution and Job Lifecycle (`beam run`, `beam wait`, `beam cancel`)

### Submitting Pipelines to Dataflow
Execute pipelines directly on Dataflow by specifying `--runner=DataflowRunner`:
```bash
beam run pipeline.yaml \
         --runner=DataflowRunner \
         --project=my-project \
         --region=us-central1 \
         --temp_location=gs://my-bucket/tmp \
         --wait-timeout=30s \
         --json
```
If `--wait-timeout` expires while the job is still running, the CLI outputs `detached: true` with exit code `7`, tracking the job ID in `~/.beam/jobs/<job_id>.json`.

### Monitoring Detached Jobs (`beam wait`)
Poll the status of a detached Dataflow job:
```bash
beam wait <JOB_ID> --timeout=5m --json
```

### Canceling or Draining Jobs (`beam cancel`)
Safely stop running streaming or batch jobs:
```bash
# Gracefully drain a streaming Dataflow job
beam cancel <JOB_ID> --drain --json

# Immediately cancel a job
beam cancel <JOB_ID> --json
```

---

## 4. Stream Peeking and Live Tailing (`beam tail`)

AI agents executing commands synchronously should **always** use bounded inspection rather than unbounded streaming tailing.

### Bounded Stream Peeking (Recommended for Agents)
Peek at the next $N$ messages or wait up to a timeout before returning cleanly:
```bash
# Peek up to 10 messages or wait at most 5 seconds
beam tail pubsub://projects/my-project/subscriptions/events-sub \
          -n 10 --duration 5s --json
```

### Kafka Stream Tailing (`beam tail kafka://...`)
`beam tail` connects to Apache Kafka topics with built-in guardrails designed specifically for operational debugging and agents:
- **Ephemeral Non-Committing Consumer Groups**: `beam tail` connects with an ephemeral consumer group (`beam-tail-<uuid>`) and explicitly sets `enable.auto.commit=false`. It reads messages without committing offsets, preventing any disruption to production consumer groups or consumer lag metrics.
- **Payload Decoding**: Automatically decodes UTF-8 text and JSON lines. When working with Kafka Schema Registry (Avro or Protobuf payloads), pass the registry URL via `--from-opt schema_registry_url=http://registry:8081` or ingest pre-decoded topics.
- **Authentication**: Supports standard Kafka client security properties:
  - `--from-opt security_protocol=SASL_SSL`
  - `--from-opt sasl_mechanism=PLAIN` (or `SCRAM-SHA-512`)
  - Standard environment variables: `BEAM_KAFKA_BOOTSTRAP_SERVERS`, `BEAM_KAFKA_SASL_USERNAME`, `BEAM_KAFKA_SASL_PASSWORD`
  - *Never pass credentials in the URI string (rejected with exit code 2).*

```bash
# Bounded tail of a secure Kafka topic
beam tail "kafka://broker.example.com:9092/user-activity" \
          -n 5 --duration 5s \
          --from-opt security_protocol=SASL_SSL \
          --from-opt sasl_mechanism=SCRAM-SHA-512 \
          --json
```

---

## 5. Production Scheduling & Operational Boundaries

`beam` is a single-pipeline execution CLI and driver; it is **not** an orchestrator, daemon, or background scheduler.

### Orchestrating Recurring Pipelines
`beam` does not include an internal cron daemon or workflow scheduler. For production recurring schedules, multi-stage DAGs, or SLA alerting:
1. **Package Declarative Pipelines**: Package your pipeline with `beam package pipeline.yaml --to gs://my-bucket/templates/pipeline.json --json`.
2. **Trigger via Orchestrator**: Schedule executions using **Google Cloud Composer (Apache Airflow)**, **Cloud Scheduler + Cloud Functions / Cloud Run**, or **dbt** for warehouse transformations.
3. **Programmatic Submission**: Trigger template runs via `gcloud dataflow flex-template run <JOB_NAME> --template-file-gcs-location=gs://.../pipeline.json`.

### Progress Markers & Streaming Checkpointing
`beam` maintains no local state or checkpoint databases on the CLI client. 
- In streaming pipelines, progress markers, committed offsets, and watermarks are managed entirely within the target runner (e.g. Google Cloud Dataflow managed state backend or Kafka broker consumer groups when run on Dataflow).
- In batch incremental loads and CDC backfills, state tracking is achieved deterministically through version-pinned watermarks (`?version=V0`, `?start_version=...`) as detailed in [references/sql-and-cdc.md](sql-and-cdc.md).

---

## 6. Guardrails & Confirmation for Autonomous Agents

Autonomous agents interacting with `beam` must uphold strict operational guardrails:
1. **Confirm Paid Cloud Compute**: Always prompt the user for confirmation before submitting jobs with `--runner=DataflowRunner`, as this provisions paid Google Cloud Compute Engine worker VMs.
2. **Confirm Destructive Overwrites**: Always obtain user confirmation before specifying `--if-exists replace` or running operations that overwrite existing tables or storage paths.
3. **Confirm Job Termination**: Always confirm before canceling or draining active production jobs via `beam cancel <JOB_ID>`.
4. **Pre-Flight Cost Guard**: When running large ad-hoc scans on cloud storage or BigQuery, always include `--max-bytes <BUDGET>` (e.g., `--max-bytes 10GB`) to halt execution before incurring unexpected scan costs (exits with code 8 `EXIT_COST_LIMIT`).
