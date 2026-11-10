# Apache Beam CLI — Cloud, Flex Templates, and Validation Reference

This guide covers Zero-Docker Dataflow Flex Template packaging, offline validation, cloud execution, and stream monitoring with `beam`.

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
beam check pipeline.yaml --json
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

### Live Following (`-f`)
For interactive terminal monitoring of live streams or lakehouse commit logs:
```bash
beam tail -f delta://gs://my-bucket/delta/orders -n 20 --json
```
