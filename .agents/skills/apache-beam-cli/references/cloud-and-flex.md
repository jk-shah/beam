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

## 5. Production Scheduling & Operational Boundaries (`beam export-schedule`)

`beam` is a single-pipeline execution CLI and driver; it is **not** an orchestrator daemon. Instead of running a local cron process, `beam export-schedule` compiles pipelines directly into enterprise orchestration deployment definitions:

### Export to Google Cloud Composer (Apache Airflow)
Generates an idiomatic, PEP 8 Airflow DAG utilizing `DataflowStartFlexTemplateOperator`:
```bash
beam export-schedule pipeline.yaml \
             --schedule "0 2 * * *" \
             --orchestrator composer \
             --project=my-project \
             --region=us-central1 \
             --template-gcs-path=gs://my-bucket/templates/pipeline.json \
             --to dags/daily_orders_dag.py \
             --json
```

### Export to Serverless Cloud Scheduler
Generates a bash setup script creating a Cloud Scheduler HTTP trigger targeting Dataflow's `flexTemplates:launch` REST endpoint with OAuth authentication:
```bash
beam export-schedule pipeline.yaml \
             --schedule "0 2 * * *" \
             --orchestrator cloud-scheduler \
             --project=my-project \
             --region=us-central1 \
             --template-gcs-path=gs://my-bucket/templates/pipeline.json \
             --service-account-email=scheduler-sa@my-project.iam.gserviceaccount.com \
             --to deploy_scheduler.sh \
             --json
```

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

---

## 7. Data Quality Expectations & Tagged DLQ Routing (`--expect`)

Declare schema-aware data quality assertions directly on pipeline flows:
```bash
beam pipe --from gs://my-bucket/raw/transactions.csv \
          --to bigquery://my-project.analytics.transactions \
          --expect "positive_amount: amount > 0.0" \
          --expect "valid_currency: currency in ['USD', 'EUR', 'GBP']" \
          --expect "customer_id is not None -> fail" \
          --on-violation dlq \
          --dlq gs://my-bucket/dlq/transactions.jsonl \
          --assert-counts \
          --runner=DataflowRunner \
          --project=my-project \
          --json
```

### Violation Actions
- `dlq` (default): Routes invalid records to `--dlq` sink with full error metadata (`error_type`, `error`, `raw_payload`).
- `drop`: Silently filters out invalid records while incrementing audit counters.
- `fail`: Halts pipeline execution immediately upon detecting a violation (raising `ExpectationViolationError`).

### Live Google Cloud Dataflow Telemetry
When executed on `DataflowRunner`, each expectation rule automatically publishes custom Beam metrics (`Metrics.counter` in `beam_cli` namespace):
- `expectation_passed_{rule}`
- `expectation_quarantined_{rule}`
- `expectation_dropped_{rule}`
- `expectation_failed_{rule}`
These surface as live real-time time-series charts in the Google Cloud Dataflow Monitoring Console UI.

---

## 8. Apache Iceberg Catalog Integration Hierarchy

When writing to or reading from Apache Iceberg lakehouses (`iceberg://`), the CLI resolves table locations and registers snapshots using a strict three-tier catalog hierarchy:

### Tier 1: BigQuery Managed Iceberg API (Primary)
Direct integration with BigQuery managed tables storing data in Apache Iceberg format:
- **API Endpoint**: `https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}`
- **Storage Mapping**: Tables are resolved and registered via `bigLakeConfiguration.storageUri` with `tableFormat: "ICEBERG"` and `fileFormat: "PARQUET"`.
- **Authentication**: Seamlessly authenticated via Google Application Default Credentials (ADC) or active environment tokens.
- **Auto-Resolution**: 3-part identifiers (`project.dataset.table`) automatically query the BigQuery API first. If the table already exists, updates are pushed via `PATCH /tables/{tableId}`; otherwise, it is created via `POST /tables`.

```bash
# Register or write directly to BigQuery managed Iceberg table
beam pipe --from gs://my-bucket/clean_orders.parquet \
          --to "iceberg://my-proj.analytics.orders?catalog=bigquery" \
          --json
```

### Tier 2: Google Cloud Lakehouse / BigLake REST Catalog (Secondary)
When an enterprise Iceberg catalog is configured in BigLake:
- **API Endpoint**: `https://biglake.googleapis.com/iceberg/v1/restcatalog`
- **Catalog Prefix**: Resolves tables under `/projects/{project}/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`.
- **Registration**: Publishes table metadata locations to `/namespaces/{namespace}/register` with GCP OAuth 2.0 bearer tokens.

```bash
# Register to BigLake REST Catalog
beam pipe --from delta://gs://my-lakehouse/delta/events \
          --to "iceberg://analytics.events?warehouse=gs://my-lakehouse/wh" \
          --to-opt catalog_type=biglake \
          --to-opt uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
          --zero-copy --json
```

### Tier 3: Generic Iceberg REST Catalog (Tertiary)
Portable standard Iceberg REST Catalog for open lakehouse deployments:
- **API Endpoint**: Standard Iceberg REST OpenAPI `/v1/namespaces/{namespace}/tables/{table}`.
- **Configuration**: Explicitly configured via `--to-opt catalog_type=rest` and `--to-opt uri=https://catalog.example.com/v1`.

---

## 9. Partition-Aware Zero-Copy Lakehouse Bridge (`--zero-copy`)

`beam pipe --zero-copy` translates Delta Lake tables into Apache Iceberg v2 tables in-place without copying or re-encoding underlying Parquet data files (`bytes_copied: 0`):

### Supported Capabilities
1. **Partitioned & Unpartitioned Support**: Handles both flat tables and Hive-style partitioned tables (e.g. `date=2026-11-10/region=US/part-00000.parquet`).
2. **Dynamic Manifest Schema Generation**: Dynamically inspects Delta partition columns, maps them into Iceberg schema field types, and generates dynamic Avro manifest schemas (`r102`) embedding typed partition records (`r102_partition`).
3. **Iceberg v2 Metadata Generation**: Generates compliant `metadata.json` with `format-version: 2`, `partition-specs`, and `default-spec-id`.
4. **Transparent Partition Column Reconstruction**: Reconstructs partition columns from manifest metadata during read operations across:
   - In-process local fast reader (`read_iceberg_rows_local`)
   - Portable Beam runner (`PrismRunner`)
   - Google Cloud Dataflow (`DataflowRunner`)

```bash
# Zero-copy conversion of partitioned Delta table with BigQuery managed Iceberg catalog registration
beam pipe --from delta://gs://my-lakehouse/orders_delta \
          --to "iceberg://my-proj.analytics.orders_iceberg?catalog=bigquery" \
          --zero-copy \
          --verify --verify-key order_id \
          --json
```

---

## 10. Pre-Flight Schema Contract & Compatibility Guard (`beam schema diff`)

Detect schema regressions, dropped columns, and incompatible type coercions **before** launching distributed compute on Dataflow:
```bash
# Validate that source Parquet schema safely maps to target BigQuery table
beam schema diff gs://my-bucket/raw/orders_v2.parquet \
                 bigquery://my-proj.analytics.orders \
                 --mode=backward \
                 --json
```

### Compatibility Modes
- `BACKWARD` (default): Source can be safely written to target. Target may contain additional nullable columns. Source cannot drop non-nullable target columns or narrow column data types.
- `FORWARD`: Readers expecting older schemas can read new source datasets without failure.
- `FULL`: Enforces strict bidirectional compatibility across both source and target.

### Detected Violations & Risk Classification
- **`DROPPED_REQUIRED_COLUMN`**: Target schema requires a non-null column that the source does not provide (Exits with code `2`).
- **`TYPE_NARROWING_ERROR`**: Unsafe type coercion that causes fractional precision loss, integer overflow, or runtime parse errors (e.g. `STRING` $\to$ `INT64`, `FLOAT64` $\to$ `INT32`).
- **`NULLABILITY_MISMATCH`**: Source column allows `NULL`, but target table constraint is `REQUIRED`.
- **`SAFE_TYPE_WIDENING`**: Safe type expansions (e.g. `INT32` $\to$ `INT64`, `FLOAT32` $\to$ `FLOAT64`) are logged as informational warnings under standard execution, or flagged as violations under `--strict`.

### Actionable Remediation Hints
When contract violations are detected, `beam schema diff` outputs concrete command remediation hints:
```json
{
  "action": "explicit_type_cast",
  "field": "customer_id",
  "command_hint": "--select \"CAST(customer_id AS INT64) AS customer_id, *\"",
  "description": "Explicitly cast column 'customer_id' to INT64 before writing."
}
```

---

## 11. Turnkey Stream-to-Lakehouse Pipeline Declarations (Kafka $\to$ Iceberg/BigLake)

`beam pipe` enables one-line continuous stream ingestion from Apache Kafka into Apache Iceberg v2 and Google Cloud BigLake REST catalogs with automatic schema resolution and runner tuning:

```bash
beam pipe --from "kafka://broker:9092/orders_topic?schema_registry=http://registry:8081&consumer_group=lakehouse_sync" \
          --to "iceberg://biglake.prod_analytics.events?warehouse=gs://my-bkt/wh" \
          --runner=DataflowRunner \
          --project=my-project \
          --region=us-central1 \
          --json
```

### Supported Streaming Invariants & Auto-Tuning
1. **Schema Registry Integration**:
   - Parses `schema_registry`, `schema_registry_url`, `confluent_schema_registry_url`, and `schema_registry_subject`.
   - Defaults Kafka format to `'AVRO'` when Schema Registry is specified unless explicit format is provided.
2. **Consumer Group & Offset Guarantees**:
   - `consumer_group` / `group_id` wired directly into Kafka consumer configuration.
   - `auto_offset_reset` (`'earliest'` or `'latest'`) ensures deterministic replay or real-time ingestion.
   - `timestamp_policy` (`'CreateTime'` or `'LogAppendTime'`) controls event-time extraction.
3. **Dataflow Streaming Engine Auto-Injection**:
   - For unbounded streaming sources (Kafka, Pub/Sub) targeting `DataflowRunner`, `beam` automatically injects `--streaming` and `--enable_streaming_engine`.
   - Emits top-level `options: {'streaming': True}` into generated Beam YAML pipelines.
4. **Triggering Frequency Derivation**:
   - For Iceberg sinks, automatically derives commit frequency (`triggering_frequency_seconds`) from `--window` window period/size (e.g. `--window fixed:60s` $\to$ 60s) or defaults to 60s for unbounded streams.
5. **BigLake REST Catalog Identification**:
   - Detects `biglake.` and `bigquery.` prefixes on Iceberg table identifiers, automatically configuring Google Cloud BigLake REST Catalog (`https://biglake.googleapis.com/iceberg/v1/restcatalog`) with Google OAuth 2.0 authentication.


