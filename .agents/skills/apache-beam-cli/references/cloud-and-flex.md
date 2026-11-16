# Streaming Operations, Kafka Tailing, Scheduling, Zero-Copy & Scoring (`beam`)

Operational runbook for Kafka-to-Iceberg streaming, live Kafka inspection (`beam tail`), environment-agnostic nightly scheduling (`cron` / K8s / Airflow / Composer), low-delay ML transaction scoring, and confirm-before-write guardrails in `apache-beam-cli-preview` `v0.2.0`.

---

## 1. Kafka to Iceberg: Format, Commit Interval, SASL, & Day-2 Operations

### Configuring JSON vs. Avro Schema Registry, SASL, & Sub-Minute Commits
- **Schema Registry format override**: Passing `?schema_registry=https://...` defaults `ReadFromKafka` to `AVRO`. For **JSON Schema** or plain JSON topics, add **`&format=json`** (or `--from-format json`).
- **Commit interval (`triggering_frequency_seconds`)**: Streaming Iceberg sinks default to `60s` snapshot commits. Set `--to-opt triggering_frequency_seconds=30` (or `15`) to meet `<60s` freshness targets without excessive small files.
- **Confluent / Broker SASL & TLS (`chmod 0600 kafka.opts`)**: Accepts YAML (`key: value`) or Java/Kafka `.properties` (`key=value`) syntax:
  ```ini
  security_protocol=SASL_SSL
  sasl_mechanism=PLAIN
  sasl_username=CLUSTER_API_KEY
  sasl_password=CLUSTER_API_SECRET
  ```
- **Runner Choice (No Extra Cloud Platform Required)**: Run directly on an existing **`FlinkRunner`** (`--runner=FlinkRunner --flink_master=http://flink-jm:8081`), **`SparkRunner`**, single-node **`PrismRunner`**, or **`DataflowRunner`** on GCP:

```bash
beam pipe \
  --from "kafka://pkc-xyz.confluent.cloud:9092/payments?schema_registry=https://psrc-xyz.confluent.cloud&format=json&consumer_group=payments-iceberg-v1&commit_offset_in_finalize=true&auto_offset_reset=earliest" \
  --to "iceberg://lake.payments?catalog=rest&uri=https://catalog.internal&warehouse=s3://my-bkt/wh" \
  --where "status = 'CAPTURED' AND amount > 0" \
  --select "payment_id, sha256(user_id) AS user_hash, mask_email(email) AS email, mask_last4(card_number) AS card_masked, amount, event_ts" \
  --to-opt partition_fields="day(event_ts)" \
  --to-opt triggering_frequency_seconds=30 \
  --dlq s3://my-bkt/dlq/payments/ --dlq-redact-fields email,card_number \
  --from-opt-file kafka.opts \
  --runner=FlinkRunner --flink_master=http://flink-jm:8081 --wait-timeout 30s --json
```

### Delivery Guarantees, Monitoring, Restart, & Upgrade Runbook
1. **Delivery Guarantees**: `commit_offset_in_finalize=true` commits Kafka offsets only after runner checkpoints complete, and `WriteToIceberg` commits atomic Iceberg v2 manifests every `triggering_frequency_seconds` (at-least-once across worker restarts; add `--dedupe-by payment_id --dedupe-window 15m` or `--cdc --primary-key payment_id` for idempotent deduplication).
2. **Monitoring**: `--wait-timeout 30s --json` writes `~/.beam/jobs/<job_id>.json` and returns `exit_code: 7` (`detached: true`). Poll status and counters (`rows_read`, `rows_written`, `dlq_rows`) via `beam wait <JOB_ID> --timeout 10s --json`, and monitor Kafka `payments-iceberg-v1` consumer lag + DLQ prefix object count.
3. **Graceful Drain & Restart**: Confirm with the user, then run `beam cancel <JOB_ID> --drain --json` to stop Kafka fetches, commit the final in-flight Iceberg snapshot, and finalize offsets. Relaunching the same command with `consumer_group=payments-iceberg-v1` resumes from the last committed offset.
4. **Pipeline Upgrade**: Validate schema and YAML changes pre-flight with `beam check` and `beam schema`. On **Dataflow**, pass `--update --job_name=payments-iceberg` (`--transform_name_mapping` if step names changed); on **Flink**, trigger `flink stop --savepointPath s3://my-bkt/savepoints <FLINK_JOB_ID>` and relaunch with `--runner=FlinkRunner --savepoint_path=...`.

---

## 2. Nightly JSON to Partitioned Iceberg (Cron, K8s, Airflow, or GCP Flex)

`--to-opt partition_fields=dt` (or `day(event_ts)`, `month(ts)`, `year(ts)`, `hour(ts)`, `truncate(col, N)`) writes Hive-partitioned Parquet files and Iceberg v2 `partition-specs`. Subsequent `--if-exists append` runs automatically inherit the existing table's partition spec.

### Option A: Environment-Agnostic Cron / Kubernetes `CronJob` / Airflow `BashOperator`
Because `beam pipe ... --json` exits `0` on verified completion, `2` on pre-flight schema mismatch, `8` on scan-cap overrun, and `1` on runtime/conservation failure, any scheduler can invoke it directly without Cloud Composer or Cloud Scheduler:

```bash
#!/usr/bin/env bash
set -euo pipefail
DT="${1:-$(date -u -d 'yesterday' +%F)}"
beam pipe \
  --from "s3://my-bkt/raw/events/${DT}/*.jsonl" \
  --to "iceberg://lake.events?warehouse=s3://my-bkt/wh" \
  --check-schema --max-bytes 50GB \
  --select "*, '${DT}' AS dt" --to-opt partition_fields=dt \
  --if-exists append \
  --dlq "s3://my-bkt/dlq/events/${DT}/bad.jsonl" \
  --assert-counts --json
```

### Option B: Google Cloud Dataflow Flex Template + Composer / Cloud Scheduler
When running on Google Cloud:
```bash
beam pipe --from "gs://my-bkt/raw/events/{{ ds }}/*.jsonl" \
          --to "iceberg://lake.events?warehouse=gs://my-bkt/wh" \
          --select "*, '{{ ds }}' AS dt" --to-opt partition_fields=dt \
          --if-exists append --dlq "gs://my-bkt/dlq/events/{{ ds }}/" \
          --emit-yaml > nightly_events.yaml
beam package nightly_events.yaml --to gs://my-bkt/templates/nightly_events.json --json
beam export-schedule nightly_events.yaml --template-gcs-path gs://my-bkt/templates/nightly_events.json \
     --orchestrator composer --schedule "0 2 * * *" --to dags/nightly_events_dag.py --json
```

---

## 3. Live Kafka Inspection & Schema Validation (`beam tail`)

`beam tail` inspects live `kafka://`, `pubsub://`, `delta://`, `iceberg://`, and local/`gs://` files in Python (`pip install confluent-kafka`) with zero JVM startup:

- **Zero-Group Partition Assign (`?no_group=true`) vs. Ephemeral Non-Committing Group**:
  - Pass **`?no_group=true`** (or `--from-opt no_group=true`) to read partitions directly via `consumer.assign([TopicPartition(...)])` without sending `JoinGroup` or registering any consumer group on the broker (works even when broker `Group` ACLs are restricted).
  - By default, uses `'beam-tail-<uuid>'` with `'enable.auto.commit': False` (never commits offsets).
- **Confluent Wire Header (`schema_id`), Avro & Live JSON Schema Validation**:
  - Whenever a message carries Confluent's 5-byte wire header (`0x00` + big-endian uint32 ID), `beam tail` surfaces **`"schema_id": <int>`** on the returned record.
  - With `?schema_registry=https://...` (and optional Basic Auth via `BEAM_SCHEMA_REGISTRY_USER_INFO="KEY:SECRET"` or `schema_registry_user_info` in `0600` `kafka.opts`), `beam tail` fetches `/schemas/ids/<id>`:
    - **`AVRO` subjects**: decodes binary Avro via `fastavro` and sets `"schema_valid": true`.
    - **`JSON` subjects**: parses UTF-8 JSON and validates each live message against the subject's JSON Schema via `jsonschema`, setting **`"schema_valid": true|false`** and `"schema_error"` on contract violations.
- **Raw Wire Hex Inspection (`?raw_bytes=true` or `--format raw`)**: Adds `"size_bytes"` and `"raw_hex"` (first 256 raw wire bytes, including magic byte and schema ID) alongside decoded `"payload"`.
- **Interactive TUI (`-i`) + `--json`**: Renders rolling 5s `msg/s` throughput and colored fields to `stderr` while keeping `stdout` a pure RFC-8259 JSON envelope.

```bash
# Zero-group peek showing schema_id, live Schema Registry validation, and raw wire hex
beam tail "kafka://broker:9092/payments?schema_registry=https://sr.internal:8081&no_group=true&raw_bytes=true&auto_offset_reset=latest" \
  -n 10 --duration 5s --from-opt-file kafka.opts --json
```

---

## 4. Low-Delay Streaming Transaction Scoring (`--enrich` + `--infer`)

Combine in-VPC `--enrich` (`bigtable:` / `vertexai:` / `feast:` with TLS `rediss://` read-through cache) and `--infer` micro-batching (`min_batch_size=1,max_batch_duration_ms=10`) on `FlinkRunner` or `DataflowRunner`:
- `max_batch_duration_ms=10` caps micro-batch buffering at `10ms` (`0.01s`) under low QPS while batching up to `max_batch_size` under spikes.
- If a batch fails inference, $O(\log N)$ in-worker binary bisection isolates poison-pill transactions to `--dlq` (`--dlq-redact-fields`) without dropping healthy peers or reloading local `@<40-hex-sha>` `safetensors` weights.

```bash
beam pipe \
  --from "kafka://broker:9092/transactions?format=json&auto_offset_reset=latest" \
  --to "kafka://broker:9092/scored-transactions?format=json" \
  --enrich "bigtable:project=my-proj,instance=risk-bt,table=velocity,key=card_id,columns=[stats:txn_1h:int,stats:spend_24h:float],redis=rediss://10.0.0.5:6379,on_miss=null" \
  --infer "vertexai:endpoint=projects/my-proj/locations/us-central1/endpoints/123,min_batch_size=1,max_batch_size=16,max_batch_duration_ms=10,rate_limit=2000" \
  --infer-fields "amount, txn_1h, spend_24h" --infer-flatten \
  --dlq gs://my-bkt/dlq/txn_scoring/ --dlq-redact-fields card_id \
  --from-opt-file kafka.opts --runner=DataflowRunner --project=my-proj --region=us-central1 --json
```

---

## 5. Confirm-Before-Write Checklist

Always preview (`--dry-run` / `--emit-yaml` / `beam inspect`) and obtain explicit human confirmation before running:
1. **`beam catalog sync`** (without `--dry-run`) — updates target Iceberg catalog table pointers; run `beam catalog sync --dry-run` first.
2. **`beam pipe --zero-copy`** — commits a new Iceberg snapshot referencing existing Parquet files (supports both partitioned and unpartitioned `delta://`, `iceberg://`, and Hive Parquet sources).
3. **`--if-exists replace`** — overwrites destination files, BigQuery tables (`WRITE_TRUNCATE`), or Iceberg snapshots.
4. **Remote cluster submission (`--runner=DataflowRunner`, `FlinkRunner`, `SparkRunner`)** — launches distributed worker compute.
5. **`beam cancel <JOB_ID>`** — cancels or `--drain`s an active remote job.
