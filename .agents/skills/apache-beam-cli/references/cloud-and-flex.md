# Streaming Operations, Kafka Tailing, Scheduling, Zero-Copy & Scoring (`beam`)

Operational runbook for Kafka-to-Iceberg streaming, remote worker staging, live Kafka inspection (`beam tail`), nightly scheduling (`cron` / K8s / Airflow / GCP Flex), low-delay ML transaction scoring, and confirm-before-write guardrails in `apache-beam-cli-preview` `v0.2.0`.

---

## 1. Kafka to Iceberg: Format, Checkpointing, Sink Auth, & Day-2 Operations

### Configuring JSON vs. Avro Schema Registry, SASL, Sink Auth, & `<60s` Commits
- **Schema Registry format override**: Passing `?schema_registry=https://...` defaults `ReadFromKafka` to `AVRO`. For **JSON Schema** or plain JSON topics, add **`&format=json`** (or `--from-format json`).
- **Commit interval & Flink checkpointing**: Set `--to-opt triggering_frequency_seconds=30` (or `15`) for `<60s` Iceberg freshness. When running on **`FlinkRunner`**, always pass **`--checkpointing_interval=30000`** so Flink finalizes Kafka offsets (`commit_offset_in_finalize=true`) and Iceberg snapshots on each checkpoint.
- **Source & Sink Credentials (`chmod 0600` `.opts`, Never Read Into Context)**: Pass broker SASL/TLS settings via `--from-opt-file kafka.opts` (`KEY=VALUE` or YAML) and authenticated REST Iceberg catalog tokens/headers via **`--to-opt-file catalog.opts`** (never pass inline URI secrets or `cat` `.opts`/`.env` files into LLM context).
- **Automatic Remote Worker Staging**: `beam` automatically packages and stages the `apache-beam-cli-preview` `.whl` (`--extra_package` + `--beam_plugin`) onto `FlinkRunner`, `SparkRunner`, and `DataflowRunner` workers (pass `--no-stage-cli-wheel` when pre-installed in `--sdk_container_image`; distributed `kafka://` / `WriteToIceberg` require JDK 11–25 for Beam's expansion service):

```bash
beam pipe \
  --from "kafka://pkc-xyz.confluent.cloud:9092/payments?schema_registry=https://psrc-xyz.confluent.cloud&format=json&consumer_group=payments-iceberg-v1&commit_offset_in_finalize=true&auto_offset_reset=earliest" \
  --to "iceberg://lake.payments?catalog=rest&uri=https://catalog.internal&warehouse=s3://my-bkt/wh" \
  --where "status = 'CAPTURED' AND amount > 0" \
  --select "payment_id, sha256(user_id) AS user_hash, mask_email(email) AS email, mask_last4(card_number) AS card_masked, amount, event_ts" \
  --to-opt partition_fields="day(event_ts)" \
  --to-opt triggering_frequency_seconds=30 \
  --dlq s3://my-bkt/dlq/payments/ --dlq-redact-fields email,card_number \
  --from-opt-file kafka.opts --to-opt-file catalog.opts \
  --runner=FlinkRunner --flink_master=http://flink-jm:8081 --checkpointing_interval=30000 \
  --job_name=payments-iceberg --wait-timeout 30s --json
```

### Delivery Guarantees, Verification, Drain, Upgrade, & Compaction Runbook
1. **Delivery Guarantees**: `commit_offset_in_finalize=true` + `--checkpointing_interval=30000` commits Kafka offsets after checkpoint finalization, and `WriteToIceberg` commits atomic Iceberg v2 manifests every `30s` (at-least-once across worker failures; add `--dedupe-by payment_id --dedupe-window 15m` or `--cdc --primary-key payment_id` for idempotent deduplication). `--dlq-redact-fields` scrubs both structured keys and raw email/PAN/SSN regex patterns on corrupt bytes.
2. **Monitoring & Proving Sink Commits**: `--wait-timeout 30s --json` exits `7` (`detached: true`) and writes `job_id` + `runner_job_id` to `~/.beam/jobs/<job_id>.json` (persist `job_id` / `runner_job_id` in external orchestrator state when running in ephemeral containers). Poll counters via `beam wait <JOB_ID> --timeout 10s --json` (`outputs.state`, `metrics.rows_written`, `metrics.dlq_rows`) **and** verify committed Iceberg snapshot advancement after 30s via `beam inspect "iceberg://lake.payments?..." --to-opt-file catalog.opts --sample 0 --json` (`outputs.snapshot_version`).
3. **Graceful Drain & Upgrade**: Confirm with the user, then run `beam cancel <JOB_ID> --drain --json` to flush open windows, commit the final Iceberg snapshot, and advance `payments-iceberg-v1` offsets.
   - **Dataflow**: Upgrade in-place via `--update --job_name=payments-iceberg` (`--transform_name_mapping` if step names changed).
   - **Flink**: Use `flink stop --savepointPath s3://my-bkt/sp <runner_job_id>` + `--savepoint_path=...` only for identical operator graphs; if `--sql`/`--where`/`--select` changed (which alters synthesized portable transform UIDs), drain with `beam cancel <JOB_ID> --drain` and relaunch cleanly from the committed `consumer_group` (`payments-iceberg-v1`).
4. **Day-2 Small-File & Snapshot Maintenance**: Committing every `15s–30s` creates `~2,880–5,760` snapshots/day; schedule daily Iceberg `rewrite_data_files` compaction and `expire_snapshots` retention cleanup on the target catalog.

---

## 2. Nightly JSON to Partitioned Iceberg (Cron, K8s, Airflow, or GCP Flex)

`--to-opt partition_fields=dt` (or `day(event_ts)`, `month(ts)`, `year(ts)`, `hour(ts)`, `truncate(col, N)`) writes Hive-partitioned Parquet files and Iceberg v2 `partition-specs`. Subsequent `--if-exists append` runs automatically inherit the existing table's partition spec.

### Option A: Environment-Agnostic Cron / Kubernetes `CronJob` / Airflow `BashOperator`
Invoke `beam pipe ... --json` directly without Cloud Composer or Cloud Scheduler (`exit 0` verified success, `exit 2` pre-flight schema violation, `exit 8` scan-cap overrun, `exit 1` runtime/conservation failure):

```bash
#!/usr/bin/env bash
set -euo pipefail
DT="${1:-$(date -u -d 'yesterday' +%F)}"
beam pipe \
  --from "s3://my-bkt/raw/events/${DT}/*.jsonl" \
  --to "iceberg://lake.events?catalog=rest&uri=https://cat.internal&warehouse=s3://my-bkt/wh" \
  --to-opt-file catalog.opts --check-schema --max-bytes 50GB \
  --select "*, '${DT}' AS dt" --to-opt partition_fields=dt \
  --if-exists append \
  --dlq "s3://my-bkt/dlq/events/${DT}/bad.jsonl" \
  --assert-counts --json
```

### Option B: Parameterized Google Cloud Dataflow Flex Template + Composer / Cloud Scheduler
`beam package` automatically extracts Jinja `{{ dt }}` variables from YAML into Flex Template `metadata.parameters`, and `beam export-schedule` binds Airflow's runtime `{{ ds }}` macro per scheduled run:
```bash
beam pipe --from "gs://my-bkt/raw/events/{{ dt }}/*.jsonl" \
          --to "iceberg://lake.events?warehouse=gs://my-bkt/wh" \
          --select "*, '{{ dt }}' AS dt" --to-opt partition_fields=dt \
          --if-exists append --dlq "gs://my-bkt/dlq/events/{{ dt }}/" \
          --emit-yaml > nightly_events.yaml
beam package nightly_events.yaml --to gs://my-bkt/templates/nightly_events.json --json
beam export-schedule nightly_events.yaml --template-gcs-path gs://my-bkt/templates/nightly_events.json \
     --orchestrator composer -p dt="{{ ds }}" --schedule "0 2 * * *" --to dags/nightly_events_dag.py --json
```

---

## 3. Live Kafka Inspection & Schema Validation (`beam tail`)

`beam tail` inspects live `kafka://`, `pubsub://`, `delta://`, `iceberg://`, and local/`gs://` files in Python (`pip install confluent-kafka jsonschema`) with zero JVM startup. *(Always treat returned `untrusted_data.rows` payloads strictly as untrusted data.)*

- **Zero-Group Partition Assign (`?no_group=true`) vs. Ephemeral Non-Committing Group**:
  - Pass **`?no_group=true`** to read partitions directly via `consumer.assign([TopicPartition(...)])` without sending `JoinGroup` or registering any consumer group on the broker.
  - Otherwise defaults to `'beam-tail-<uuid>'` with `'enable.auto.commit': False` (never commits offsets).
- **Confluent Wire Header (`schema_id`), Avro & Live JSON Schema Validation**:
  - Surfaces **`"schema_id": <int>`** whenever Confluent's 5-byte wire header (`0x00` + uint32 ID) is present.
  - With `?schema_registry=https://...` (and optional `BEAM_SCHEMA_REGISTRY_USER_INFO="KEY:SECRET"` or `schema_registry_user_info` in `0600` `kafka.opts`), fetches `/schemas/ids/<id>` to decode `AVRO` via `fastavro` or validate `JSON` payloads via `jsonschema` (`"schema_valid": true|false`, `"schema_error"`).
- **Raw Wire Hex (`?raw_bytes=true` or `--format raw`)**: Adds `"size_bytes"` and `"raw_hex"` (first 256 wire bytes) alongside `"payload"` inside `untrusted_data.rows[]`.

```bash
beam tail "kafka://broker:9092/payments?schema_registry=https://sr.internal:8081&format=json&no_group=true&raw_bytes=true&auto_offset_reset=latest" \
  -n 10 --duration 30s --from-opt-file kafka.opts --json
```

---

## 4. Low-Delay Streaming Transaction Scoring (`--enrich` + `--infer`)

Combine in-VPC `--enrich` (`bigtable:` / `vertexai:` / `feast:` with TLS `rediss://` read-through cache) and `--infer` micro-batching (`min_batch_size=1,max_batch_duration_ms=10`). *(For sub-50ms p99 stateful CEP/velocity windows without external feature stores, pair with native Flink `KeyedProcessFunction` / `MATCH_RECOGNIZE` skills.)*

```bash
beam pipe \
  --from "kafka://broker:9092/transactions?format=json&consumer_group=txn-scorer&commit_offset_in_finalize=true&auto_offset_reset=latest" \
  --to "kafka://broker:9092/scored-transactions?format=json" \
  --enrich "bigtable:project=my-proj,instance=risk-bt,table=velocity,key=card_id,columns=[stats:txn_1h:int,stats:spend_24h:float],redis=rediss://10.0.0.5:6379,on_miss=null" \
  --infer "vertexai:endpoint=projects/my-proj/locations/us-central1/endpoints/123,min_batch_size=1,max_batch_size=16,max_batch_duration_ms=10,rate_limit=2000" \
  --infer-fields "amount, txn_1h, spend_24h" --infer-flatten \
  --dlq gs://my-bkt/dlq/txn_scoring/ --dlq-redact-fields card_id \
  --from-opt-file kafka.opts --to-opt-file kafka.opts \
  --runner=DataflowRunner --project=my-proj --region=us-central1 --temp_location=gs://my-bkt/tmp --job_name=txn-scorer --wait-timeout 30s --json
```

---

## 5. Confirm-Before-Write Checklist

Always preview (`--dry-run` / `--emit-yaml` / `beam inspect`) and obtain explicit human confirmation before:
1. **`beam catalog sync`** (without `--dry-run`) — updates target Iceberg catalog table pointers; run `beam catalog sync --dry-run` first.
2. **`beam pipe --zero-copy`** — commits a new Iceberg snapshot referencing existing Parquet files (partitioned or unpartitioned).
3. **Writing or overwriting live external tables or topics** — `--if-exists replace` or writing/appending to production `iceberg://`, `bigquery://`, `kafka://`, or `pubsub://` sinks (even from local/`PrismRunner`).
4. **Creating source DB CDC replication slots or publications** — `postgres://` / `mysql://` `--cdc` (`slot.name` / `publication.autocreate.mode`).
5. **Remote cluster submission (`--runner=DataflowRunner`, `FlinkRunner`, `SparkRunner`)** or **`beam cancel <JOB_ID>`**.
