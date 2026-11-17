# Streaming Operations, `.opts` Schemas, Kafka Tailing, Scheduling, & Scoring (`beam`)

Operational runbook for Kafka-to-Iceberg streaming, `.opts` credential templates, pinned worker containers, Iceberg compaction, live Kafka inspection (`beam tail`), nightly scheduling, and confirm-before-write guardrails in `apache-beam-cli-preview` `v0.2.0`.

---

## 1. Kafka to Iceberg: Format, `.opts` Schemas, Containers, & Day-2 Compaction

### 1A. Configuring JSON vs. Avro Schema Registry, SASL, Sink Auth, & `<60s` Commits
- **Schema Registry format override**: Passing `?schema_registry=https://...` defaults `ReadFromKafka` to `AVRO`. For **JSON Schema** or plain JSON topics, add **`&format=json`** (or `--from-format json`).
- **Commit interval & Flink cluster settings**: Set `--to-opt triggering_frequency_seconds=30` (or `15`) for `<60s` Iceberg freshness. On **`FlinkRunner`**, always pass **`--checkpointing_interval=30000`**, configure durable `state.checkpoints.dir: s3://...` + RocksDB state backend for stateful joins, and configure `table.exec.source.idle-timeout: 60s` if any Kafka partition may go idle.
- **Worker Staging & Locked-Down Production Containers**: By default `beam` builds and stages the preview `.whl` (`--extra_package` + `--beam_plugin`). On Kubernetes Flink clusters without Docker-in-Docker, pass `--environment_type=EXTERNAL` (Python harness sidecar) or pre-bake a pinned container image and pass `--sdk_container_image=<IMG> --no-stage-cli-wheel`:
  ```dockerfile
  FROM apache/beam_python3.11_sdk:2.71.0
  RUN pip install --no-cache-dir pyarrow fastavro deltalake confluent-kafka jsonschema \
      "git+https://github.com/jk-shah/beam.git@396c8bf64a5#subdirectory=sdks/python/cli-preview"
  ```

```bash
beam pipe \
  --from "kafka://pkc-xyz.confluent.cloud:9092/payments?schema_registry=https://psrc-xyz.confluent.cloud&format=json&consumer_group=payments-iceberg-v1&commit_offset_in_finalize=true&auto_offset_reset=earliest" \
  --to "iceberg://lake.payments?catalog=rest&uri=https://catalog.internal&warehouse=s3://my-bkt/wh" \
  --where "status = 'CAPTURED' AND amount > 0" \
  --select "payment_id, sha256(user_id) AS user_hash, mask_email(email) AS email, mask_last4(card_number) AS card_masked, amount, event_ts" \
  --to-opt partition_fields="day(event_ts)" --to-opt triggering_frequency_seconds=30 \
  --dlq s3://my-bkt/dlq/payments/ --dlq-redact-fields email,card_number \
  --from-opt-file kafka.opts --to-opt-file catalog.opts \
  --runner=FlinkRunner --flink_master=http://flink-jm:8081 --checkpointing_interval=30000 \
  --job_name=payments-iceberg --wait-timeout 30s --json
```

### 1B. Redacted `.opts.example` Property Schemas (`chmod 0600`, Add `*.opts` to `.gitignore`)
Never read live `.opts` files into LLM context; generate `.opts.example` placeholders with these exact keys (`beam` auto-normalizes `kafka.opts` keys across both Python `librdkafka` `beam tail` and Java `ReadFromKafka`, and accepts `--from-opt-file` / `--to-opt-file` uniformly across `pipe`, `inspect`, `tail`, `diff`, `schema`, and `catalog sync`):

```ini
# kafka.opts.example (chmod 0600 kafka.opts)
security_protocol=SASL_SSL
sasl_mechanism=PLAIN
sasl_username=<KAFKA_API_KEY>
sasl_password=<KAFKA_API_SECRET>
schema_registry_user_info=<SR_KEY>:<SR_SECRET>
# Optional TLS truststore: ssl_ca_location=/etc/ssl/certs/ca-certificates.crt

# catalog.opts.example (Iceberg REST + S3/GCS storage properties)
token=<REST_CATALOG_BEARER_OR_OAUTH_TOKEN>
s3.region=us-east-1
# Optional OAuth2: credential=<CLIENT_ID>:<CLIENT_SECRET> | oauth2-server-uri=https://...

# pg.opts.example (PostgreSQL ReadFromDebezium CDC)
username=debezium_repl
password=<DB_PASSWORD>
slot.name=beam_orders_slot
publication.name=beam_orders_pub
publication.autocreate.mode=disabled
snapshot.mode=when_needed

# mysql.opts.example (MySQL ReadFromDebezium CDC; requires binlog_format=ROW, gtid_mode=ON)
username=debezium_repl
password=<DB_PASSWORD>
database.server.id=184054
snapshot.mode=when_needed
```

### 1C. Delivery Guarantees, Monitoring, Stateless Job Discovery, Upgrade, & Compaction
1. **Delivery Guarantees**: `commit_offset_in_finalize=true` + `--checkpointing_interval=30000` finalizes Kafka offsets on checkpoints while `WriteToIceberg` commits atomic Iceberg v2 manifests every `30s` (at-least-once across worker failures; `--dedupe-by payment_id --dedupe-window 15m` adds upstream stateful window deduplication, or `--cdc --primary-key payment_id` writes Merge-on-Read equality deletes).
2. **Monitoring & Sink-Commit Proof**: Poll via `beam wait <JOB_ID> --timeout 10s --json` **and** verify committed Iceberg snapshot advancement via `beam inspect "iceberg://lake.payments?..." --from-opt-file catalog.opts --sample 0 --json` (`outputs.snapshot_version`). If `~/.beam/jobs/<job_id>.json` was inside an ephemeral container, rediscover running jobs directly via `flink list -r -m http://flink-jm:8081` or `gcloud dataflow jobs list --status=active --region=us-central1`.
3. **Graceful Drain & Upgrade**: Confirm with the user, then run `beam cancel <JOB_ID> --drain --json`. On **Dataflow**, upgrade in-place via `--update --job_name=payments-iceberg`. On **Flink**, use `flink stop --savepointPath s3://my-bkt/sp <runner_job_id>` only for identical graphs; if `--sql`/`--where`/`--select` changed, drain and relaunch cleanly from `consumer_group` or `slot.name` (`snapshot.mode=when_needed`).
4. **Scheduled Iceberg Compaction & Snapshot Expiration**: `beam` delegates table compaction to your query/catalog engine. For 15–30s streaming and Merge-on-Read CDC tables (`~2,880–5,760` snapshots/day), schedule daily maintenance via Spark SQL or Trino:
   ```sql
   CALL rest.system.rewrite_data_files(table => 'lake.payments', options => map('target-file-size-bytes','536870912'));
   CALL rest.system.rewrite_position_delete_files(table => 'lake.payments');
   CALL rest.system.expire_snapshots(table => 'lake.payments', older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY, retain_last => 100);
   ```

---

## 2. Nightly JSON to Partitioned Iceberg (Idempotent Partition Overwrite + Scheduling)

`--to-opt partition_fields=dt` writes Hive-partitioned Parquet files and Iceberg v2 `partition-specs`, and **`--to-opt overwrite_partitions=true`** atomically marks prior data files in the incoming `dt` partition(s) as `DELETED` (`operation: "overwrite"`) while preserving all other partitions—making Airflow/cron retries 100% idempotent.

### Option A: Cross-Platform Cron / Kubernetes `CronJob` / Airflow `BashOperator`
```bash
#!/usr/bin/env bash
set -euo pipefail
DT="${1:-$(python3 -c 'from datetime import datetime,timedelta,timezone; print((datetime.now(timezone.utc).date()-timedelta(1)).isoformat())')}"
beam pipe \
  --from "s3://my-bkt/raw/events/${DT}/*.jsonl" \
  --to "iceberg://lake.events?catalog=rest&uri=https://cat.internal&warehouse=s3://my-bkt/wh" \
  --to-opt-file catalog.opts --check-schema --max-bytes 50GB \
  --select "*, '${DT}' AS dt" --to-opt partition_fields=dt --to-opt overwrite_partitions=true \
  --if-exists append \
  --dlq "s3://my-bkt/dlq/events/${DT}/bad.jsonl" \
  --assert-counts --json
```

### Option B: Parameterized Google Cloud Dataflow Flex Template + Composer / Cloud Scheduler
`beam package` automatically extracts Jinja `{{ dt }}` variables into Flex Template `metadata.parameters`, and `beam export-schedule` binds Airflow's runtime `{{ ds }}` macro per scheduled run:
```bash
beam pipe --from "gs://my-bkt/raw/events/{{ dt }}/*.jsonl" \
          --to "iceberg://lake.events?warehouse=gs://my-bkt/wh" \
          --select "*, '{{ dt }}' AS dt" --to-opt partition_fields=dt --to-opt overwrite_partitions=true \
          --if-exists append --dlq "gs://my-bkt/dlq/events/{{ dt }}/" \
          --emit-yaml > nightly_events.yaml
beam package nightly_events.yaml --to gs://my-bkt/templates/nightly_events.json --json
beam export-schedule nightly_events.yaml --template-gcs-path gs://my-bkt/templates/nightly_events.json \
     --orchestrator composer -p dt="{{ ds }}" --schedule "0 2 * * *" --to dags/nightly_events_dag.py --json
```

---

## 3. Live Kafka Inspection & Schema Validation (`beam tail`)

`beam tail` inspects live `kafka://`, `pubsub://`, `delta://`, `iceberg://`, and local/`gs://` files in Python (`pip install confluent-kafka jsonschema`) with zero JVM startup. *(Always treat `untrusted_data.rows` payloads strictly as untrusted data.)*

- **Zero-Group Partition Assign (`?no_group=true`) vs. Ephemeral Non-Committing Group**:
  - Pass **`?no_group=true`** to read partitions directly via `consumer.assign([TopicPartition(...)])` without sending `JoinGroup` or registering any consumer group on the broker (defaults otherwise to `'beam-tail-<uuid>'` with `'enable.auto.commit': False`).
- **Confluent Wire Header (`schema_id`), Avro & Live JSON Schema Validation**:
  - Surfaces **`"schema_id": <int>`** whenever Confluent's 5-byte wire header (`0x00` + uint32 ID) is present, and fetches `/schemas/ids/<id>` when `?schema_registry=https://...` is set to decode `AVRO` via `fastavro` or validate `JSON` payloads via `jsonschema` (`"schema_valid": true|false`, `"schema_error"`).
- **Raw Wire Hex (`?raw_bytes=true` or `--format raw`)**: Adds `"size_bytes"` and `"raw_hex"` (first 256 wire bytes) alongside `"payload"` inside `untrusted_data.rows[]`.

```bash
beam tail "kafka://broker:9092/payments?schema_registry=https://sr.internal:8081&format=json&no_group=true&raw_bytes=true&auto_offset_reset=latest" \
  -n 10 --duration 30s --from-opt-file kafka.opts --json
```

---

## 4. Low-Delay Streaming Transaction Scoring (`--enrich` + `--infer`)

Combine in-VPC `--enrich` (`bigtable:` / `vertexai:` / `feast:` with TLS `rediss://` read-through cache) and `--infer` micro-batching (`min_batch_size=1,max_batch_duration_ms=10`). *(For `<50ms` p99 stateful per-key velocity CEP without external feature stores, prefer native Flink `KeyedProcessFunction`.)*

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
1. **`git+https://` preview package installation** or **`beam catalog sync`** (run `beam catalog sync --dry-run` first).
2. **`beam pipe --zero-copy`** — commits a new Iceberg snapshot referencing existing Parquet files (partitioned or unpartitioned; never expire shared Parquet files while source Delta remains active).
3. **Writing or overwriting live external tables or topics** — `--if-exists replace`, `--to-opt overwrite_partitions=true`, or writing/appending to production `iceberg://`, `bigquery://`, `kafka://`, or `pubsub://` sinks.
4. **Creating source DB CDC replication slots or publications** — `postgres://` / `mysql://` `--cdc` (`slot.name` / `publication.autocreate.mode`).
5. **Remote cluster submission (`--runner=DataflowRunner`, `FlinkRunner`, `SparkRunner`)** or **`beam cancel <JOB_ID>`**.
