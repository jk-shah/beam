# SQL, Multi-Table Quality Pipelines, Masking, & CDC Reference (`beam`)

Reference for expressions, PII masking & raw-line DLQ redaction, multi-table SQL joins with `--expect`, multi-sink Bronze -> Silver -> Gold Beam YAML pipelines, Change Data Capture (`--cdc`), and cross-store Lakehouse reconciliation in `apache-beam-cli-preview` `v0.2.0`.

---

## 1. `--where`, `--select`, Dirty CSV Typing, & Raw-Line PII DLQ Scrubbing

`beam pipe` compiles `--where`, `--pre-select`, `--post-where`, and `--select` through an AST-allowlisted compiler with SQL three-valued NULL logic (`NULL` comparisons evaluate to `NULL`, never raising `TypeError`):

- **Explicit Dirty-CSV Column Typing (`--from-opt fields=...`)**: When a CSV column contains `1–2%` non-numeric strings (e.g., `"N/A"` in `amount`), pass `--from-opt fields=amount:float` so `amount` stays numeric (`FLOAT`) rather than widening to `STRING`; unparseable numeric cells and broken CSV quotes route straight to `--dlq` with `--dlq-redact-fields`.
- **Operators & Casts**: `=` / `==`, `!=` / `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`, `[NOT] IN (...)`, `[NOT] BETWEEN a AND b`, `[NOT] LIKE` / `ILIKE`, `CAST(col AS INT|FLOAT|STRING|BOOL)`, and backticked reserved column names (`` `class` ``, `` `from` ``, `` `in` ``).
- **Scalar & String Helpers**: `lower(s)`, `upper(s)`, `strip(s)` / `trim(s)`, `len(s)` / `length(s)`, `substr(s, start[, len])` / `substring(s, start[, len])`, `concat(a, b, ...)`, `coalesce(a, b, ...)` / `ifnull(a, b)`, `abs(x)`, `round(x[, n])`, `int(x)`, `float(x)`, `str(x)`, `bool(x)`.
- **PII Masking & Hashing Helpers**:
  - `sha256(val)`: Deterministic lowercase hex SHA-256 digest (`None` -> `None`) for joinable pseudonymous keys.
  - `mask(val[, keep_last=4, mask_char='*'])` / `mask_last4(val)`: Masks leading characters (`mask_last4('4111222233334444')` -> `'************4444'`).
  - `mask_email(val)`: Preserves first local character and domain (`mask_email('alice@example.com')` -> `'a***@example.com'`).
  - `redact(val)`: Replaces any non-null value with `'<REDACTED>'` (`None` -> `None`).
  - *Portability*: Available in `--select` / `--where` across **all** runners (`local_fast`, `FlinkRunner`, `DataflowRunner`) and in local SQLite `--sql`. On distributed Calcite `--sql`, keep standard SQL inside `--sql` and apply `mask_email(...)` / `sha256(...)` in `--select`.
- **DLQ Redaction on Corrupt Raw Lines (`--dlq-redact-fields`)**:
  - Replaces named keys/columns with `'<REDACTED>'` on parsed records (`local_fast` stays enabled for files `<= 256 MiB`) **and** falls back to key-value + regex scrubbing of emails, 13–19 digit payment card PANs, and `XXX-XX-XXXX` SSNs when raw CSV quoting or truncated JSON fails parsing before column binding.

---

## 2. Multi-Source `--sql` Engines, Portability, & Percentile / Quantile Scales

Pass `--sql "SELECT ..."` or `--sql-file query.sql` across repeatable `--from alias=URI` / `--join alias=URI` inputs (`csv`, `json`, `parquet`, `avro`, `text`, `gs://`, `s3://`, `delta://`, `iceberg://`, `kafka://`, `pubsub://`):

| Engine | Flag / Automatic Trigger | Limits & Portability | Supported Dialect Extensions |
| :--- | :--- | :--- | :--- |
| **SQLite** *(default locally)* | Default when no `--runner` is passed | In-memory (`30s` / `1,000,000` result rows via `BEAM_CLI_SQL_MAX_SECONDS` / `BEAM_CLI_SQL_MAX_RESULT_ROWS`); reads/writes local & `gs://`/`s3://` `--to` and `--dlq` files | `to_utc`, `split_part`, `sha256`, `mask`, `mask_last4`, `mask_email`, `redact`, `median(x)`, `percentile(x, 95)`, `quantile(x, 0.95)`, `stddev`, `corr`, `OVER (...)` |
| **DuckDB** | `--sql-engine duckdb` (`pip install duckdb`) | Vectorized C++ in-memory (**`2GB`** default via `BEAM_CLI_DUCKDB_MAX_MEMORY`, external SQL file access disabled for sandbox safety) | `QUANTILE_CONT(x, 0.95)`, `PERCENTILE_CONT(0.95) WITHIN GROUP (...)`, `QUALIFY`, `GROUPING SETS`, nested structs |
| **Python Lowering** | `--sql-engine python` **or automatic** for 2-source `ASOF JOIN` (on all runners) & local TVFs | Pure-Python stateful Beam lowering (no Java required for `ASOF JOIN` / TVFs; runs locally and on `FlinkRunner` / `DataflowRunner`) | 2-source `ASOF [LEFT] JOIN`, single-source `GROUP BY` / `HAVING`, `TUMBLE`/`HOP`/`SESSION` TVFs, 2-source `BETWEEN ... INTERVAL` |
| **Beam Calcite** | `--runner=DataflowRunner\|FlinkRunner\|SparkRunner` or `--portable` (non-`ASOF` SQL) | Distributed execution (requires Java 11–25 expansion service) | Standard Apache Calcite SQL; pass `--portable` locally to reject SQLite-only functions pre-flight (exit `2`) |

- **`percentile` vs. `quantile` Scale Agreement**:
  - **`percentile(col, p)`** takes **`p` in `0..100`** (percentage scale, e.g., `percentile(latency_ms, 95)`).
  - **`quantile(col, q)`** (and DuckDB `QUANTILE_CONT(col, q)`) takes **`q` in `0.0..1.0`** (fraction scale, e.g., `quantile(latency_ms, 0.95)`).

---

## 3. Multi-Table Pipelines with Quality Rules (`--expect` & Multi-Sink YAML)

### Pattern A: Multi-Table SQL Join + Per-Rule `--expect` Routing
`--sql` composes directly with repeatable `--expect "<name>: <sql_or_python_expr> -> dlq|drop|fail"` (`fail` exits `1`; `dlq` routes violating rows with rule metadata to `--dlq`; `drop` increments `expectation_dropped_rows`):

```bash
beam pipe \
  --from o=orders.parquet --from c=customers.parquet --from fx=fx_rates.csv \
  --to out/curated_orders.parquet --dlq out/quality_dlq.jsonl \
  --sql "SELECT o.order_id, o.customer_id, c.tier, ROUND(o.amount * fx.usd_rate, 2) AS amount_usd
         FROM o LEFT JOIN c ON o.customer_id = c.customer_id LEFT JOIN fx ON o.currency = fx.currency" \
  --expect "pk_not_null: order_id IS NOT NULL -> fail" \
  --expect "known_tier: tier IS NOT NULL -> dlq" \
  --expect "bounded_usd: amount_usd > 0 AND amount_usd <= 1000000 -> dlq" \
  --json
```

### Pattern B: Multi-Sink Bronze -> Silver -> Gold DAG in One Beam YAML File
Validate offline with `beam check medallion.yaml -p catalog_token=placeholder --json` and run with `beam run medallion.yaml -p catalog_token="$BEAM_ICEBERG_TOKEN" --json` (injecting secrets at runtime via `-p` from environment variables rather than hardcoding in YAML; use Python boolean syntax `is not None` / `and` inside raw YAML `CliExpect` `rules:`):

```yaml
pipeline:
  transforms:
    - name: ReadOrders
      type: ReadFromParquet
      config: {path: "s3://my-bkt/bronze/orders/*.parquet"}
    - name: ReadCustomers
      type: ReadFromParquet
      config: {path: "s3://my-bkt/bronze/customers/*.parquet"}
    - name: JoinSilver
      type: Sql
      input: {o: ReadOrders, c: ReadCustomers}
      config:
        query: "SELECT o.order_id, o.customer_id, c.tier, o.amount FROM o LEFT JOIN c ON o.customer_id = c.customer_id"
    - name: SilverQuality
      type: CliExpect
      input: JoinSilver
      config:
        rules:
          - {name: pk_ok, expression: "order_id is not None", on_violation: fail}
          - {name: positive_amt, expression: "amount > 0 and tier is not None", on_violation: dlq}
        error_handling: {output: errors}
    - name: WriteSilver
      type: WriteToIceberg
      input: SilverQuality.good
      config:
        table: lake.silver_orders
        catalog_name: rest
        catalog_properties: {type: rest, uri: "https://cat.internal", warehouse: "s3://my-bkt/wh", token: "{{ catalog_token }}"}
    - name: RollupGold
      type: Sql
      input: SilverQuality.good
      config:
        query: "SELECT tier, COUNT(*) AS order_cnt, SUM(amount) AS total_rev FROM PCOLLECTION GROUP BY tier"
    - name: WriteGold
      type: WriteToIceberg
      input: RollupGold
      config:
        table: lake.gold_revenue_by_tier
        catalog_name: rest
        catalog_properties: {type: rest, uri: "https://cat.internal", warehouse: "s3://my-bkt/wh", token: "{{ catalog_token }}"}
    - name: NormalizeQualityDlq
      type: NormalizeDlq
      input: SilverQuality.errors
      config: {step: CliExpect}
    - name: WriteDlq
      type: WriteToJson
      input: NormalizeQualityDlq
      config: {path: "s3://my-bkt/dlq/medallion_bad.jsonl"}
providers:
  - type: python
    config: {}
    transforms:
      CliExpect: apache_beam.cli.expect.cli_expect
      NormalizeDlq: apache_beam.cli.dlq.normalize_dlq_records
```

---

## 4. Two-Stream Kafka Joins (`INTERVAL` & `ASOF`) & Event-Time Watermarks

- **Automatic Per-Stream Payload Event-Time Assignment & Out-of-Orderness**: On 2-source `BETWEEN ... INTERVAL` and `ASOF` SQL joins, `beam pipe` automatically extracts **both** payload event-time columns (`c.click_ts` + `o.order_ts`, or `t.trade_ts` + `q.quote_ts`) into `CliAssignTimestamps` on each stream, and **`--allowed-lateness 5m`** retains keyed join state for late out-of-order arrivals.
- **Lagging-Stream State Protection & Auth**: Pass `--max-watermark-skew 30m --skew-action dlq` to bound state-buffer growth when one topic lags, configure Flink RocksDB state backend + `table.exec.source.idle-timeout` for idle partitions, and pass **both** `--from-opt-file kafka.opts` and `--join-opt-file kafka.opts`:

```bash
# Stream-stream temporal interval join (clicks -> orders within 15m)
beam pipe \
  --from "c=kafka://broker:9092/clicks?format=json&timestamp_policy=CreateTime&consumer_group=attr-clicks&commit_offset_in_finalize=true" \
  --join "o=kafka://broker:9092/orders?format=json&timestamp_policy=CreateTime&consumer_group=attr-orders&commit_offset_in_finalize=true" \
  --to "iceberg://lake.attributed_orders?warehouse=s3://my-bkt/wh" --to-opt triggering_frequency_seconds=30 \
  --allowed-lateness 5m \
  --sql "SELECT o.order_id, o.user_id, c.campaign_id, o.amount, o.order_ts
         FROM c JOIN o ON c.user_id = o.user_id
                      AND o.order_ts BETWEEN c.click_ts AND c.click_ts + INTERVAL '15' MINUTE" \
  --max-watermark-skew 30m --skew-action dlq --dlq s3://my-bkt/dlq/skew/ \
  --from-opt-file kafka.opts --join-opt-file kafka.opts --to-opt-file catalog.opts \
  --runner=FlinkRunner --flink_master=http://flink-jm:8081 --checkpointing_interval=30000 --job_name=attr-join --wait-timeout 30s --json

# Point-in-time ASOF LEFT JOIN (auto-lowered to stateful Python CoGroup/Timers on local & Flink/Dataflow)
beam pipe \
  --from "t=kafka://broker:9092/trades?format=json&timestamp_policy=CreateTime&consumer_group=priced-trades&commit_offset_in_finalize=true" \
  --join "q=kafka://broker:9092/quotes?format=json&timestamp_policy=CreateTime&consumer_group=priced-quotes&commit_offset_in_finalize=true" \
  --to "iceberg://lake.priced_trades?warehouse=s3://my-bkt/wh" --to-opt triggering_frequency_seconds=15 \
  --allowed-lateness 1m \
  --sql "SELECT t.trade_id, t.symbol, t.qty, q.bid, q.ask, t.trade_ts
         FROM t ASOF LEFT JOIN q
           MATCH_CONDITION (t.trade_ts >= q.quote_ts AND t.trade_ts <= q.quote_ts + INTERVAL '5' SECOND)
           ON t.symbol = q.symbol" \
  --from-opt-file kafka.opts --join-opt-file kafka.opts --to-opt-file catalog.opts \
  --runner=FlinkRunner --flink_master=http://flink-jm:8081 --checkpointing_interval=30000 --job_name=priced-trades --wait-timeout 30s --json
```

---

## 5. PostgreSQL / MySQL CDC, BigQuery CDC, & Cross-Store Reconciliation

### Pattern A: PostgreSQL & MySQL CDC (`ReadFromDebezium` -> Iceberg Merge-on-Read or BigQuery UPSERT)
Obtain explicit user/DBA confirmation before creating replication slots or publications, and use `chmod 0600` `pg.opts` / `mysql.opts` (key schemas in [cloud-and-flex.md](cloud-and-flex.md) §1B):
- **PostgreSQL prerequisites**: `wal_level = logical`, `REPLICATION` role, `ALTER TABLE public.orders REPLICA IDENTITY FULL` if non-PK or TOASTed columns require before-images on `DELETE`/`UPDATE`, `snapshot.mode=when_needed` in `pg.opts` (so stateless job relaunches resume from `slot.name`'s `confirmed_flush_lsn` instead of re-snapshotting OLTP tables), `max_slot_wal_keep_size` + `pg_replication_slots` lag alerts, and `SELECT pg_drop_replication_slot('beam_orders_slot');` upon permanent teardown. Additive `ALTER TABLE ADD COLUMN` evolves Iceberg nullable columns automatically (`schema-id` increments).
- **MySQL prerequisites**: `binlog_format = ROW`, `binlog_row_image = FULL`, `gtid_mode = ON`, `enforce_gtid_consistency = ON`, and a unique `database.server.id` in `mysql.opts`.
- **BigQuery CDC sink prerequisite**: Target table must declare `PRIMARY KEY (order_id) NOT ENFORCED` in BigQuery DDL (`Storage Write API` `UPSERT`/`DELETE` change stream).

```bash
# PostgreSQL CDC -> Iceberg v2 Merge-on-Read (equality-deletes)
beam pipe --from "postgres://pg.internal:5432/prod?table=public.orders" \
          --to "iceberg://lake.orders?catalog=rest&uri=https://cat.internal&warehouse=s3://my-bkt/wh" \
          --cdc --primary-key order_id --to-opt triggering_frequency_seconds=30 \
          --from-opt-file pg.opts --to-opt-file catalog.opts \
          --runner=FlinkRunner --flink_master=http://flink-jm:8081 --checkpointing_interval=30000 --job_name=pg-orders-cdc --wait-timeout 30s --json

# MySQL CDC -> BigQuery Storage Write API UPSERT/DELETE
beam pipe --from "mysql://mysql.internal:3306/shop?table=shop.orders" \
          --to "bigquery://acme-analytics:core.orders" \
          --cdc --primary-key order_id --from-opt-file mysql.opts \
          --runner=DataflowRunner --project=acme-analytics --region=us-central1 --temp_location=gs://my-bkt/tmp --job_name=mysql-bq-cdc --wait-timeout 30s --json
```

### Pattern B: Partitioned Delta Lake -> Iceberg Zero-Copy & Cross-Warehouse `beam diff`
`beam catalog sync` and `beam pipe --zero-copy` support **both partitioned and unpartitioned** `delta://`, `iceberg://`, and Hive Parquet (`col=val/*.parquet`) sources (`bytes_copied: 0`, writing `schema.name-mapping.default` + partition specs; Delta v3+ Deletion Vectors / Column Mapping rejected). When running `beam diff` against a live mutating source, pin `?version=${V0}` / `?snapshot_id=...` or bound `--where "updated_at < '...'"` so in-flight OLTP mutations do not trigger false-positive `exit 6`:

```bash
beam catalog sync --dry-run "delta://s3://my-bkt/delta/shipments" "iceberg://lake.shipments?catalog=rest&uri=https://cat.internal&warehouse=s3://my-bkt/wh" --to-opt-file catalog.opts --json
V0=$(beam inspect "delta://s3://my-bkt/delta/shipments" --sample 0 --json | jq -r '.outputs.snapshot_version')
# After user confirmation:
beam pipe --from "delta://s3://my-bkt/delta/shipments?version=${V0}" --to "iceberg://lake.shipments?catalog=rest&uri=https://cat.internal&warehouse=s3://my-bkt/wh" \
          --to-opt-file catalog.opts --zero-copy --verify --verify-key shipment_id --runner=FlinkRunner --flink_master=http://flink-jm:8081 --json

# Cross-warehouse reconciliation (e.g., 40M-row Delta on S3 vs BigQuery; normalizes INT32/INT64 & DECIMAL/NUMERIC; --rtol/--atol for floats)
beam schema diff "delta://s3://my-bkt/delta/orders" "bigquery://acme-analytics:core.orders" --from-opt-file aws.opts --json
beam diff "delta://s3://my-bkt/delta/orders?version=${V0}" "bigquery://acme-analytics:core.orders" \
          --key order_id --rtol 1e-6 --from-opt-file aws.opts \
          --runner=DataflowRunner --project=acme-analytics --region=us-central1 --temp_location=gs://my-bkt/tmp --json
```

---

## 6. Schema Mismatch Exit Code Disambiguation

| Scenario | Command / Flag | Exit Code | Error Type / Envelope Signal |
| :--- | :--- | :--- | :--- |
| **Pre-flight contract violation** (before compute runs) | `beam schema [diff\|check]` or `beam pipe --check-schema` | **`2`** (`EXIT_USAGE`) | `SchemaContractViolationError` + `next_actions` `CAST(...)` hint |
| **Post-hoc dataset or schema drift** (between materialized datasets) | `beam diff` or `beam pipe --verify` | **`6`** (`EXIT_DIFF_MISMATCH`) | `outputs.schema_diff` & `outputs.differing_keys` |
| **Unquarantined runtime drift** (unseen JSONL key without `--dlq`/`--drop-bad`) | `beam pipe` / `beam run` | **`1`** (`EXIT_PIPELINE_FAILED`) | `SchemaDriftError` (pass `--dlq` or `--from-opt on_unknown_fields=ignore`) |
