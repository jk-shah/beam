# SQL, Multi-Table Quality Pipelines, Masking, & CDC Reference (`beam`)

Reference for expressions, PII masking, multi-table SQL joins with `--expect`, multi-sink Bronze -> Silver -> Gold Beam YAML pipelines, Change Data Capture (`--cdc`), and zero-copy Lakehouse sync in `apache-beam-cli-preview` `v0.2.0`.

---

## 1. `--where` & `--select` Expressions + PII Masking

`beam pipe` compiles `--where`, `--pre-select`, `--post-where`, and `--select` through an AST-allowlisted compiler with SQL three-valued NULL logic (`NULL` comparisons evaluate to `NULL`, never raising `TypeError`):

- **Operators & Casts**: `=` / `==`, `!=` / `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`, `[NOT] IN (...)`, `[NOT] BETWEEN a AND b`, `[NOT] LIKE` / `ILIKE`, `CAST(col AS INT|FLOAT|STRING|BOOL)`, and backticked reserved column names (`` `class` ``, `` `from` ``, `` `in` ``).
- **Scalar & String Helpers**: `lower(s)`, `upper(s)`, `strip(s)` / `trim(s)`, `len(s)` / `length(s)`, `substr(s, start[, len])` / `substring(s, start[, len])` (1-based positive index, negative tail index), `concat(a, b, ...)`, `coalesce(a, b, ...)` / `ifnull(a, b)`, `abs(x)`, `round(x[, n])`, `int(x)`, `float(x)`, `str(x)`, `bool(x)`.
- **PII Masking & Hashing Helpers** (available identically in `--select`, `--pre-select`, `--where`, and local `--sql`):
  - `sha256(val)`: Deterministic lowercase hex SHA-256 digest (`None` -> `None`) for joinable pseudonymous keys.
  - `mask(val[, keep_last=4, mask_char='*'])`: Masks leading characters while keeping the trailing `keep_last` characters (e.g., `mask('4111222233334444')` -> `'************4444'`).
  - `mask_last4(val)`: Shorthand for `mask(val, 4, '*')`.
  - `mask_email(val)`: Preserves first local character and domain (`mask_email('alice@example.com')` -> `'a***@example.com'`).
  - `redact(val)`: Replaces any non-null value with `'<REDACTED>'` (`None` -> `None`).
- **Timestamp & JSON Helpers**: `parse_timestamp(ts)` / `to_utc(ts)`, `epoch_ms(ts)` / `to_epoch_ms(ts)`, `year(ts)`, `month(ts)`, `day(ts)`, `hour(ts)`, `minute(ts)`, `second(ts)`, `date_part(part, ts)`, `json_get(col, '$.a.b')` / `json_value(col, '$.a.b')`.

---

## 2. Multi-Source `--sql` Engines, Portability, & Percentile / Quantile Scales

Pass `--sql "SELECT ..."` or `--sql-file query.sql` across repeatable `--from alias=URI` / `--join alias=URI` inputs (`csv`, `json`, `parquet`, `avro`, `text`, `gs://`, `s3://`, `delta://`, `iceberg://`, `kafka://`, `pubsub://`):

| Engine | Flag / Trigger | Limits & Portability | Supported Dialect Extensions |
| :--- | :--- | :--- | :--- |
| **SQLite** *(default locally)* | Default when no `--runner` is passed | In-memory (`30s` / `1,000,000` result rows via `BEAM_CLI_SQL_MAX_SECONDS` / `BEAM_CLI_SQL_MAX_RESULT_ROWS`); reads/writes local & `gs://`/`s3://` files directly | `to_utc`, `split_part`, `sha256`, `mask`, `mask_last4`, `mask_email`, `redact`, `median(x)`, `percentile(x, 95)`, `quantile(x, 0.95)`, `stddev`, `corr`, `regr_slope`, `OVER (...)` |
| **DuckDB** | `--sql-engine duckdb` (`pip install duckdb`) | In-memory (`512MB` default via `BEAM_CLI_DUCKDB_MAX_MEMORY`, external file access disabled inside SQL) | `PERCENTILE_CONT(0.95) WITHIN GROUP (...)`, `QUALIFY`, `GROUPING SETS`, nested structs |
| **Python Lowering** | `--sql-engine python` (or TVFs without JVM) | Pure-Python Beam YAML lowering (no Java required locally or on cloud runners) | Single-source `GROUP BY` / `HAVING`, `TUMBLE`/`HOP`/`SESSION` TVFs, 2-source `ASOF JOIN` & `BETWEEN ... INTERVAL` |
| **Beam Calcite** | `--runner=DataflowRunner\|FlinkRunner\|SparkRunner` or `--portable` | Distributed execution (requires Java 11–25 expansion service) | Standard Apache Calcite SQL; pass `--portable` locally to reject SQLite-only functions pre-flight (exit `2`) |

- **`percentile` vs. `quantile` Scale Agreement**:
  - **`percentile(col, p)`** takes **`p` in `0..100`** (percentage scale, e.g., `percentile(latency_ms, 95)`).
  - **`quantile(col, q)`** takes **`q` in `0.0..1.0`** (fraction scale, e.g., `quantile(latency_ms, 0.95)`).

---

## 3. Multi-Table Pipelines with Quality Rules (`--expect` & Multi-Sink YAML)

### Pattern A: Multi-Table SQL Join + Per-Rule `--expect` Routing
`--sql` composes directly with repeatable `--expect "<name>: <boolean_expr> -> dlq|drop|fail"`:
- `fail`: Aborts immediately with exit code `1` (`ExpectationViolationError`) on broken primary-key or critical invariants.
- `dlq`: Routes violating rows to `--dlq` (`step: "CliExpect"`, `error_type: "ExpectationViolation"`) while writing clean rows to `--to`.
- `drop`: Drops violating rows cleanly and records `expectation_dropped_<rule>` and `expectation_dropped_rows` in `metrics`.

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
When one job must materialize **multiple output tables** (e.g., Silver orders + Gold tier rollups + shared DLQ) with quality rules between layers, define a multi-transform YAML file, validate offline with `beam check medallion.yaml --json`, and run locally or on `FlinkRunner` / `DataflowRunner` with `beam run medallion.yaml --json`:

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
        catalog_name: hadoop
        catalog_properties: {type: hadoop, warehouse: "s3://my-bkt/wh"}
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
        catalog_name: hadoop
        catalog_properties: {type: hadoop, warehouse: "s3://my-bkt/wh"}
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

## 4. Two-Stream Kafka Joins (`INTERVAL` & `ASOF`)

Join two `kafka://` topics using per-URI query options (`?format=json|avro`, `?schema_registry=https://...`, `?timestamp_policy=CreateTime|LogAppendTime`, `?consumer_group=...`, `?commit_offset_in_finalize=true`) and `--max-watermark-skew` state protection on `FlinkRunner` or `DataflowRunner`:

```bash
# Stream-stream temporal interval join (clicks -> orders within 15m)
beam pipe \
  --from "c=kafka://broker:9092/clicks?format=json&timestamp_policy=CreateTime&consumer_group=attr-clicks&commit_offset_in_finalize=true" \
  --join "o=kafka://broker:9092/orders?format=json&timestamp_policy=CreateTime&consumer_group=attr-orders&commit_offset_in_finalize=true" \
  --to "iceberg://lake.attributed_orders?warehouse=s3://my-bkt/wh" \
  --to-opt triggering_frequency_seconds=30 \
  --sql "SELECT o.order_id, o.user_id, c.campaign_id, o.amount, o.order_ts
         FROM c JOIN o ON c.user_id = o.user_id
                      AND o.order_ts BETWEEN c.click_ts AND c.click_ts + INTERVAL '15' MINUTE" \
  --max-watermark-skew 30m --skew-action dlq --dlq s3://my-bkt/dlq/skew/ \
  --from-opt-file kafka.opts --runner=FlinkRunner --flink_master=http://flink-jm:8081 --json

# Point-in-time ASOF LEFT JOIN (latest quote <= 5s prior to each trade)
beam pipe \
  --from "t=kafka://broker:9092/trades?format=json&auto_offset_reset=latest" \
  --join "q=kafka://broker:9092/quotes?format=json&auto_offset_reset=latest" \
  --to "iceberg://lake.priced_trades?warehouse=s3://my-bkt/wh" \
  --to-opt triggering_frequency_seconds=15 \
  --sql "SELECT t.trade_id, t.symbol, t.qty, q.bid, q.ask, t.trade_ts
         FROM t ASOF LEFT JOIN q
           MATCH_CONDITION (t.trade_ts >= q.quote_ts AND t.trade_ts <= q.quote_ts + INTERVAL '5' SECOND)
           ON t.symbol = q.symbol" \
  --from-opt-file kafka.opts --runner=FlinkRunner --flink_master=http://flink-jm:8081 --json
```

---

## 5. PostgreSQL / MySQL CDC & Partitioned Delta Lake -> Iceberg

### Pattern A: PostgreSQL to Iceberg Replication (`ReadFromDebezium` -> Merge-on-Read)
Requires logical WAL (`wal_level = logical`) and `REPLICATION` privileges on PostgreSQL. Store credentials and Debezium slot settings in a `chmod 0600` `pg.opts` file (`username=replicator`, `password=...`, `connection_properties=slot.name=beam_orders_slot;publication.autocreate.mode=filtered`):

```bash
beam pipe \
  --from "postgres://pg.internal:5432/prod?table=public.orders" \
  --to "iceberg://lake.orders?warehouse=s3://my-bkt/wh" \
  --cdc --primary-key order_id \
  --to-opt triggering_frequency_seconds=30 \
  --from-opt-file pg.opts \
  --runner=FlinkRunner --flink_master=http://flink-jm:8081 --wait-timeout 30s --json
```
- **Delivery & Restart Semantics**: `ReadFromDebezium` takes a consistent `initial` snapshot then tails WAL changes from `slot.name`. Iceberg commits `mode=merge-on-read` snapshots (`equality-deletes` on `order_id`), deduplicating replays within and across commit windows by `ts_ms`/LSN. Draining (`beam cancel <JOB_ID> --drain`) and restarting with the same `slot.name` resumes from the last acknowledged LSN without re-snapshotting.

### Pattern B: Partitioned or Unpartitioned Delta Lake to Iceberg (`--zero-copy` + Incremental `--cdc`)
`beam catalog sync` and `beam pipe --zero-copy` support **both partitioned and unpartitioned** `delta://`, `iceberg://`, and Hive-partitioned Parquet (`col=val/*.parquet`) sources (`0` data bytes copied, preserving partition specs). If the source Delta table uses v3+ Deletion Vectors or Column Mapping, omit `--zero-copy` (`pip install deltalake`) so live rows are rewritten into clean Parquet.

```bash
# 1. Read-only dry-run preview first, then ask user confirmation before catalog write
beam catalog sync --dry-run "delta://s3://my-bkt/delta/orders" "iceberg://lake.orders?warehouse=s3://my-bkt/wh" --json
V0=$(beam inspect "delta://s3://my-bkt/delta/orders" --sample 0 --json | jq -r '.outputs.snapshot_version')

# 2. After user confirmation: register snapshot V0 + verify equality (exit 6 on mismatch)
beam pipe --from "delta://s3://my-bkt/delta/orders?version=${V0}" \
          --to "iceberg://lake.orders?warehouse=s3://my-bkt/wh" \
          --zero-copy --verify --verify-key order_id --json

# 3. Incremental Delta Change Data Feed catch-up [V0+1 .. V1] into Iceberg Merge-on-Read
V1=$(beam inspect "delta://s3://my-bkt/delta/orders" --sample 0 --json | jq -r '.outputs.snapshot_version')
beam pipe --from "delta://s3://my-bkt/delta/orders?start_version=$((V0 + 1))&end_version=${V1}" \
          --to "iceberg://lake.orders?warehouse=s3://my-bkt/wh" \
          --cdc --primary-key order_id --if-exists append --json
```

---

## 6. Schema Mismatch Exit Code Disambiguation

| Scenario | Command / Flag | Exit Code | Error Type / Envelope Signal |
| :--- | :--- | :--- | :--- |
| **Pre-flight contract violation** (before compute runs) | `beam schema [diff\|check]` or `beam pipe --check-schema` | **`2`** (`EXIT_USAGE`) | `SchemaContractViolationError` + `next_actions` `CAST(...)` hint |
| **Post-hoc dataset or schema drift** (between materialized datasets) | `beam diff` or `beam pipe --verify` | **`6`** (`EXIT_DIFF_MISMATCH`) | `outputs.schema_diff` & `outputs.differing_keys` |
| **Unquarantined runtime drift** (unseen JSONL key without `--dlq`/`--drop-bad`) | `beam pipe` / `beam run` | **`1`** (`EXIT_PIPELINE_FAILED`) | `SchemaDriftError` (pass `--dlq` or `--from-opt on_unknown_fields=ignore`) |
