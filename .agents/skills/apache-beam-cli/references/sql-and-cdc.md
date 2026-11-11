# Apache Beam CLI — SQL, Windowing, CDC, and Lakehouse Reference

This guide covers advanced SQL analytics, event-time windowing, Change Data Capture (CDC), and lakehouse bridging with `beam pipe`.

---

## 1. Canonical SQL Inputs and Table Aliasing

To avoid ambiguity, **always bind inputs explicitly using `--from <alias>=<URI>` and query `FROM <alias>`**.

### Single-Input SQL
```bash
beam pipe --from src=data/events.parquet \
          --to out/summary.json \
          --sql "SELECT event_type, COUNT(*) AS count, AVG(latency_ms) AS avg_latency FROM src GROUP BY 1" \
          --json
```

### Multi-Input SQL Joins (Repeatable `--from`)
Repeat `--from <alias>=<URI>` for each table (`--join` is also accepted as an alias). When `--to` writes to a file, `untrusted_data.rows` is omitted by default to keep JSON envelopes compact; add `--preview` to include sampled output rows alongside the file write.
```bash
beam pipe --from o=orders.parquet \
          --from c=customers.csv \
          --from p=products.jsonl \
          --to out/enriched_orders.parquet \
          --sql "SELECT o.order_id, c.customer_name, p.product_name, o.amount FROM o JOIN c ON o.customer_id = c.customer_id JOIN p ON o.product_id = p.product_id" \
          --portable --json
```

### Direct `--select` Shorthand
You can project columns directly without handwriting `--sql "SELECT ..."` boilerplate:
```bash
# Project columns directly to destination or omit --to to print/preview instantly
beam pipe --from src=data/events.parquet \
          --select "event_id, lower(email) AS email, amount" \
          --to out/clean.parquet --json
```

---

## 2. Built-in SQL Functions & Engines

### Default Local Engine (SQLite In-Process)
Small queries execute in-process (`<100ms`) with extended analytical and statistical functions:
- **Timestamp Normalization**: `to_utc(ts)` parses ISO 8601 strings (with or without `Z` or offsets like `+02:00`), naive timestamps, and UNIX epoch timestamps (seconds or milliseconds), converting them to canonical `YYYY-MM-DDTHH:MM:SSZ`.
- **Statistical Aggregates**:
  - `median(x)`, `percentile(x, 0.95)`, `quantile(x, 0.95)` (exact linear interpolation)
  - `stddev(x)`, `variance(x)`, `stddev_pop(x)`, `var_pop(x)`
  - `covar_samp(x, y)`, `covar_pop(x, y)`, `corr(x, y)`
  - `regr_slope(y, x)`, `regr_intercept(y, x)`, `regr_r2(y, x)`
- **Window Functions**:
  `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY to_utc(event_ts) DESC)`

```bash
# Normalize mixed timestamps and rank events
beam pipe --from e=events.parquet \
          --to out/ranked.parquet \
          --sql "SELECT user_id, to_utc(created_at) AS event_time, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY to_utc(created_at) DESC) AS rank FROM e" \
          --json
```

### Alternative Engine: DuckDB (`--sql-engine duckdb`)
For advanced ANSI SQL features like `PERCENTILE_CONT(...) WITHIN GROUP`, `QUALIFY`, or complex nested struct filtering:
```bash
beam pipe --from tx=transactions.parquet \
          --to out/p99_by_region.parquet \
          --sql-engine duckdb \
          --sql "SELECT region, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount) AS p99_amount FROM tx GROUP BY 1" \
          --json
```

### Local SQL Engine vs. Remote Beam Calcite SQL Portability Cliff
Be aware of dialect boundaries when graduating from local testing to distributed cloud runners:
| Construct / Function | Local In-Process Engine | Beam Calcite SQL (Dataflow/Flink/Spark) | Portability Guidance |
| :--- | :--- | :--- | :--- |
| `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | Supported | Supported | Fully portable ANSI standard |
| `ROW_NUMBER() OVER (...)` | Supported | Supported | Fully portable standard windowing |
| `to_utc(ts)` | Supported (Built-in) | Unsupported | Use standard timestamp casting or formatting |
| `median(x)`, `quantile(x, p)` | Supported (Built-in) | Unsupported | Compute via standard quantiles transform in Beam YAML |
| `regr_slope(y, x)`, `corr(x, y)` | Supported (Built-in) | Unsupported | Pre-calculate in Python transform or Beam Combiner |
| `strftime(...)` | Supported (SQLite syntax) | Unsupported | Use Calcite `EXTRACT(...)` or standard date functions |

*(Pass `--portable` / `--strict-beam` or check `outputs.portable_to_runner` on `beam pipe` / `beam check` to verify Calcite compatibility before submitting to Dataflow.)*

---

## 3. Mixed-Format Local Directory Globs

When a local directory contains heterogeneous file formats (e.g. `.csv` and `.jsonl` partitions), `beam pipe` automatically inspects matched extensions, unifies schemas by column name, and loads them into a unified table:
```bash
beam pipe --from src="data/shards/*" \
          --to out/unified.parquet \
          --sql "SELECT tx_id, to_utc(ts) AS ts, val FROM src WHERE val > 0" \
          --json
```

---

## 4. Event-Time Windowing & Timestamps

`beam pipe` supports declarative event-time windowing directly via `--window` and `--timestamp-col`.

Supported window formats:
- **Tumbling**: `--window fixed:60s` (or plain `60s`)
- **Sliding**: `--window sliding:5m,1m` (window size, slide period)
- **Session**: `--window session:30m` (inactivity gap)
- **Inline Aggregations**: `--window "fixed:60s,sum(metric)"` or `--window "fixed:60s,by=user,count(),avg(latency)"` (automatically pairs `WindowInto` with `Combine`)

```bash
# Apply 60-second tumbling windows by event_ts and emit Beam YAML
beam pipe --from src=events.jsonl \
          --timestamp-col event_ts \
          --window "fixed:60s,total_bytes=sum(bytes)" \
          --to out/windowed.parquet \
          --emit-yaml
```

When `--emit-yaml` is passed, `beam` generates idiomatic `AssignTimestamps`, `WindowInto`, and optional `Combine` YAML steps compatible with Dataflow and Beam runners.

---

## 5. Cross-Engine CDC Replication & Lakehouse Bridging

Replicate CDC streams or snapshots with state deduplication, retaining the latest record per primary key (`--primary-key`, `--key`, `--cdc-pk`, and `--pk` are aliases; passing any key flag automatically activates `--cdc`):
```bash
# Replicate from stream/file and keep latest state per primary key
beam pipe --from src=cdc_updates.parquet \
          --to out/current_state.parquet \
          --key account_id \
          --cdc-seq-col updated_at \
          --assert-counts --json
```
- Deduplication sorts by `lsn`, `ts_ms`, `seq_ts`, `seq`, `updated_at`, `event_ts`, `timestamp`, or an explicit `--cdc-seq-col`.
- Rows marked as deleted (`op = 'd'` or `_change_type = 'delete'`) are pruned automatically.

### Lakehouse Bridging Limits & Requirements
1. **Zero-Copy Lakehouse Invariants**: `--zero-copy` translates Delta transaction logs (`_delta_log/*.json`) directly to Iceberg Avro manifests without duplicating underlying Parquet files (`bytes_copied: 0`).
   - Requires **unpartitioned Delta tables**. If a table is partitioned, physical data rewrite is required (`--if-exists append` or standard copy).
   - Requires source and target to reside in the **same cloud storage bucket**.
2. **Delta Deletion Vectors & Column Mapping**: Delta tables using deletion vectors or column mapping require the optional `deltalake` Python package. If missing, `beam` halts with exit code 4 (`EXIT_MISSING_PACKAGE`).

### Zero-Gap Full Sync + Incremental CDC (Never Miss Intermediate Commits)

To avoid losing writes that land **while** the initial full backfill is running, always pin or overlap the CDC watermark before reading the snapshot:

1. **Delta Lake -> Apache Iceberg (Version-Pinned Watermark Handoff)**
   *(Native Java Managed `ReadFromDelta` + `ReadFromDeltaCDC` + `WriteToIceberg(mode="merge-on-read")` in Beam `2.77+`; built-in Python Iceberg v2 / Delta bridge on Beam `2.71–2.76`. Enable `delta.enableChangeDataFeed = true` on Delta before pinning `V0`.)*
   ```bash
   # 1. Pin snapshot version V0 BEFORE initial backfill
   V0=$(beam inspect delta://gs://my-bkt/delta/orders --sample 0 --json | jq -r '.outputs.snapshot_version')

   # 2. Full load at pinned ?version=$V0 (add --zero-copy if unpartitioned on same bucket)
   beam pipe --from "delta://gs://my-bkt/delta/orders?version=${V0}" \
             --to "iceberg://lake.orders?warehouse=gs://my-bkt/wh" \
             --assert-counts --json

   # 3. Pin upper bound V1 and apply [V0+1 .. V1] via Iceberg v2 Merge-on-Read CDC
   V1=$(beam inspect delta://gs://my-bkt/delta/orders --sample 0 --json | jq -r '.outputs.snapshot_version')
   beam pipe --from "delta://gs://my-bkt/delta/orders?start_version=$((V0 + 1))&end_version=${V1}" \
             --to "iceberg://lake.orders?warehouse=gs://my-bkt/wh" \
             --cdc --primary-key id --if-exists append --json

   # 4. Reconcile exact parity at V1 (exits 0 if identical, exits 6 if mismatch)
   beam diff "delta://gs://my-bkt/delta/orders?version=${V1}" \
             "iceberg://lake.orders?warehouse=gs://my-bkt/wh" --key id --json
   ```

2. **PostgreSQL / MySQL + Debezium -> BigQuery or Iceberg (All Core Beam `2.71+`)**
   - **Single-pipeline (`ReadFromDebezium`)**: `beam pipe --from "postgres://host:5432/db?table=public.orders" --from-opt-file pg.yaml --to bigquery://proj.ds.orders --cdc --primary-key id --runner=DataflowRunner ...` performs a consistent `snapshot.mode=initial` read (`op='r'`) at WAL LSN $L_0$ and transitions directly to WAL streaming (`op='c'|'u'|'d'`) with zero gap.
   - **Pub/Sub or Kafka Buffered Debezium**: Start the subscription/topic capture **before** taking the initial snapshot, run the snapshot load, then run `beam pipe --from pubsub://... --to bigquery://... --cdc --primary-key id`. Monotonic `source.lsn` -> `_CHANGE_SEQUENCE_NUMBER` ordering makes overlapping events idempotent.

---

## 6. Keyed JSON Document Outputs

By default, writing to a `.json` file outputs JSON Lines (NDJSON). To produce a single structured JSON object keyed by a column:
```bash
# Single JSON document keyed by region: {"West": {"sales": 100, ...}, "East": ...}
beam pipe --from src=sales.parquet \
          --to out/regions.json \
          --to-opt key=region \
          --json

# Project specific value column: {"West": 100, "East": 250}
beam pipe --from src=sales.parquet \
          --to out/totals.json \
          --to-opt key=region --to-opt value=total_revenue \
          --json
```

---

## 7. Declarative Event-Time Interval Joins (`IntervalJoin` & Beam SQL)

Event-time interval joins correlate records between two streaming or batch collections that occur within a dynamic temporal window (`BETWEEN t_l + lower_bound AND t_l + upper_bound`):

### 1. Beam SQL Interval Joins
`beam pipe --sql` supports standard ANSI / Calcite interval joins:
```bash
beam pipe --from c=clicks.jsonl \
          --from o=orders.jsonl \
          --to out/attributed.jsonl \
          --sql "SELECT c.user_id, c.campaign, o.amount, o.order_time - c.click_time AS lag_sec
                 FROM c JOIN o
                   ON c.user_id = o.user_id
                  AND o.order_time BETWEEN c.click_time AND c.click_time + INTERVAL '15' MINUTE" \
          --json
```
- **Local Engine (<50ms)**: Automatically normalizes `col + INTERVAL '<val>' <unit>` into SQLite scalar functions `BEAM_INTERVAL_ADD` and `BEAM_INTERVAL_SUB`, executing in-process with zero JVM.
- **Remote Engine Portability**: Compiles directly to Apache Calcite Beam SQL for distributed execution on Google Cloud Dataflow (`DataflowRunner`) or portable runners (`PrismRunner`).

### 2. Beam YAML & Python Declarative Interval Joins (`type: IntervalJoin`)
When authoring Beam YAML pipelines, declare interval joins as first-class transforms without writing custom DoFns:
```yaml
pipeline:
  transforms:
    - type: ReadFromJson
      name: ReadClicks
      config:
        file_pattern: 'gs://my-bkt/clicks/*.json'
    - type: ReadFromJson
      name: ReadOrders
      config:
        file_pattern: 'gs://my-bkt/orders/*.json'
    - type: IntervalJoin
      name: CorrelateAttribution
      input:
        left: ReadClicks
        right: ReadOrders
      config:
        key: user_id
        left_time: click_time
        right_time: order_time
        lower_bound: 0s
        upper_bound: 15m
        join_type: inner
        select:
          user_id: left.user_id
          campaign: left.campaign
          amount: right.amount
    - type: WriteToParquet
      name: WriteAttributed
      input: CorrelateAttribution
      config:
        file_pattern_prefix: 'gs://my-bkt/attributed/out'
```

### Execution Architecture & Guarantees
- **Batch Processing**: Groups records by key via `CoGroupByKey` and computes bounded temporal matches in-memory.
- **Streaming on Dataflow Windmill**: Uses stateful `BagStateSpec` buffers (`left_buffer`, `right_buffer`) and event-time watermark timers (`TimeDomain.WATERMARK`). When records arrive, forward and backward interval matches emit immediately, while expired records are garbage-collected as watermarks advance, preventing state leaks.
- **Durations Supported**: `ms`, `s`, `m`, `h`, `d` (e.g. `500ms`, `30s`, `15m`, `2h`, `1d`). Negative lower bounds are supported (e.g. `lower_bound: -5m`, `upper_bound: 10m`).
- **Join Semantics**: Supports `inner`, `left`, and `full` outer joins.
