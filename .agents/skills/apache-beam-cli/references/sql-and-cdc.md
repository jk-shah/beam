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
*(Pass `--portable` / `--strict-beam` or inspect `outputs.portable_to_runner` to verify that a local `--sql` query uses strictly ANSI/Calcite constructs supported on distributed Beam runners such as `DataflowRunner`.)*

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

```bash
# Apply 60-second tumbling windows by event_ts and emit Beam YAML
beam pipe --from src=events.jsonl \
          --timestamp-col event_ts \
          --window fixed:60s \
          --to out/windowed.parquet \
          --emit-yaml
```

When `--emit-yaml` is passed, `beam` generates idiomatic `AssignTimestamps` and `WindowInto` YAML steps compatible with Dataflow and Beam runners.

---

## 5. Cross-Engine CDC Replication

Replicate CDC streams or snapshots with state deduplication, retaining the latest record per primary key:
```bash
# Replicate from stream/file and keep latest state per primary key
beam pipe --from src=cdc_updates.parquet \
          --to out/current_state.parquet \
          --cdc --primary-key account_id \
          --cdc-seq-col updated_at \
          --assert-counts --json
```
- Deduplication sorts by `lsn`, `ts_ms`, `seq_ts`, `seq`, `updated_at`, `event_ts`, `timestamp`, or an explicit `--cdc-seq-col`.
- Rows marked as deleted (`op = 'd'` or `is_deleted = true`) are pruned automatically.

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
