#!/usr/bin/env bash
# End-to-end interactive 12-step showcase of the Apache Beam CLI (`beam`) preview.
set -euo pipefail

BEAM_BIN="${BEAM_BIN:-beam}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/beam-cli-showcase.XXXXXX")"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "=================================================================="
echo " Apache Beam CLI Preview v0.2.0 Showcase  (workspace: ${WORK_DIR})"
echo "=================================================================="

echo ""
echo "1) Sub-90ms Diagnostics & Self-Discovery (--version, doctor, io)"
"${BEAM_BIN}" --version --json
"${BEAM_BIN}" doctor --json
"${BEAM_BIN}" io describe parquet --json

echo ""
echo "2) Create Dirty Multi-Shard CSV Dataset (multiline note, leading-zero zip, corrupt rows)"
mkdir -p "${WORK_DIR}/raw"
cat > "${WORK_DIR}/raw/orders-00.csv" <<'EOF'
id,email,region,zip,amount,ordered_at,note
1,ADA@EXAMPLE.COM,east,07030,149.95,2026-11-08T10:15:00-05:00,"Priority order
Leave at front desk"
2,corrupt_missing_columns
3,bob@example.com,west,94107,-25.00,2026-11-08T08:00:00-08:00,"Refund adjustment"
EOF

cat > "${WORK_DIR}/raw/orders-01.csv" <<'EOF'
id,email,region,zip,amount,ordered_at,note
4,GRACE@EXAMPLE.COM,east,10001,320.50,2026-11-08T16:30:00Z,"Standard shipping"
5,badfloat@example.com,west,98101,NOT_A_NUMBER,2026-11-08T17:00:00Z,"Corrupt amount cell"
EOF

echo ""
echo "3) Inspect Dirty Source Schema, Stats & Sanitized Sample (zip preserved as str)"
"${BEAM_BIN}" inspect "${WORK_DIR}/raw/orders-*.csv" --stats --sample 3 --json

echo ""
echo "4) Cost Guardrail Pre-Flight Check (--max-bytes 10B -> Exit Code 8)"
set +e
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/raw/orders-*.csv" \
  --to "${WORK_DIR}/blocked/out.parquet" \
  --max-bytes 10B \
  --json
COST_RC=$?
set -e
test "${COST_RC}" -eq 8
echo "   -> Verified cost guardrail exited with code: ${COST_RC} (EXIT_COST_LIMIT = 8)"

echo ""
echo "5) Sub-90ms Zero-Code ETL (--where, --select, --dlq & --assert-counts)"
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/raw/orders-*.csv" \
  --to "${WORK_DIR}/clean/orders.parquet" \
  --where "amount > 0 AND id IS NOT NULL" \
  --select "id, lower(email) AS email, region, zip, round(amount, 2) AS amount, ordered_at, note" \
  --dlq "${WORK_DIR}/dlq/rejected.jsonl" \
  --assert-counts \
  --json

echo ""
echo "6) Atomic --if-exists replace Guarantee (failed run preserves existing output intact)"
ORIG_BYTES="$(wc -c < "${WORK_DIR}/clean/orders.parquet" | tr -d ' ')"
set +e
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/raw/orders-*.csv" \
  --to "${WORK_DIR}/clean/orders.parquet" \
  --if-exists replace \
  --json >/dev/null 2>&1
FAIL_RC=$?
set -e
AFTER_BYTES="$(wc -c < "${WORK_DIR}/clean/orders.parquet" | tr -d ' ')"
test "${FAIL_RC}" -ne 0 && test "${ORIG_BYTES}" = "${AFTER_BYTES}"
echo "   -> Failed replacement exited ${FAIL_RC}; previous orders.parquet intact (${AFTER_BYTES} bytes)"

echo ""
echo "7) Local Analytical SQL (--sql, multi-source --join, median/quantile/to_utc, keyed JSON)"
cat > "${WORK_DIR}/fx_rates.csv" <<'EOF'
region,tax_rate
east,0.08
west,0.09
EOF
"${BEAM_BIN}" pipe \
  --from "o=${WORK_DIR}/clean/orders.parquet" \
  --join "t=${WORK_DIR}/fx_rates.csv" \
  --sql "SELECT o.region, COUNT(*) AS orders, ROUND(SUM(o.amount * (1 + t.tax_rate)), 2) AS gross_total, median(o.amount) AS median_amt, MIN(to_utc(o.ordered_at)) AS first_order_utc FROM o JOIN t ON o.region = t.region GROUP BY o.region" \
  --to "${WORK_DIR}/summary/by_region.json" \
  --to-opt key=region \
  --json

echo ""
echo "8) Late JSONL Schema-Drift Protection (>100 rows -> quarantined to --dlq + explicit warning)"
python3 - "${WORK_DIR}/events_120.jsonl" <<'PY'
import json, sys
with open(sys.argv[1], 'w', encoding='utf-8') as f:
  for i in range(1, 121):
    rec = {'id': i, 'kind': 'click'}
    if i == 105:
      rec['promo_code'] = 'SAVE20'
    f.write(json.dumps(rec) + '\n')
PY
BEAM_CLI_JSON_AUTO_SCAN_MAX_BYTES=0 "${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/events_120.jsonl" \
  --to "${WORK_DIR}/events_clean.parquet" \
  --dlq "${WORK_DIR}/dlq/events_drift.jsonl" \
  --assert-counts \
  --json

echo ""
echo "9) Export Portable Beam YAML (--emit-yaml) & Static Validation (beam check)"
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/raw/orders-*.csv" \
  --to "${WORK_DIR}/clean2/orders.parquet" \
  --where "amount > 0" \
  --emit-yaml > "${WORK_DIR}/pipeline.yaml"
"${BEAM_BIN}" check "${WORK_DIR}/pipeline.yaml" --json

echo ""
echo "10) Distributed Reconciliation (beam diff --columns, --to report.json & --exit-zero)"
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/clean/orders.parquet" \
  --to "${WORK_DIR}/mirror/orders.jsonl" \
  --json
"${BEAM_BIN}" diff \
  "${WORK_DIR}/clean/orders.parquet" \
  "${WORK_DIR}/mirror/orders.jsonl" \
  --key id \
  --columns id,email,region,amount \
  --to "${WORK_DIR}/diff_report.json" \
  --exit-zero \
  --json

echo ""
echo "11) Zero-Docker Flex Template Packaging (beam package) & Live Follow (beam tail -f)"
cat > "${WORK_DIR}/parameterized.yaml" <<'EOF'
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config:
        path: "{{ input_csv }}"
    - type: Filter
      config:
        language: python
        keep:
          callable: "lambda row: float(row.amount) >= {{ min_amount | default(100) }}"
    - type: WriteToParquet
      config:
        path: "{{ output_parquet }}"
EOF
"${BEAM_BIN}" package "${WORK_DIR}/parameterized.yaml" \
  --to "${WORK_DIR}/orders_flex_template.json" \
  --project=demo-project \
  --region=us-central1 \
  --dry-run \
  --json

touch "${WORK_DIR}/live_events.jsonl"
(
  sleep 0.3
  printf '{"seq":1,"event":"open"}\n{"seq":2,"event":"purchase"}\n' >> "${WORK_DIR}/live_events.jsonl"
) &
"${BEAM_BIN}" tail -f -n 2 --duration 5s "${WORK_DIR}/live_events.jsonl" --json
wait

echo ""
echo "12) Zero-Copy Iceberg v2 Registration (--zero-copy) & Debezium CDC Materialization (--cdc)"
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/clean/orders.parquet" \
  --to "iceberg://lake.orders_zc?warehouse=${WORK_DIR}/iceberg_wh" \
  --zero-copy \
  --json
cat > "${WORK_DIR}/cdc_events.jsonl" <<'EOF'
{"op":"c","source":{"lsn":101},"after":{"id":1,"email":"ada@example.com","amount":149.95}}
{"op":"c","source":{"lsn":102},"after":{"id":2,"email":"bob@example.com","amount":50.00}}
{"op":"u","source":{"lsn":105},"after":{"id":1,"email":"ada@example.com","amount":199.95}}
{"op":"d","source":{"lsn":106},"before":{"id":2,"email":"bob@example.com","amount":50.00},"after":null}
EOF
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/cdc_events.jsonl" \
  --to "${WORK_DIR}/cdc_live_orders.parquet" \
  --cdc --primary-key id \
  --json
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/cdc_events.jsonl" \
  --to "iceberg://lake.orders_cdc?warehouse=${WORK_DIR}/iceberg_wh" \
  --cdc --primary-key id \
  --runner=FnApiRunner \
  --json
"${BEAM_BIN}" inspect "iceberg://lake.orders_cdc?warehouse=${WORK_DIR}/iceberg_wh" --json

echo ""
echo "All 12 showcase steps completed successfully!"
