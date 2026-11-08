#!/usr/bin/env bash
# End-to-end interactive showcase of the Apache Beam CLI (`beam`) preview.
set -euo pipefail

BEAM_BIN="${BEAM_BIN:-beam}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/beam-cli-showcase.XXXXXX")"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "=================================================================="
echo " Apache Beam CLI Preview Showcase  (workspace: ${WORK_DIR})"
echo "=================================================================="

echo ""
echo "1) Sub-90ms Diagnostics & Self-Discovery (--version, doctor, io)"
"${BEAM_BIN}" --version --json
"${BEAM_BIN}" doctor --json
"${BEAM_BIN}" io describe parquet --json

echo ""
echo "2) Create Dirty Multi-Shard CSV Dataset (multiline note + corrupt rows)"
mkdir -p "${WORK_DIR}/raw"
cat > "${WORK_DIR}/raw/orders-00.csv" <<'EOF'
id,email,amount,note
1,ADA@EXAMPLE.COM,149.95,"Priority order
Leave at front desk"
2,corrupt_missing_columns
3,bob@example.com,-25.00,"Refund adjustment"
EOF

cat > "${WORK_DIR}/raw/orders-01.csv" <<'EOF'
id,email,amount,note
4,GRACE@EXAMPLE.COM,320.50,"Standard shipping"
5,badfloat@example.com,NOT_A_NUMBER,"Corrupt amount cell"
EOF

echo ""
echo "3) Inspect Dirty Source Schema, Stats & Sanitized Sample"
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
echo "   -> Cost guardrail exited with code: ${COST_RC} (expected 8)"

echo ""
echo "5) Zero-Code ETL with --where, --select, --dlq & --assert-counts"
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/raw/orders-*.csv" \
  --to "${WORK_DIR}/clean/orders.parquet" \
  --where "amount > 0 AND id IS NOT NULL" \
  --select "id, lower(email) AS email, round(amount, 2) AS amount, note" \
  --dlq "${WORK_DIR}/dlq/rejected.json" \
  --assert-counts \
  --runner=FnApiRunner \
  --json

echo ""
echo "6) Inspect Clean Parquet Output & Dead-Letter Queue (auto-resolves -00000-of-NNNNN shards)"
"${BEAM_BIN}" inspect "${WORK_DIR}/clean/orders.parquet" --stats --json
"${BEAM_BIN}" tail "${WORK_DIR}/dlq/rejected.json" --limit 5 --json

echo ""
echo "7) Export Portable Beam YAML (--dry-run) & Static Check (beam check)"
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/raw/orders-*.csv" \
  --to "${WORK_DIR}/clean2/orders.parquet" \
  --where "amount > 0" \
  --dry-run > "${WORK_DIR}/pipeline.yaml"
"${BEAM_BIN}" check "${WORK_DIR}/pipeline.yaml" --json

echo ""
echo "8) Distributed O(1)-Memory Reconciliation (beam diff)"
"${BEAM_BIN}" pipe \
  --from "${WORK_DIR}/clean/orders.parquet" \
  --to "${WORK_DIR}/mirror/orders.json" \
  --runner=FnApiRunner \
  --json
"${BEAM_BIN}" diff \
  "${WORK_DIR}/clean/orders.parquet" \
  "${WORK_DIR}/mirror/orders.json" \
  --key id \
  --json

echo ""
echo "9) Zero-Docker Flex Template Packaging (beam package) & Live Follow (beam tail -f)"
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

# Stream live appended JSONL events via `beam tail -f -n 2`
touch "${WORK_DIR}/live_events.jsonl"
(
  sleep 0.3
  printf '{"seq":1,"event":"open"}\n{"seq":2,"event":"purchase"}\n' >> "${WORK_DIR}/live_events.jsonl"
) &
"${BEAM_BIN}" tail -f -n 2 --duration 5s "${WORK_DIR}/live_events.jsonl" --json
wait

echo ""
echo "10) Zero-Copy Iceberg v2 Registration (--zero-copy) & Debezium CDC (--cdc)"
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
  --to "iceberg://lake.orders_cdc?warehouse=${WORK_DIR}/iceberg_wh" \
  --cdc \
  --primary-key id \
  --runner=FnApiRunner \
  --json
"${BEAM_BIN}" inspect "iceberg://lake.orders_cdc?warehouse=${WORK_DIR}/iceberg_wh" --json

echo ""
echo "Showcase completed successfully!"
