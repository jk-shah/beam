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
echo "Showcase completed successfully!"
