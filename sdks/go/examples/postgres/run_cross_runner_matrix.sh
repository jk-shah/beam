#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="${SCRIPT_DIR}"
DOT_DIR="/tmp/beam_test_graphs"
LOG_DIR="/tmp/beam_runner_test_logs"
mkdir -p "${DOT_DIR}" "${LOG_DIR}"

GCP_PROJECT="${GCP_PROJECT:-beam-test-project}"
GCP_REGION="${GCP_REGION:-us-central1}"
GCS_STAGING="${GCS_STAGING:-gs://beam-test-staging-bucket/staging}"
JOB_ENDPOINT="${JOB_ENDPOINT:-localhost:8073}"

echo "================================================================================"
echo " Apache Beam Go PostgreSQL Cross-Runner Compatibility Matrix Test"
echo "================================================================================"
echo "Script Base:   ${BASE_DIR}"
echo "Dot Graphs:    ${DOT_DIR}"
echo "Log Directory: ${LOG_DIR}"
echo "Job Endpoint:  ${JOB_ENDPOINT}"
echo "GCP Project:   ${GCP_PROJECT} (${GCP_REGION})"
echo "================================================================================"

# Array of all batch examples
BATCH_EXAMPLES=(
  "vectorized_batch_etl:${BASE_DIR}/vectorized_batch_etl"
  "dead_letter_queue:${BASE_DIR}/dead_letter_queue"
  "deduplication:${BASE_DIR}/deduplication"
  "relational_enrichment:${BASE_DIR}/relational_enrichment"
  "scd_type2:${BASE_DIR}/scd_type2"
  "multi_dimensional_olap:${BASE_DIR}/advanced_use_cases/multi_dimensional_olap"
  "top_n_ranking:${BASE_DIR}/advanced_use_cases/top_n_ranking"
  "graph_vertex_degrees:${BASE_DIR}/advanced_use_cases/graph_vertex_degrees"
  "ml_feature_engineering:${BASE_DIR}/advanced_use_cases/ml_feature_engineering"
  "sessionization:${BASE_DIR}/advanced_use_cases/sessionization"
  "data_reconciliation_diff:${BASE_DIR}/advanced_use_cases/data_reconciliation_diff"
)

# Streaming example
STREAMING_EXAMPLE="streaming_aggregation:${BASE_DIR}/streaming_aggregation"

RUNNERS=("dot" "direct" "prism" "universal" "flink" "spark" "dataflow")

declare -A RESULTS

run_pipeline() {
  local name="$1"
  local dir="$2"
  local runner="$3"
  local log_file="${LOG_DIR}/${name}_${runner}.log"

  local cmd=""
  case "${runner}" in
    "dot")
      local dot_out="${DOT_DIR}/${name}.dot"
      cmd="go run main.go --runner=dot --dot_file=${dot_out}"
      ;;
    "direct")
      cmd="go run main.go --runner=direct"
      ;;
    "prism")
      cmd="go run main.go --runner=prism"
      ;;
    "universal")
      cmd="go run main.go --runner=universal --endpoint=${JOB_ENDPOINT} --environment_type=LOOPBACK"
      ;;
    "flink")
      cmd="go run main.go --runner=flink --endpoint=${JOB_ENDPOINT} --environment_type=LOOPBACK"
      ;;
    "spark")
      cmd="go run main.go --runner=spark --endpoint=${JOB_ENDPOINT} --environment_type=LOOPBACK"
      ;;
    "dataflow")
      cmd="go run main.go --runner=dataflow --project=${GCP_PROJECT} --region=${GCP_REGION} --staging_location=${GCS_STAGING} --dry_run=true"
      ;;
    *)
      echo "Unknown runner: ${runner}"
      return 1
      ;;
  esac

  local start_ts
  start_ts=$(date +%s%N)

  if (cd "${dir}" && eval "${cmd}") > "${log_file}" 2>&1; then
    local end_ts
    end_ts=$(date +%s%N)
    local dur_ms=$(( (end_ts - start_ts) / 1000000 ))
    RESULTS["${name}:${runner}"]="PASS (${dur_ms}ms)"
  else
    RESULTS["${name}:${runner}"]="FAIL (check log: ${log_file})"
  fi
}

echo ""
echo "--- Testing Batch Pipelines Across All Runners ---"
for item in "${BATCH_EXAMPLES[@]}"; do
  name="${item%%:*}"
  dir="${item##*:}"
  echo "[Testing Example]: ${name}"

  for runner in "${RUNNERS[@]}"; do
    run_pipeline "${name}" "${dir}" "${runner}"
    printf "  -> Runner %-10s : %s\n" "${runner}" "${RESULTS["${name}:${runner}"]}"
  done
done

echo ""
echo "--- Testing Streaming Aggregation Pipeline (Graph & Translation) ---"
s_name="${STREAMING_EXAMPLE%%:*}"
s_dir="${STREAMING_EXAMPLE##*:}"
echo "[Testing Example]: ${s_name}"

# Streaming pipeline graph validation
run_pipeline "${s_name}" "${s_dir}" "dot"
printf "  -> Runner %-10s : %s\n" "dot" "${RESULTS["${s_name}:dot"]}"

# Streaming pipeline Dataflow translation validation
run_pipeline "${s_name}" "${s_dir}" "dataflow"
printf "  -> Runner %-10s : %s\n" "dataflow" "${RESULTS["${s_name}:dataflow"]}"

# For streaming loopback runners, document continuous streaming requirement
for r in "direct" "prism" "universal" "flink" "spark"; do
  RESULTS["${s_name}:${r}"]="STREAMING_CDC (continuous slot listener)"
  printf "  -> Runner %-10s : %s\n" "${r}" "${RESULTS["${s_name}:${r}"]}"
done

echo ""
echo "================================================================================"
echo " Full Compatibility Matrix Summary"
echo "================================================================================"
printf "%-26s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" \
  "Example" "dot" "direct" "prism" "universal" "flink" "spark" "dataflow"
echo "---------------------------+------------+------------+------------+------------+------------+------------+------------"

ALL_EXAMPLES=("${BATCH_EXAMPLES[@]}" "${STREAMING_EXAMPLE}")
for item in "${ALL_EXAMPLES[@]}"; do
  name="${item%%:*}"
  printf "%-26s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" \
    "${name}" \
    "${RESULTS["${name}:dot"]%% *}" \
    "${RESULTS["${name}:direct"]%% *}" \
    "${RESULTS["${name}:prism"]%% *}" \
    "${RESULTS["${name}:universal"]%% *}" \
    "${RESULTS["${name}:flink"]%% *}" \
    "${RESULTS["${name}:spark"]%% *}" \
    "${RESULTS["${name}:dataflow"]%% *}"
done
echo "================================================================================"
