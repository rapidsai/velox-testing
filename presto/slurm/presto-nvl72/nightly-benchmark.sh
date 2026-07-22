#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# Nightly Presto Benchmark Runner
# ==============================================================================
# Runs in a persistent tmux/screen session, sleeping until 3 AM EST (08:00 UTC)
# each day, then:
#   1. Pulls the latest nightly-pinned coordinator and GPU worker images from GHCR
#      into a temporary directory (does not overwrite IMAGE_DIR images).
#   2. Runs a 1-node / 4-GPU TPC-H sf1000 benchmark.
#   3. Runs a 2-node / 8-GPU TPC-H sf3000 benchmark.
#   4. Posts results to the benchmarking DB via post_results.py.
#   5. Removes the temporary images.
#
# Required environment variables (no defaults — set before launching):
#   SKU_NAME                  Hardware SKU name (--sku-name for post_results.py)
#   STORAGE_CONFIG_1K         --storage-configuration-name for the sf1000 run
#   STORAGE_CONFIG_3K         --storage-configuration-name for the sf3000 run
#
# Note: BENCHMARK_API_URL and BENCHMARK_API_KEY must also be set in the
# environment so that post_results.py can authenticate with the DB.
#
# Optional environment variables:
#   ITERATIONS      Number of benchmark iterations per run (default: 5)
#   RUN_HOUR_UTC    Hour (UTC) at which to fire each night (default: 8, i.e. 3 AM EST)
#
# Usage:
#   export SKU_NAME="..."
#   export STORAGE_CONFIG_1K="..."
#   export STORAGE_CONFIG_3K="..."
#   export BENCHMARK_API_URL="..."
#   export BENCHMARK_API_KEY="..."
#   tmux new -s nightly-presto
#   ./nightly-benchmark.sh
#
# To trigger immediately (e.g. for testing), pass --now:
#   ./nightly-benchmark.sh --now

set -euo pipefail

RUN_NOW=0
for arg in "$@"; do
    case "${arg}" in
        --now) RUN_NOW=1 ;;
        *) echo "Unknown option: ${arg}" >&2; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${SCRIPT_DIR}/defaults.env"
source "${SCRIPT_DIR}/echo_helpers.sh"

# ------------------------------------------------------------------------------
# Image constants — the -latest tags are only updated by nightly pinned CI runs
# ------------------------------------------------------------------------------
REGISTRY="ghcr.io/rapidsai/velox-testing-images"
COORD_TAG="presto-coordinator-latest"
WORKER_TAG="presto-latest-gpu-cuda13.1"

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
: "${ITERATIONS:=5}"
: "${RUN_HOUR_UTC:=8}"

NIGHTLY_IMAGE_DIR="${IMAGE_DIR}/nightly-temp"
COORD_SQSH="${NIGHTLY_IMAGE_DIR}/presto-coordinator-nightly.sqsh"
WORKER_SQSH="${NIGHTLY_IMAGE_DIR}/presto-gpu-cuda13.1-nightly.sqsh"

INTER_RUN_SLEEP=90

# Validate required env vars before entering the loop
for req in SKU_NAME STORAGE_CONFIG_1K STORAGE_CONFIG_3K; do
    [[ -n "${!req:-}" ]] || echo_error "Required env var ${req} is not set"
done

# Append all output to a persistent log file
mkdir -p "${RESULTS_BASE}"
LOG_FILE="${RESULTS_BASE}/nightly-benchmark.log"
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "Nightly benchmark daemon started (logging to ${LOG_FILE})"
echo "  RUN_HOUR_UTC=${RUN_HOUR_UTC}  ITERATIONS=${ITERATIONS}"

# ------------------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------------------

sleep_until_next_run() {
    local now next target_time next_str delay
    now=$(date -u +%s)
    target_time=$(printf '%02d:00:00' "${RUN_HOUR_UTC}")
    next=$(date -u -d "$(date -u +%Y-%m-%d) ${target_time}" +%s)
    if [[ ${next} -le ${now} ]]; then
        next=$(( next + 86400 ))
    fi
    delay=$(( next - now ))
    next_str=$(date -u -d "@${next}" '+%Y-%m-%d %H:%M:%S UTC')
    echo "Sleeping ${delay}s until next run at ${next_str}..."
    sleep "${delay}"
}

pull_images() {
    mkdir -p "${NIGHTLY_IMAGE_DIR}"
    echo "Pulling coordinator image (${COORD_TAG})..."
    "${SCRIPT_DIR}/pull_ghcr_image.sh" \
        "${REGISTRY}:${COORD_TAG}" \
        --output "${COORD_SQSH}" \
        --overwrite
    echo "Pulling worker image (${WORKER_TAG})..."
    "${SCRIPT_DIR}/pull_ghcr_image.sh" \
        "${REGISTRY}:${WORKER_TAG}" \
        --output "${WORKER_SQSH}" \
        --overwrite
}

run_benchmark() {
    local nodes="$1" gpus_per_node="$2" scale_factor="$3" output_dir="$4"
    rm -rf "${output_dir}"
    "${SCRIPT_DIR}/launch-run.sh" \
        -n "${nodes}" \
        -g "${gpus_per_node}" \
        -s "${scale_factor}" \
        --gpu \
        -c "${COORD_SQSH}" \
        -w "${WORKER_SQSH}" \
        -o "${output_dir}" \
        -i "${ITERATIONS}"
}

post_results() {
    local output_dir="$1" benchmark_name="$2" storage_config="$3"
    "${VT_ROOT}/scripts/run_py_script.sh" \
        -p "${VT_ROOT}/benchmark_reporting_tools/post_results.py" \
        "${output_dir}" \
        --sku-name "${SKU_NAME}" \
        --storage-configuration-name "${storage_config}" \
        --cache-state "warm" \
        --benchmark-name "${benchmark_name}" \
        --label "nightly"
}

cleanup_images() {
    rm -f "${COORD_SQSH}" "${WORKER_SQSH}"
    rmdir --ignore-fail-on-non-empty "${NIGHTLY_IMAGE_DIR}" 2>/dev/null || true
}

run_nightly() {
    local date_tag
    date_tag=$(date -u +%Y%m%d)
    local output_1k="${RESULTS_BASE}/nightly-${date_tag}/tpch-1k"
    local output_3k="${RESULTS_BASE}/nightly-${date_tag}/tpch-3k"

    echo_success "=== Nightly benchmark starting: ${date_tag} ==="
    echo "  Coordinator image: ${REGISTRY}:${COORD_TAG}"
    echo "  Worker image:      ${REGISTRY}:${WORKER_TAG}"
    echo "  Iterations:        ${ITERATIONS}"

    pull_images

    echo ""
    echo "--- Run 1/2: 1-node, 4-GPU, tpch-sf1000 ---"
    run_benchmark 1 4 1000 "${output_1k}"
    echo "Posting results for tpch-1k..."
    post_results "${output_1k}" "tpch-1k" "${STORAGE_CONFIG_1K}"

    echo ""
    echo "Waiting ${INTER_RUN_SLEEP}s for UCX ports to be released before next run..."
    sleep "${INTER_RUN_SLEEP}"

    echo ""
    echo "--- Run 2/2: 2-node, 8-GPU (4/node), tpch-sf3000 ---"
    run_benchmark 2 4 3000 "${output_3k}"
    echo "Posting results for tpch-3k..."
    post_results "${output_3k}" "tpch-3k" "${STORAGE_CONFIG_3K}"

    echo ""
    echo_success "=== Nightly benchmark complete: ${date_tag} ==="
}

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------
while true; do
    [[ "${RUN_NOW}" -eq 1 ]] || sleep_until_next_run
    RUN_NOW=0
    (run_nightly) || echo_warning "Nightly run FAILED — will retry tomorrow"
    cleanup_images || true
done
