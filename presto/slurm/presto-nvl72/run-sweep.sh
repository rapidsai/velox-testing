# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/bin/bash
# ==============================================================================
# Presto TPC-H Benchmark Sweep
# ==============================================================================
# Runs launch-run.sh + post_results.py for every combination of nodes and
# scale factors defined below.
#
# Usage: ./run-sweep.sh [OPTIONS]
#
# Required options:
#   --sku-name        Hardware SKU name (e.g. raplab-gb200-nvl72)
#   --velox-branch    Velox branch used to build the worker image
#   --presto-branch   Presto branch used to build the worker image
#   --velox-repo      Velox repository URL
#   --presto-repo     Presto repository URL
#
# Optional:
#   -n, --nodes          Space-separated node counts to sweep (default: "8")
#   -s, --scale-factors  Space-separated scale factors to sweep (default: "30000")
#   -i, --iterations     Number of benchmark iterations (default: 3)
#   --cache-state        Override cache state (default: derived from iterations:
#                        1 iteration -> "lukewarm", 2+ iterations -> "warm")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${SCRIPT_DIR}/defaults.env"

# ------------------------------------------------------------------------------
# Argument parsing
# ------------------------------------------------------------------------------

NODE_COUNTS=(8)
SCALE_FACTORS=(30000)
ITERATIONS=3
SKU_NAME=""
CACHE_STATE=""
VELOX_BRANCH=""
PRESTO_BRANCH=""
VELOX_REPO=""
PRESTO_REPO=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sku-name)         SKU_NAME="$2";       shift 2 ;;
        --cache-state)      CACHE_STATE="$2";    shift 2 ;;
        --velox-branch)     VELOX_BRANCH="$2";   shift 2 ;;
        --presto-branch)    PRESTO_BRANCH="$2";  shift 2 ;;
        --velox-repo)       VELOX_REPO="$2";     shift 2 ;;
        --presto-repo)      PRESTO_REPO="$2";    shift 2 ;;
        -n|--nodes)         read -ra NODE_COUNTS <<< "$2"; shift 2 ;;
        -s|--scale-factors) read -ra SCALE_FACTORS <<< "$2"; shift 2 ;;
        -i|--iterations)    ITERATIONS="$2";     shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

for req in SKU_NAME VELOX_BRANCH PRESTO_BRANCH VELOX_REPO PRESTO_REPO; do
    [[ -n "${!req}" ]] || { echo "Error: --${req//_/-} is required"; exit 1; }
done

if [[ -z "${CACHE_STATE}" ]]; then
    [[ "${ITERATIONS}" -eq 1 ]] && CACHE_STATE="lukewarm" || CACHE_STATE="warm"
fi

# Seconds to wait between runs to allow the previous job's cudf exchange UCX
# sockets to release their ports (10003, 10013, ...).  These ports are
# deterministic (http_port+3 per worker) so a new job on the same nodes will
# collide if the previous job's containers haven't fully torn down yet.
INTER_RUN_SLEEP=90

# ------------------------------------------------------------------------------

total=$(( ${#NODE_COUNTS[@]} * ${#SCALE_FACTORS[@]} ))
run=0

for SF in "${SCALE_FACTORS[@]}"; do
    for N in "${NODE_COUNTS[@]}"; do
        run=$(( run + 1 ))
        OUTPUT_DIR="${RESULTS_BASE}/result_sf${SF}_n${N}"

        echo "========================================"
        echo "Run ${run}/${total}: nodes=${N} scale_factor=${SF}"
        echo "Output: ${OUTPUT_DIR}"
        echo "========================================"

        rm -rf "${OUTPUT_DIR}"
        "${SCRIPT_DIR}/launch-run.sh" \
            -n "${N}" \
            -s "${SF}" \
            -i "${ITERATIONS}" \
            -o "${OUTPUT_DIR}"

        echo ""
        echo "Posting results for sf=${SF} n=${N}..."

        "${VT_ROOT}/scripts/run_py_script.sh" \
            -p "${VT_ROOT}/benchmark_reporting_tools/post_results.py" \
            "${OUTPUT_DIR}" \
            --sku-name "${SKU_NAME}" \
            --storage-configuration-name "raplab-nvl72-tpch-rs-float-no-delta-scale-${SF}" \
            --cache-state "${CACHE_STATE}" \
            --benchmark-name "tpch-rs-${SF}" \
            --velox-branch "${VELOX_BRANCH}" \
            --presto-branch "${PRESTO_BRANCH}" \
            --velox-repo "${VELOX_REPO}" \
            --presto-repo "${PRESTO_REPO}"

        echo ""
        echo "Done: sf=${SF} n=${N}"

        if (( run < total )); then
            echo "Waiting ${INTER_RUN_SLEEP}s for worker UCX ports to be released before next run..."
            sleep "${INTER_RUN_SLEEP}"
        fi
        echo ""
    done
done

echo "========================================"
echo "Sweep complete: ${total} runs finished."
echo "========================================"
