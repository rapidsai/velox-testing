# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/bin/bash
# ==============================================================================
# Presto TPC-H Benchmark Sweep
# ==============================================================================
# Runs launch-run.sh + post_results.py for every combination of nodes and
# scale factors defined below.
#
# Usage: ./run-sweep.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ------------------------------------------------------------------------------
# Sweep configuration — edit these arrays to change what gets benchmarked
# ------------------------------------------------------------------------------

NODE_COUNTS=(8)
SCALE_FACTORS=(30000)
#NODE_COUNTS=(8 4 2)
#SCALE_FACTORS=(3000 10000)
ITERATIONS=3

# post_results.py fixed arguments
SKU_NAME="raplab-gb200-nvl72"
CACHE_STATE="warm"
VELOX_BRANCH="ibm-research-preview_2026_03_03_pr16201_pr16488"
PRESTO_BRANCH="ibm-research-preview_2026_03_03_and_fixes_and_PR27215"
VELOX_REPO="https://github.com/karthikeyann/velox"
PRESTO_REPO="https://github.com/karthikeyann/presto"

#VELOX_BRANCH="ibm-research-preview_2026_03_11"
#PRESTO_BRANCH="ibm-research-preview_2026_03_11"
#VELOX_REPO="https://github.com/IBM/velox"
#PRESTO_REPO="https://github.com/prestodb/presto"

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
        OUTPUT_DIR="${HOME}/Misiu/result_sf${SF}_n${N}"

        echo "========================================"
        echo "Run ${run}/${total}: nodes=${N} scale_factor=${SF}"
        echo "Output: ${OUTPUT_DIR}"
        echo "========================================"

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
