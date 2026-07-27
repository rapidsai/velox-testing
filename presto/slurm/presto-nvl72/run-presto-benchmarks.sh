#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -exuo pipefail

# ==============================================================================
# Presto TPC-H Benchmark Execution Script
# ==============================================================================
# This script runs the actual benchmark execution after environment is configured
# by the slurm launcher script. All configuration is passed via environment vars.

# Source helper functions
source $SCRIPT_DIR/echo_helpers.sh
source $SCRIPT_DIR/functions.sh

function collect_coordinator_queries {
    if [[ -x "${SCRIPT_DIR}/collect-coordinator-queries.sh" && -n "${COORD:-}" && -n "${PORT:-}" ]]; then
        "${SCRIPT_DIR}/collect-coordinator-queries.sh" \
            --server "http://${COORD}:${PORT}" \
            --output-dir "${RESULT_DIR:-${SCRIPT_DIR}/result_dir}/coordinator_queries" || true
    fi
}

# Ensure metadata/log collection runs even if the script exits early (e.g. a
# query OOMs).  DEBUG_HOLD_ON_FAILURE_SECONDS keeps the Slurm allocation alive
# long enough to inspect the coordinator and worker logs before teardown.
function on_exit {
    local rc=$?
    trap - EXIT

    # A perf capture may still be active when pytest or verification fails.
    # Stop it and publish every node-local raw capture before Slurm tears down
    # worker 0. Optional report generation also runs here, after all queries.
    stop_perf_sampler || true
    inject_benchmark_metadata || true

    if [[ "${rc}" != "0" ]]; then
        collect_coordinator_queries
        echo "Benchmark failed with exit code ${rc}; collecting logs before teardown."
        collect_results || true

        if [[ -n "${DEBUG_HOLD_ON_FAILURE_SECONDS:-}" && "${DEBUG_HOLD_ON_FAILURE_SECONDS}" != "0" ]]; then
            echo "Holding failed cluster for ${DEBUG_HOLD_ON_FAILURE_SECONDS}s."
            echo "Coordinator: http://${COORD}:${PORT}"
            echo "Logs: ${LOGS}"
            echo "Result dir: ${RESULT_DIR:-${SCRIPT_DIR}/result_dir}"
            sleep "${DEBUG_HOLD_ON_FAILURE_SECONDS}" || true
        fi
    fi

    if ! stop_cluster; then
        echo "Cluster cleanup did not complete cleanly." >&2
        [[ "${rc}" == "0" ]] && rc=1
    fi
    # Capture teardown and port-release diagnostics as well as startup logs.
    collect_results || true

    exit "${rc}"
}
trap on_exit EXIT

# ==============================================================================
# Setup and Validation
# ==============================================================================
echo "Setting up Presto environment..."
setup

# ==============================================================================
# Start Coordinator + Workers
# ==============================================================================
start_cluster

# ==============================================================================
# Run Queries
# ==============================================================================
echo "Running TPC-H queries (${NUM_ITERATIONS} iterations, scale factor ${SCALE_FACTOR})..."
run_queries ${NUM_ITERATIONS} ${SCALE_FACTOR}
# Host perf must publish every node-local raw capture before worker teardown.
# Optional post-processing happens only now, after all measured queries. This is
# a no-op for ordinary and nsys runs.
stop_perf_sampler
collect_coordinator_queries
verify_cpu_ucx_runtime

# ==============================================================================
# Process Results
# ==============================================================================
echo "Processing results..."
mkdir -p "${RESULT_DIR:-${SCRIPT_DIR}/result_dir}"
cp -r "${LOGS}/cli.log" "${RESULT_DIR:-${SCRIPT_DIR}/result_dir}/summary.txt"

echo "Collecting configs and logs into result directory..."
collect_results

wait_for_nsys_report_generation
stop_cluster
# Include explicit shutdown and port-release diagnostics in the immutable
# job-specific result directory.
collect_results

echo "========================================"
echo "Benchmark complete!"
echo "Results saved to: ${RESULT_DIR:-${SCRIPT_DIR}/result_dir}"
echo "Logs available at: ${LOGS}"
echo "========================================"
