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

# Ensure metadata injection runs even if the script exits early (e.g. a worker
# fails to register).  This guarantees benchmark_result.json always has a
# context block with image_digest before the results are copied out.
trap 'inject_benchmark_metadata' EXIT

# ==============================================================================
# Setup and Validation
# ==============================================================================
echo "Setting up Presto environment..."
setup

# ==============================================================================
# Start Coordinator
# ==============================================================================
echo "Starting Presto coordinator on ${COORD}..."
run_coordinator
wait_until_coordinator_is_running

# ==============================================================================
# Start Workers
# ==============================================================================
echo "Starting ${NUM_WORKERS} Presto workers across ${NUM_NODES} nodes..."

worker_id=0
for node in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
    for gpu_id in $(seq 0 $((NUM_GPUS_PER_NODE - 1))); do
        echo "  Starting worker ${worker_id} on node ${node} GPU ${gpu_id}"
        run_worker "${gpu_id}" "$WORKER_IMAGE" "${node}" "$worker_id"
        worker_id=$((worker_id + 1))
    done
done

# ==============================================================================
# Wait for Workers to Register
# ==============================================================================
echo "Waiting for ${NUM_WORKERS} workers to register with coordinator..."
wait_for_workers_to_register $NUM_WORKERS

# Not currently needed because we are copying the hive metastore from the data source.
#echo "Creating TPC-H schema and registering tables for scale factor ${SCALE_FACTOR}..."
#setup_benchmark ${SCALE_FACTOR}

# ==============================================================================
# Run Queries
# ==============================================================================
echo "Running TPC-H queries (${NUM_ITERATIONS} iterations, scale factor ${SCALE_FACTOR})..."
run_queries ${NUM_ITERATIONS} ${SCALE_FACTOR}

# ==============================================================================
# Process Results
# ==============================================================================
echo "Processing results..."
mkdir -p ${SCRIPT_DIR}/result_dir
cp -r ${LOGS}/cli.log ${SCRIPT_DIR}/result_dir/summary.txt

echo "Collecting configs and logs into result directory..."
collect_results

if [[ "${ENABLE_NSYS}" == "1" ]]; then
    echo "Waiting for nsys report generation..."
    if [[ -n "${QUERIES:-}" ]]; then
        IFS=',' read -ra qlist <<< "${QUERIES}"
    else
        qlist=({1..22})
    fi

    declare -A prev_sizes
    stable_count=0
    for i in {1..120}; do
        all_stable=true
        for qnum in "${qlist[@]}"; do
            report="${LOGS}/nsys_worker_0_Q${qnum}.nsys-rep"
            fallback="${LOGS}/nsys_worker_0_Q${qnum}.qdstrm"
            if [[ -f "$report" ]]; then
                target="$report"
            elif [[ -f "$fallback" ]]; then
                target="$fallback"
            else
                echo "    Q${qnum}: no file yet"
                all_stable=false
                continue
            fi
            cur_size=$(stat -c%s "$target" 2>/dev/null || echo 0)
            prev=${prev_sizes["$target"]:-0}
            echo "    Q${qnum}: cur=${cur_size} prev=${prev}"
            if (( cur_size == 0 || cur_size != prev )); then
                all_stable=false
            fi
            prev_sizes["$target"]=$cur_size
        done
        echo "  all_stable=${all_stable} stable_count=${stable_count}"
        if $all_stable; then
            stable_count=$((stable_count + 1))
            if (( stable_count >= 3 )); then
                echo "All ${#qlist[@]} nsys reports stable."
                break
            fi
        else
            stable_count=0
        fi
        sleep 5
    done

    echo "Copying nsys reports to ${result_dir}/..."
    cp "${LOGS}"/*.nsys-rep "${result_dir}/"
fi

echo "========================================"
echo "Benchmark complete!"
echo "Results saved to: ${SCRIPT_DIR}/results_dir"
echo "Logs available at: ${LOGS}"
echo "========================================"
