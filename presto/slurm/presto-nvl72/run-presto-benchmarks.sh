#!/bin/bash
set -e
set -x

# ==============================================================================
# Trino TPC-H Benchmark Execution Script
# ==============================================================================
# This script runs the actual benchmark execution after environment is configured
# by the slurm launcher script. All configuration is passed via environment vars.

# Source helper functions
source $SCRIPT_DIR/echo_helpers.sh
source $SCRIPT_DIR/functions.sh

# ==============================================================================
# Setup and Validation
# ==============================================================================
echo "Setting up Trino environment..."
setup

# ==============================================================================
# Start Coordinator
# ==============================================================================
echo "Starting Trino coordinator on ${COORD}..."
run_coordinator
wait_until_coordinator_is_running

# ==============================================================================
# Start Workers
# ==============================================================================
echo "Starting ${NUM_WORKERS} Trino workers across ${NUM_NODES} nodes..."

worker_id=0
for node in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
    echo "  Starting worker ${worker_id} on node ${node}"
    run_worker "0" "$WORKER_IMAGE" "${node}" "$worker_id"
    worker_id=$((worker_id + 1))
done

# ==============================================================================
# Wait for Workers to Register
# ==============================================================================
echo "Waiting for ${NUM_WORKERS} workers to register with coordinator..."
wait_for_workers_to_register $NUM_WORKERS

# ==============================================================================
# Create Schema and Register Tables
# ==============================================================================
echo "Creating TPC-H schema and registering tables for scale factor ${SCALE_FACTOR}..."
setup_benchmark ${SCALE_FACTOR}

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

echo "========================================"
echo "Benchmark complete!"
echo "Results saved to: ${SCRIPT_DIR}/results_dir"
echo "Logs available at: ${LOGS}"
echo "========================================"
