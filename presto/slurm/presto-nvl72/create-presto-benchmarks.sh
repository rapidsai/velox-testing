#!/bin/bash
set -e
set -x

# ==============================================================================
# Presto TPC-H Schema Creation Script
# ==============================================================================
# This script creates the Presto schema and tables for existing TPC-H data

# Source helper functions
source ./echo_helpers.sh
source ./functions.sh

# ==============================================================================
# Setup and Validation
# ==============================================================================
echo "Setting up Presto environment for schema creation..."
setup

# ==============================================================================
# Start Coordinator
# ==============================================================================
echo "Starting Presto coordinator on ${COORD}..."
run_coordinator
wait_until_coordinator_is_running

# ==============================================================================
# Start Workers (GPU workers for schema creation)
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

# ==============================================================================
# Create Schema and Tables
# ==============================================================================
echo "Creating TPC-H schema and tables for scale factor ${SCALE_FACTOR}..."
setup_benchmark ${SCALE_FACTOR}

echo "========================================"
echo "Schema creation complete!"
echo "Schema: tpchsf${SCALE_FACTOR}"
echo "Logs available at: ${LOGS}"
echo "========================================"
