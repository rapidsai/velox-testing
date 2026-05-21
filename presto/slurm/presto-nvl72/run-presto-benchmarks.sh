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
# Start Coordinator + Workers
# ==============================================================================
start_cluster

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

wait_for_nsys_report_generation

echo "========================================"
echo "Benchmark complete!"
echo "Results saved to: ${SCRIPT_DIR}/results_dir"
echo "Logs available at: ${LOGS}"
echo "========================================"
