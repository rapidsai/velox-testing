#!/bin/bash
#SBATCH --job-name=presto-tpch-run
#SBATCH --ntasks-per-node=1
#SBATCH --mem=0
#SBATCH --exclusive
# Note: --time, --cpus-per-task, --partition, and --account are passed as sbatch
# CLI flags by launch-run.sh (sourced from cluster_config.env) so they can vary
# per cluster/partition without editing this file.

# ==============================================================================
# Presto TPC-H Benchmark Job Script
# ==============================================================================

# Mode-specific required vars
if [ -z "${NUM_ITERATIONS:-}" ]; then
    echo "Error: NUM_ITERATIONS is required. Set via launcher: -i|--iterations" >&2
    exit 1
fi
export NUM_ITERATIONS

source "${SCRIPT_DIR}/slurm_common.sh"

# Single-node execution mode (workers and coordinator share the same node)
if [ "${NUM_WORKERS}" -eq "1" ]; then
    export SINGLE_NODE_EXECUTION=true
else
    export SINGLE_NODE_EXECUTION=false
fi

echo "Iterations: ${NUM_ITERATIONS}"
echo "Single node execution: ${SINGLE_NODE_EXECUTION}"
echo "========================================"

bash "${SCRIPT_DIR}/run-presto-benchmarks.sh"
