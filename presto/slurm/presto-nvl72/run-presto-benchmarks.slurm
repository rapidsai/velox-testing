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

set -euo pipefail

# Mode-specific required vars
if [ -z "${NUM_ITERATIONS:-}" ]; then
    echo "Error: NUM_ITERATIONS is required. Set via launcher: -i|--iterations" >&2
    exit 1
fi
export NUM_ITERATIONS

# Never write a new job into the previous run's directory. The job-specific
# path remains immutable after completion, so an scp/rsync of an older result
# cannot be interrupted by a later launch.
export RESULT_DIR="${SCRIPT_DIR}/result_dir_${SLURM_JOB_ID}"

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
