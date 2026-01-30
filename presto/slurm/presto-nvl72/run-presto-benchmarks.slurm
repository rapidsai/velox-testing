#!/bin/bash
#SBATCH --job-name=presto-tpch-run
#SBATCH --time=01:00:00
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --gres=gpu:4
#SBATCH --exclusive

# ==============================================================================
# User Configuration - Edit these values directly
# ==============================================================================
# TPC-H Configuration
if [ -z "${SCALE_FACTOR:-}" ]; then
    echo "Error: SCALE_FACTOR is required. Set via launcher: -s|--scale-factor" >&2
    exit 1
fi
export SCALE_FACTOR
if [ -z "${NUM_ITERATIONS:-}" ]; then
    echo "Error: NUM_ITERATIONS is required. Set via launcher: -i|--iterations" >&2
    exit 1
fi
export NUM_ITERATIONS

# Directory Configuration
export SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" >/dev/null 2>&1 && pwd -P)"
# Assumes the repo root is four steps up from the script directory.  This should refer to velox-testing.
export REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd -P)"
export DATA=/mnt/data/tpch-rs
export IMAGE_DIR=/mnt/data/images/presto
export LOGS=$SCRIPT_DIR/logs
export VARIANT_TYPE=gpu
export CONFIGS=$REPO_ROOT/presto/docker/config/generated/$VARIANT_TYPE

# Container Images
# Coordinator: ${IMAGE_DIR}/presto-coordinator-test.sqsh
# Worker: ${IMAGE_DIR}/${WORKER_IMAGE}.sqsh
export WORKER_IMAGE=presto-native-worker-$VARIANT_TYPE
export NUM_NODES=$SLURM_JOB_NUM_NODES
export NUM_GPUS_PER_NODE=4

# Presto Configuration
export PORT=9200
export CUDF_LIB=/usr/lib64/presto-native-libs

# UCX Configuration
export UCX_TLS=^ib,ud:aux,sm
export UCX_MAX_RNDV_RAILS=1
export UCX_RNDV_PIPELINE_ERROR_HANDLING=y
export UCX_TCP_KEEPINTVL=1ms
export UCX_KEEPALIVE_INTERVAL=1ms

# ==============================================================================
# Computed Values
# ==============================================================================
# Get the head node (coordinator node) - export for use inside container
export COORD=$(scontrol show hostnames $SLURM_JOB_NODELIST | head -n 1)
export NUM_WORKERS=$((NUM_NODES * NUM_GPUS_PER_NODE))

# Single node execution mode
if [ "${NUM_WORKERS}" -eq "1" ]; then
    export SINGLE_NODE_EXECUTION=true
else
    export SINGLE_NODE_EXECUTION=false
fi

# ==============================================================================
# Pre-flight Info
# ==============================================================================
echo "========================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Nodes: $SLURM_JOB_NUM_NODES"
echo "Node list: $SLURM_JOB_NODELIST"
echo "Coordinator node: $COORD"
echo "Worker image: $WORKER_IMAGE"
echo "Scale factor: $SCALE_FACTOR"
echo "Iterations: $NUM_ITERATIONS"
echo "Data directory: $DATA"
echo "Logs directory: $LOGS"
echo "Total workers: $NUM_WORKERS (${NUM_NODES} nodes × ${NUM_GPUS_PER_NODE} GPUs)"
echo "Single node execution: $SINGLE_NODE_EXECUTION"
echo "========================================"

# Create necessary directories
mkdir -p ${LOGS}

# Launch the job script
bash $SCRIPT_DIR/run-presto-benchmarks.sh
