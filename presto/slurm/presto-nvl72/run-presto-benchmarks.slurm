#!/bin/bash
#SBATCH --job-name=presto-tpch-run
#SBATCH --output=/mnt/home/misiug/veloxtesting/presto-nvl72/%x_%j.out
#SBATCH --error=/mnt/home/misiug/veloxtesting/presto-nvl72/%x_%j.err
#SBATCH --time=01:00:00
#SBATCH --nodes=10
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --gres=gpu:4
#SBATCH --exclusive

# ==============================================================================
# User Configuration - Edit these values directly
# ==============================================================================
# TPC-H Configuration
export SCALE_FACTOR=10000
export NUM_ITERATIONS=1

# Directory Configuration
export WORKSPACE=/mnt/home/misiug
export DATA=/mnt/data/tpch-rs
export IMAGE_DIR=/mnt/home/misiug/images
export LOGS=/mnt/home/misiug/veloxtesting/presto-nvl72/logs
export CONFIGS=/mnt/home/misiug/veloxtesting/config/generated/gpu
#export CONFIGS=/mnt/home/misiug/veloxtesting/config/generated/cpu

# Container Images
# Coordinator: ${IMAGE_DIR}/presto-coordinator-test.sqsh
# Worker: ${IMAGE_DIR}/${WORKER_IMAGE}.sqsh
#export WORKER_IMAGE=presto-native-worker-cpu
export WORKER_IMAGE=presto-native-worker-gpu
export NUM_NODES=$SLURM_JOB_NUM_NODES
export NUM_GPUS_PER_NODE=${SLURM_GPUS_ON_NODE:-4}

# Presto Configuration
export PORT=9200
export CUDF_LIB=/usr/lib64/presto-native-libs

# UCX Configuration
export UCX_TLS=^ib,ud:aux
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
mkdir -p ${DATA}

# Launch the job script
bash /mnt/home/misiug/veloxtesting/presto-nvl72/run-presto-benchmarks.sh
