#!/bin/bash
#SBATCH --job-name=presto-analyze-tables
#SBATCH --time=08:00:00
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --exclusive

# ==============================================================================
# Presto Analyze Tables Job Script
# ==============================================================================
# Starts coordinator + workers, registers TPC-H tables, and runs ANALYZE TABLE
# to collect statistics for the given scale factor.
#
# Required env vars (passed via --export):
#   SCALE_FACTOR      - TPC-H scale factor (e.g. 100)
#   SCRIPT_DIR        - Absolute path to this scripts directory
#   NUM_GPUS_PER_NODE - Number of GPU workers per node
#   WORKER_IMAGE      - Worker container image name (without .sqsh)
#   COORD_IMAGE       - Coordinator container image name (without .sqsh)

# ==============================================================================
# Validate required variables
# ==============================================================================
if [ -z "${SCALE_FACTOR:-}" ]; then
    echo "Error: SCALE_FACTOR is required." >&2
    exit 1
fi
export SCALE_FACTOR

if [ -z "${SCRIPT_DIR:-}" ]; then
    echo "Error: SCRIPT_DIR is required." >&2
    exit 1
fi
export SCRIPT_DIR

if [ -z "${WORKER_IMAGE:-}" ]; then
    echo "Error: WORKER_IMAGE is required." >&2
    exit 1
fi
export WORKER_IMAGE

if [ -z "${COORD_IMAGE:-}" ]; then
    echo "Error: COORD_IMAGE is required." >&2
    exit 1
fi
export COORD_IMAGE

if [ -z "${NUM_GPUS_PER_NODE:-}" ]; then
    echo "Error: NUM_GPUS_PER_NODE is required." >&2
    exit 1
fi
export NUM_GPUS_PER_NODE

# ==============================================================================
# Paths - overriding defaults to use /scratch mount (only available on compute nodes)
# ==============================================================================
export VT_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd -P)"
source "${SCRIPT_DIR}/defaults.env"
export DATA
export HIVE_METASTORE_SHARED_ROOT HIVE_METASTORE_VERSION
# Dataset subdirectory name under DATA (defaults to scale-<SF> if not set)
: "${DATASET_NAME:=scale-${SCALE_FACTOR}}"
export DATASET_NAME
export IMAGE_DIR
export LOGS=$SCRIPT_DIR/logs
export VARIANT_TYPE=gpu
export USE_NUMA=${USE_NUMA:-0}
export CONFIGS=$VT_ROOT/presto/docker/config/generated/$VARIANT_TYPE

# ==============================================================================
# Computed values
# ==============================================================================
export NUM_NODES=$SLURM_JOB_NUM_NODES
export COORD=$(scontrol show hostnames $SLURM_JOB_NODELIST | head -n 1)
export NUM_WORKERS=$((NUM_NODES * NUM_GPUS_PER_NODE))

# Presto configuration
export PORT=9200
export CUDF_LIB=/usr/lib64/presto-native-libs

# ==============================================================================
# Pre-flight Info
# ==============================================================================
echo "========================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Nodes: $SLURM_JOB_NUM_NODES"
echo "Node list: $SLURM_JOB_NODELIST"
echo "Coordinator node: $COORD"
echo "Worker image: $WORKER_IMAGE"
echo "Coord image: $COORD_IMAGE"
echo "Scale factor: $SCALE_FACTOR"
echo "Data directory: $DATA"
echo "Image directory: $IMAGE_DIR"
echo "Logs directory: $LOGS"
echo "Total workers: $NUM_WORKERS (${NUM_NODES} nodes x ${NUM_GPUS_PER_NODE} GPUs)"
echo "========================================"

mkdir -p ${LOGS}

bash $SCRIPT_DIR/run-analyze-tables.sh
