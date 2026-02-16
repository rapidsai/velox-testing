#!/bin/bash
# ==============================================================================
# Presto TPC-H Benchmark Launcher - Self-Contained Version
# ==============================================================================
# Usage: ./launch.sh -n <nodes> -s <scale-factor> [-i <iterations>]
# ==============================================================================

set -e

# ==============================================================================
# Configuration
# ==============================================================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DATA_DIR="${DATA_DIR:-/mnt/data/tpch-rs}"
IMAGE_DIR="${IMAGE_DIR:-/mnt/data/images/presto}"
WORKER_IMAGE="${WORKER_IMAGE:-presto-native-worker-gpu}"
COORD_IMAGE="${COORD_IMAGE:-presto-coordinator}"

# ==============================================================================
# Parse Arguments
# ==============================================================================
NODES_COUNT=""
SCALE_FACTOR=""
NUM_ITERATIONS="1"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--nodes)
            NODES_COUNT="$2"
            shift 2
            ;;
        -s|--scale-factor)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        -i|--iterations)
            NUM_ITERATIONS="$2"
            shift 2
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -z "$NODES_COUNT" ]]; then
    echo "Error: -n|--nodes is required"
    echo "Usage: $0 -n <nodes> -s <scale-factor> [-i <iterations>]"
    exit 1
fi

if [[ -z "$SCALE_FACTOR" ]]; then
    echo "Error: -s|--scale-factor is required"
    echo "Usage: $0 -n <nodes> -s <scale-factor> [-i <iterations>]"
    exit 1
fi

# ==============================================================================
# Clean Up and Prepare
# ==============================================================================
cd "$SCRIPT_DIR"
rm -rf logs result_dir worker_data_* configs *.out *.err 2>/dev/null || true
mkdir -p logs result_dir

echo "Submitting Presto TPC-H benchmark job..."
echo "  Nodes: $NODES_COUNT"
echo "  Scale Factor: $SCALE_FACTOR"
echo "  Iterations: $NUM_ITERATIONS"
echo ""

# ==============================================================================
# Submit Job
# ==============================================================================
OUT_FMT="presto-tpch_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.out"
ERR_FMT="presto-tpch_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.err"

JOB_ID=$(sbatch \
    --nodes="$NODES_COUNT" \
    --export="ALL,SCALE_FACTOR=${SCALE_FACTOR},NUM_ITERATIONS=${NUM_ITERATIONS},SCRIPT_DIR=${SCRIPT_DIR},VT_ROOT=${VT_ROOT},DATA_DIR=${DATA_DIR},IMAGE_DIR=${IMAGE_DIR},WORKER_IMAGE=${WORKER_IMAGE},COORD_IMAGE=${COORD_IMAGE}" \
    --output="$OUT_FMT" \
    --error="$ERR_FMT" \
    "${EXTRA_ARGS[@]}" \
    "${SCRIPT_DIR}/run.slurm" | awk '{print $NF}')

OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

echo "Job submitted with ID: $JOB_ID"
echo ""

# ==============================================================================
# Get First Node IP for WebUI
# ==============================================================================
echo "Waiting for node allocation..."
for i in {1..60}; do
    NODELIST=$(squeue -j "$JOB_ID" -h -o "%N" 2>/dev/null || true)
    if [[ -n "$NODELIST" && "$NODELIST" != "(null)" ]]; then
        FIRST_NODE=$(scontrol show hostnames "$NODELIST" | head -n 1)
        if [[ -n "$FIRST_NODE" ]]; then
            FIRST_IP=$(scontrol getaddrs "$FIRST_NODE" 2>/dev/null | awk 'NR==1{print $2}' | cut -d: -f1)
            echo "WebUI will be available at: http://${FIRST_IP}:9200"
            echo "To access from your machine:"
            echo "  ssh -N -L 9200:${FIRST_IP}:9200 sunk.pocf62-use13a.coreweave.app"
            echo "  Then open: http://localhost:9200"
            echo ""
            break
        fi
    fi
    sleep 2
done

echo "Monitor with:"
echo "  squeue -j $JOB_ID"
echo "  tail -f $OUT_FILE"
echo "  tail -f logs/coordinator.log"
echo "  tail -f logs/worker_*.log"
echo ""
echo "Waiting for job to complete..."

# ==============================================================================
# Wait for Completion
# ==============================================================================
while squeue -j "$JOB_ID" 2>/dev/null | grep -q "$JOB_ID"; do
    sleep 5
done

echo ""
echo "Job completed!"
echo "Results in: ${SCRIPT_DIR}/result_dir/"
echo "Logs in: ${SCRIPT_DIR}/logs/"
