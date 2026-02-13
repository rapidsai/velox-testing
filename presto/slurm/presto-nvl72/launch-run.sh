

#!/bin/bash
# ==============================================================================
# Presto TPC-H Benchmark Launcher
# ==============================================================================
# Simple launcher script to submit the presto benchmark job to slurm
#
# Usage:
#   ./launch-run.sh -n|--nodes <count> -s|--scale-factor <sf> [-i|--iterations <n>] [additional sbatch options]
#
# To change configuration, edit run-presto-benchmarks.slurm directly
# ==============================================================================

set -e

# Change to script directory
cd "$(dirname "$0")"

# Clean up old output files
rm -f result_dir/* logs/* *.out *.err 2>/dev/null || true
mkdir -p result_dir logs

echo "Submitting Presto TPC-H benchmark job..."
echo "Configuration is set in run-presto-benchmarks.slurm"
echo ""

# Parse required -n/--nodes and -s/--scale-factor, optional -i/--iterations, and collect extra sbatch args
NODES_COUNT=""
SCALE_FACTOR=""
NUM_ITERATIONS="1"
EXTRA_ARGS=()
NUM_GPUS_PER_NODE="4"
WORKER_IMAGE="presto-native-worker-gpu"
COORD_IMAGE="presto-coordinator"
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--nodes)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                NODES_COUNT="$2"
                shift 2
            else
                echo "Error: -n|--nodes requires a value."
                echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [additional sbatch options]"
                exit 1
            fi
            ;;
        -s|--scale-factor)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                SCALE_FACTOR="$2"
                shift 2
            else
                echo "Error: -s|--scale-factor requires a value."
                echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [additional sbatch options]"
                exit 1
            fi
            ;;
        -i|--iterations)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                NUM_ITERATIONS="$2"
                shift 2
            else
                echo "Error: -i|--iterations requires a value"
                echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [-i|--iterations <n>] [additional sbatch options]"
                exit 1
            fi
            ;;
	-g|--num-gpus-per-node)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                NUM_GPUS_PER_NODE="$2"
                shift 2
            else
                echo "Error: -g|--num-gpus-per-node requires a value"
                echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [-i|--iterations <n>] [additional sbatch options]"
                exit 1
            fi
            ;;
	-w|--worker-image)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                WORKER_IMAGE="$2"
                shift 2
            else
                echo "Error: -w|--worker-image requires a value"
                echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [-i|--iterations <n>] [additional sbatch options]"
                exit 1
            fi
            ;;
	-c|--coord-image)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                COORD_IMAGE="$2"
                shift 2
            else
                echo "Error: -c|--coord-image requires a value"
                echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [-i|--iterations <n>] [additional sbatch options]"
                exit 1
            fi
            ;;
        --)
            shift
            break
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -z "${NODES_COUNT}" ]]; then
    echo "Error: -n|--nodes is required"
    echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [-i|--iterations <n>] [additional sbatch options]"
    exit 1
fi
if [[ -z "${SCALE_FACTOR}" ]]; then
    echo "Error: -s|--scale-factor is required"
    echo "Usage: $0 -n|--nodes <count> -s|--scale-factor <sf> [-i|--iterations <n>] [additional sbatch options]"
    exit 1
fi

# Submit job (include nodes/SF/iterations in file names)
OUT_FMT="presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.out"
ERR_FMT="presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.err"
SCRIPT_DIR="$PWD"
JOB_ID=$(sbatch --nodes="${NODES_COUNT}" --export="ALL,SCALE_FACTOR=${SCALE_FACTOR},NUM_ITERATIONS=${NUM_ITERATIONS},SCRIPT_DIR=${SCRIPT_DIR},NUM_GPUS_PER_NODE=${NUM_GPUS_PER_NODE},WORKER_IMAGE=${WORKER_IMAGE},COORD_IMAGE=${COORD_IMAGE}" \
--output="${OUT_FMT}" --error="${ERR_FMT}" "${EXTRA_ARGS[@]}" --gres="gpu:${NUM_GPUS_PER_NODE}" \
run-presto-benchmarks.slurm | awk '{print $NF}')
OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

# Resolve and print first node IP once nodes are allocated
echo "Resolving first node IP..."
for i in {1..60}; do
    STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null || true)
    NODELIST=$(squeue -j "$JOB_ID" -h -o "%N" 2>/dev/null || true)
    if [[ -n "${NODELIST:-}" && "${NODELIST}" != "(null)" ]]; then
        FIRST_NODE=$(scontrol show hostnames "$NODELIST" | head -n 1)
        if [[ -n "${FIRST_NODE:-}" ]]; then
            part=$(scontrol getaddrs "$FIRST_NODE" 2>/dev/null | awk 'NR==1{print $2}')
	    FIRST_IP="${part%%:*}"
            echo "Run this command on a machine to get access to the webUI:
  ssh -N -L 9200:$FIRST_IP:9200 sunk.pocf62-use13a.coreweave.app
The UI will be available at http://localhost:9200"
	    echo ""
            break
        fi
    fi
    sleep 5
done

echo "Job submitted with ID: $JOB_ID"
echo ""
echo "Monitor job with:"
echo "  squeue -j $JOB_ID"
echo "  tail -f ${OUT_FILE}"
echo "  tail -f ${ERR_FILE}"
echo "  tail -f logs/coord.log"
echo "  tail -f logs/worker_*.log"
echo "  tail -f logs/cli.log"
echo ""
echo "Waiting for job to complete..."

# Wait for job to finish
while squeue -j $JOB_ID 2>/dev/null | grep -q $JOB_ID; do
    sleep 5
done

echo ""
echo "Job completed!"
echo ""
echo "Output files:"
ls -lh "${OUT_FILE}" "${ERR_FILE}" 2>/dev/null || echo "No output files found"
echo ""
echo "Showing job output:"
echo "========================================"
cat "${OUT_FILE}" 2>/dev/null || echo "No output available"
