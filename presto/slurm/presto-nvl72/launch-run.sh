# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


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

source ./defaults.env

# Clean up old output files — use rm -rf so subdirectories (e.g. query_results/)
# are fully removed and stale benchmark_result.json cannot survive a cancelled run.
rm -rf result_dir logs 2>/dev/null || true
rm -f *.out *.err 2>/dev/null || true
mkdir -p result_dir logs

echo "Submitting Presto TPC-H benchmark job..."
echo "Configuration is set in run-presto-benchmarks.slurm"
echo ""

# Parse required -n/--nodes and -s/--scale-factor, optional -i/--iterations, and collect extra sbatch args
NODES_COUNT=""
SCALE_FACTOR=""
NUM_ITERATIONS="2"
EXTRA_ARGS=()
NUM_GPUS_PER_NODE="4"
USE_NUMA="1"
VARIANT_TYPE="gpu"
# WORKER_IMAGE="presto-native-worker-gpu"
WORKER_IMAGE="presto-native-worker-gpu-karth-Mar11-with-nsys"
COORD_IMAGE="presto-coordinator-karth-Mar11"
#COORD_IMAGE="presto-coordinator-ibm-03-11"
#WORKER_IMAGE="presto-native-worker-gpu-ibm-03-11"
#WORKER_IMAGE="velox-testing-images-presto-471cf1a-velox-1a2f63f-gpu-cuda13.1-20260312-arm64"
#COORD_IMAGE="presto-coordinator"
OUTPUT_PATH=""
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
        --no-numa)
            USE_NUMA="0"
            shift
            ;;
        --cpu)
            VARIANT_TYPE="cpu"
            NUM_GPUS_PER_NODE="1"
            USE_NUMA="0"
            shift
            ;;
        -o|--output-path)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                OUTPUT_PATH="$2"
                shift 2
            else
                echo "Error: -o|--output-path requires a value"
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
JOB_NAME="presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}"
# Node 5 has known issues; nodes above 10 are not yet functional.
NODELIST="${NODELIST:-${DEFAULT_NODELIST}}"
GRES_OPT=$([[ "$VARIANT_TYPE" == "gpu" ]] && echo "--gres=gpu:${NUM_GPUS_PER_NODE}" || echo "")
JOB_ID=$(sbatch --job-name="${JOB_NAME}" --nodes="${NODES_COUNT}" --nodelist="${NODELIST}" \
--export="ALL,SCALE_FACTOR=${SCALE_FACTOR},NUM_ITERATIONS=${NUM_ITERATIONS},SCRIPT_DIR=${SCRIPT_DIR},NUM_GPUS_PER_NODE=${NUM_GPUS_PER_NODE},WORKER_IMAGE=${WORKER_IMAGE},COORD_IMAGE=${COORD_IMAGE},USE_NUMA=${USE_NUMA},VARIANT_TYPE=${VARIANT_TYPE}" \
--output="${OUT_FMT}" --error="${ERR_FMT}" "${EXTRA_ARGS[@]}" ${GRES_OPT} \
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
echo "Showing benchmark results:"
cat logs/cli.log 2>/dev/null || echo "No CLI output available"

if [[ -n "${OUTPUT_PATH}" ]]; then
    echo ""
    echo "Copying results to ${OUTPUT_PATH}..."
    mkdir -p "${OUTPUT_PATH}"
    cp -r result_dir/. "${OUTPUT_PATH}/"
    echo "Results copied to ${OUTPUT_PATH}"
fi
