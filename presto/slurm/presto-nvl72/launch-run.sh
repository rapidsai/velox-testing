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
WORKER_IMAGE=""
COORD_IMAGE=""
OUTPUT_PATH=""
SCRIPT_DIR="$PWD"
WORKER_ENV_FILE="${SCRIPT_DIR}/worker.env"
ENABLE_GDS=1
ENABLE_METRICS=0
ENABLE_NSYS=0
NSYS_WORKER_ID=0
QUERIES=""

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
        --worker-env-file)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                WORKER_ENV_FILE="$2"
                shift 2
            else
                echo "Error: --worker-env-file requires a value"
                exit 1
            fi
            ;;
        --disable-gds)
            ENABLE_GDS=0
            shift
            ;;
        -m|--metrics)
            ENABLE_METRICS=1
            shift
            ;;
        -p|--profile)
            ENABLE_NSYS=1
            shift
            ;;
        --nsys-worker-id)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                NSYS_WORKER_ID="$2"
                shift 2
            else
                echo "Error: --nsys-worker-id requires a value"
                exit 1
            fi
            ;;
        -q|--queries)
          if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
            QUERIES="$2"
            shift 2
          else
            echo "Error: --queries requires a value"
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
if [[ -z "${WORKER_IMAGE}" ]]; then
    echo "Error: -w|--worker-image is required"
    exit 1
fi
if [[ -z "${COORD_IMAGE}" ]]; then
    echo "Error: -c|--coord-image is required"
    exit 1
fi

# Submit job (include nodes/SF/iterations in file names)
OUT_FMT="presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.out"
ERR_FMT="presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.err"
JOB_NAME="presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}"
# NODELIST is unset by default -- Slurm picks any available nodes.
# Export NODELIST=<host-or-range> before invoking to pin.
NODELIST="${NODELIST:-}"
NODELIST_ARG=()
if [[ -n "${NODELIST}" ]]; then
    NODELIST_ARG=(--nodelist="${NODELIST}")
fi
GRES_OPT=$([[ "$VARIANT_TYPE" == "gpu" ]] && echo "--gres=gpu:${NUM_GPUS_PER_NODE}" || echo "")

EXPORT_VARS="ALL"
EXPORT_VARS+=",SCALE_FACTOR=${SCALE_FACTOR}"
EXPORT_VARS+=",NUM_ITERATIONS=${NUM_ITERATIONS}"
EXPORT_VARS+=",SCRIPT_DIR=${SCRIPT_DIR}"
EXPORT_VARS+=",NUM_GPUS_PER_NODE=${NUM_GPUS_PER_NODE}"
EXPORT_VARS+=",WORKER_IMAGE=${WORKER_IMAGE}"
EXPORT_VARS+=",COORD_IMAGE=${COORD_IMAGE}"
EXPORT_VARS+=",USE_NUMA=${USE_NUMA}"
EXPORT_VARS+=",VARIANT_TYPE=${VARIANT_TYPE}"
EXPORT_VARS+=",WORKER_ENV_FILE=${WORKER_ENV_FILE}"
EXPORT_VARS+=",ENABLE_GDS=${ENABLE_GDS}"
EXPORT_VARS+=",ENABLE_METRICS=${ENABLE_METRICS}"
EXPORT_VARS+=",ENABLE_NSYS=${ENABLE_NSYS}"
EXPORT_VARS+=",NSYS_WORKER_ID=${NSYS_WORKER_ID}"
if [[ -n "${QUERIES}" ]]; then
    # Do not directly append a comma separated list to EXPORT_VARS as the comma separator
    # is also used to separate different env vars.
    # Also do not use single quote around the comma separate list as it is found to cause
    # further issues down the line.
    # Using export is the simplest, correct approach to make the env vars visible in the
    # worker container.
    export QUERIES
fi

# Forward shared-metastore config from the calling shell so the slurm job
# can populate from the shared snapshot when opted in.
if [[ -n "${HIVE_METASTORE_VERSION:-}" ]]; then
    EXPORT_VARS="${EXPORT_VARS},HIVE_METASTORE_VERSION=${HIVE_METASTORE_VERSION}"
fi
if [[ -n "${HIVE_METASTORE_SHARED_ROOT:-}" ]]; then
    EXPORT_VARS="${EXPORT_VARS},HIVE_METASTORE_SHARED_ROOT=${HIVE_METASTORE_SHARED_ROOT}"
fi
JOB_ID=$(sbatch --job-name="${JOB_NAME}" --nodes="${NODES_COUNT}" "${NODELIST_ARG[@]}" \
--export="${EXPORT_VARS}" \
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
