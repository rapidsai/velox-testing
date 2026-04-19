#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# Presto Analyze Tables Launcher
# ==============================================================================
# Submits run-analyze-tables.slurm to Slurm.
#
# Usage:
#   ./launch-analyze-tables.sh -s|--scale-factor <sf> [-n|--nodes <count>]
#                              [-d|--data-dir <path>] [additional sbatch options]
#
# Examples:
#   # SF100, single node, default data path
#   ./launch-analyze-tables.sh -s 100
#
#   # SF3000, 2 nodes, custom data directory
#   ./launch-analyze-tables.sh -s 3000 -n 2 -d /scratch/$USER/my-workspace/tpch-rs-float-no-delta
#
#   # Override wall-clock limit for very large scale factors
#   ./launch-analyze-tables.sh -s 3000 -n 4 -d /scratch/$USER/my-workspace/tpch-rs-float-no-delta --time=12:00:00
# ==============================================================================

set -e

cd "$(dirname "$0")"

# Defaults
NODES_COUNT="1"
SCALE_FACTOR=""
NUM_GPUS_PER_NODE="4"
WORKER_IMAGE="presto-native-worker-gpu"
COORD_IMAGE="presto-coordinator"
DATA_DIR=""   # empty => slurm script uses its built-in default
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--scale-factor)
            [[ -n "${2:-}" && "${2:0:1}" != "-" ]] || { echo "Error: $1 requires a value"; exit 1; }
            SCALE_FACTOR="$2"; shift 2 ;;
        -n|--nodes)
            [[ -n "${2:-}" && "${2:0:1}" != "-" ]] || { echo "Error: $1 requires a value"; exit 1; }
            NODES_COUNT="$2"; shift 2 ;;
        -d|--data-dir)
            [[ -n "${2:-}" && "${2:0:1}" != "-" ]] || { echo "Error: $1 requires a value"; exit 1; }
            DATA_DIR="$2"; shift 2 ;;
        -g|--num-gpus-per-node)
            [[ -n "${2:-}" && "${2:0:1}" != "-" ]] || { echo "Error: $1 requires a value"; exit 1; }
            NUM_GPUS_PER_NODE="$2"; shift 2 ;;
        -w|--worker-image)
            [[ -n "${2:-}" && "${2:0:1}" != "-" ]] || { echo "Error: $1 requires a value"; exit 1; }
            WORKER_IMAGE="$2"; shift 2 ;;
        -c|--coord-image)
            [[ -n "${2:-}" && "${2:0:1}" != "-" ]] || { echo "Error: $1 requires a value"; exit 1; }
            COORD_IMAGE="$2"; shift 2 ;;
        --) shift; break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

if [[ -z "${SCALE_FACTOR}" ]]; then
    echo "Error: -s|--scale-factor is required"
    echo "Usage: $0 -s <sf> [-n <nodes>] [-d <data_dir>] [sbatch options...]"
    exit 1
fi

# Clean up stale logs/output files from previous runs
rm -f logs/* *.out *.err 2>/dev/null || true
mkdir -p logs

SCRIPT_DIR="$PWD"

EXPORT_VARS="ALL,SCALE_FACTOR=${SCALE_FACTOR},SCRIPT_DIR=${SCRIPT_DIR},NUM_GPUS_PER_NODE=${NUM_GPUS_PER_NODE},WORKER_IMAGE=${WORKER_IMAGE},COORD_IMAGE=${COORD_IMAGE}"
if [[ -n "${DATA_DIR}" ]]; then
    EXPORT_VARS="${EXPORT_VARS},DATA=${DATA_DIR}"
fi

OUT_FMT="presto-analyze_n${NODES_COUNT}_sf${SCALE_FACTOR}_%j.out"
ERR_FMT="presto-analyze_n${NODES_COUNT}_sf${SCALE_FACTOR}_%j.err"

echo "Submitting Presto Analyze Tables job..."
echo "  Scale factor : SF${SCALE_FACTOR}"
echo "  Nodes        : ${NODES_COUNT}"
echo "  GPUs/node    : ${NUM_GPUS_PER_NODE}  (total workers: $((NODES_COUNT * NUM_GPUS_PER_NODE)))"
echo "  Worker image : ${WORKER_IMAGE}"
echo "  Data dir     : ${DATA_DIR:-<default>}"
echo ""

JOB_ID=$(sbatch \
    --nodes="${NODES_COUNT}" \
    --gres="gpu:${NUM_GPUS_PER_NODE}" \
    --export="${EXPORT_VARS}" \
    --output="${OUT_FMT}" \
    --error="${ERR_FMT}" \
    "${EXTRA_ARGS[@]}" \
    run-analyze-tables.slurm | awk '{print $NF}')

OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

echo "Job submitted with ID: $JOB_ID"
echo ""
echo "Monitor with:"
echo "  squeue -j $JOB_ID"
echo "  tail -f ${OUT_FILE}"
echo "  tail -f ${ERR_FILE}"
echo "  tail -f logs/coord.log"
echo "  tail -f logs/worker_*.log"
echo "  tail -f logs/cli.log"
echo ""
echo "Waiting for job to complete..."

while squeue -j "$JOB_ID" 2>/dev/null | grep -q "$JOB_ID"; do
    sleep 10
done

echo ""
echo "Job completed!"
echo ""
echo "Hive metastore updated at: $(cd ../../.. && pwd -P)/.hive_metastore"
echo ""
echo "Showing job output:"
echo "========================================"
cat "${OUT_FILE}" 2>/dev/null || echo "No output available"
echo ""
echo "Showing CLI log:"
cat logs/cli.log 2>/dev/null || echo "No CLI output available"
