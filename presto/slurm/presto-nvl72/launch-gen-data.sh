#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# TPC-H Data Generation Launcher
# ==============================================================================
# Submits a SLURM job to generate TPC-H parquet data using tpchgen-rs.
#
# Usage:
#   ./launch-gen-data.sh [--scale-factor <sf>] [--output-dir <path>] [--parallelism <n>] [additional sbatch options]
#
# To change container image or encoding flags, edit gen-tpch-data.slurm directly.
# ==============================================================================

set -e

cd "$(dirname "$0")"

module load slurm 2>/dev/null || true

source "$(dirname "$0")/defaults.env"

SCALE_FACTOR="100"
OUTPUT_DIR="${OUTPUT_DIR:-/scratch/${USER}/${VT_WORKSPACE}/tpch-rs-float/scale-100-no-delta}"
PARALLELISM="100"
# NODELIST defaults to empty -- Slurm picks any available node.
# Override via env or -N/--nodelist to pin.
NODELIST="${NODELIST:-}"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--scale-factor)
            SCALE_FACTOR="$2"; shift 2 ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"; shift 2 ;;
        -j|--parallelism)
            PARALLELISM="$2"; shift 2 ;;
        -N|--nodelist)
            NODELIST="$2"; shift 2 ;;
        --) shift; break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

echo "Submitting TPC-H data generation job..."
echo "  Scale factor: $SCALE_FACTOR"
echo "  Output dir:   $OUTPUT_DIR"
echo "  Parallelism:  $PARALLELISM"
echo "  Node:         ${NODELIST:-<any available>}"
echo ""

OUT_FMT="gen-tpch-data_sf${SCALE_FACTOR}_%j.out"
ERR_FMT="gen-tpch-data_sf${SCALE_FACTOR}_%j.err"

NODELIST_ARG=()
if [[ -n "${NODELIST}" ]]; then
    NODELIST_ARG=(--nodelist="${NODELIST}")
fi

JOB_ID=$(sbatch \
  --nodes=1 \
  "${NODELIST_ARG[@]}" \
  --export="ALL,SCALE_FACTOR=${SCALE_FACTOR},OUTPUT_DIR=${OUTPUT_DIR},PARALLELISM=${PARALLELISM}" \
  --output="${OUT_FMT}" \
  --error="${ERR_FMT}" \
  "${EXTRA_ARGS[@]}" \
  gen-tpch-data.slurm | awk '{print $NF}')

OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

echo "Job submitted with ID: $JOB_ID"
echo ""
echo "Monitor job with:"
echo "  squeue -j $JOB_ID"
echo "  tail -f ${OUT_FILE}"
echo "  tail -f ${ERR_FILE}"
echo ""
echo "Waiting for job to complete..."

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
