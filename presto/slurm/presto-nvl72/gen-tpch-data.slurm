#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#SBATCH --job-name=gen-tpch-data
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --exclusive

# ==============================================================================
# TPC-H Data Generation Job Script
# ==============================================================================
# Generates TPC-H parquet data using the tpchgen-rs container.
#
# Required env vars (passed via --export or set below):
#   SCALE_FACTOR  - TPC-H scale factor (e.g. 100)
#   OUTPUT_DIR    - Absolute path to the output directory on the host
#   PARALLELISM   - Number of parallel generation threads (default: 100)
#   IMAGE         - tpchgen-rs container image
#   EXTRA_ARGS    - Additional tpchgen-rs flags

# ==============================================================================
# Defaults
# ==============================================================================
: "${SCALE_FACTOR:=100}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/defaults.env"
: "${OUTPUT_DIR:=/scratch/${USER}/${VT_WORKSPACE}/tpch-rs-float/scale-100-no-delta}"
: "${PARALLELISM:=100}"
: "${IMAGE:=ghcr.io/tomaugspurger/tpchgen-rs:tom-sync-upstream-clean}"
: "${EXTRA_ARGS:=--no-delta-length-byte-array --use-float-type}"

# ==============================================================================
# Pre-flight Info
# ==============================================================================
echo "========================================"
echo "Job ID:       $SLURM_JOB_ID"
echo "Node:         $SLURM_JOB_NODELIST"
echo "Scale factor: $SCALE_FACTOR"
echo "Output dir:   $OUTPUT_DIR"
echo "Parallelism:  $PARALLELISM"
echo "Image:        $IMAGE"
echo "Extra args:   $EXTRA_ARGS"
echo "========================================"

mkdir -p "${OUTPUT_DIR}"

# ==============================================================================
# Generate Data
# ==============================================================================
# Mount OUTPUT_DIR as /data inside the container, then run tpchgen-rs.
# pyxis/enroot runs the container as the submitting user automatically.
srun --ntasks=1 \
  --container-image="${IMAGE}" \
  --container-mounts="${OUTPUT_DIR}:/data" \
  -- /usr/local/bin/python3.12 /usr/local/bin/generate_tpch.py \
    -s "${SCALE_FACTOR}" -o /data/ ${EXTRA_ARGS} -j "${PARALLELISM}"

echo "========================================"
echo "Data generation complete!"
echo "Output: ${OUTPUT_DIR}"
echo "========================================"
