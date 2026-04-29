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
# Generates TPC-H parquet data using benchmark_data_tools/generate_data_files.py
# from velox-testing, running inside the tpchgen-cli container image.
#
# Required env vars (passed via --export or set below):
#   SCALE_FACTOR  - TPC-H scale factor (e.g. 100)
#   OUTPUT_DIR    - Absolute path to the output directory on the host
#   PARALLELISM   - Number of parallel generation threads (default: 100)
#
# Pre-requisite: pull the image with:
#   ./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:tpchgen-cli

# ==============================================================================
# Defaults
# ==============================================================================
: "${SCALE_FACTOR:=100}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${SCRIPT_DIR}/defaults.env"
: "${OUTPUT_DIR:=/scratch/${USER}/${VT_WORKSPACE}/tpch-rs-float/scale-100-no-delta}"
: "${PARALLELISM:=100}"
: "${IMAGE:=${IMAGE_DIR}/velox-testing-images-tpchgen-cli.sqsh}"

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
echo "========================================"

if [[ ! -f "${IMAGE}" ]]; then
    echo "Error: tpchgen-cli image not found at: ${IMAGE}"
    echo "Pull it first with:"
    echo "  ${SCRIPT_DIR}/pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:tpchgen-cli"
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"

# ==============================================================================
# Generate Data
# ==============================================================================
# Mount OUTPUT_DIR as /data and VT_ROOT as /workspace inside the container.
# generate_data_files.py calls tpchgen-cli (present in the image) to produce
# per-table partitioned parquet files, then rearranges them and writes metadata.json.
srun --ntasks=1 \
  --container-image="${IMAGE}" \
  --container-mounts="${OUTPUT_DIR}:/data,${VT_ROOT}:/workspace" \
  -- python3 /workspace/benchmark_data_tools/generate_data_files.py \
    -b tpch \
    -d /data \
    -s "${SCALE_FACTOR}" \
    --convert-decimals-to-floats \
    -j "${PARALLELISM}" \
    --verbose

echo "========================================"
echo "Data generation complete!"
echo "Output: ${OUTPUT_DIR}"
echo "========================================"
