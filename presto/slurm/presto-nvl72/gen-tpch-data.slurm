#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#SBATCH --job-name=gen-tpch-data
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=0
#SBATCH --exclusive
# Note: --cpus-per-task, --partition, and --account are passed as sbatch CLI
# args by launch-gen-data.sh, sourced from ~/.cluster_config.env.

set -euo pipefail

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
# Prefer SCRIPT_DIR from --export (set by launch-gen-data.sh).  Fall back to
# BASH_SOURCE only when invoked outside the launcher; slurm copies the script
# to a per-job spool dir so BASH_SOURCE won't find the sibling defaults.env.
: "${SCRIPT_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
VT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${SCRIPT_DIR}/defaults.env"
# functions.sh is pure function definitions — sourced for miniforge_mount_arg
# so this slurm uses the same miniforge3 bind-mount logic as run_coord_image.
source "${SCRIPT_DIR}/functions.sh"
: "${OUTPUT_DIR:=${DATA}/tpch-rs-${SCALE_FACTOR}}"
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

# Ensure the parent of OUTPUT_DIR exists on the host; the bind-mount target
# must exist before srun can mount it.  We mount the *parent* (not OUTPUT_DIR
# itself) because generate_data_files.py does shutil.rmtree on its target
# before regenerating — that fails with EBUSY on a bind-mount point.
OUTPUT_PARENT="$(dirname "${OUTPUT_DIR}")"
OUTPUT_BASENAME="$(basename "${OUTPUT_DIR}")"
mkdir -p "${OUTPUT_PARENT}"

# ==============================================================================
# Generate Data
# ==============================================================================
# Mount OUTPUT_DIR as /data and VT_ROOT as /workspace inside the container.
# generate_data_files.py calls tpchgen-cli (present in the image) to produce
# per-table partitioned parquet files, then rearranges them and writes metadata.json.
#
# The tpchgen-cli image ships the tpchgen-cli binary + python3 but no duckdb
# (generate_data_files.py needs it for table introspection).  Use the same
# pattern the analyze / benchmark / integ flows use: run the script via
# run_py_script.sh, which calls init_python_virtual_env from py_env_functions.sh.
# That tries python3.12 -m venv first, then falls back to conda via
# MINIFORGE_HOME.  Miniforge3 is bootstrapped under ${VT_ROOT}/miniforge3 on
# the first run and reused thereafter (the host path is bind-mounted at the
# same absolute path inside the container so conda shebangs resolve).
srun --ntasks=1 \
  --container-image="${IMAGE}" \
  --container-mounts="${OUTPUT_PARENT}:/data_out,${VT_ROOT}:/workspace$(miniforge_mount_arg)" \
  --export="ALL,MINIFORGE_HOME=/workspace/miniforge3,HOME=/workspace" \
  -- bash -c "
    set -e
    # Pyxis/enroot may reset HOME based on the image's passwd entry, so the
    # srun --export above isn't always honored inside the container. Force it
    # here so pip's cache and duckdb's extension dir land on the writable
    # /workspace bind-mount instead of the read-only image /home/\$USER.
    export HOME=/workspace
    cd /workspace/benchmark_data_tools
    ../scripts/run_py_script.sh -p ./generate_data_files.py -- \
      -b tpch \
      -d /data_out/${OUTPUT_BASENAME} \
      -s ${SCALE_FACTOR} \
      -j ${PARALLELISM} \
      --verbose
  "

echo "========================================"
echo "Data generation complete!"
echo "Output: ${OUTPUT_DIR}"
echo "========================================"
