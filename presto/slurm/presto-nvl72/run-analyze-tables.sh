#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -exuo pipefail

# ==============================================================================
# Presto Analyze Tables Execution Script
# ==============================================================================
# Starts coordinator + workers, registers TPC-H tables in the Hive metastore,
# then runs ANALYZE TABLE for the given scale factor.
#
# All configuration is passed via environment variables set by the .slurm script.

source $SCRIPT_DIR/echo_helpers.sh
source $SCRIPT_DIR/functions.sh

# ==============================================================================
# Setup: generate configs and prepare directories
# ==============================================================================
echo "Generating Presto configs..."
generate_configs

# ANALYZE TABLE is not supported with cudf enabled. Disable it in all worker
# configs so workers run in CPU mode while still using the GPU worker image.
echo "Disabling cudf in worker configs for ANALYZE TABLE compatibility..."
for worker_conf in ${CONFIGS}/etc_worker*/config_native.properties; do
    sed -i 's/^cudf\.enabled=true/cudf.enabled=false/' "${worker_conf}"
done
for worker_hive in ${CONFIGS}/etc_worker*/catalog/hive.properties; do
    sed -i 's/^cudf\./#cudf./' "${worker_hive}"
done

echo "Creating hive metastore directory..."
mkdir -p ${VT_ROOT}/.hive_metastore

validate_config_directory

# ==============================================================================
# Start Coordinator + Workers
# ==============================================================================
start_cluster

# ==============================================================================
# Register Tables and Run ANALYZE TABLE
# ==============================================================================
echo "Registering TPC-H tables and running ANALYZE TABLE for tpchsf${SCALE_FACTOR}..."
# The coordinator container only has Python 3.9, so python3.12 -m venv fails.
# py_env_functions.sh falls back to conda when MINIFORGE_HOME is set.
# Miniforge is installed at ${VT_ROOT}/miniforge3, which is mounted as
# /workspace/miniforge3 inside the container.
run_coord_image "export PRESTO_DATA_DIR=/var/lib/presto/data/hive/data/user_data; \
    export MINIFORGE_HOME=/workspace/miniforge3; \
    export HOME=/workspace; \
    cd /workspace/presto/scripts; \
    ./setup_benchmark_tables.sh \
        -b tpch \
        -d ${DATASET_NAME} \
        -s tpchsf${SCALE_FACTOR} \
        -H ${COORD} \
        -p ${PORT} \
        --no-docker" "cli"

echo "========================================"
echo "Analyze tables complete!"
echo "Hive metastore updated at: ${VT_ROOT}/.hive_metastore"
echo "Logs available at: ${LOGS}"
echo "========================================"

# Auto-publish to the shared metastore when HIVE_METASTORE_VERSION is set and
# the target slot is still empty.  See functions.sh:publish_hive_metastore_to_shared.
if [[ -n "${HIVE_METASTORE_VERSION:-}" ]]; then
    publish_hive_metastore_to_shared
fi
