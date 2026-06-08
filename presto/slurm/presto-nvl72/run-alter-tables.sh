#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -exuo pipefail

# ==============================================================================
# Presto Alter Tables Execution Script
# ==============================================================================
# Starts coordinator + workers (cudf disabled, mirroring the analyze flow,
# since cudf-enabled workers reject some DDL), then runs the SQL file in
# ${ALTER_SQL_FILE} against the hive.tpchsf${SCALE_FACTOR} schema via
# alter_tables.py.

source $SCRIPT_DIR/echo_helpers.sh
source $SCRIPT_DIR/functions.sh

# ==============================================================================
# Setup: generate configs and prepare directories
# ==============================================================================
echo "Generating Presto configs..."
generate_configs

# DDL via Presto requires cudf-disabled workers (same constraint as ANALYZE).
echo "Disabling cudf in worker configs..."
for worker_conf in ${CONFIGS}/etc_worker*/config_native.properties; do
    sed -i 's/^cudf\.enabled=true/cudf.enabled=false/' "${worker_conf}"
done
for worker_hive in ${CONFIGS}/etc_worker*/catalog/hive.properties; do
    sed -i 's/^cudf\./#cudf./' "${worker_hive}"
done

# Presto's Hive connector enforces per-operation access checks via the
# hive.allow-<op> family (LegacyAccessControl). ALTER TABLE ADD CONSTRAINT
# returns PERMISSION_DENIED without an explicit allow flag, mirroring the
# existing hive.allow-drop-table=true that the base config carries.
#
# Also strip hive.allow-drop-table: the kyle-test Java Presto build does NOT
# recognize that property and Bootstrap's strict-config check refuses to
# start the connector when an unused property is present. We're not running
# DROP TABLE in this flow, so the property is dead-weight here.
echo "Adjusting hive.properties for DDL job (constraint allow; strip unused)..."
for hive_conf in ${CONFIGS}/etc_coordinator/catalog/hive.properties \
                 ${CONFIGS}/etc_worker*/catalog/hive.properties; do
    [[ -f "${hive_conf}" ]] || continue
    sed -i '/^hive\.allow-drop-table=/d' "${hive_conf}"
    if grep -q "^hive\.allow-add-constraint=" "${hive_conf}"; then
        sed -i 's|^hive\.allow-add-constraint=.*|hive.allow-add-constraint=true|' "${hive_conf}"
    else
        echo "hive.allow-add-constraint=true" >> "${hive_conf}"
    fi
done

# Ensure the local hive metastore dir exists; this is where the configured
# bind-mount sources from. Tests should snapshot it beforehand if they want a
# rollback point — see launch-alter-tables.sh header.
echo "Verifying hive metastore directory..."
mkdir -p ${VT_ROOT}/.hive_metastore

validate_config_directory

# ==============================================================================
# Resolve the SQL file's container-visible path
# ==============================================================================
# run_coord_image bind-mounts ${VT_ROOT}:/workspace, so any host path under
# VT_ROOT is visible inside the cli container under /workspace.
case "${ALTER_SQL_FILE}" in
    "${VT_ROOT}"/*)
        CONTAINER_SQL_FILE="/workspace${ALTER_SQL_FILE#${VT_ROOT}}"
        ;;
    *)
        echo "Error: ALTER_SQL_FILE must be under VT_ROOT (${VT_ROOT}); got ${ALTER_SQL_FILE}" >&2
        exit 1
        ;;
esac
[[ -f "${ALTER_SQL_FILE}" ]] || { echo "Error: SQL file not found on host: ${ALTER_SQL_FILE}" >&2; exit 1; }
echo "Container SQL path: ${CONTAINER_SQL_FILE}"

# ==============================================================================
# Start Coordinator + Workers
# ==============================================================================
start_cluster

# ==============================================================================
# Run the DDL statements
# ==============================================================================
echo "Running ALTER TABLE statements against tpchsf${SCALE_FACTOR}..."
# Same Python-env story as analyze: coord container has Python 3.9, so we
# rely on miniforge (mounted from the checkout) via run_py_script.sh's
# conda fallback path.
run_coord_image "export MINIFORGE_HOME=/workspace/miniforge3; \
    export HOME=/workspace; \
    cd /workspace/scripts; \
    ./run_py_script.sh \
        -p /workspace/presto/testing/integration_tests/alter_tables.py \
        -r /workspace/presto/testing/requirements.txt \
        --schema-name tpchsf${SCALE_FACTOR} \
        --sql-file ${CONTAINER_SQL_FILE} \
        --host ${COORD} \
        --port ${PORT} \
        --verbose" "cli"

echo "========================================"
echo "Alter tables complete."
echo "Hive metastore updated at: ${VT_ROOT}/.hive_metastore"
echo "Logs available at: ${LOGS}"
echo "========================================"
