#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# slurm_common.sh — shared env setup sourced by the run-*.slurm wrappers.
# ==============================================================================
# Sourced (not executed) by .slurm files after their #SBATCH directives and
# any mode-specific required-var checks. Validates the env vars common to
# every workflow, sets up paths, computes derived values, exports the shared
# Presto / UCX environment, and prints the shared pre-flight info block.
# Callers may print additional mode-specific lines and the closing "===="
# separator after sourcing.
#
# Required env vars (must be set via --export by the launcher):
#   SCALE_FACTOR, SCRIPT_DIR, WORKER_IMAGE, COORD_IMAGE, NUM_GPUS_PER_NODE
# ==============================================================================

for _var in SCALE_FACTOR SCRIPT_DIR WORKER_IMAGE COORD_IMAGE NUM_GPUS_PER_NODE; do
    if [ -z "${!_var:-}" ]; then
        echo "Error: ${_var} is required." >&2
        exit 1
    fi
    export "${_var}"
done
unset _var

# Paths
export VT_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd -P)"
source "${SCRIPT_DIR}/defaults.env"
export DATA IMAGE_DIR HIVE_METASTORE_SHARED_ROOT HIVE_METASTORE_VERSION
export LOGS="${SCRIPT_DIR}/logs"
export VARIANT_TYPE="${VARIANT_TYPE:-gpu}"
export USE_NUMA="${USE_NUMA:-0}"
export CONFIGS="${VT_ROOT}/presto/docker/config/generated/${VARIANT_TYPE}"

# Computed values
export NUM_NODES="${SLURM_JOB_NUM_NODES}"
export COORD="$(scontrol show hostnames "${SLURM_JOB_NODELIST}" | head -n 1)"
export NUM_WORKERS=$((NUM_NODES * NUM_GPUS_PER_NODE))

# Presto configuration
export PORT="${PORT:-${CLUSTER_DEFAULT_PORT:?CLUSTER_DEFAULT_PORT not set — check cluster_config.env}}"
export CUDF_LIB=/usr/lib64/presto-native-libs

# UCX configuration (common subset; .slurm files may add mode-specific knobs)
export UCX_TLS=^ib,ud:aux,sm
if [[ -n "${CLUSTER_UCX_NET_DEVICES:-}" ]]; then
    export UCX_NET_DEVICES="${CLUSTER_UCX_NET_DEVICES}"
fi
export UCX_RNDV_PIPELINE_ERROR_HANDLING=y
export UCX_TCP_KEEPINTVL=1ms
export UCX_KEEPALIVE_INTERVAL=1ms

mkdir -p "${LOGS}"

# Pre-flight info — shared header without a trailing separator so callers can
# echo mode-specific lines before closing the block.
echo "========================================"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Nodes: ${SLURM_JOB_NUM_NODES}"
echo "Node list: ${SLURM_JOB_NODELIST}"
echo "Coordinator node: ${COORD}"
echo "Worker image: ${WORKER_IMAGE}"
echo "Coord image: ${COORD_IMAGE}"
echo "Scale factor: ${SCALE_FACTOR}"
echo "Variant: ${VARIANT_TYPE}"
echo "Data directory: ${DATA}"
echo "Image directory: ${IMAGE_DIR}"
echo "Logs directory: ${LOGS}"
echo "Total workers: ${NUM_WORKERS} (${NUM_NODES} nodes x ${NUM_GPUS_PER_NODE} workers/node)"
