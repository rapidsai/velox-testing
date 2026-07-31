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
    export "${_var}"="${!_var:?${_var} is required}"
done
unset _var

# Paths
VT_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd -P)"
export VT_ROOT
source "${SCRIPT_DIR}/defaults.env"
export DATA IMAGE_DIR HIVE_METASTORE_SHARED_ROOT HIVE_METASTORE_VERSION
export LOGS="${SCRIPT_DIR}/logs"
export RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/result_dir}"
export VARIANT_TYPE="${VARIANT_TYPE:-gpu}"
export USE_NUMA="${USE_NUMA:-0}"
export CONFIGS="${VT_ROOT}/presto/docker/config/generated/${VARIANT_TYPE}"

# Computed values
export NUM_NODES="${SLURM_JOB_NUM_NODES}"
COORD="$(scontrol show hostnames "${SLURM_JOB_NODELIST}" | head -n 1)"
export COORD
export NUM_WORKERS=$((NUM_NODES * NUM_GPUS_PER_NODE))
# generate_presto_config.sh runs once on a host but describes the full cluster.
# Keep local resource partitioning separate from NUM_WORKERS so a two-host,
# one-worker-per-host CPU run does not mistakenly divide each host by two.
export CPU_WORKERS_PER_HOST="${CPU_WORKERS_PER_HOST:-${NUM_GPUS_PER_NODE}}"

# Presto configuration
export PORT="${PORT:-${CLUSTER_DEFAULT_PORT:?CLUSTER_DEFAULT_PORT not set — check cluster_config.env}}"
export CUDF_LIB=/usr/lib64/presto-native-libs

# UCX configuration (common subset; .slurm files may add mode-specific knobs).
# Keep exported UCX_* values when the launcher sets an experiment explicitly.
# Remapped CPU containers default to sysv for same-host traffic and TCP between
# hosts; explicit experiments can still select POSIX or RDMA transports.
# Retain the existing conservative GPU default for runs that do not opt in.
_ucx_tls_default="^ib,ud:aux,sm"
[[ "${VARIANT_TYPE}" == "cpu" ]] && _ucx_tls_default="sysv,tcp,self"
export UCX_TLS="${UCX_TLS:-${_ucx_tls_default}}"
unset _ucx_tls_default
if [[ -n "${UCX_NET_DEVICES:-}" ]]; then
    export UCX_NET_DEVICES
elif [[ -n "${CLUSTER_UCX_NET_DEVICES:-}" ]]; then
    export UCX_NET_DEVICES="${CLUSTER_UCX_NET_DEVICES}"
fi
if [[ "${UCX_TLS}" == *^ib* && ( "${UCX_NET_DEVICES:-}" == *mlx5_* || "${CLUSTER_UCX_NET_DEVICES_BY_LOCAL_WORKER:-}" == *mlx5_* || "${CLUSTER_UCX_NET_DEVICES_BY_GPU:-}" == *mlx5_* ) ]]; then
    echo "Error: the configured UCX device selection includes mlx5 RDMA, but UCX_TLS=${UCX_TLS} excludes IB/RDMA." >&2
    echo "Set UCX_TLS explicitly, e.g. UCX_TLS=rc,cuda_copy,cuda_ipc,self, or use a TCP netdev such as bond0." >&2
    exit 1
fi
if [[ -n "${CLUSTER_UCX_NET_DEVICES_BY_GPU:-}" && -z "${PRESTO_INTERNAL_ADDRESS_INTERFACE_BY_GPU:-${CLUSTER_INTERNAL_ADDRESS_INTERFACE_BY_GPU:-}}" ]]; then
    echo "Warning: CLUSTER_UCX_NET_DEVICES_BY_GPU is set without a per-GPU internal address interface map." >&2
    echo "         Each worker must advertise an IP on the same rail as its UCX_NET_DEVICES entry." >&2
fi
export UCX_RNDV_PIPELINE_ERROR_HANDLING="${UCX_RNDV_PIPELINE_ERROR_HANDLING:-y}"
export UCX_TCP_KEEPINTVL="${UCX_TCP_KEEPINTVL:-1ms}"
export UCX_KEEPALIVE_INTERVAL="${UCX_KEEPALIVE_INTERVAL:-1ms}"
[[ -n "${UCX_MAX_RNDV_RAILS:-}" ]] && export UCX_MAX_RNDV_RAILS
[[ -n "${UCX_PROTO_INFO:-}" ]] && export UCX_PROTO_INFO
[[ -n "${UCX_LOG_LEVEL:-}" ]] && export UCX_LOG_LEVEL

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
echo "Results directory: ${RESULT_DIR}"
echo "Total workers: ${NUM_WORKERS} (${NUM_NODES} nodes x ${NUM_GPUS_PER_NODE} workers/node)"
[[ "${VARIANT_TYPE}" == "cpu" ]] && echo "CPU workers per host: ${CPU_WORKERS_PER_HOST}"
