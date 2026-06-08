#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Interactive shell on a compute node with a container image.
#
# Cluster-specific values (partition, account, cpus-per-task, image, workers
# per node, extra mounts) are read from ~/.cluster_config.env (or the path in
# $CLUSTER_CONFIG).  See cluster_config.env.example.
#
# By default Slurm picks any available node in the partition.  Set NODELIST
# to pin to a specific node or a range.
#
# Usage:
#   ./run-interactive.sh [--cpu] [additional srun options]
#
# Environment overrides:
#   IMAGE, TIME_LIMIT, NODELIST — overridable per-run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/defaults.env"
source "${SCRIPT_DIR}/launcher_common.sh"

VARIANT_TYPE=""
EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cpu) VARIANT_TYPE="cpu"; shift ;;
        --gpu) VARIANT_TYPE="gpu"; shift ;;
        --) shift; break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

# Default falls through CLUSTER_DEFAULT_VARIANT (set in ~/.cluster_config.env)
# to "gpu" so existing GPU-cluster users see no change.
VARIANT_TYPE="${VARIANT_TYPE:-${CLUSTER_DEFAULT_VARIANT:-gpu}}"
# resolve_cluster_variant populates WORKER_IMAGE; alias it to IMAGE for this
# script's existing convention (single image for the interactive shell).
resolve_cluster_variant "${VARIANT_TYPE}"
: "${IMAGE:=${WORKER_IMAGE}}"

: "${TIME_LIMIT:=01:00:00}"

IMAGE=$(resolve_image_path "${IMAGE}")

VTYPE_UPPER="${VARIANT_TYPE^^}"
[[ -z "${IMAGE}" ]]                  && { echo "Error: worker image not set — set CLUSTER_${VTYPE_UPPER}_DEFAULT_WORKER_IMAGE in cluster_config.env or export IMAGE"; exit 1; }
[[ -z "${CLUSTER_CPUS_PER_TASK}" ]]  && { echo "Error: CLUSTER_${VTYPE_UPPER}_CPUS_PER_TASK not set in cluster_config.env"; exit 1; }
if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
    [[ -z "${CLUSTER_NUM_WORKERS_PER_NODE}" ]] && { echo "Error: CLUSTER_GPU_NUM_WORKERS_PER_NODE not set in cluster_config.env"; exit 1; }
fi

# build_cluster_sbatch_args produces --partition / --account / --cpus-per-task,
# which srun accepts as well; reuse it (with no --time) so this script stays in
# step with the sbatch-based launchers.
build_cluster_sbatch_args
preflight_image "${IMAGE}" \
    "Pull the image (see ./pull_ghcr_image.sh) or export IMAGE=<path>"

GRES_ARG=()
[[ "${VARIANT_TYPE}" == "gpu" ]] && GRES_ARG=(--gres="gpu:${CLUSTER_NUM_WORKERS_PER_NODE}")

NODELIST_ARG=()
if [[ -n "${NODELIST:-}" ]]; then
    NODELIST_ARG=(--nodelist="${NODELIST}")
fi

MOUNTS="${HOME}:${HOME}"
[[ -n "${CLUSTER_EXTRA_MOUNTS:-}" ]] && MOUNTS="${MOUNTS},${CLUSTER_EXTRA_MOUNTS}"

srun --nodes=1 \
     "${NODELIST_ARG[@]}" \
     --ntasks-per-node=1 \
     "${CLUSTER_SBATCH_ARGS[@]}" \
     "${GRES_ARG[@]}" \
     --exclusive \
     --time="${TIME_LIMIT}" \
     --container-image="${IMAGE}" \
     --container-mounts="${MOUNTS}" \
     --container-remap-root \
     --container-writable \
     "${EXTRA_ARGS[@]}" \
     --pty bash
