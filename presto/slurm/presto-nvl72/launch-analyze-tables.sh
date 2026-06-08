#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# Presto Analyze Tables Launcher
# ==============================================================================
# Submits run-analyze-tables.slurm to Slurm.  ANALYZE TABLE disables cudf in
# the worker configs (see run-analyze-tables.sh), so this is a CPU-only
# workload regardless of cluster — values are always pulled from the
# CLUSTER_CPU_* section of ~/.cluster_config.env.  Override -w/-c if you
# need a non-default worker/coordinator image.
#
# Usage:
#   ./launch-analyze-tables.sh -s|--scale-factor <sf> [-n|--nodes <count>]
#                              [-d|--data-dir <path>]
#                              [-g|--num-workers-per-node <n>]
#                              [-w|--worker-image <name>] [-c|--coord-image <name>]
#                              [additional sbatch options]
#
# Examples:
#   # SF100, single node, default data path
#   ./launch-analyze-tables.sh -s 100
#
#   # SF3000, 2 nodes
#   ./launch-analyze-tables.sh -s 3000 -n 2
# ==============================================================================

set -e

cd "$(dirname "$0")"
source ./defaults.env
source ./launcher_common.sh

# Defaults
NODES_COUNT="1"
SCALE_FACTOR=""
NUM_GPUS_PER_NODE=""   # resolved from cluster config after arg parsing
COORD_IMAGE=""         # resolved from cluster config after arg parsing; override with -c
WORKER_IMAGE=""        # resolved from cluster config after arg parsing; override with -w
DATA_DIR=""            # empty => slurm script uses default from cluster_config.env
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--scale-factor)         requires_value "$1" "${2:-}"; SCALE_FACTOR="$2"; shift 2 ;;
        -n|--nodes)                requires_value "$1" "${2:-}"; NODES_COUNT="$2"; shift 2 ;;
        -d|--data-dir)             requires_value "$1" "${2:-}"; DATA_DIR="$2"; shift 2 ;;
        -g|--num-workers-per-node) requires_value "$1" "${2:-}"; NUM_GPUS_PER_NODE="$2"; shift 2 ;;
        -w|--worker-image)         requires_value "$1" "${2:-}"; WORKER_IMAGE="$2"; shift 2 ;;
        -c|--coord-image)          requires_value "$1" "${2:-}"; COORD_IMAGE="$2"; shift 2 ;;
        --) shift; break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

if [[ -z "${SCALE_FACTOR}" ]]; then
    echo "Error: -s|--scale-factor is required"
    echo "Usage: $0 -s <sf> [-n <nodes>] [-d <data_dir>] [sbatch options...]"
    exit 1
fi

# ANALYZE TABLE disables cudf in worker configs — it's a CPU-only workload.
# Always resolve from the CPU section regardless of cluster topology.
VARIANT_TYPE="cpu"
resolve_cluster_variant cpu
: "${NUM_GPUS_PER_NODE:=${CLUSTER_NUM_WORKERS_PER_NODE:-}}"
: "${USE_NUMA:=${CLUSTER_USE_NUMA:-0}}"

# Validate required values before submitting
[[ -z "${WORKER_IMAGE}" ]]          && { echo "Error: worker image not set — set CLUSTER_CPU_DEFAULT_WORKER_IMAGE in cluster_config.env or pass -w"; exit 1; }
[[ -z "${COORD_IMAGE}" ]]           && { echo "Error: coordinator image not set — set CLUSTER_CPU_DEFAULT_COORD_IMAGE in cluster_config.env or pass -c"; exit 1; }
[[ -z "${CLUSTER_CPUS_PER_TASK}" ]] && { echo "Error: CLUSTER_CPU_CPUS_PER_TASK not set in cluster_config.env"; exit 1; }
[[ -z "${CLUSTER_TIME_ANALYZE}" ]]  && { echo "Error: CLUSTER_CPU_TIME_ANALYZE not set in cluster_config.env"; exit 1; }
[[ -z "${NUM_GPUS_PER_NODE}" ]]     && { echo "Error: CLUSTER_CPU_NUM_WORKERS_PER_NODE not set in cluster_config.env or pass -g"; exit 1; }
[[ -z "${CLUSTER_DEFAULT_PORT}" ]]  && { echo "Error: CLUSTER_CPU_DEFAULT_PORT not set in cluster_config.env"; exit 1; }

# Build sbatch arguments sourced from cluster config
build_cluster_sbatch_args "${CLUSTER_TIME_ANALYZE}"

# Pre-flight: verify prerequisites before queueing the job.
preflight_image "${WORKER_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -w <name>"
preflight_image "${COORD_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -c <name>"
preflight_dir "${DATA_DIR:-${DATA}}" "TPC-H data" \
    "./launch-gen-data.sh -s ${SCALE_FACTOR} -o ${DATA_DIR:-${DATA}}"

# Clean up stale logs/output files from previous runs
rm -f logs/* *.out *.err 2>/dev/null || true
mkdir -p logs

SCRIPT_DIR="$PWD"

build_common_export_vars
[[ -n "${DATA_DIR}" ]] && EXPORT_VARS+=",DATA=${DATA_DIR}"
# Forward DATASET_NAME explicitly. The slurm wrapper has `:= tpch-rs-${SF}`
# as the default, so relying on `--export=ALL` inheritance is too fragile when
# the on-disk layout uses the older `scale-${SF}` convention.
[[ -n "${DATASET_NAME:-}" ]] && EXPORT_VARS+=",DATASET_NAME=${DATASET_NAME}"

OUT_FMT="presto-analyze_n${NODES_COUNT}_sf${SCALE_FACTOR}_%j.out"
ERR_FMT="presto-analyze_n${NODES_COUNT}_sf${SCALE_FACTOR}_%j.err"

echo "Submitting Presto Analyze Tables job..."
echo "  Scale factor   : SF${SCALE_FACTOR}"
echo "  Nodes          : ${NODES_COUNT}"
echo "  Workers/node   : ${NUM_GPUS_PER_NODE}  (total workers: $((NODES_COUNT * NUM_GPUS_PER_NODE)))"
echo "  Worker image   : ${WORKER_IMAGE}"
echo "  Data dir       : ${DATA_DIR:-<default from cluster_config.env>}"
echo ""

JOB_ID=$(sbatch \
    --nodes="${NODES_COUNT}" \
    "${CLUSTER_SBATCH_ARGS[@]}" \
    --export="${EXPORT_VARS}" \
    --output="${OUT_FMT}" \
    --error="${ERR_FMT}" \
    "${EXTRA_ARGS[@]}" \
    run-analyze-tables.slurm | awk '{print $NF}')

OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

echo "Job submitted with ID: $JOB_ID"
echo ""
print_monitor_hints "${JOB_ID}" "${OUT_FILE}" "${ERR_FILE}" \
    "tail -f logs/coord.log" \
    "tail -f logs/worker_*.log" \
    "tail -f logs/cli.log"
echo ""
echo "Waiting for job to complete..."
wait_for_job "${JOB_ID}" 10

show_job_output "${OUT_FILE}" "${ERR_FILE}" "logs/cli.log" "CLI log"
[[ "${JOB_STATE}" == "COMPLETED" ]] || exit 1

echo ""
echo "Hive metastore updated at: ${VT_ROOT}/.hive_metastore"
