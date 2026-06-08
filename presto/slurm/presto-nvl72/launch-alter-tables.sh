#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# Presto Alter Tables Launcher
# ==============================================================================
# Submits run-alter-tables.slurm to Slurm. Starts a CPU-mode Presto cluster
# (cudf disabled, mirroring launch-analyze-tables.sh) and runs the user-
# supplied SQL file against the schema tpchsf<sf>.
#
# The SQL file must live under VT_ROOT (this checkout) so it is visible
# inside the coordinator/cli container via the standard ${VT_ROOT}:/workspace
# bind mount.
#
# Tip: snapshot ${VT_ROOT}/.hive_metastore first if you may want to restore.
#
# Usage:
#   ./launch-alter-tables.sh -s|--scale-factor <sf> -f|--sql-file <path>
#                            [-n|--nodes <count>] [-g|--num-workers-per-node <n>]
#                            [-w|--worker-image <name>] [-c|--coord-image <name>]
#                            [additional sbatch options]
#
# Examples:
#   # Apply alterations defined in a SQL file to the SF1000 schema:
#   ./launch-alter-tables.sh -s 1000 -f ./alterations/add_partitioning.sql
# ==============================================================================

set -e

cd "$(dirname "$0")"
source ./defaults.env
source ./launcher_common.sh

NODES_COUNT="1"
SCALE_FACTOR=""
SQL_FILE=""
NUM_GPUS_PER_NODE=""   # resolved from cluster config after arg parsing
COORD_IMAGE=""         # resolved from cluster config after arg parsing; override with -c
WORKER_IMAGE=""        # resolved from cluster config after arg parsing; override with -w
EXTRA_ARGS=()

usage() {
    cat <<EOF
Usage: $0 -s <sf> -f <sql-file> [OPTIONS]

Starts a CPU-mode Presto cluster, then applies the ;-separated SQL statements
in <sql-file> against the schema tpchsf<sf>.

Required:
  -s, --scale-factor <sf>             Scale factor (target schema = tpchsf<sf>)
  -f, --sql-file <path>               File of ;-separated SQL statements
                                      (must live under VT_ROOT = ${VT_ROOT})

Options:
  -n, --nodes <count>                 Nodes for the job (default: 1)
  -g, --num-workers-per-node <n>      Workers per node (override cluster default)
  -w, --worker-image <name>           Override worker image
  -c, --coord-image <name>            Override coordinator image
  -h, --help                          Show this help

Cluster config (~/.cluster_config.env) supplies partition, account, time limit,
default images, and per-variant defaults — same as the analyze launcher.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--scale-factor)         requires_value "$1" "${2:-}"; SCALE_FACTOR="$2"; shift 2 ;;
        -f|--sql-file)             requires_value "$1" "${2:-}"; SQL_FILE="$2"; shift 2 ;;
        -n|--nodes)                requires_value "$1" "${2:-}"; NODES_COUNT="$2"; shift 2 ;;
        -g|--num-workers-per-node) requires_value "$1" "${2:-}"; NUM_GPUS_PER_NODE="$2"; shift 2 ;;
        -w|--worker-image)         requires_value "$1" "${2:-}"; WORKER_IMAGE="$2"; shift 2 ;;
        -c|--coord-image)          requires_value "$1" "${2:-}"; COORD_IMAGE="$2"; shift 2 ;;
        -h|--help)                 usage; exit 0 ;;
        --) shift; break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

if [[ -z "${SCALE_FACTOR}" ]]; then
    echo "Error: -s|--scale-factor is required" >&2
    usage >&2
    exit 1
fi
if [[ -z "${SQL_FILE}" ]]; then
    echo "Error: -f|--sql-file is required" >&2
    usage >&2
    exit 1
fi
if [[ ! -f "${SQL_FILE}" ]]; then
    echo "Error: SQL file not found: ${SQL_FILE}" >&2
    exit 1
fi

# Resolve to absolute path so the under-VT_ROOT check is unambiguous.
SQL_FILE="$(readlink -f "${SQL_FILE}")"
case "${SQL_FILE}" in
    "${VT_ROOT}"/*) : ;;
    *)
        echo "Error: --sql-file must live under VT_ROOT (${VT_ROOT}); got ${SQL_FILE}" >&2
        echo "Move or symlink the file into the checkout so the worker container can mount it." >&2
        exit 1
        ;;
esac

# DDL via Presto disables cudf in the workers — always resolve from the CPU
# section, regardless of cluster default. Same rationale as launch-analyze-tables.sh.
VARIANT_TYPE="cpu"
resolve_cluster_variant cpu
: "${NUM_GPUS_PER_NODE:=${CLUSTER_NUM_WORKERS_PER_NODE:-}}"
: "${USE_NUMA:=${CLUSTER_USE_NUMA:-0}}"

[[ -z "${WORKER_IMAGE}" ]]          && { echo "Error: worker image not set — set CLUSTER_CPU_DEFAULT_WORKER_IMAGE in cluster_config.env or pass -w"; exit 1; }
[[ -z "${COORD_IMAGE}" ]]           && { echo "Error: coordinator image not set — set CLUSTER_CPU_DEFAULT_COORD_IMAGE in cluster_config.env or pass -c"; exit 1; }
[[ -z "${CLUSTER_CPUS_PER_TASK}" ]] && { echo "Error: CLUSTER_CPU_CPUS_PER_TASK not set in cluster_config.env"; exit 1; }
[[ -z "${CLUSTER_TIME_ANALYZE}" ]]  && { echo "Error: CLUSTER_CPU_TIME_ANALYZE not set in cluster_config.env"; exit 1; }
[[ -z "${NUM_GPUS_PER_NODE}" ]]     && { echo "Error: CLUSTER_CPU_NUM_WORKERS_PER_NODE not set in cluster_config.env or pass -g"; exit 1; }
[[ -z "${CLUSTER_DEFAULT_PORT}" ]]  && { echo "Error: CLUSTER_CPU_DEFAULT_PORT not set in cluster_config.env"; exit 1; }

# Reuse the analyze time budget — both are short metastore workloads.
build_cluster_sbatch_args "${CLUSTER_TIME_ANALYZE}"

preflight_image "${WORKER_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -w <name>"
preflight_image "${COORD_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -c <name>"

# Clean up stale logs/output files
rm -f logs/* *.out *.err 2>/dev/null || true
mkdir -p logs

SCRIPT_DIR="$PWD"

build_common_export_vars
# ALTER_SQL_FILE has no commas, so riding EXPORT_VARS directly is safe.
EXPORT_VARS+=",ALTER_SQL_FILE=${SQL_FILE}"

OUT_FMT="presto-alter_n${NODES_COUNT}_sf${SCALE_FACTOR}_%j.out"
ERR_FMT="presto-alter_n${NODES_COUNT}_sf${SCALE_FACTOR}_%j.err"

echo "Submitting Presto Alter Tables job..."
echo "  Scale factor : SF${SCALE_FACTOR} (target schema = tpchsf${SCALE_FACTOR})"
echo "  Nodes        : ${NODES_COUNT}"
echo "  Workers/node : ${NUM_GPUS_PER_NODE}"
echo "  Worker image : ${WORKER_IMAGE}"
echo "  SQL file     : ${SQL_FILE}"
echo ""

JOB_ID=$(sbatch \
    --nodes="${NODES_COUNT}" \
    "${CLUSTER_SBATCH_ARGS[@]}" \
    --export="${EXPORT_VARS}" \
    --output="${OUT_FMT}" \
    --error="${ERR_FMT}" \
    "${EXTRA_ARGS[@]}" \
    run-alter-tables.slurm | awk '{print $NF}')

OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

echo "Job submitted with ID: $JOB_ID"
echo ""
print_monitor_hints "${JOB_ID}" "${OUT_FILE}" "${ERR_FILE}" \
    "tail -f logs/coord.log" \
    "tail -f logs/cli.log"
echo ""
echo "Waiting for job to complete..."
wait_for_job "${JOB_ID}" 10

show_job_output "${OUT_FILE}" "${ERR_FILE}" "logs/cli.log" "CLI log"
[[ "${JOB_STATE}" == "COMPLETED" ]] || exit 1

echo ""
echo "Hive metastore updated at: ${VT_ROOT}/.hive_metastore"
