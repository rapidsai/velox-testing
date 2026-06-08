#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# ==============================================================================
# Presto TPC-H Benchmark Launcher
# ==============================================================================
# Submits a Presto TPC-H benchmark job to Slurm.  Cluster-specific values
# (partition, time limits, image names, etc.) are read from ~/.cluster_config.env
# (or the path in $CLUSTER_CONFIG).  See cluster_config.env.example.
#
# Usage:
#   ./launch-run.sh -n|--nodes <count> -s|--scale-factor <sf>
#                  [-i|--iterations <n>] [--cpu] [-g|--num-workers-per-node <n>]
#                  [-w|--worker-image <name>] [-c|--coord-image <name>]
#                  [-o|--output-path <dir>] [-q|--queries <filter>]
#                  [--disable-gds] [-m|--metrics] [-p|--profile]
#                  [additional sbatch options]
# ==============================================================================

set -euo pipefail

# Change to script directory
cd "$(dirname "$0")"

source ./defaults.env
source ./launcher_common.sh

NODES_COUNT=""
SCALE_FACTOR=""
NUM_ITERATIONS="2"
EXTRA_ARGS=()
NUM_GPUS_PER_NODE=""   # resolved from cluster config after arg parsing
USE_NUMA=""            # resolved from cluster config after arg parsing
VARIANT_TYPE=""        # set by --cpu; resolved from cluster config after arg parsing
WORKER_IMAGE=""        # resolved from cluster config after arg parsing; override with -w
COORD_IMAGE=""         # resolved from cluster config after arg parsing; override with -c
OUTPUT_PATH=""
SCRIPT_DIR="$PWD"
# WORKER_ENV_FILE defaults to ${SCRIPT_DIR}/worker.env via launcher_common.sh.
# Override with --worker-env-file <path> below.
ENABLE_GDS=1
ENABLE_METRICS=0
ENABLE_NSYS=0
NSYS_WORKER_ID=0
QUERIES=""

usage() {
    cat <<EOF
Usage: $0 -n <nodes> -s <sf> [OPTIONS] [-- <additional sbatch options>]

Submits a Presto TPC-H benchmark job to Slurm.

Required:
  -n, --nodes <count>          Number of nodes for the benchmark job
  -s, --scale-factor <sf>      TPC-H scale factor (e.g. 100, 1000, 3000)

Options:
  -i, --iterations <n>         Iterations per query (default: ${NUM_ITERATIONS})
  -g, --num-workers-per-node <n>  Override workers per node from cluster config
  -w, --worker-image <name>    Override worker image from cluster config
  -c, --coord-image <name>     Override coordinator image from cluster config
  -o, --output-path <dir>      Copy results into this directory after the run
  -q, --queries <list>         Comma-separated query filter (e.g. "1,6,21")
      --worker-env-file <path> Override worker.env (default: ./worker.env)
      --cpu                    Use CPU partition/images (overrides cluster default)
      --gpu                    Use GPU partition/images (overrides cluster default)
      --no-numa                Disable NUMA pinning for workers
      --disable-gds            Disable GPU Direct Storage
  -m, --metrics                Enable metrics collection
  -p, --profile                Enable nsys profiling
      --nsys-worker-id <n>     Worker ID to profile (default: ${NSYS_WORKER_ID})
  -h, --help                   Show this help message and exit

Any arguments after -- are passed directly to sbatch.

Cluster config (~/.cluster_config.env or \$CLUSTER_CONFIG) supplies partition,
account, time limits, image names, and per-variant defaults. See
cluster_config.env.example.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--nodes)               requires_value "$1" "${2:-}"; NODES_COUNT="$2"; shift 2 ;;
        -s|--scale-factor)        requires_value "$1" "${2:-}"; SCALE_FACTOR="$2"; shift 2 ;;
        -i|--iterations)          requires_value "$1" "${2:-}"; NUM_ITERATIONS="$2"; shift 2 ;;
        -g|--num-workers-per-node) requires_value "$1" "${2:-}"; NUM_GPUS_PER_NODE="$2"; shift 2 ;;
        -w|--worker-image)        requires_value "$1" "${2:-}"; WORKER_IMAGE="$2"; shift 2 ;;
        -c|--coord-image)         requires_value "$1" "${2:-}"; COORD_IMAGE="$2"; shift 2 ;;
        -o|--output-path)         requires_value "$1" "${2:-}"; OUTPUT_PATH="$2"; shift 2 ;;
        -q|--queries)             requires_value "$1" "${2:-}"; QUERIES="$2"; shift 2 ;;
        --worker-env-file)        requires_value "$1" "${2:-}"; WORKER_ENV_FILE="$2"; shift 2 ;;
        --nsys-worker-id)         requires_value "$1" "${2:-}"; NSYS_WORKER_ID="$2"; shift 2 ;;
        --cpu)         VARIANT_TYPE="cpu"; shift ;;
        --gpu)         VARIANT_TYPE="gpu"; shift ;;
        --no-numa)     USE_NUMA="0"; shift ;;
        --disable-gds) ENABLE_GDS=0; shift ;;
        -m|--metrics)  ENABLE_METRICS=1; shift ;;
        -p|--profile)  ENABLE_NSYS=1; shift ;;
        -h|--help)     usage; exit 0 ;;
        --) shift; break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

[[ -z "${NODES_COUNT}"  ]] && { echo "Error: -n|--nodes is required (see --help)" >&2; exit 1; }
[[ -z "${SCALE_FACTOR}" ]] && { echo "Error: -s|--scale-factor is required (see --help)" >&2; exit 1; }

# Clean up old output files — use rm -rf so subdirectories (e.g. query_results/)
# are fully removed and stale benchmark_result.json cannot survive a cancelled run.
rm -rf result_dir logs 2>/dev/null || true
rm -f *.out *.err 2>/dev/null || true
mkdir -p result_dir logs

echo "Submitting Presto TPC-H benchmark job..."
echo ""

# Resolve variant-specific cluster values now that VARIANT_TYPE is known.
# Default falls through CLUSTER_DEFAULT_VARIANT (set in ~/.cluster_config.env)
# to "gpu" so existing GPU-cluster users see no change.
VARIANT_TYPE="${VARIANT_TYPE:-${CLUSTER_DEFAULT_VARIANT:-gpu}}"
resolve_cluster_variant "${VARIANT_TYPE}"
: "${NUM_GPUS_PER_NODE:=${CLUSTER_NUM_WORKERS_PER_NODE:-}}"
: "${USE_NUMA:=${CLUSTER_USE_NUMA:-0}}"

# Validate required values before submitting
VTYPE_UPPER="${VARIANT_TYPE^^}"
[[ -z "${WORKER_IMAGE}" ]]           && { echo "Error: worker image not set — set CLUSTER_${VTYPE_UPPER}_DEFAULT_WORKER_IMAGE in cluster_config.env or pass -w"; exit 1; }
[[ -z "${COORD_IMAGE}" ]]            && { echo "Error: coordinator image not set — set CLUSTER_${VTYPE_UPPER}_DEFAULT_COORD_IMAGE in cluster_config.env or pass -c"; exit 1; }
[[ -z "${CLUSTER_CPUS_PER_TASK}" ]]  && { echo "Error: CLUSTER_${VTYPE_UPPER}_CPUS_PER_TASK not set in cluster_config.env"; exit 1; }
[[ -z "${CLUSTER_TIME_BENCHMARK}" ]] && { echo "Error: CLUSTER_${VTYPE_UPPER}_TIME_BENCHMARK not set in cluster_config.env"; exit 1; }
[[ -z "${NUM_GPUS_PER_NODE}" ]]      && { echo "Error: CLUSTER_${VTYPE_UPPER}_NUM_WORKERS_PER_NODE not set in cluster_config.env or pass -g"; exit 1; }
[[ -z "${CLUSTER_DEFAULT_PORT}" ]]   && { echo "Error: CLUSTER_${VTYPE_UPPER}_DEFAULT_PORT not set in cluster_config.env"; exit 1; }

# Build sbatch arguments sourced from cluster config
build_cluster_sbatch_args "${CLUSTER_TIME_BENCHMARK}"

# Pre-flight: verify prerequisites before queueing the job.
ANALYZE_HINT="./launch-analyze-tables.sh -s ${SCALE_FACTOR}"
[[ "${VARIANT_TYPE}" == "cpu" ]] && ANALYZE_HINT+=" --cpu"
preflight_image "${WORKER_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -w <name>"
preflight_image "${COORD_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -c <name>"
preflight_dir "${DATA}/tpch-rs-${SCALE_FACTOR}" "TPC-H SF${SCALE_FACTOR} data" \
    "./launch-gen-data.sh -s ${SCALE_FACTOR} -o ${DATA}/tpch-rs-${SCALE_FACTOR}"
preflight_metastore "${SCALE_FACTOR}" "${ANALYZE_HINT}"

# Submit job (include nodes/SF/iterations in file names)
OUT_FMT="logs/presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.out"
ERR_FMT="logs/presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}_i${NUM_ITERATIONS}_%j.err"
JOB_NAME="presto-tpch-run_n${NODES_COUNT}_sf${SCALE_FACTOR}"
# NODELIST is unset by default -- Slurm picks any available nodes.
# Export NODELIST=<host-or-range> before invoking to pin.
NODELIST="${NODELIST:-}"
NODELIST_ARG=()
if [[ -n "${NODELIST}" ]]; then
    NODELIST_ARG=(--nodelist="${NODELIST}")
fi
GRES_OPT=$([[ "$VARIANT_TYPE" == "gpu" ]] && echo "--gres=gpu:${NUM_GPUS_PER_NODE}" || echo "")

build_common_export_vars
EXPORT_VARS+=",NUM_ITERATIONS=${NUM_ITERATIONS}"
EXPORT_VARS+=",ENABLE_GDS=${ENABLE_GDS},ENABLE_METRICS=${ENABLE_METRICS}"
EXPORT_VARS+=",ENABLE_NSYS=${ENABLE_NSYS},NSYS_WORKER_ID=${NSYS_WORKER_ID}"
# Comma-separated query list can't ride EXPORT_VARS (comma is the separator);
# export it so sbatch picks it up via the ALL inheritance.
[[ -n "${QUERIES}" ]] && export QUERIES
JOB_ID=$(sbatch --job-name="${JOB_NAME}" --nodes="${NODES_COUNT}" "${NODELIST_ARG[@]}" \
"${CLUSTER_SBATCH_ARGS[@]}" \
--export="${EXPORT_VARS}" \
--output="${OUT_FMT}" --error="${ERR_FMT}" "${EXTRA_ARGS[@]}" ${GRES_OPT} \
run-presto-benchmarks.slurm | awk '{print $NF}')
OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

# Resolve and print first node IP once nodes are allocated
echo "Resolving first node IP..."
for i in {1..60}; do
    STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null || true)
    NODELIST=$(squeue -j "$JOB_ID" -h -o "%N" 2>/dev/null || true)
    if [[ -n "${NODELIST:-}" && "${NODELIST}" != "(null)" ]]; then
        FIRST_NODE=$(scontrol show hostnames "$NODELIST" | head -n 1)
        if [[ -n "${FIRST_NODE:-}" ]]; then
            part=$(scontrol getaddrs "$FIRST_NODE" 2>/dev/null | awk 'NR==1{print $2}')
            FIRST_IP="${part%%:*}"
            if [[ -n "${CLUSTER_SSH_TUNNEL_HOST:-}" ]]; then
                echo "Run this command to access the Presto Web UI:"
                echo "  ssh -N -L ${CLUSTER_DEFAULT_PORT}:${FIRST_IP}:${CLUSTER_DEFAULT_PORT} ${CLUSTER_SSH_TUNNEL_HOST}"
                echo "The UI will be available at http://localhost:${CLUSTER_DEFAULT_PORT}"
            else
                echo "Coordinator is accessible at ${FIRST_IP}:${CLUSTER_DEFAULT_PORT}"
            fi
            echo ""
            break
        fi
    fi
    sleep 5
done

echo "Job submitted with ID: $JOB_ID"
echo ""
print_monitor_hints "${JOB_ID}" "${OUT_FILE}" "${ERR_FILE}" \
    "tail -f logs/coord.log" \
    "tail -f logs/worker_*.log" \
    "tail -f logs/cli.log"
echo ""
echo "Waiting for job to complete..."
wait_for_job "${JOB_ID}"

echo ""
echo "Output files:"
ls -lh "${OUT_FILE}" "${ERR_FILE}" 2>/dev/null || echo "No output files found"
show_job_output "${OUT_FILE}" "${ERR_FILE}" "logs/cli.log" "benchmark results"
[[ "${JOB_STATE}" == "COMPLETED" ]] || exit 1

if [[ -n "${OUTPUT_PATH}" ]]; then
    echo ""
    echo "Copying results to ${OUTPUT_PATH}..."
    mkdir -p "${OUTPUT_PATH}"
    cp -r result_dir/. "${OUTPUT_PATH}/"
    echo "Results copied to ${OUTPUT_PATH}"
fi
