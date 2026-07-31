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
#                  [--queries-file <path>]
#                  [--verify-cpu-ucx]
#                  [--legacy-cpu-numa]
#                  [--disable-gds] [-m|--metrics] [-p|--profile] [--perf]
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
ENABLE_PERF=0
NSYS_WORKER_IDS="0"
PROFILE_ITERATIONS=""    # comma-separated iter indices; empty = combined per query
NSYS_LAUNCH_OPTS="-t nvtx,cuda"   # trace flags passed to `nsys launch`
PERF_WORKER_ID=0
PERF_FREQUENCY="${PERF_FREQUENCY:-19}"
PERF_EVENT="${PERF_EVENT:-cpu-clock:u}"
PERF_CALL_GRAPH="${PERF_CALL_GRAPH:-dwarf,8192}"
PERF_USER_REGS="${PERF_USER_REGS:-auto}"
PERF_NOFILE_LIMIT="${PERF_NOFILE_LIMIT:-65536}"
PERF_THREADS_PER_SHARD="${PERF_THREADS_PER_SHARD:-128}"
PERF_RECORD_STOP_TIMEOUT="${PERF_RECORD_STOP_TIMEOUT:-120}"
PERF_POSTPROCESS="${PERF_POSTPROCESS:-0}"
PERF_FINALIZE_TIMEOUT="${PERF_FINALIZE_TIMEOUT:-0}"
QUERIES=""
QUERIES_FILE=""
CONFIG_OVERRIDES=""
# Deliberately CLI-only. A stale exported VERIFY_CPU_UCX must not silently turn
# verification on in a later run; --verify-cpu-ucx below is the only enabler.
VERIFY_CPU_UCX=0

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
      --queries-file <path>    Custom JSON query file passed to run_benchmark.sh
      --worker-env-file <path> Override worker.env (default: ./worker.env)
      --verify-cpu-ucx         Assert every CPU UCX listener is ready before
                               queries; for multi-worker runs, also assert that
                               a query uses CPU UCX replacement operators.
                               Enables UCX diagnostics.
      --config-overrides <s>   Semicolon-separated property overrides applied
                               after generate_configs (e.g.
                               "task.max-drivers-per-task=8;cudf.batch_size_min_threshold=200000")
      --cpu                    Use CPU partition/images (overrides cluster default)
      --gpu                    Use GPU partition/images (overrides cluster default)
      --numa                   Enable NUMA pinning for workers
      --no-numa                Disable NUMA pinning for workers
      --disable-gds            Disable GPU Direct Storage
  -m, --metrics                Enable metrics collection
  -p, --profile                Enable nsys profiling
      --perf                   Sample worker 0 with host perf during each
                               selected query/profile iteration. Captures raw
                               perf.data plus a portable symfs under
                               result_dir_<jobid>/profiles/perf/worker_0/.
      --perf-frequency <hz>    Sampling frequency (default: ${PERF_FREQUENCY})
      --perf-event <event>     perf event (default: ${PERF_EVENT})
      --perf-call-graph <mode> perf call-graph mode (default: ${PERF_CALL_GRAPH})
      --perf-user-regs <regs>  Registers passed to perf --user-regs, or
                               'auto'/'perf-default' (default: ${PERF_USER_REGS})
      --perf-nofile-limit <n>  Minimum host perf soft nofile limit
                               (default: ${PERF_NOFILE_LIMIT})
      --perf-threads-per-shard <n>
                               Maximum target TIDs assigned to one perf
                               recorder (default: ${PERF_THREADS_PER_SHARD})
      --perf-record-stop-timeout <seconds>
                               Maximum time for perf record to flush and stop
                               after a query (default:
                               ${PERF_RECORD_STOP_TIMEOUT})
      --perf-postprocess       Generate reports and flamegraphs after all raw
                               captures are published. By default this expensive
                               work is deferred to process-perf-captures.sh.
      --perf-finalize-timeout <seconds>
                               Maximum wait for post-suite artifact publication;
                               0 uses the Slurm job time limit (default:
                               ${PERF_FINALIZE_TIMEOUT})
      --nsys-worker-ids <list> Worker IDs to profile: comma list (e.g. "0,3,5") or
                               'all' to profile every worker (default: ${NSYS_WORKER_IDS})
      --nsys-worker-id <n>     Alias for --nsys-worker-ids accepting a single ID
      --profile-iterations <list>
                               Comma-separated 0-based iteration indices to
                               profile separately (e.g. "1" to skip iteration
                               0, or "0,1" to capture both). Applies to nsys
                               and perf. When unset, one capture spans every
                               iteration of each selected query.
      --nsys-launch-opts <str> Options passed to \`nsys launch\` controlling
                               what is traced. Default: "-t nvtx,cuda".
                               Examples:
                                 "-t nvtx,cuda,osrt"       (add OS runtime)
                                 "-t nvtx,ucx,osrt --sample=process-tree --backtrace=dwarf"
                                 "-t nvtx,cuda --gpu-metrics-device=all"
                                 "-t nvtx,cuda --cuda-memory-usage=true"
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
        --queries-file)           requires_value "$1" "${2:-}"; QUERIES_FILE="$2"; shift 2 ;;
        --worker-env-file)        requires_value "$1" "${2:-}"; WORKER_ENV_FILE="$2"; shift 2 ;;
        --verify-cpu-ucx)         VERIFY_CPU_UCX=1; shift ;;
        --config-overrides)       requires_value "$1" "${2:-}"; CONFIG_OVERRIDES="$2"; shift 2 ;;
        --nsys-worker-id)         requires_value "$1" "${2:-}"; NSYS_WORKER_IDS="$2"; shift 2 ;;
        --nsys-worker-ids)        requires_value "$1" "${2:-}"; NSYS_WORKER_IDS="$2"; shift 2 ;;
        --profile-iterations)     requires_value "$1" "${2:-}"; PROFILE_ITERATIONS="$2"; shift 2 ;;
        --nsys-launch-opts)
            # Can't use requires_value here — it disallows dash-prefixed values,
            # but valid nsys flags like "-t nvtx,ucx --sample=process-tree" start with -.
            [[ -n "${2:-}" ]] || { echo "Error: $1 requires a value" >&2; exit 1; }
            NSYS_LAUNCH_OPTS="$2"; shift 2 ;;
        --cpu)         VARIANT_TYPE="cpu"; shift ;;
        --gpu)         VARIANT_TYPE="gpu"; shift ;;
        --numa)        USE_NUMA="1"; shift ;;
        --no-numa)     USE_NUMA="0"; shift ;;
        --disable-gds) ENABLE_GDS=0; shift ;;
        -m|--metrics)  ENABLE_METRICS=1; shift ;;
        -p|--profile)  ENABLE_NSYS=1; shift ;;
        --perf)        ENABLE_PERF=1; shift ;;
        --perf-frequency) requires_value "$1" "${2:-}"; PERF_FREQUENCY="$2"; shift 2 ;;
        --perf-event)     requires_value "$1" "${2:-}"; PERF_EVENT="$2"; shift 2 ;;
        --perf-call-graph)
            [[ -n "${2:-}" ]] || { echo "Error: $1 requires a value" >&2; exit 1; }
            PERF_CALL_GRAPH="$2"; shift 2 ;;
        --perf-user-regs)
            requires_value "$1" "${2:-}"
            PERF_USER_REGS="$2"; shift 2 ;;
        --perf-nofile-limit)
            requires_value "$1" "${2:-}"
            PERF_NOFILE_LIMIT="$2"; shift 2 ;;
        --perf-threads-per-shard)
            requires_value "$1" "${2:-}"
            PERF_THREADS_PER_SHARD="$2"; shift 2 ;;
        --perf-record-stop-timeout)
            requires_value "$1" "${2:-}"
            PERF_RECORD_STOP_TIMEOUT="$2"; shift 2 ;;
        --perf-postprocess) PERF_POSTPROCESS=1; shift ;;
        --perf-finalize-timeout)
            requires_value "$1" "${2:-}"
            PERF_FINALIZE_TIMEOUT="$2"; shift 2 ;;
        -h|--help)     usage; exit 0 ;;
        --) shift; EXTRA_ARGS+=("$@"); break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

[[ -z "${NODES_COUNT}"  ]] && { echo "Error: -n|--nodes is required (see --help)" >&2; exit 1; }
[[ -z "${SCALE_FACTOR}" ]] && { echo "Error: -s|--scale-factor is required (see --help)" >&2; exit 1; }
if [[ -n "${PROFILE_ITERATIONS}" ]] && ! [[ "${PROFILE_ITERATIONS}" =~ ^[0-9]+(,[0-9]+)*$ ]]; then
    echo "Error: --profile-iterations expects a comma-separated list of 0-based ints; got '${PROFILE_ITERATIONS}'" >&2
    exit 1
fi
if [[ "${ENABLE_NSYS}" == "1" && "${ENABLE_PERF}" == "1" ]]; then
    echo "Error: --profile (nsys) and --perf must run in separate jobs" >&2
    exit 1
fi
if [[ "${PERF_POSTPROCESS}" == "1" && "${ENABLE_PERF}" != "1" ]]; then
    echo "Error: --perf-postprocess requires --perf" >&2
    exit 1
fi
if ! [[ "${PERF_FREQUENCY}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --perf-frequency must be a positive integer; got '${PERF_FREQUENCY}'" >&2
    exit 1
fi
if ! [[ "${PERF_NOFILE_LIMIT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --perf-nofile-limit must be a positive integer; got '${PERF_NOFILE_LIMIT}'" >&2
    exit 1
fi
if ! [[ "${PERF_THREADS_PER_SHARD}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --perf-threads-per-shard must be a positive integer; got '${PERF_THREADS_PER_SHARD}'" >&2
    exit 1
fi
if ! [[ "${PERF_RECORD_STOP_TIMEOUT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --perf-record-stop-timeout must be a positive integer; got '${PERF_RECORD_STOP_TIMEOUT}'" >&2
    exit 1
fi
if ! [[ "${PERF_POSTPROCESS}" =~ ^[01]$ ]]; then
    echo "Error: PERF_POSTPROCESS must be 0 or 1; got '${PERF_POSTPROCESS}'" >&2
    exit 1
fi
if ! [[ "${PERF_FINALIZE_TIMEOUT}" =~ ^[0-9]+$ ]]; then
    echo "Error: --perf-finalize-timeout must be a non-negative integer; got '${PERF_FINALIZE_TIMEOUT}'" >&2
    exit 1
fi
if [[ -n "${PROFILE_ITERATIONS}" ]]; then
    IFS=',' read -ra _profile_iteration_list <<< "${PROFILE_ITERATIONS}"
    for _profile_iteration in "${_profile_iteration_list[@]}"; do
        if (( _profile_iteration >= NUM_ITERATIONS )); then
            echo "Error: profile iteration ${_profile_iteration} is outside -i ${NUM_ITERATIONS} (indices are 0-based)" >&2
            exit 1
        fi
    done
    unset _profile_iteration _profile_iteration_list
fi

echo "Submitting Presto TPC-H benchmark job..."
echo ""

# Resolve variant-specific cluster values now that VARIANT_TYPE is known.
# Default falls through CLUSTER_DEFAULT_VARIANT (set in ~/.cluster_config.env)
# to "gpu" so existing GPU-cluster users see no change.
VARIANT_TYPE="${VARIANT_TYPE:-${CLUSTER_DEFAULT_VARIANT:-gpu}}"
resolve_cluster_variant "${VARIANT_TYPE}"
: "${NUM_GPUS_PER_NODE:=${CLUSTER_NUM_WORKERS_PER_NODE:-}}"
: "${USE_NUMA:=${CLUSTER_USE_NUMA:-0}}"

# GDS is a GPU-only data path. CPU launches should not require --disable-gds
# and should never expose cufile settings to a CPU worker image.
if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
    ENABLE_GDS=0
fi

if [[ "${VERIFY_CPU_UCX}" == "1" ]]; then
    [[ "${VARIANT_TYPE}" == "cpu" ]] || { echo "Error: --verify-cpu-ucx requires --cpu" >&2; exit 1; }
    export UCX_LOG_LEVEL="${UCX_LOG_LEVEL:-info}"
    export UCX_PROTO_INFO="${UCX_PROTO_INFO:-y}"
fi

# Expand --nsys-worker-ids=all to the full 0..N-1 range now that NUM_GPUS_PER_NODE
# is known. Done here (not at arg-parse time) because total worker count depends
# on cluster-resolved values.
if [[ "${NSYS_WORKER_IDS}" == "all" ]]; then
    _total_workers=$(( NODES_COUNT * NUM_GPUS_PER_NODE ))
    NSYS_WORKER_IDS="$(seq -s, 0 $(( _total_workers - 1 )))"
    unset _total_workers
fi

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
preflight_file "${WORKER_ENV_FILE}" "worker environment" \
    "Set WORKER_ENV_FILE or pass --worker-env-file <path>"
WORKER_ENV_FILE="$(canonicalize_file_path "${WORKER_ENV_FILE}")"

if [[ -n "${QUERIES_FILE}" ]]; then
    preflight_file "${QUERIES_FILE}" "queries JSON"
    QUERIES_FILE="$(canonicalize_file_path "${QUERIES_FILE}")"
    if [[ "${QUERIES_FILE}" != "${VT_ROOT}/"* ]]; then
        echo "Error: --queries-file must be inside ${VT_ROOT} so it is available in the CLI container." >&2
        echo "       Got: ${QUERIES_FILE}" >&2
        exit 1
    fi
    if ! python3 -m json.tool "${QUERIES_FILE}" >/dev/null 2>&1; then
        echo "Error: queries file is not valid JSON: ${QUERIES_FILE}" >&2
        exit 1
    fi
    QUERIES_FILE="/workspace/${QUERIES_FILE#"${VT_ROOT}/"}"
fi
preflight_image_roles "${WORKER_IMAGE}" "${COORD_IMAGE}"
preflight_image "${WORKER_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -w <name>"
preflight_image "${COORD_IMAGE}" \
    "Pull it (see ./pull_ghcr_image.sh) or override with -c <name>"
preflight_dir "${DATA}/tpch-rs-${SCALE_FACTOR}" "TPC-H SF${SCALE_FACTOR} data" \
    "./launch-gen-data.sh -s ${SCALE_FACTOR} -o ${DATA}/tpch-rs-${SCALE_FACTOR}"
preflight_metastore "${SCALE_FACTOR}" "${ANALYZE_HINT}"

# Only transient live logs are reused. Completed result directories are named
# with their Slurm job ID and are never removed by a later launch, so an scp or
# rsync of an earlier run cannot race this cleanup.
rm -rf logs 2>/dev/null || true
rm -f *.out *.err 2>/dev/null || true
mkdir -p logs

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
EXPORT_VARS+=",ENABLE_NSYS=${ENABLE_NSYS}"
EXPORT_VARS+=",ENABLE_PERF=${ENABLE_PERF},PERF_WORKER_ID=${PERF_WORKER_ID}"
EXPORT_VARS+=",VERIFY_CPU_UCX=${VERIFY_CPU_UCX}"
# Comma-separated values can't ride EXPORT_VARS (comma is the sbatch --export
# field separator); export them so sbatch picks them up via the ALL inheritance.
[[ -n "${QUERIES}" ]] && export QUERIES
[[ -n "${QUERIES_FILE}" ]] && export QUERIES_FILE
export NSYS_WORKER_IDS
[[ -n "${PROFILE_ITERATIONS}" ]] && export PROFILE_ITERATIONS
# NSYS_LAUNCH_OPTS may contain spaces / equals signs / commas — ride ALL
# inheritance rather than EXPORT_VARS so the whole quoted string survives.
export NSYS_LAUNCH_OPTS
# PERF_CALL_GRAPH and PERF_USER_REGS contain commas, and the remaining values
# may contain perf punctuation. Inherit them through --export=ALL instead of
# splitting the sbatch export list.
export PERF_FREQUENCY PERF_EVENT PERF_CALL_GRAPH PERF_USER_REGS
export PERF_NOFILE_LIMIT PERF_THREADS_PER_SHARD PERF_RECORD_STOP_TIMEOUT
export PERF_POSTPROCESS PERF_FINALIZE_TIMEOUT
# CONFIG_OVERRIDES uses ';' internally but values may contain commas (e.g.
# "32MB,64MB" would break, but more pressingly any later additions); easiest
# to keep it off EXPORT_VARS entirely.
[[ -n "${CONFIG_OVERRIDES}" ]] && export CONFIG_OVERRIDES
JOB_ID=$(sbatch --job-name="${JOB_NAME}" --nodes="${NODES_COUNT}" "${NODELIST_ARG[@]}" \
"${CLUSTER_SBATCH_ARGS[@]}" \
--export="${EXPORT_VARS}" \
--output="${OUT_FMT}" --error="${ERR_FMT}" "${EXTRA_ARGS[@]}" ${GRES_OPT} \
    run-presto-benchmarks.slurm | awk '{print $NF}')
RUN_RESULT_DIR="result_dir_${JOB_ID}"
OUT_FILE="${OUT_FMT//%j/${JOB_ID}}"
ERR_FILE="${ERR_FMT//%j/${JOB_ID}}"

# A text pointer is safe for automation: callers resolve it once and then copy
# the immutable job-specific directory. Unlike a mutable result_dir symlink, it
# cannot redirect individual files to a newer run midway through rsync/scp.
LATEST_RESULT_POINTER="${SCRIPT_DIR}/latest_result_dir.txt"
LATEST_RESULT_POINTER_TMP="${LATEST_RESULT_POINTER}.tmp.$$"
printf '%s\n' "${RUN_RESULT_DIR}" > "${LATEST_RESULT_POINTER_TMP}"
mv -f "${LATEST_RESULT_POINTER_TMP}" "${LATEST_RESULT_POINTER}"

echo "Job submitted with ID: $JOB_ID"
echo ""

# Resolve and print first node IP once nodes are allocated
echo "Resolving first node IP..."
for i in {1..60}; do
    STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null || true)
    NODELIST=$(squeue -j "$JOB_ID" -h -o "%N" 2>/dev/null || true)
    if [[ -z "${STATE:-}" ]]; then
        echo "Job ${JOB_ID} is no longer in squeue before node allocation."
        break
    fi
    if [[ "${STATE}" =~ ^(CANCELLED|FAILED|TIMEOUT|COMPLETED|NODE_FAIL|PREEMPTED|BOOT_FAIL|DEADLINE|OUT_OF_MEMORY)$ ]]; then
        echo "Job ${JOB_ID} reached state ${STATE} before node allocation."
        break
    fi
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

if [[ -d "${RUN_RESULT_DIR}" ]]; then
    echo ""
    echo "Job results preserved at: ${SCRIPT_DIR}/${RUN_RESULT_DIR}"
    echo "Latest-result pointer: ${LATEST_RESULT_POINTER}"
else
    echo "No job result directory was created at ${SCRIPT_DIR}/${RUN_RESULT_DIR}" >&2
fi

if [[ -n "${OUTPUT_PATH}" && -d "${RUN_RESULT_DIR}" ]]; then
    echo ""
    echo "Copying results to ${OUTPUT_PATH}..."
    mkdir -p "${OUTPUT_PATH}"
    cp -r "${RUN_RESULT_DIR}/." "${OUTPUT_PATH}/"
    echo "Results copied to ${OUTPUT_PATH}"
fi

[[ "${JOB_STATE}" == "COMPLETED" ]] || exit 1
