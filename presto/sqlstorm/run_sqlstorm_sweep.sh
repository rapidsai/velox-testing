#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

Runs the SQLStorm StackOverflow sweep across Presto engines using the public prestodb
images. For each engine the cluster is started, the queries are run, and the cluster is
stopped. Queries are converted from PostgreSQL to Presto SQL on the fly; a query counts
as converted if it passes EXPLAIN on Presto Java. Results are resumable: rerunning with
the same output directory skips queries that already have a result.

PRESTO_DATA_DIR must contain the Parquet data in stackoverflow_dba/ (see README.md).

OPTIONS:
    -h, --help           Show this help message.
    -s, --sqlstorm-dir   Checkout of github.com/SQL-Storm/SQLStorm (required).
    -o, --output-dir     Results directory (required).
    -e, --engines        Space-separated engines to run (default: "java cpu gpu gpu_nofallback").
    -l, --limit          Number of queries to consider, in numeric filename order (default: all).
    --offset             Skip this many queries first (default: 0).
    -j, --concurrency    Concurrent queries for the java and cpu engines (default: 8). GPU engines
                         always run one query at a time because a worker crash fails every
                         running query.
    -t, --timeout        Per-query execution time limit in seconds (default: 10).

ENVIRONMENT VARIABLES:
    PRESTO_DATA_DIR          Parent directory of stackoverflow_dba/ (required).
    PRESTO_IMAGE_TAG         Run local images with this tag (e.g. built from source) instead of
                             pulling the public images.
    GPU_EXTRA_WORKER_CONFIG  Space-separated properties appended to the GPU worker config
                             (default: none). On aarch64 images without the stack trace fix,
                             "cudf.ast_expression_enabled=false" avoids worker crashes (see
                             README.md).

EXAMPLES:
    $0 -s ~/SQLStorm -o results -l 10
    $0 -s ~/SQLStorm -o results -e "java cpu" -j 16

EOF
}

SQLSTORM_DIR=""
OUTPUT_DIR=""
ENGINES="java cpu gpu gpu_nofallback"
LIMIT_ARGS=()
OFFSET=0
CONCURRENCY=8
TIMEOUT=10
GPU_EXTRA_WORKER_CONFIG=${GPU_EXTRA_WORKER_CONFIG-}

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help) print_help; exit 0 ;;
    -s|--sqlstorm-dir) SQLSTORM_DIR=$(readlink -f "$2"); shift 2 ;;
    -o|--output-dir) OUTPUT_DIR=$2; shift 2 ;;
    -e|--engines) ENGINES=$2; shift 2 ;;
    -l|--limit) LIMIT_ARGS=(--limit "$2"); shift 2 ;;
    --offset) OFFSET=$2; shift 2 ;;
    -j|--concurrency) CONCURRENCY=$2; shift 2 ;;
    -t|--timeout) TIMEOUT=$2; shift 2 ;;
    *) echo "Error: Unknown argument $1"; print_help; exit 1 ;;
  esac
done

if [[ -z ${SQLSTORM_DIR} || -z ${OUTPUT_DIR} ]]; then
  echo "Error: --sqlstorm-dir and --output-dir are required"
  print_help
  exit 1
fi
if [[ ! -d ${PRESTO_DATA_DIR:-}/stackoverflow_dba ]]; then
  echo "Error: PRESTO_DATA_DIR must contain stackoverflow_dba/ (see README.md)"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRESTO_SCRIPTS="${SCRIPT_DIR}/../scripts"
GENERATED_CONFIG="${SCRIPT_DIR}/../docker/config/generated"
mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR=$(readlink -f "${OUTPUT_DIR}")

source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"
init_python_virtual_env "${SCRIPT_DIR}/.venv"
pip install -q -r "${SCRIPT_DIR}/requirements.txt"
cd "${SCRIPT_DIR}"

COMMON_ARGS=(--sqlstorm-dir "${SQLSTORM_DIR}" --offset "${OFFSET}" "${LIMIT_ARGS[@]}" --timeout "${TIMEOUT}")
EXPLAIN_CSV="${OUTPUT_DIR}/explain_java.csv"

wait_for_workers() {
  for _ in $(seq 1 120); do
    if curl -sf localhost:8080/v1/cluster | grep -q '"activeWorkers":[1-9]'; then
      return 0
    fi
    sleep 2
  done
  echo "Error: no Presto worker registered with the coordinator" >&2
  exit 1
}

# Usage: start_cluster <java|cpu|gpu> [worker property ...]
start_cluster() {
  local variant=$1
  shift
  local start_script
  case $variant in
    java) start_script=start_java_presto.sh ;;
    cpu) start_script=start_native_cpu_presto.sh ;;
    gpu) start_script=start_native_gpu_presto.sh ;;
  esac
  (cd "${PRESTO_SCRIPTS}" && VARIANT_TYPE=$variant NUM_WORKERS=1 OVERWRITE_CONFIG=true ./generate_presto_config.sh)
  for property in "$@"; do
    for config in "${GENERATED_CONFIG}/${variant}"/etc_worker*/config_native.properties; do
      echo "${property}" >> "${config}"
    done
  done
  local image_args=(--use-public-images)
  if [[ -n ${PRESTO_IMAGE_TAG:-} ]]; then
    image_args=()
  fi
  (cd "${PRESTO_SCRIPTS}" && "./${start_script}" "${image_args[@]}" --skip-generate-config)
  wait_for_workers
}

# Usage: run_gpu_sweep <engine>
run_gpu_sweep() {
  local container=presto-native-worker-gpu
  python -m sqlstorm.sweep "${COMMON_ARGS[@]}" --engine "$1" --mode run --concurrency 1 \
    --converted-from "${EXPLAIN_CSV}" --output "${OUTPUT_DIR}/run_$1.csv" \
    --restart-command "${SCRIPT_DIR}/restart_worker.sh ${container}" \
    --worker-check-command "docker inspect -f '{{.State.Running}}' ${container} | grep -q true"
}

read -ra GPU_PROPERTIES <<< "${GPU_EXTRA_WORKER_CONFIG}"

if [[ ! -f ${EXPLAIN_CSV} || " ${ENGINES} " == *" java "* ]]; then
  start_cluster java
  python -m sqlstorm.create_tables --sqlstorm-dir "${SQLSTORM_DIR}"
  python -m sqlstorm.sweep "${COMMON_ARGS[@]}" --engine java --mode explain --concurrency 16 --output "${EXPLAIN_CSV}"
fi

for engine in ${ENGINES}; do
  echo "=== ${engine} ==="
  case $engine in
    java)
      python -m sqlstorm.sweep "${COMMON_ARGS[@]}" --engine java --mode run --concurrency "${CONCURRENCY}" \
        --converted-from "${EXPLAIN_CSV}" --output "${OUTPUT_DIR}/run_java.csv"
      ;;
    cpu)
      start_cluster cpu
      python -m sqlstorm.sweep "${COMMON_ARGS[@]}" --engine cpu --mode run --concurrency "${CONCURRENCY}" \
        --converted-from "${EXPLAIN_CSV}" --output "${OUTPUT_DIR}/run_cpu.csv"
      ;;
    gpu)
      start_cluster gpu "${GPU_PROPERTIES[@]}"
      run_gpu_sweep gpu
      ;;
    gpu_nofallback)
      start_cluster gpu "${GPU_PROPERTIES[@]}" cudf.allow_cpu_fallback=false
      run_gpu_sweep gpu_nofallback
      ;;
    *)
      echo "Error: unknown engine ${engine}" >&2
      exit 1
      ;;
  esac
done

"${PRESTO_SCRIPTS}/stop_presto.sh"
python -m sqlstorm.summarize "${OUTPUT_DIR}"
