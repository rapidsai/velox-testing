#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# LOGS_DIR points to the directory where server log files (including nvidia-smi
# output) are written so that run_context.py can parse GPU info.
export LOGS_DIR="${LOGS_DIR:-${SCRIPT_DIR}/presto_logs}"

source "${SCRIPT_DIR}/presto_connection_defaults.sh"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

Run a multi-stream TPC throughput benchmark against a running Presto cluster.
Does not drop OS page caches. Verifies ANALYZE TABLE has been run unless
--skip-analyze-check is set.

OPTIONS:
    -h, --help                  Show this help message.
    -b, --benchmark-type        Benchmark type. Currently only "tpch" is supported.
    -q, --queries               Comma-separated allow-list of query numbers (default: all).
    --exclude-queries           Comma-separated query numbers to exclude (for p99 work).
    --queries-file              Path to a custom JSON file containing query definitions.
    -H, --hostname              Hostname of the Presto coordinator.
    --port                      Port number of the Presto coordinator.
    -u, --user                  User who queries will be executed as.
    -s, --schema-name           Existing schema containing the benchmark tables.
    -o, --output-dir            Output directory (default: \$(pwd)/throughput_benchmark_output).
    -t, --tag                   Optional tag subdirectory under --output-dir.
    --num-streams               Number of concurrent client streams / cursors (default: 1).
    --suite-repeats             Times each stream runs the full (shuffled) suite (default: 1).
    --repetitions-per-query     Times each query is run before advancing (default: 1).
    --run-seed                  Seed for deterministic per-stream shuffle (default: 1).
    --skip-analyze-check        Skip ANALYZE TABLE verification.
    --reference-results-dir     Gold parquet dir (typically Presto C++ query_results/).
    --no-save-results           Do not write query_results parquet files.
    -v, --verbose               Print debug logs for worker/engine detection.

Semantics:
    With --num-streams N, --suite-repeats R, --repetitions-per-query P:
      each of N streams runs the query suite R times (seeded shuffle per suite),
      and within a suite each query is executed P times before moving on.

    Execution retries up to 10 times with no backoff; failed-attempt time is
    included in the recorded latency. Validation (if --reference-results-dir is
    set) runs in an untimed region after a successful execution.

EXAMPLES:
    $0 -b tpch -s bench_sf100 --num-streams 4 --suite-repeats 5 --run-seed 42
    $0 -b tpch -s bench_sf1000 -q "1,3,6,12" --exclude-queries "15,17" --num-streams 8
    $0 -b tpch -s bench_sf100 --reference-results-dir /path/to/cpu/query_results

EOF
}

BENCHMARK_TYPE=""
QUERIES=""
EXCLUDE_QUERIES=""
QUERIES_FILE=""
HOST_NAME=""
PORT=""
USER_NAME=""
SCHEMA_NAME=""
OUTPUT_DIR=""
TAG=""
NUM_STREAMS=""
SUITE_REPEATS=""
REPETITIONS_PER_QUERY=""
RUN_SEED=""
SKIP_ANALYZE_CHECK=""
PRESTO_EXPECTED_RESULTS_DIR=""
EXPLICIT_REFERENCE_DIR=""
NO_SAVE_RESULTS=""

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -b|--benchmark-type)
        BENCHMARK_TYPE=$2; shift 2
        ;;
      -q|--queries)
        QUERIES=$2; shift 2
        ;;
      --exclude-queries)
        EXCLUDE_QUERIES=$2; shift 2
        ;;
      --queries-file)
        QUERIES_FILE=$2; shift 2
        ;;
      -H|--hostname)
        HOST_NAME=$2; shift 2
        ;;
      --port)
        PORT=$2; shift 2
        ;;
      -u|--user)
        USER_NAME=$2; shift 2
        ;;
      -s|--schema-name)
        SCHEMA_NAME=$2; shift 2
        ;;
      -o|--output-dir)
        OUTPUT_DIR=$2; shift 2
        ;;
      -t|--tag)
        TAG=$2; shift 2
        ;;
      --num-streams)
        NUM_STREAMS=$2; shift 2
        ;;
      --suite-repeats)
        SUITE_REPEATS=$2; shift 2
        ;;
      --repetitions-per-query)
        REPETITIONS_PER_QUERY=$2; shift 2
        ;;
      --run-seed)
        RUN_SEED=$2; shift 2
        ;;
      --skip-analyze-check)
        SKIP_ANALYZE_CHECK=true; shift
        ;;
      --reference-results-dir)
        PRESTO_EXPECTED_RESULTS_DIR=$2
        EXPLICIT_REFERENCE_DIR=true
        shift 2
        ;;
      --no-save-results)
        NO_SAVE_RESULTS=true; shift
        ;;
      -v|--verbose)
        export PRESTO_BENCHMARK_DEBUG=1
        shift
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

if [[ -z ${BENCHMARK_TYPE} || ${BENCHMARK_TYPE} != "tpch" ]]; then
  echo "Error: A valid benchmark type is required (currently only tpch). Use -b/--benchmark-type."
  print_help
  exit 1
fi

if [[ -z ${SCHEMA_NAME} ]]; then
  echo "Error: A schema name must be set. Use the -s or --schema-name argument."
  print_help
  exit 1
fi

if [[ ${EXPLICIT_REFERENCE_DIR} == true && ! -d ${PRESTO_EXPECTED_RESULTS_DIR} ]]; then
  echo "[Validation] Error: --reference-results-dir not found: ${PRESTO_EXPECTED_RESULTS_DIR}" >&2
  exit 1
fi

set_presto_coordinator_defaults

OUTPUT_DIR=${OUTPUT_DIR:-"$(pwd)/throughput_benchmark_output"}

# Reuse performance_benchmarks/conftest.py while preserving throughput
# semantics: no cache drop and no power-run hot/lukewarm iteration model.
PYTEST_ARGS=(
  "--schema-name ${SCHEMA_NAME}"
  "--iterations 1"
  "--skip-drop-cache"
)

if [[ -n ${QUERIES} ]]; then
  PYTEST_ARGS+=("--queries ${QUERIES}")
fi
if [[ -n ${EXCLUDE_QUERIES} ]]; then
  PYTEST_ARGS+=("--exclude-queries ${EXCLUDE_QUERIES}")
fi
if [[ -n ${QUERIES_FILE} ]]; then
  PYTEST_ARGS+=("--queries-file ${QUERIES_FILE}")
fi
if [[ -n ${HOST_NAME} ]]; then
  PYTEST_ARGS+=("--hostname ${HOST_NAME}")
fi
if [[ -n ${PORT} ]]; then
  PYTEST_ARGS+=("--port ${PORT}")
fi
if [[ -n ${USER_NAME} ]]; then
  PYTEST_ARGS+=("--user ${USER_NAME}")
fi
if [[ -n ${OUTPUT_DIR} ]]; then
  PYTEST_ARGS+=("--output-dir ${OUTPUT_DIR}")
fi
if [[ -n ${TAG} ]]; then
  if [[ ! ${TAG} =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Error: Invalid --tag value. Tags must contain only alphanumeric and underscore characters."
    exit 1
  fi
  PYTEST_ARGS+=("--tag ${TAG}")
fi
if [[ -n ${NUM_STREAMS} ]]; then
  PYTEST_ARGS+=("--num-streams ${NUM_STREAMS}")
fi
if [[ -n ${SUITE_REPEATS} ]]; then
  PYTEST_ARGS+=("--suite-repeats ${SUITE_REPEATS}")
fi
if [[ -n ${REPETITIONS_PER_QUERY} ]]; then
  PYTEST_ARGS+=("--repetitions-per-query ${REPETITIONS_PER_QUERY}")
fi
if [[ -n ${RUN_SEED} ]]; then
  PYTEST_ARGS+=("--run-seed ${RUN_SEED}")
fi
if [[ "${SKIP_ANALYZE_CHECK}" == "true" ]]; then
  PYTEST_ARGS+=("--skip-analyze-check")
fi
if [[ -n ${PRESTO_EXPECTED_RESULTS_DIR} ]]; then
  PYTEST_ARGS+=("--reference-results-dir ${PRESTO_EXPECTED_RESULTS_DIR}")
fi
if [[ "${NO_SAVE_RESULTS}" == "true" ]]; then
  PYTEST_ARGS+=("--no-save-results")
fi

source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

trap delete_python_virtual_env EXIT

init_python_virtual_env

TEST_DIR=$(readlink -f "${SCRIPT_DIR}/../testing")
pip install -q -r ${TEST_DIR}/requirements.txt

source "${SCRIPT_DIR}/common_functions.sh"

wait_for_worker_node_registration "$HOST_NAME" "$PORT"

echo "Running throughput bench"
echo "  schema=${SCHEMA_NAME} streams=${NUM_STREAMS:-1} suite_repeats=${SUITE_REPEATS:-1} repetitions=${REPETITIONS_PER_QUERY:-1} seed=${RUN_SEED:-1}"

PERFORMANCE_TEST_DIR=${TEST_DIR}/performance_benchmarks
# Ensure repo root is importable (common.* / presto.testing.*)
export PYTHONPATH="$(readlink -f "${SCRIPT_DIR}/../.."):${PYTHONPATH:-}"

pytest -q -s ${PERFORMANCE_TEST_DIR}/tpch_throughput_test.py ${PYTEST_ARGS[*]}

EFFECTIVE_OUTPUT_DIR="$(readlink -f "${OUTPUT_DIR:-$(pwd)/throughput_benchmark_output}")"
if [[ -n "${TAG}" ]]; then
  EFFECTIVE_BENCHMARK_DIR="${EFFECTIVE_OUTPUT_DIR}/${TAG}"
else
  EFFECTIVE_BENCHMARK_DIR="${EFFECTIVE_OUTPUT_DIR}"
fi

if [[ -d "${LOGS_DIR}" ]]; then
  mkdir -p "${EFFECTIVE_BENCHMARK_DIR}/logs"
  cp -r "${LOGS_DIR}/." "${EFFECTIVE_BENCHMARK_DIR}/logs/" || true
  echo "Snapshotted logs to ${EFFECTIVE_BENCHMARK_DIR}/logs"
fi

CONFIG_SRC="$(readlink -f "${SCRIPT_DIR}/../docker/config/generated")"
if [[ -d "${CONFIG_SRC}" ]]; then
  mkdir -p "${EFFECTIVE_BENCHMARK_DIR}/config"
  cp -r "${CONFIG_SRC}/." "${EFFECTIVE_BENCHMARK_DIR}/config/" || true
  echo "Snapshotted configs to ${EFFECTIVE_BENCHMARK_DIR}/config"
fi
