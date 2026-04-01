#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs the specified type of benchmark.

OPTIONS:
    -h, --help                Show this help message.
    -b, --benchmark-type      Type of benchmark to run. Only "tpch" and "tpcds" are currently supported.
    -q, --queries             Set of benchmark queries to run. This should be a comma separate list of query numbers.
                              By default, all benchmark queries are run.
    -d, --dataset-name        Name of the dataset containing the Parquet files that will be queried.
                              This should be a directory name under the path specified by the SPARK_DATA_DIR environment
                              variable.
    -o, --output-dir          Directory path that will contain the output files from the benchmark run.
                              By default, output files are written to "\$(pwd)/benchmark_output".
    -i, --iterations          Number of query run iterations. By default, 5 iterations are run.
    -t, --tag                 Tag associated with the benchmark run. When a tag is specified, benchmark output will be
                              stored inside a directory under the --output-dir path with a name matching the tag name.
                              Tags must contain only alphanumeric and underscore characters.
    --skip-drop-cache         Skip dropping system caches before running benchmark queries (dropped by default).
    --hostname                Hostname of the Spark Connect server (default: localhost).
    --port                    Port of the Spark Connect gRPC service (default: 15002).
    -p, --profile             Enable profiling of benchmark queries. The Spark Connect server must have been started
                              with --profile.
    --reset-venv              Delete and recreate the Python virtual environment before running.

EXAMPLES:
    $0 -b tpch -d bench_sf100
    $0 -b tpch -q "1,2" -d bench_sf100
    $0 -b tpch -d bench_sf100 -i 10 -o ~/tpch_benchmark_output
    $0 -b tpch -d bench_sf100 -t gh200_cpu_sf100
    $0 -b tpch -d bench_sf100 -p
    $0 -h

EOF
}

OUTPUT_DIR="benchmark_output"
parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -b|--benchmark-type)
        if [[ -n $2 ]]; then
          BENCHMARK_TYPE=$2
          shift 2
        else
          echo "Error: --benchmark-type requires a value"
          exit 1
        fi
        ;;
      -q|--queries)
        if [[ -n $2 ]]; then
          QUERIES=$2
          shift 2
        else
          echo "Error: --queries requires a value"
          exit 1
        fi
        ;;
      -d|--dataset-name)
        if [[ -n $2 ]]; then
          DATASET_NAME=$2
          shift 2
        else
          echo "Error: --dataset-name requires a value"
          exit 1
        fi
        ;;
      -o|--output-dir)
        if [[ -n $2 ]]; then
          OUTPUT_DIR=$2
          shift 2
        else
          echo "Error: --output-dir requires a value"
          exit 1
        fi
        ;;
      -i|--iterations)
        if [[ -n $2 ]]; then
          ITERATIONS=$2
          shift 2
        else
          echo "Error: --iterations requires a value"
          exit 1
        fi
        ;;
      -t|--tag)
        if [[ -n $2 ]]; then
          TAG=$2
          shift 2
        else
          echo "Error: --tag requires a value"
          exit 1
        fi
        ;;
      --skip-drop-cache)
        SKIP_DROP_CACHE=true
        shift
        ;;
      --hostname)
        if [[ -n $2 ]]; then
          HOST_NAME=$2
          shift 2
        else
          echo "Error: --hostname requires a value"
          exit 1
        fi
        ;;
      --port)
        if [[ -n $2 ]]; then
          PORT=$2
          shift 2
        else
          echo "Error: --port requires a value"
          exit 1
        fi
        ;;
      -p|--profile)
        PROFILE=true
        shift
        ;;
      --reset-venv)
        RESET_VENV=true
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

if [[ -z ${SPARK_DATA_DIR} ]]; then
  echo "Error: The SPARK_DATA_DIR environment variable must be set to a directory containing the benchmark datasets."
  print_help
  exit 1
fi

if [[ -z ${BENCHMARK_TYPE} || ! ${BENCHMARK_TYPE} =~ ^tpc(h|ds)$ ]]; then
  echo "Error: A valid benchmark type (tpch or tpcds) is required. Use the -b or --benchmark-type argument."
  print_help
  exit 1
fi

if [[ -z ${DATASET_NAME} ]]; then
  echo "Error: A dataset name must be set. Use the -d or --dataset-name argument."
  print_help
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

PYTEST_ARGS=("--dataset-name" "${DATASET_NAME}")

if [[ -n ${QUERIES} ]]; then
  PYTEST_ARGS+=("--queries" "${QUERIES}")
fi

if [[ -n ${OUTPUT_DIR} ]]; then
  PYTEST_ARGS+=("--output-dir" "${OUTPUT_DIR}")
fi

if [[ -n ${ITERATIONS} ]]; then
  PYTEST_ARGS+=("--iterations" "${ITERATIONS}")
fi

if [[ -n ${TAG} ]]; then
  if [[ ! ${TAG} =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Error: Invalid --tag value. Tags must contain only alphanumeric and underscore characters."
    print_help
    exit 1
  fi
  PYTEST_ARGS+=("--tag" "${TAG}")
fi

if [[ "${SKIP_DROP_CACHE}" == "true" ]]; then
  PYTEST_ARGS+=("--skip-drop-cache")
fi

PYTEST_ARGS+=("--hostname" "${HOST_NAME:-localhost}")
PYTEST_ARGS+=("--port" "${PORT:-15002}")

if [[ "${PROFILE}" == "true" ]]; then
  PYTEST_ARGS+=("--profile" "--profile-script-path" \
    "$(readlink -f "${SCRIPT_DIR}/profiler_functions.sh")")
fi

VENV_DIR=".benchmark_venv"

# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/py_env_functions.sh"

if [[ "${RESET_VENV}" == "true" ]]; then
  delete_python_virtual_env "${VENV_DIR}"
fi

TEST_DIR=$(readlink -f "${SCRIPT_DIR}/../testing")

init_python_virtual_env "${VENV_DIR}"
pip install --disable-pip-version-check -q -r "${TEST_DIR}/requirements.txt"

LOG_FILE="${OUTPUT_DIR}/spark_warnings.log"
mkdir -p "${OUTPUT_DIR}"

TEST_FILE="${SCRIPT_DIR}/../testing/performance_benchmarks/${BENCHMARK_TYPE}_test.py"

echo "Warnings/stderr redirected to ${LOG_FILE}"
pytest -q -s "${TEST_FILE}" "${PYTEST_ARGS[@]}" 2>"${LOG_FILE}"
EXIT_CODE=$?

if [[ -s "${LOG_FILE}" ]]; then
  echo "Warning log saved to ${LOG_FILE} ($(wc -l < "${LOG_FILE}") lines)"
fi

exit "${EXIT_CODE}"
