#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs the specified type of benchmark.

OPTIONS:
    -h, --help              Show this help message.
    -b, --benchmark-type    Type of benchmark to run. Only "tpch" and "tpcds" are currently supported.
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.
    -d, --dataset-name      Name of the dataset containing the Parquet files that will be queried.
                            This should be a directory name under the path specified by the SPARK_DATA_DIR environment
                            variable.
    -o, --output-dir        Directory path that will contain the output files from the benchmark run.
                            By default, output files are written to "$(pwd)/benchmark_output".
    -i, --iterations        Number of query run iterations. By default, 5 iterations are run.
    -t, --tag               Tag associated with the benchmark run. When a tag is specified, benchmark output will be
                            stored inside a directory under the --output-dir path with a name matching the tag name.
                            Tags must contain only alphanumeric and underscore characters.
    --skip-drop-cache       Skip dropping system caches before running benchmark queries (dropped by default).
    --gluten-jar-path       Path to Gluten JAR file. By default, the "spark_gluten/testing/spark-gluten-install"
                            path is searched for a file that matches the format: "gluten-*.jar".

EXAMPLES:
    $0 -b tpch -d bench_sf100
    $0 -b tpch -q "1,2" -d bench_sf100
    $0 -b tpch -d bench_sf100 -i 10 -o ~/tpch_benchmark_output
    $0 -b tpch -d bench_sf100 -t gh200_cpu_sf100
    $0 -b tpch -d bench_sf100 --gluten-jar-path /path/to/custom/gluten/build.jar
    $0 -h

EOF
}

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
      --gluten-jar-path)
        if [[ -n $2 ]]; then
          GLUTEN_JAR_PATH=$2
          shift 2
        else
          echo "Error: --gluten-jar-path requires a value"
          exit 1
        fi
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

if [[ -n ${GLUTEN_JAR_PATH} ]]; then
  PYTEST_ARGS+=("--gluten-jar-path" "${GLUTEN_JAR_PATH}")
fi

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

trap delete_python_virtual_env EXIT

init_python_virtual_env

TEST_DIR=$(readlink -f "${SCRIPT_DIR}/../testing")
pip install --disable-pip-version-check -q -r "${TEST_DIR}/requirements.txt"

BENCHMARK_TEST_DIR=${TEST_DIR}/performance_benchmarks
pytest -q -s "${BENCHMARK_TEST_DIR}/${BENCHMARK_TYPE}_test.py" "${PYTEST_ARGS[@]}"
