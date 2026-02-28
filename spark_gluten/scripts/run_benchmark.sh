#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs the specified type of benchmark.

The script operates in two modes – in both cases the benchmarks execute inside a
Docker container:

  Static JAR mode   – When --static-gluten-jar-path is specified, benchmarks run
                      in a lightweight Python 3.12 / JDK 21 image with the given
                      JAR mounted from the host.

  Docker image mode – When --static-gluten-jar-path is NOT specified, benchmarks
                      run in the image apache/gluten:<image-tag> which must
                      contain pre-built Gluten JARs in /opt/gluten/jars/.

OPTIONS:
    -h, --help              Show this help message.
    -b, --benchmark-type    Type of benchmark to run. Only "tpch" and "tpcds" are currently supported.
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.
    -d, --dataset-name      Name of the dataset containing the Parquet files that will be queried.
                            This should be a directory name under the path specified by the SPARK_DATA_DIR environment
                            variable.
    -o, --output-dir        Directory path that will contain the output files from the benchmark run.
                            By default, output files are written to "\$(pwd)/benchmark_output".
    -i, --iterations        Number of query run iterations. By default, 5 iterations are run.
    -t, --tag               Tag associated with the benchmark run. When a tag is specified, benchmark output will be
                            stored inside a directory under the --output-dir path with a name matching the tag name.
                            Tags must contain only alphanumeric and underscore characters.
    --skip-drop-cache       Skip dropping system caches before running benchmark queries (dropped by default).
    --static-gluten-jar-path  Path to a statically-linked Gluten JAR file on the host.  When specified the
                            benchmarks run in a lightweight Python 3.12 / JDK 21 image.
    --image-tag             Docker image tag to use when running in Docker image mode.
                            The full image reference is apache/gluten:<image-tag>.
                            Default: "dynamic_gpu_\${USER:-latest}".

EXAMPLES:
    $0 -b tpch -d bench_sf100
    $0 -b tpch -q "1,2" -d bench_sf100
    $0 -b tpch -d bench_sf100 --image-tag dynamic_cpu_myuser
    $0 -b tpch -d bench_sf100 --static-gluten-jar-path /path/to/gluten-velox-bundle.jar
    $0 -b tpch -d bench_sf100 -i 10 -o ~/tpch_benchmark_output
    $0 -b tpch -d bench_sf100 -t gh200_cpu_sf100
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
      --static-gluten-jar-path)
        if [[ -n $2 ]]; then
          GLUTEN_JAR_PATH=$2
          shift 2
        else
          echo "Error: --static-gluten-jar-path requires a value"
          exit 1
        fi
        ;;
      --image-tag)
        if [[ -n $2 ]]; then
          IMAGE_TAG=$2
          shift 2
        else
          echo "Error: --image-tag requires a value"
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

# Build pytest args.
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

VENV_DIR=".benchmark_venv"
EFFECTIVE_OUTPUT_DIR="${OUTPUT_DIR:-benchmark_output}"

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/run_in_docker.sh"

# Mount the data directory so the benchmark can access the datasets.
EXTRA_DOCKER_ARGS+=(-v "${SPARK_DATA_DIR}:${SPARK_DATA_DIR}" -e SPARK_DATA_DIR="${SPARK_DATA_DIR}")

TEST_FILE="../testing/performance_benchmarks/${BENCHMARK_TYPE}_test.py"

run_in_docker \
  "${VENV_DIR}" \
  "${EFFECTIVE_OUTPUT_DIR}" \
  -q -s "${TEST_FILE}" "${PYTEST_ARGS[@]}"
