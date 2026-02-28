#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs integration tests for the specified type of benchmark.

The script operates in two modes – in both cases the tests execute inside a
Docker container:

  Static JAR mode   – When --static-gluten-jar-path is specified, tests run in
                      a lightweight Python 3.12 / JDK 21 image with the given
                      JAR mounted from the host.

  Docker image mode – When --static-gluten-jar-path is NOT specified, tests run
                      in the image apache/gluten:<image-tag> which must contain
                      pre-built Gluten JARs in /opt/gluten/jars/.

OPTIONS:
    -h, --help                          Show this help message.
    -b, --benchmark-type                Type of benchmark to run tests for. Only "tpch" and "tpcds" are currently
                                        supported.
    -q, --queries                       Set of benchmark queries to run. This should be a comma separate list of query
                                        numbers. By default, all benchmark queries are run.
    -d, --dataset-name                  Name of the dataset containing the Parquet files that will be queried (if
                                        unspecified, the default 0.01 scale factor dataset is used).
    -o, --output-dir                    Directory path that will contain any output files from the integration test run.
                                        Default path is "integ_test_output".
    -r, --reference-results-dir         If specified, use the results in the specified directory for comparison. The
                                        results are expected to be in the form of Parquet files with names matching the
                                        relevant query number. For example, "{reference-results-dir}/q1.parquet",
                                        "{reference-results-dir}/q2.parquet", etc.
    --store-spark-results               If this argument is specified, store the Spark query execution results.
                                        Results will be stored in Parquet files in a "spark_results" directory
                                        under the --output-dir path.
    --store-reference-results           If this argument is specified, store the reference results (from DuckDB).
                                        Results will be stored in Parquet files in a "reference_results" directory under
                                        the --output-dir path. This argument is ignored if --reference-results-dir is
                                        specified.
    --show-spark-result-preview         If this argument is specified, for each query, show a preview of the rows
                                        returned by Spark.
    --show-reference-result-preview     If this argument is specified, for each query, show a preview of the rows from
                                        a reference result.
    --preview-rows-count                Number of rows to include in the preview i.e. when
                                        --show-spark-result-preview or --show-reference-result-preview is specified.
    --skip-reference-comparison         Skip Spark rows comparison against a reference set of rows.
    --reuse-venv                        If this argument is specified, reuse the existing Python virtual environment if
                                        one exists and skip dependency installation.
    --static-gluten-jar-path            Path to a statically-linked Gluten JAR file on the host.  When specified the
                                        tests run in a lightweight Python 3.12 / JDK 21 image.
    --image-tag                         Docker image tag to use when running in Docker image mode.
                                        The full image reference is apache/gluten:<image-tag>.
                                        Default: "dynamic_gpu_\${USER:-latest}".



EXAMPLES:
    $0 -b tpch
    $0 -b tpch -q "1,2"
    $0 -b tpch -q "1,2" --image-tag dynamic_cpu_myuser
    $0 -b tpch --static-gluten-jar-path .build_artifacts/cpu_static/gluten-velox-bundle.jar
    $0 -b tpch -q "1,2" -d my_sf1_dataset
    $0 -b tpch -q "1,2" -d my_sf1_dataset -r my_reference_results_dir
    $0 -b tpch -q "1,2" -d my_sf1_dataset --store-spark-results
    $0 -b tpch -q "1,2" -d my_sf1_dataset --store-reference-results
    $0 -b tpch -q "1,2" -d my_sf1_dataset --show-spark-result-preview --show-reference-result-preview --preview-rows-count 5
    $0 -b tpch -q "1,2" -d my_sf1_dataset --store-spark-results --skip-reference-comparison
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
      -r|--reference-results-dir)
        if [[ -n $2 ]]; then
          REFERENCE_RESULTS_DIR=$2
          shift 2
        else
          echo "Error: --reference-results-dir requires a value"
          exit 1
        fi
        ;;
      --store-spark-results)
        STORE_SPARK_RESULTS=true
        shift
        ;;
      --store-reference-results)
        STORE_REFERENCE_RESULTS=true
        shift
        ;;
      --show-spark-result-preview)
        SHOW_SPARK_ROWS_PREVIEW=true
        shift
        ;;
      --show-reference-result-preview)
        SHOW_REFERENCE_ROWS_PREVIEW=true
        shift
        ;;
      --preview-rows-count)
        if [[ -n $2 ]]; then
          PREVIEW_ROWS_COUNT=$2
          shift 2
        else
          echo "Error: --preview-rows-count requires a value"
          exit 1
        fi
        ;;
      --skip-reference-comparison)
        SKIP_REFERENCE_COMPARISON=true
        shift
        ;;
      --reuse-venv)
        REUSE_VENV=true
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

if [[ -z ${BENCHMARK_TYPE} || ! ${BENCHMARK_TYPE} =~ ^tpc(h|ds)$ ]]; then
  echo "Error: A valid benchmark type (tpch or tpcds) is required. Use the -b or --benchmark-type argument."
  print_help
  exit 1
fi

# Build pytest args.
PYTEST_ARGS=()

if [[ -n ${QUERIES} ]]; then
  PYTEST_ARGS+=("--queries" "${QUERIES}")
fi

if [[ -n ${DATASET_NAME} ]]; then
  PYTEST_ARGS+=("--dataset-name" "${DATASET_NAME}")
fi

if [[ -n ${OUTPUT_DIR} ]]; then
  PYTEST_ARGS+=("--output-dir" "${OUTPUT_DIR}")
fi

if [[ -n ${REFERENCE_RESULTS_DIR} ]]; then
  PYTEST_ARGS+=("--reference-results-dir" "${REFERENCE_RESULTS_DIR}")
fi

if [[ -n ${STORE_SPARK_RESULTS} ]]; then
  PYTEST_ARGS+=("--store-spark-results")
fi

if [[ -n ${STORE_REFERENCE_RESULTS} ]]; then
  PYTEST_ARGS+=("--store-reference-results")
fi

if [[ -n ${SHOW_SPARK_ROWS_PREVIEW} ]]; then
  PYTEST_ARGS+=("--show-spark-result-preview")
fi

if [[ -n ${SHOW_REFERENCE_ROWS_PREVIEW} ]]; then
  PYTEST_ARGS+=("--show-reference-result-preview")
fi

if [[ -n ${PREVIEW_ROWS_COUNT} ]]; then
  PYTEST_ARGS+=("--preview-rows-count" "${PREVIEW_ROWS_COUNT}")
fi

if [[ -n ${SKIP_REFERENCE_COMPARISON} ]]; then
  PYTEST_ARGS+=("--skip-reference-comparison")
fi

VENV_DIR=".integ_test_venv"
EFFECTIVE_OUTPUT_DIR="${OUTPUT_DIR:-integ_test_output}"

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/run_in_docker.sh"

TEST_FILE="../testing/integration_tests/${BENCHMARK_TYPE}_test.py"

run_in_docker \
  "${VENV_DIR}" \
  "${EFFECTIVE_OUTPUT_DIR}" \
  -s -v --durations=0 "${TEST_FILE}" "${PYTEST_ARGS[@]}"
