#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs integration tests for the specified type of benchmark.

OPTIONS:
    -h, --help                       Show this help message.
    -b, --benchmark-type             Type of benchmark to run tests for. Only "tpch" and "tpcds" are currently supported.
    -q, --queries                    Set of benchmark queries to run. This should be a comma separate list of query numbers.
                                     By default, all benchmark queries are run.
    -k, --keep-tables                If this argument is specified, created benchmark tables will not be dropped.
    -H, --hostname                   Hostname of the Presto coordinator.
    -p, --port                       Port number of the Presto coordinator.
    -u, --user                       User who queries will be executed as.
    -s, --schema-name                Name of the schema containing the tables that will be queried (if unspecified a default
                                     schema is auto-generated).
    -o, --output-dir                 Directory path that will contain any output files from the integration test run. Default
                                     path is "integ_test_output".
    -r, --reference-results-dir      If specified, use the results in the specified directory for comparison. The results are
                                     expected to be in the form of Parquet files with names matching the relevant query number.
                                     For example, "{reference-results-dir}/q1.parquet", "{reference-results-dir}/q1.parquet", etc.
    --store-presto-results           If this argument is specified, store Presto query execution results. Results will be stored
                                     in Parquet files in a "presto_results" directory under the --output-dir path.
    --store-reference-results        If this argument is specified, store the reference results (from DuckDB). Results will
                                     be stored in Parquet files in a "reference_results" directory under the --output-dir path.
                                     This argument is ignored if --reference-results-dir is specified.
    --show-presto-result-preview     If this argument is specified, for each query, show a preview of the rows returned by Presto.
    --show-reference-result-preview  If this argument is specified, for each query, show a preview of the rows from a reference
                                     result.
    --preview-rows-count             Number of rows to include in the preview i.e. when --show-presto-result-preview or
                                     --show-reference-result-preview is specified.
    --skip-reference-comparison      Skip Presto rows comparison against a reference set of rows.
    --reuse-venv                     If this argument is specified, reuse the existing Python virtual environment if one exists
                                     and skip dependency installation.



EXAMPLES:
    $0 -b tpch
    $0 -b tpch -q "1,2" --keep-tables
    $0 -b tpch -q "1,2" -s my_sf1_schema
    $0 -b tpch -q "1,2" -H myhostname.com -p 8081 -s my_sf1_schema
    $0 -b tpch -q "1,2" -s my_sf1_schema -r my_reference_results_dir
    $0 -b tpch -q "1,2" -s my_sf1_schema --store-presto-results
    $0 -b tpch -q "1,2" -s my_sf1_schema --store-reference-results
    $0 -b tpch -q "1,2" -s my_sf1_schema --show-presto-result-preview --show-reference-result-preview --preview-rows-count 5
    $0 -b tpch -q "1,2" -s my_sf1_schema --store-presto-results --skip-reference-comparison
    $0 -h

EOF
}

KEEP_TABLES=false

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
      -k|--keep-tables)
        KEEP_TABLES=true
        shift
        ;;
      -H|--hostname)
        if [[ -n $2 ]]; then
          HOST_NAME=$2
          shift 2
        else
          echo "Error: --hostname requires a value"
          exit 1
        fi
        ;;
      -p|--port)
        if [[ -n $2 ]]; then
          PORT=$2
          shift 2
        else
          echo "Error: --port requires a value"
          exit 1
        fi
        ;;
      -u|--user)
        if [[ -n $2 ]]; then
          USER_NAME=$2
          shift 2
        else
          echo "Error: --user requires a value"
          exit 1
        fi
        ;;
      -s|--schema-name)
        if [[ -n $2 ]]; then
          SCHEMA_NAME=$2
          shift 2
        else
          echo "Error: --schema-name requires a value"
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
      --store-presto-results)
        STORE_PRESTO_RESULTS=true
        shift
        ;;
      --store-reference-results)
        STORE_REFERENCE_RESULTS=true
        shift
        ;;
      --show-presto-result-preview)
        SHOW_PRESTO_ROWS_PREVIEW=true
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

PYTEST_ARGS=()

if [[ "${KEEP_TABLES}" == "true" ]]; then
  PYTEST_ARGS+=("--keep-tables")
fi

if [[ -n ${QUERIES} ]]; then
  PYTEST_ARGS+=("--queries ${QUERIES}")
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

if [[ -n ${SCHEMA_NAME} ]]; then
  PYTEST_ARGS+=("--schema-name ${SCHEMA_NAME}")
fi

if [[ -n ${OUTPUT_DIR} ]]; then
  PYTEST_ARGS+=("--output-dir ${OUTPUT_DIR}")
fi

if [[ -n ${REFERENCE_RESULTS_DIR} ]]; then
  PYTEST_ARGS+=("--reference-results-dir ${REFERENCE_RESULTS_DIR}")
fi

if [[ -n ${STORE_PRESTO_RESULTS} ]]; then
  PYTEST_ARGS+=("--store-presto-results")
fi

if [[ -n ${STORE_REFERENCE_RESULTS} ]]; then
  PYTEST_ARGS+=("--store-reference-results")
fi

if [[ -n ${SHOW_PRESTO_ROWS_PREVIEW} ]]; then
  PYTEST_ARGS+=("--show-presto-result-preview")
fi

if [[ -n ${SHOW_REFERENCE_ROWS_PREVIEW} ]]; then
  PYTEST_ARGS+=("--show-reference-result-preview")
fi

if [[ -n ${PREVIEW_ROWS_COUNT} ]]; then
  PYTEST_ARGS+=("--preview-rows-count ${PREVIEW_ROWS_COUNT}")
fi

if [[ -n ${SKIP_REFERENCE_COMPARISON} ]]; then
  PYTEST_ARGS+=("--skip-reference-comparison")
fi


# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

VENV_DIR=".integ_test_venv"

if [[ "$REUSE_VENV" != "true" ]]; then
  trap 'delete_python_virtual_env "$VENV_DIR"' EXIT
fi

TEST_DIR=$(readlink -f "${SCRIPT_DIR}/../testing")

if [[ ! -d $VENV_DIR || "$REUSE_VENV" != "true" ]]; then
  init_python_virtual_env $VENV_DIR

  # PyPI can occasionally be slow/flaky on GitHub-hosted runners; harden pip with retries/timeouts.
  # You can override these if needed:
  #   PIP_TIMEOUT=180 PIP_RETRIES=12 ./run_integ_test.sh ...
  PIP_TIMEOUT="${PIP_TIMEOUT:-120}"
  PIP_RETRIES="${PIP_RETRIES:-8}"
  python -m pip install \
    --disable-pip-version-check \
    --no-input \
    --progress-bar off \
    --retries "${PIP_RETRIES}" \
    --timeout "${PIP_TIMEOUT}" \
    -q -r "${TEST_DIR}/requirements.txt"
else
  activate_python_virtual_env $VENV_DIR
fi

source "${SCRIPT_DIR}/common_functions.sh"

wait_for_worker_node_registration "$HOST_NAME" "$PORT"

INTEGRATION_TEST_DIR=${TEST_DIR}/integration_tests
pytest -s -v --durations=0 ${INTEGRATION_TEST_DIR}/${BENCHMARK_TYPE}_test.py ${PYTEST_ARGS[*]}
