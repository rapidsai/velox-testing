#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script drops and recreates all tables in a Hive schema from scratch,
ensuring a clean Presto state with no cached metadata. Optionally runs
ANALYZE TABLE on all recreated tables.

NOTE: The PRESTO_DATA_DIR environment variable must be set.

OPTIONS:
    -h, --help              Show this help message.
    -b, --benchmark-type    Type of benchmark (tpch or tpcds). Required.
    -s, --schema-name       Name of the schema to drop and recreate. Required.
    -d, --data-dir-name     Name of the data directory under PRESTO_DATA_DIR. Required.
    -v, --verbose           Enable verbose output.
    -H, --hostname          Hostname of the Presto coordinator (default: localhost).
    -p, --port              Port number of the Presto coordinator (default: 8080).
    -u, --user              User who queries will be executed as (default: test_user).
    --skip-analyze          Skip running ANALYZE TABLE after recreation.

EXAMPLES:
    $0 -b tpch -s my_schema -d sf100
    $0 -b tpch -s my_schema -d sf100 -H localhost -p 8080 -v
    $0 -h

EOF
}

SCRIPT_ARGS=()

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
          SCRIPT_ARGS+=(--benchmark-type "$2")
          shift 2
        else
          echo "Error: --benchmark-type requires a value"
          exit 1
        fi
        ;;
      -s|--schema-name)
        if [[ -n $2 ]]; then
          SCHEMA_NAME=$2
          SCRIPT_ARGS+=(--schema-name "$2")
          shift 2
        else
          echo "Error: --schema-name requires a value"
          exit 1
        fi
        ;;
      -d|--data-dir-name)
        if [[ -n $2 ]]; then
          SCRIPT_ARGS+=(--data-dir-name "$2")
          shift 2
        else
          echo "Error: --data-dir-name requires a value"
          exit 1
        fi
        ;;
      -v|--verbose)
        SCRIPT_ARGS+=(--verbose)
        shift
        ;;
      -H|--hostname)
        if [[ -n $2 ]]; then
          SCRIPT_ARGS+=(--host "$2")
          shift 2
        else
          echo "Error: --hostname requires a value"
          exit 1
        fi
        ;;
      -p|--port)
        if [[ -n $2 ]]; then
          SCRIPT_ARGS+=(--port "$2")
          shift 2
        else
          echo "Error: --port requires a value"
          exit 1
        fi
        ;;
      -u|--user)
        if [[ -n $2 ]]; then
          SCRIPT_ARGS+=(--user "$2")
          shift 2
        else
          echo "Error: --user requires a value"
          exit 1
        fi
        ;;
      --skip-analyze)
        SCRIPT_ARGS+=(--skip-analyze)
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

if [[ -z ${BENCHMARK_TYPE} ]]; then
  echo "Error: Benchmark type is required. Use the -b or --benchmark-type argument."
  print_help
  exit 1
fi

if [[ -z ${SCHEMA_NAME} ]]; then
  echo "Error: Schema name is required. Use the -s or --schema-name argument."
  print_help
  exit 1
fi

if [[ -z ${PRESTO_DATA_DIR} ]]; then
  echo "Error: PRESTO_DATA_DIR environment variable must be set."
  print_help
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DROP_RECREATE_SCRIPT_PATH="${SCRIPT_DIR}/../testing/integration_tests/drop_and_recreate_tables.py"
REQUIREMENTS_PATH="${SCRIPT_DIR}/../testing/requirements.txt"

"${SCRIPT_DIR}/../../scripts/run_py_script.sh" -p "$DROP_RECREATE_SCRIPT_PATH" -r "$REQUIREMENTS_PATH" "${SCRIPT_ARGS[@]}"
