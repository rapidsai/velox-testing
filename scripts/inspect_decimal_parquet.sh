#!/bin/bash
#
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs inspect_decimal_parquet.py in a temporary virtual environment
and installs the benchmark_data_tools requirements.

OPTIONS:
    -b, --benchmark-type  Benchmark type (tpch or tpcds).
    -s, --schema-name     Schema name (e.g. decimal_sf100).
    -d, --data-dir-name   Name of the directory inside PRESTO_DATA_DIR.
    --schema-path         Path to a schema .sql file or a directory of schemas.
    --data-dir            Path to the directory containing table parquet data.
    --max-files           Max parquet files to scan per table (0 = all).
    -h, --help            Show this help message.

EXAMPLES:
    $0 -b tpch -s decimal_sf100 -d sf100 --max-files 5

    $0 --schema-path ../presto/testing/common/schemas/tpch/lineitem.sql \
       --data-dir /path/to/data/tpch --max-files 5

    $0 --schema-path ../presto/testing/common/schemas/tpch \
       --data-dir /path/to/data/tpch --max-files 0

EOF
}

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PY_SCRIPT_PATH=$(readlink -f "${SCRIPT_DIR}/../benchmark_data_tools/inspect_decimal_parquet.py")
REQUIREMENTS_PATH=$(readlink -f "${SCRIPT_DIR}/../benchmark_data_tools/requirements.txt")

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
          shift 2
        else
          echo "Error: --benchmark-type requires a value"
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
      -d|--data-dir-name)
        if [[ -n $2 ]]; then
          DATA_DIR_NAME=$2
          shift 2
        else
          echo "Error: --data-dir-name requires a value"
          exit 1
        fi
        ;;
      --schema-path)
        if [[ -n $2 ]]; then
          SCHEMA_PATH=$2
          shift 2
        else
          echo "Error: --schema-path requires a value"
          exit 1
        fi
        ;;
      --data-dir)
        if [[ -n $2 ]]; then
          DATA_DIR_PATH=$2
          shift 2
        else
          echo "Error: --data-dir requires a value"
          exit 1
        fi
        ;;
      --max-files)
        if [[ -n $2 ]]; then
          MAX_FILES=$2
          shift 2
        else
          echo "Error: --max-files requires a value"
          exit 1
        fi
        ;;
      *)
        SCRIPT_ARGS+=($1)
        shift
        ;;
    esac
  done
}

parse_args "$@"

if [[ -z $SCHEMA_PATH ]]; then
  if [[ -z $BENCHMARK_TYPE || ! $BENCHMARK_TYPE =~ ^tpc(h|ds)$ ]]; then
    echo "Error: --benchmark-type must be set to tpch or tpcds when --schema-path is not provided."
    print_help
    exit 1
  fi
  if [[ -z $SCHEMA_NAME ]]; then
    echo "Error: --schema-name is required when --schema-path is not provided."
    print_help
    exit 1
  fi
  if [[ -z $DATA_DIR_NAME ]]; then
    echo "Error: --data-dir-name is required when --schema-path is not provided."
    print_help
    exit 1
  fi
  if [[ -z $PRESTO_DATA_DIR ]]; then
    echo "Error: PRESTO_DATA_DIR must be set when using --data-dir-name."
    print_help
    exit 1
  fi
  SCHEMA_PATH="${SCRIPT_DIR}/../presto/testing/common/schemas/${BENCHMARK_TYPE}"
  DATA_DIR_PATH="${PRESTO_DATA_DIR}/${DATA_DIR_NAME}"
fi

if [[ -z $DATA_DIR_PATH ]]; then
  echo "Error: --data-dir is required when --schema-path is provided."
  print_help
  exit 1
fi

if [[ ! -e $SCHEMA_PATH ]]; then
  echo "Error: schema path not found: $SCHEMA_PATH"
  exit 1
fi

if [[ ! -d $DATA_DIR_PATH ]]; then
  echo "Error: data directory not found: $DATA_DIR_PATH"
  exit 1
fi

PY_ARGS=(--schema-path "$SCHEMA_PATH" --data-dir "$DATA_DIR_PATH")
if [[ -n $MAX_FILES ]]; then
  PY_ARGS+=(--max-files "$MAX_FILES")
fi

"${SCRIPT_DIR}/run_py_script.sh" -p "$PY_SCRIPT_PATH" -r "$REQUIREMENTS_PATH" "${PY_ARGS[@]}" "${SCRIPT_ARGS[@]}"
