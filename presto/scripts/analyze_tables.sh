#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs ANALYZE TABLE on all tables in a given schema to collect 
statistics for query optimization. This helps Presto create better query plans 
with lower peak memory usage.

NOTE: This script assumes a Presto server is already running and the schema is
pre-created.

IMPORTANT: Currently, you should run this with CPU Presto (start_java_presto.sh 
or start_native_cpu_presto.sh) rather than GPU Presto, as ANALYZE TABLE may not 
be fully supported on GPU Presto.

OPTIONS:
    -h, --help              Show this help message.
    -s, --schema-name       Name of the schema containing the tables to analyze (required).
    -v, --verbose           Enable verbose output.
    -H, --hostname          Hostname of the Presto coordinator (default: localhost).
    -p, --port              Port number of the Presto coordinator (default: 8080).
    -u, --user              User who queries will be executed as (default: test_user).

EXAMPLES:
    $0 -s my_tpch_sf100
    $0 --schema-name my_tpcds_sf1 -H localhost -p 8080
    $0 -s my_schema -v
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
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

if [[ -z ${SCHEMA_NAME} ]]; then
  echo "Error: Schema name is required. Use the -s or --schema-name argument."
  print_help
  exit 1
fi

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ANALYZE_TABLES_SCRIPT_PATH="${SCRIPT_DIR}/../testing/integration_tests/analyze_tables.py"
REQUIREMENTS_PATH="${SCRIPT_DIR}/../testing/requirements.txt"

"${SCRIPT_DIR}/../../scripts/run_py_script.sh" -p "$ANALYZE_TABLES_SCRIPT_PATH" -r "$REQUIREMENTS_PATH" "${SCRIPT_ARGS[@]}"
