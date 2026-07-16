#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

Registers benchmark external tables in a Presto Hive schema whose data lives at an
arbitrary EXTERNAL_LOCATION URI. The location scheme is not interpreted here, so any
URI works (e.g. file:, s3://).

The tables are created with EXTERNAL_LOCATION = <base>/<table_name>, where <base>
is the --external-location-base value (which must already include the scale-factor
directory, e.g. s3://bucket/prefix/sf100).

NOTE: This script assumes a Presto server is already running.

OPTIONS:
    -h, --help                      Show this help message.
    -b, --benchmark-type            Benchmark type: "tpch" or "tpcds" (required).
    -s, --schema-name               Name of the Hive schema to (re)create tables in (required).
                                    The schema is dropped and recreated.
    -l, --external-location-base    Full URI base that contains one subdirectory per table,
                                    e.g. s3://my-bucket/velox/sf100 (required). "/<table_name>"
                                    will be appended per table.
    -H, --hostname                  Hostname of the Presto coordinator (default: localhost).
    -p, --port                      Port number of the Presto coordinator (default: 8080).

EXAMPLES:
    $0 -b tpch -s tpch_sf100_s3 -l s3://my-bucket/velox/sf100
    $0 --benchmark-type tpch --schema-name tpch_sf100_s3 \\
       --external-location-base s3://my-bucket/velox/sf100 -H localhost -p 8080

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
      -s|--schema-name)
        if [[ -n $2 ]]; then
          SCHEMA_NAME=$2
          shift 2
        else
          echo "Error: --schema-name requires a value"
          exit 1
        fi
        ;;
      -l|--external-location-base)
        if [[ -n $2 ]]; then
          EXTERNAL_LOCATION_BASE=$2
          shift 2
        else
          echo "Error: --external-location-base requires a value"
          exit 1
        fi
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

if [[ -z ${SCHEMA_NAME} ]]; then
  echo "Error: A schema name is required. Use the -s or --schema-name argument."
  print_help
  exit 1
fi

if [[ -z ${EXTERNAL_LOCATION_BASE} ]]; then
  echo "Error: An external location base is required. Use the -l or --external-location-base argument."
  print_help
  exit 1
fi

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/presto_connection_defaults.sh"
set_presto_coordinator_defaults

source "${SCRIPT_DIR}/common_functions.sh"
wait_for_worker_node_registration "$HOST_NAME" "$PORT"

SCHEMAS_DIR=$(readlink -f "${SCRIPT_DIR}/../testing/common/schemas/${BENCHMARK_TYPE}")
CREATE_TABLES_SCRIPT_PATH=$(readlink -f "${SCRIPT_DIR}/../testing/integration_tests/create_hive_tables.py")
REQUIREMENTS_PATH=$(readlink -f "${SCRIPT_DIR}/../testing/requirements.txt")

echo "Registering ${BENCHMARK_TYPE} tables in schema '${SCHEMA_NAME}' at base: ${EXTERNAL_LOCATION_BASE}"

# HOSTNAME/PORT are read by create_hive_tables.py to connect to the coordinator.
HOSTNAME="$HOST_NAME" PORT="$PORT" "${SCRIPT_DIR}/../../scripts/run_py_script.sh" \
  -p "$CREATE_TABLES_SCRIPT_PATH" \
  -r "$REQUIREMENTS_PATH" \
  -- \
  --schema-name "$SCHEMA_NAME" \
  --schemas-dir-path "$SCHEMAS_DIR" \
  --external-location-base "$EXTERNAL_LOCATION_BASE"

echo "Done registering ${BENCHMARK_TYPE} external tables in hive.${SCHEMA_NAME}"
