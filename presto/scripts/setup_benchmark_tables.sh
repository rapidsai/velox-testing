#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DESCRIPTION="This script sets up benchmark tables under the given schema name. The benchmark data
is expected to already exist under the PRESTO_DATA_DIR path in a directory with name
that matches the value set for the --data-dir-name argument."

SCRIPT_EXAMPLE_ARGS="-b tpch -s my_tpch_sf100 -d sf100"
SCRIPT_EXTRA_OPTIONS_DESCRIPTION="-H, --hostname                   Hostname of the Presto coordinator (default: localhost).
    -p, --port                          Port number of the Presto coordinator (default: 8080)."
SCRIPT_EXTRA_OPTIONS_SHIFTS=0
SCRIPT_EXTRA_OPTIONS_PARSER=parse_extra_options

parse_extra_options() {
  case $1 in
    -H|--hostname)
      if [[ -n $2 ]]; then
        HOST_NAME=$2
        SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG=false
        SCRIPT_EXTRA_OPTIONS_SHIFTS=2
      else
        echo "Error: --hostname requires a value"
        exit 1
      fi
      ;;
    -p|--port)
      if [[ -n $2 ]]; then
        PORT=$2
        SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG=false
        SCRIPT_EXTRA_OPTIONS_SHIFTS=2
      else
        echo "Error: --port requires a value"
        exit 1
      fi
      ;;
  esac
}

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/presto_connection_defaults.sh"
source "${SCRIPT_DIR}/setup_benchmark_helper_check_instance_and_parse_args.sh"

set_presto_coordinator_defaults

if [[ ! -d ${PRESTO_DATA_DIR}/${DATA_DIR_NAME} ]]; then
  echo "Error: Benchmark data must already exist inside: ${PRESTO_DATA_DIR}/${DATA_DIR_NAME}"
  exit 1
fi

SCHEMA_GEN_SCRIPT_PATH=$(readlink -f "${SCRIPT_DIR}/../../benchmark_data_tools/generate_table_schemas.py")
CREATE_TABLES_SCRIPT_PATH=$(readlink -f "${SCRIPT_DIR}/../../presto/testing/integration_tests/create_hive_tables.py")
CREATE_TABLES_REQUIREMENTS_PATH=$(readlink -f "${SCRIPT_DIR}/../../presto/testing/requirements.txt")
TEMP_SCHEMA_DIR=$(readlink -f "${SCRIPT_DIR}/temp-schema-dir")

function cleanup() {
  rm -rf $TEMP_SCHEMA_DIR
}

trap cleanup EXIT

# These scripts are used in some non-docker environments, so provide the option to skip
# the docker setup/teardown.
if [[ "$DOCKER_DEPLOYMENT" == "true" ]]; then
  "${SCRIPT_DIR}/start_native_cpu_presto.sh"
  source "${SCRIPT_DIR}/common_functions.sh"
  wait_for_worker_node_registration "$HOST_NAME" "$PORT"
fi


"${SCRIPT_DIR}/../../scripts/run_py_script.sh" -p $SCHEMA_GEN_SCRIPT_PATH \
                               --benchmark-type $BENCHMARK_TYPE \
                               --schemas-dir-path $TEMP_SCHEMA_DIR \
                               --data-dir-name "${PRESTO_DATA_DIR}/${DATA_DIR_NAME}"

"${SCRIPT_DIR}/../../scripts/run_py_script.sh" -p $CREATE_TABLES_SCRIPT_PATH \
                               -r $CREATE_TABLES_REQUIREMENTS_PATH \
                               --schema-name $SCHEMA_NAME \
                               --schemas-dir-path $TEMP_SCHEMA_DIR \
                               --data-dir-name $DATA_DIR_NAME

if [[ "$SKIP_ANALYZE_TABLES" == "false" ]]; then
  "${SCRIPT_DIR}/analyze_tables.sh" -s $SCHEMA_NAME -H "$HOST_NAME" -p "$PORT"
fi

if [[ "$DOCKER_DEPLOYMENT" == "true" ]]; then
  "${SCRIPT_DIR}/stop_presto.sh"
fi
