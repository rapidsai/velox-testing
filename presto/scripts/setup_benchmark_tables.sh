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

SCRIPT_DESCRIPTION="This script sets up benchmark tables under the given schema name. The benchmark data 
is expected to already exist under the PRESTO_DATA_DIR path in a directory with name
that matches the value set for the --data-dir-name argument."

SCRIPT_EXAMPLE_ARGS="-b tpch -s my_tpch_sf100 -d sf100"

RUN_ANALYZE=true
SCRIPT_EXTRA_OPTIONS_DESCRIPTION=$'-a, --analyze-tables           Run ANALYZE TABLES after setup (default: on)\n    --skip-analyze                Skip running ANALYZE TABLES.'

function extra_options_parser() {
  case $1 in
    -a|--analyze|--analyze-tables)
      RUN_ANALYZE=true
      SCRIPT_EXTRA_OPTIONS_SHIFTS=1
      SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG=false
      ;;
    --skip-analyze)
      RUN_ANALYZE=false
      SCRIPT_EXTRA_OPTIONS_SHIFTS=1
      SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG=false
      ;;
    *)
      return 0
      ;;
  esac
}
SCRIPT_EXTRA_OPTIONS_PARSER=extra_options_parser

source ./setup_benchmark_helper_check_instance_and_parse_args.sh

if [[ ! -d ${PRESTO_DATA_DIR}/${DATA_DIR_NAME} ]]; then
  echo "Error: Benchmark data must already exist inside: ${PRESTO_DATA_DIR}/${DATA_DIR_NAME}"
  exit 1
fi

SCHEMA_GEN_SCRIPT_PATH=$(readlink -f ../../benchmark_data_tools/generate_table_schemas.py)
CREATE_TABLES_SCRIPT_PATH=$(readlink -f ../../presto/testing/integration_tests/create_hive_tables.py)
CREATE_TABLES_REQUIREMENTS_PATH=$(readlink -f ../../presto/testing/requirements.txt)
TEMP_SCHEMA_DIR=$(readlink -f temp-schema-dir)

function ensure_cpu_worker_for_analyze() {
  if ! $RUN_ANALYZE; then
    return
  fi

  if command -v docker >/dev/null 2>&1; then
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q 'presto-native-worker-gpu'; then
      if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q 'presto-native-worker-cpu'; then
        cat <<EOF

ANALYZE TABLES requires a native CPU worker. Please start Presto via
./start_native_cpu_presto.sh and re-run this script or use --skip-analyze.

EOF
        exit 1
      fi
    fi
  fi
}

function cleanup() {
  rm -rf $TEMP_SCHEMA_DIR
}

trap cleanup EXIT

../../scripts/run_py_script.sh -p $SCHEMA_GEN_SCRIPT_PATH \
                               --benchmark-type $BENCHMARK_TYPE \
                               --schemas-dir-path $TEMP_SCHEMA_DIR \
                               --data-dir-name "${PRESTO_DATA_DIR}/${DATA_DIR_NAME}"

../../scripts/run_py_script.sh -p $CREATE_TABLES_SCRIPT_PATH \
                               -r $CREATE_TABLES_REQUIREMENTS_PATH \
                               --schema-name $SCHEMA_NAME \
                               --schemas-dir-path $TEMP_SCHEMA_DIR \
                               --data-dir-name $DATA_DIR_NAME

if $RUN_ANALYZE; then
  ensure_cpu_worker_for_analyze
  echo "Running ANALYZE TABLES for schema '$SCHEMA_NAME'..."
  ./analyze_tables.sh --schema-name "$SCHEMA_NAME"
  echo "ANALYZE TABLES successfully executed for schema '$SCHEMA_NAME'."
fi
