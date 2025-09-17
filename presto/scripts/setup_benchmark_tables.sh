#!/bin/bash

set -e

SCRIPT_DESCRIPTION="This script sets up benchmark tables under the given schema name. The benchmark data 
is expected to already exist under the PRESTO_DATA_DIR path in a directory with name 
that matches the value set for the --data-dir-name argument."

SCRIPT_EXAMPLE_ARGS="-b tpch -s my_tpch_sf100 -d sf100 -c"

source ./setup_benchmark_helper_check_instance_and_parse_args.sh

if [[ ! -d ${PRESTO_DATA_DIR}/${DATA_DIR_NAME} ]]; then
  echo "Error: Benchmark data must already exist inside: ${PRESTO_DATA_DIR}/${DATA_DIR_NAME}"
  exit 1
fi

SCHEMA_GEN_SCRIPT_PATH=$(readlink -f ../../benchmark_data_tools/generate_table_schemas.py)
CREATE_TABLES_SCRIPT_PATH=$(readlink -f ../../presto/testing/integration_tests/create_hive_tables.py)
TEMP_SCHEMA_DIR=$(readlink -f temp-schema-dir)

function cleanup() {
  rm -rf $TEMP_SCHEMA_DIR
}

trap cleanup EXIT 

../../scripts/run_py_script.sh -p $SCHEMA_GEN_SCRIPT_PATH --benchmark-type $BENCHMARK_TYPE \
--schema-name $SCHEMA_NAME --schemas-dir-path $TEMP_SCHEMA_DIR $CONVERT_DECIMALS_TO_FLOATS_ARG

../../scripts/run_py_script.sh -p $CREATE_TABLES_SCRIPT_PATH --schema-name $SCHEMA_NAME \
--schemas-dir-path $TEMP_SCHEMA_DIR --data-dir-name $DATA_DIR_NAME
