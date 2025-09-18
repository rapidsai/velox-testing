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

SCRIPT_DESCRIPTION="This script generates benchmark data and sets up related tables under the given schema name.
Generated data will reside under the PRESTO_DATA_DIR path in a directory with name that matches 
the value set for the --data-dir-name argument."

SCRIPT_EXAMPLE_ARGS="-b tpch -s my_tpch_sf100 -d sf100 -f 100 -c"

SCRIPT_EXTRA_OPTIONS_DESCRIPTION="-f, --scale-factor                  The scale factor of the generated dataset."

function extra_options_parser() {
  case $1 in
    -f|--scale-factor)
      if [[ -n $2 ]]; then
        SCALE_FACTOR=$2
        SCRIPT_EXTRA_OPTIONS_SHIFTS=2
        SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG=false
        return 0
      else
        echo "Error: --scale-factor requires a value"
        return 1
      fi
      ;;
    *)
      return 0
      ;;
  esac
}
SCRIPT_EXTRA_OPTIONS_PARSER=extra_options_parser

source ./setup_benchmark_helper_check_instance_and_parse_args.sh

DATA_GEN_SCRIPT_PATH=$(readlink -f ../../benchmark_data_tools/generate_data_files.py)

../../scripts/run_py_script.sh -p $DATA_GEN_SCRIPT_PATH --benchmark-type $BENCHMARK_TYPE \
--data-dir-path ${PRESTO_DATA_DIR}/${DATA_DIR_NAME} --scale-factor $SCALE_FACTOR \
$CONVERT_DECIMALS_TO_FLOATS_ARG

./setup_benchmark_tables.sh -b $BENCHMARK_TYPE -s $SCHEMA_NAME -d $DATA_DIR_NAME $CONVERT_DECIMALS_TO_FLOATS_ARG
