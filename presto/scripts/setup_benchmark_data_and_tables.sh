#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DESCRIPTION="This script generates benchmark data and sets up related tables under the given schema name.
Generated data will reside under the PRESTO_DATA_DIR path in a directory with name that matches
the value set for the --data-dir-name argument."

SCRIPT_EXAMPLE_ARGS="-b tpch -s my_tpch_sf100 -d sf100 -f 100 -c"

SCRIPT_EXTRA_OPTIONS_DESCRIPTION="-f, --scale-factor                  The scale factor of the generated dataset.
    -c, --convert-decimals-to-floats    Convert all decimal columns to float column type."

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
      shift 2
      ;;
    -c|--convert-decimals-to-floats)
      CONVERT_DECIMALS_TO_FLOATS_ARG="--convert-decimals-to-floats"
      SCRIPT_EXTRA_OPTIONS_SHIFTS=1
      SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG=false
      shift
      ;;
    *)
      return 0
      ;;
  esac
}
SCRIPT_EXTRA_OPTIONS_PARSER=extra_options_parser

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/setup_benchmark_helper_check_instance_and_parse_args.sh"

DATA_GEN_SCRIPT_PATH=$(readlink -f "${SCRIPT_DIR}/../../benchmark_data_tools/generate_data_files.py")

"${SCRIPT_DIR}/../../scripts/run_py_script.sh" -p $DATA_GEN_SCRIPT_PATH --benchmark-type $BENCHMARK_TYPE \
--data-dir-path ${PRESTO_DATA_DIR}/${DATA_DIR_NAME} --scale-factor $SCALE_FACTOR \
$CONVERT_DECIMALS_TO_FLOATS_ARG

"${SCRIPT_DIR}/setup_benchmark_tables.sh" -b $BENCHMARK_TYPE -s $SCHEMA_NAME -d $DATA_DIR_NAME
