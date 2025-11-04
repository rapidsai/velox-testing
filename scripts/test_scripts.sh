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

function check_temp_dir {
    TEMP_TEST_DIR="${TEMP_TEST_ROOT}/$1"
    [ -e ${TEMP_TEST_DIR} ] && echo "ERROR: ${TEMP_TEST_DIR} already exists; test will not override" && exit 1
    shift
    echo "TEST: $@ -d ${TEMP_TEST_DIR}"
    "$@" -d ${TEMP_TEST_DIR} > /dev/null
    rm -rf ${TEMP_TEST_DIR}
}

function check_temp_pre_dir {
    TEMP_TEST_DIR="./${TEMP_TEST_ROOT}/$1"
    [ -e ${TEMP_TEST_DIR} ] && echo "ERROR: ${TEMP_TEST_DIR} already exists; test will not override" && exit 1
    shift
    "$@" -d ${TEMP_TEST_DIR} > /dev/null
}

function check_temp_post_dir {
    TEMP_TEST_DIR="./${TEMP_TEST_ROOT}/$1"
    [ ! -e ${TEMP_TEST_DIR} ] && echo "ERROR: ${TEMP_TEST_DIR} must exist" && exit 1
    shift
    echo "TEST: $@ -d ${TEMP_TEST_DIR}"
    "$@" -d ${TEMP_TEST_DIR} > /dev/null
    rm -rf ${TEMP_TEST_DIR}
}

SCRIPT_PATH=$(dirname -- "${BASH_SOURCE[0]}")
# TEMP_TEST_ROOT is a temp directory that tests will use and clear.
[ -z $TEMP_TEST_ROOT ] && echo "Error: TEMP_TEST_ROOT must be set" && exit 1
[ -z $PRESTO_DATA_DIR ] && echo "Error: PRESTO_DATA_DIR must be set" && exit 1

# The presto scripts will usually generate data directly in PRESTO_DATA_DIR
echo "Testing presto/scripts"
#pushd ${SCRIPT_PATH}/../presto/scripts
#./setup_benchmark_data_and_tables.sh --help > /dev/null
#check_temp_dir "sf1" ./setup_benchmark_data_and_tables.sh -b tpch -s sf1 -f 1 -c
#check_temp_dir "tpcds/sf1" ./setup_benchmark_data_and_tables.sh -b tpcds -s dssf1 -f 1 -c
#check_temp_dir "sf1" ./setup_benchmark_tables.sh -b tpch -s sf1
#popd

echo "Testing benchmark_data_tools"
#pushd ${SCRIPT_PATH}/../benchmark_data_tools
#check_temp_dir "sf1" python generate_data_files.py -b tpch -s 1 -c -v -j 4
#check_temp_dir "sf001" python generate_data_files.py -b tpch -s 0.01 -c -j 2 -v
#check_temp_dir "tpcds/sf1" python generate_data_files.py -b tpcds -s 1 -c -j 2 -v

#check_temp_pre_dir "sf1" python generate_data_files.py -b tpch -s 1 -c -j 2 -v
#check_temp_post_dir "sf1" python generate_table_schemas.py -b tpch -s ../presto/testing/common/schemas/tpch -v
#check_temp_pre_dir "tpcds/sf1" python generate_data_files.py -b tpcds -s 1 -c -j 2 -v
#check_temp_post_dir "tpcds/sf1" python generate_table_schemas.py -b tpcds -s ../presto/testing/common/schemas/tpcds -v
#popd

# benchmark_data_tools/rewrite_parquet.py
# Should all be run through generate_data_files.py

echo "Testing generate_test_files"
pushd ${SCRIPT_PATH}/../presto/testing/integration_tests/scripts
#./generate_test_files.sh -c
#./generate_test_files.sh
popd

# presto/scripts/run_integration_tests.sh
pushd ${SCRIPT_PATH}/../presto/scripts
./run_integ_test.sh -b tpch
./run_integ_test.sh -b tpcds
popd
