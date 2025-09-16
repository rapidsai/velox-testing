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

function cleanup() {
  rm -rf .venv
}

trap cleanup EXIT

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs integration tests for the specified type of benchmark.

OPTIONS:
    -h, --help              Show this help message.
    -b, --benchmark-type    Type of benchmark to run tests for. Only "tpch" and "tpcds" are currently supported.
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.
    -k, --keep-tables       If this argument is specified, created benchmark tables will not be dropped.

EXAMPLES:
    $0 -b tpch
    $0 -b tpch -q "1,2" --keep-tables
    $0 -h

EOF
}

KEEP_TABLES=false

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
      -q|--queries)
        if [[ -n $2 ]]; then
          QUERIES=$2
          shift 2
        else
          echo "Error: --queries requires a value"
          exit 1
        fi
        ;;
      -k|--keep-tables)
        KEEP_TABLES=true
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

if [[ -z ${BENCHMARK_TYPE} || ! ${BENCHMARK_TYPE} =~ ^tpc(h|ds)$ ]]; then
  echo "Error: A valid benchmark type (tpch or tpcds) is required. Use the -b or --benchmark-type argument."
  print_help
  exit 1
fi

rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate

INTEGRATION_TEST_DIR=$(readlink -f ../testing/integration_tests)

pip install -q -r ${INTEGRATION_TEST_DIR}/requirements.txt

PYTEST_ARGS=()

if [[ "${KEEP_TABLES}" == "true" ]]; then
  PYTEST_ARGS+=("--keep-tables")
fi

if [[ -n ${QUERIES} ]]; then
  PYTEST_ARGS+=("--queries ${QUERIES}")
fi

source ./common_functions.sh

wait_for_worker_node_registration

pytest -v ${INTEGRATION_TEST_DIR}/${BENCHMARK_TYPE}_test.py ${PYTEST_ARGS[*]}
