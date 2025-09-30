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

This script runs integration tests for the specified type of benchmark.

OPTIONS:
    -h, --help              Show this help message.
    -b, --benchmark-type    Type of benchmark to run tests for. Only "tpch" and "tpcds" are currently supported.
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.
    -k, --keep-tables       If this argument is specified, created benchmark tables will not be dropped.
    -h, --hostname          Hostname of the Presto coordinator.
    -p, --port              Port number of the Presto coordinator.
    -u, --user              User who queries will be executed as.
    -s, --schema-name       Name of the schema containing the tables that will be queried.
    -f, --scale-factor      Scale factor of the schema containing the tables that will be queried. This must be
                            set when (and only when) --schema-name is specified.


EXAMPLES:
    $0 -b tpch
    $0 -b tpch -q "1,2" --keep-tables
    $0 -b tpch -q "1,2" -s my_sf1_schema -f 1
    $0 -b tpch -q "1,2" -h myhostname.com -p 8081 -s my_sf1_schema -f 1
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
      -h|--hostname)
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
      -u|--user)
        if [[ -n $2 ]]; then
          USER_NAME=$2
          shift 2
        else
          echo "Error: --user requires a value"
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
      -f|--scale-factor)
        if [[ -n $2 ]]; then
          SCALE_FACTOR=$2
          shift 2
        else
          echo "Error: --scale-factor requires a value"
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

PYTEST_ARGS=()

if [[ "${KEEP_TABLES}" == "true" ]]; then
  PYTEST_ARGS+=("--keep-tables")
fi

if [[ -n ${QUERIES} ]]; then
  PYTEST_ARGS+=("--queries ${QUERIES}")
fi

if [[ -n ${HOST_NAME} ]]; then
  PYTEST_ARGS+=("--hostname ${HOST_NAME}")
fi

if [[ -n ${PORT} ]]; then
  PYTEST_ARGS+=("--port ${PORT}")
fi

if [[ -n ${USER_NAME} ]]; then
  PYTEST_ARGS+=("--user ${USER_NAME}")
fi

if [[ -n ${SCHEMA_NAME} ]]; then
  if [[ -z ${SCALE_FACTOR} ]]; then
    echo "Error: A valid scale factor is required when --schema-name is specified. Use the -f or --scale-factor argument."
    print_help
    exit 1
  fi
  PYTEST_ARGS+=("--schema-name ${SCHEMA_NAME}")
fi

if [[ -n ${SCALE_FACTOR} ]]; then
  if [[ -z ${SCHEMA_NAME} ]]; then
    echo "Error: Scale factor should be set only when --schema-name is specified."
    print_help
    exit 1
  fi
  PYTEST_ARGS+=("--scale-factor ${SCALE_FACTOR}")
fi

source ../../scripts/py_env_functions.sh

trap delete_python_virtual_env EXIT

init_python_virtual_env

TEST_DIR=$(readlink -f ../testing)
pip install -q -r ${TEST_DIR}/requirements.txt

source ./common_functions.sh

wait_for_worker_node_registration "$HOST_NAME" "$PORT"

INTEGRATION_TEST_DIR=${TEST_DIR}/integration_tests
pytest -v ${INTEGRATION_TEST_DIR}/${BENCHMARK_TYPE}_test.py ${PYTEST_ARGS[*]}
