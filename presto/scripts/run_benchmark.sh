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

This script runs the specified type of benchmark.

OPTIONS:
    -h, --help              Show this help message.
    -b, --benchmark-type    Type of benchmark to run. Only "tpch" and "tpcds" are currently supported.
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.
    -h, --hostname          Hostname of the Presto coordinator.
    --port                  Port number of the Presto coordinator.
    -u, --user              User who queries will be executed as.
    -s, --schema-name       Name of the schema containing the tables that will be queried. This must be an existing
                            schema that contains the benchmark tables.
    -o, --output-dir        Directory path that will contain the output files from the benchmark run.
                            By default, output files are written to "$(pwd)/benchmark_output".
    -i, --iterations        Number of query run iterations. By default, 5 iterations are run.
    -t, --tag               Tag associated with the benchmark run. When a tag is specified, benchmark output will be
                            stored inside a directory under the --output-dir path with a name matching the tag name.
                            Tags must contain only alphanumeric and underscore characters.
    -p, --profile           Enable profiling of benchmark queries.

EXAMPLES:
    $0 -b tpch -s bench_sf100
    $0 -b tpch -q "1,2" -s bench_sf100
    $0 -b tpch -s bench_sf100 -i 10 -o ~/tpch_benchmark_output
    $0 -b tpch -s bench_sf100 -t gh200_cpu_sf100
    $0 -b tpch -s bench_sf100 --profile
    $0 -h

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
      -q|--queries)
        if [[ -n $2 ]]; then
          QUERIES=$2
          shift 2
        else
          echo "Error: --queries requires a value"
          exit 1
        fi
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
      --port)
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
      -o|--output-dir)
        if [[ -n $2 ]]; then
          OUTPUT_DIR=$2
          shift 2
        else
          echo "Error: --output-dir requires a value"
          exit 1
        fi
        ;;
      -i|--iterations)
        if [[ -n $2 ]]; then
          ITERATIONS=$2
          shift 2
        else
          echo "Error: --iterations requires a value"
          exit 1
        fi
        ;;
      -t|--tag)
        if [[ -n $2 ]]; then
          TAG=$2
          shift 2
        else
          echo "Error: --tag requires a value"
          exit 1
        fi
        ;;
      -p|--profile)
        PROFILE=true
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

if [[ -z ${SCHEMA_NAME} ]]; then
  echo "Error: A schema name must be set. Use the -s or --schema-name argument."
  print_help
  exit 1
fi

PYTEST_ARGS=("--schema-name ${SCHEMA_NAME}")

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

if [[ -n ${OUTPUT_DIR} ]]; then
  PYTEST_ARGS+=("--output-dir ${OUTPUT_DIR}")
fi

if [[ -n ${ITERATIONS} ]]; then
  PYTEST_ARGS+=("--iterations ${ITERATIONS}")
fi

if [[ -n ${TAG} ]]; then
  if [[ ! ${TAG} =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Error: Invalid --tag value. Tags must contain only alphanumeric and underscore characters."
    print_help
    exit 1
  fi
  PYTEST_ARGS+=("--tag ${TAG}")
fi

if [[ "${PROFILE}" == "true" ]]; then
  PYTEST_ARGS+=("--profile --profile-script-path $(readlink -f ./profiler_functions.sh)")
fi

source ../../scripts/py_env_functions.sh

trap delete_python_virtual_env EXIT

init_python_virtual_env

TEST_DIR=$(readlink -f ../testing)
pip install -q -r ${TEST_DIR}/requirements.txt

source ./common_functions.sh

wait_for_worker_node_registration "$HOST_NAME" "$PORT"

BENCHMARK_TEST_DIR=${TEST_DIR}/performance_benchmarks
pytest -q ${BENCHMARK_TEST_DIR}/${BENCHMARK_TYPE}_test.py ${PYTEST_ARGS[*]}
