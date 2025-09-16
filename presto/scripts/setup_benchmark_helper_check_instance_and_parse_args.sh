#!/bin/bash

set -e

if [[ -z $SCRIPT_DESCRIPTION ]]; then
  echo "Internal error: SCRIPT_DESCRIPTION must be set"
  exit 1
fi

if [[ -z $SCRIPT_EXAMPLE_ARGS ]]; then
  echo "Internal error: SCRIPT_EXAMPLE_ARGS must be set"
  exit 1
fi

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

$SCRIPT_DESCRIPTION

NOTE: The PRESTO_DATA_DIR environment variable must be set before running this script. This environment variable 
must also be set before starting the Presto instance/running the `start_*_presto.sh` script.

OPTIONS:
    -h, --help                          Show this help message.
    -b, --benchmark-type                Type of benchmark to create tables for. Only "tpch" and "tpcds" are currently supported.
    -s, --schema-name                   Name of the schema that will contain the created tables.
    -d, --data-dir-name                 Name of the directory inside the PRESTO_DATA_DIR path for the benchmark data.
    -c, --convert-decimals-to-floats    Convert all decimal columns to float column type.
    $SCRIPT_EXTRA_OPTIONS_DESCRIPTION

EXAMPLES:
    $0 $SCRIPT_EXAMPLE_ARGS
    $0 -h

EOF
}

if [[ -z $PRESTO_DATA_DIR ]]; then
  echo "Error: PRESTO_DATA_DIR must be set to the directory path that contains the benchmark data directories"
  print_help
  exit 1
fi

source ./common_functions.sh

wait_for_worker_node_registration

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
      -d|--data-dir-name)
        if [[ -n $2 ]]; then
          DATA_DIR_NAME=$2
          shift 2
        else
          echo "Error: --data-dir-name requires a value"
          exit 1
        fi
        ;;
      -c|--convert-decimals-to-floats)
        CONVERT_DECIMALS_TO_FLOATS_ARG="--convert-decimals-to-floats"
        shift
        ;;
      *)
        SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG=true
        if [[ -n $SCRIPT_EXTRA_OPTIONS_PARSER ]]; then
          $SCRIPT_EXTRA_OPTIONS_PARSER "$@"
          if [[ "$SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG" == "false" ]]; then
            shift $SCRIPT_EXTRA_OPTIONS_SHIFTS
          fi
        fi

        if [[ "$SCRIPT_EXTRA_OPTIONS_UNKNOWN_ARG" == "true" ]]; then
          echo "Error: Unknown argument $1"
          print_help
          exit 1
        fi
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
  echo "Error: Schema name is required. Use the -s or --schema-name argument."
  print_help
  exit 1
fi

if [[ -z ${DATA_DIR_NAME} ]]; then
  echo "Error: Data directory name is required. Use the -d or --data-dir-name argument."
  print_help
  exit 1
fi
