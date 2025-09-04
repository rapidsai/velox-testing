#!/bin/bash

set -e

function cleanup() {
  rm -rf .venv
}

trap cleanup EXIT

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script generates the files required to run integration tests for supported benchmarks.

OPTIONS:
    -h, --help                          Show this help message.
    -s, --scale-factor                  The scale factor of the generated dataset.
    -c, --convert-decimals-to-floats    Convert all decimal columns to float column type.
    -t, --tpch-only			Only generate TPCH data
    -v, --verbose			Generate additional logging

EXAMPLES:
    $0 -s 1
    $0 --scale-factor 0.01 --convert-decimals-to-floats

EOF
}

SCALE_FACTOR=0.01
CONVERT_DECIMALS_TO_FLOATS=false
BENCHMARK_TYPES="tpch tpcds"

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -s|--scale-factor)
        if [[ -n $2 ]]; then
          SCALE_FACTOR=$2
          shift 2
        else
          echo "Error: --scale-factor requires a value"
          exit 1
        fi
        ;;
      -c|--convert-decimals-to-floats)
        CONVERT_DECIMALS_TO_FLOATS=true
        shift
        ;;
      -t|--tpch-only)
        BENCHMARK_TYPES="tpch"
        shift
        ;;
      -v|--verbose)
        VERBOSE="--verbose"
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

rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate

BENCHMARK_DATA_TOOLS_DIR=$(readlink -f ../../../../benchmark_data_tools)

pip install -q -r $BENCHMARK_DATA_TOOLS_DIR/requirements.txt

CONVERT_DECIMALS_TO_FLOATS_ARG=""
if [[ "$CONVERT_DECIMALS_TO_FLOATS" == "true" ]]; then
  CONVERT_DECIMALS_TO_FLOATS_ARG="--convert-decimals-to-floats"
fi

for BENCHMARK_TYPE in $BENCHMARK_TYPES; do
  SCHEMAS_DIR=../schemas/$BENCHMARK_TYPE
  rm -rf $SCHEMAS_DIR
  echo "Generating table schema files for $BENCHMARK_TYPE..."
  python $BENCHMARK_DATA_TOOLS_DIR/generate_table_schemas.py --benchmark-type $BENCHMARK_TYPE \
         --schemas-dir-path $SCHEMAS_DIR $CONVERT_DECIMALS_TO_FLOATS_ARG
  echo "Table schema files generated for $BENCHMARK_TYPE"

  QUERIES_DIR=../queries/$BENCHMARK_TYPE
  rm -rf $QUERIES_DIR
  echo "Generating benchmark queries file for $BENCHMARK_TYPE..."
  python $BENCHMARK_DATA_TOOLS_DIR/generate_query_file.py --benchmark-type $BENCHMARK_TYPE \
         --queries-dir-path $QUERIES_DIR
  echo "Benchmark queries file generated for $BENCHMARK_TYPE"

  DATA_DIR=../data/$BENCHMARK_TYPE
  rm -rf $DATA_DIR
  echo "Generating benchmark data files for $BENCHMARK_TYPE..."
  python $BENCHMARK_DATA_TOOLS_DIR/generate_data_files.py --benchmark-type $BENCHMARK_TYPE \
         --data-dir-path $DATA_DIR --scale-factor $SCALE_FACTOR $CONVERT_DECIMALS_TO_FLOATS_ARG $VERBOSE
  echo "Benchmark data files generated for $BENCHMARK_TYPE"
done
