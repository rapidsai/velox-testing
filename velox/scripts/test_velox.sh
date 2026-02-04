#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/config.sh"

# Helper function to get BUILD_TYPE from container environment
get_build_type_from_container() {
    local compose_file=$1
    local container_name=$2

    docker compose -f "$compose_file" run --rm "${container_name}" bash -c "echo \$BUILD_TYPE"
}

# Get BUILD_TYPE from container environment
BUILD_TYPE=$(get_build_type_from_container "$COMPOSE_FILE" "$CONTAINER_NAME")

# expected output directory
EXPECTED_OUTPUT_DIR="/opt/velox-build/${BUILD_TYPE}"

DEVICE_TYPE="gpu"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Runs tests on the Velox adapters using ctest with parallel execution.

Options:
  -j, --num-threads NUM  Number of threads to use for testing (default: 3/4 of CPU cores).
  -d, --device-type TYPE  Device to target: cpu|gpu (default: gpu).
  -h, --help            Show this help message and exit.

Examples:
  $(basename "$0")
  $(basename "$0") -j 8
  $(basename "$0") --num-threads 4
  $(basename "$0") --device-type cpu

By default, uses 3/4 of available CPU cores for parallel test execution.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -j|--num-threads)
        if [[ -z "${2:-}" || "${2}" =~ ^- ]]; then
          echo "Error: --num-threads requires a value"
          exit 1
        fi
        if [[ "$DEVICE_TYPE" == "gpu" && "$2" -gt 2 ]]; then
          echo "Warning: Using more than 2 threads in GPU mode may cause OOM errors during testing."
        fi
        NUM_THREADS="$2"
        shift 2
        ;;
      -d|--device-type)
        if [[ -z "${2:-}" || "${2}" =~ ^- ]]; then
          echo "Error: --device-type requires a value (cpu|gpu)"
          exit 1
        fi
        DEVICE_TYPE="${2,,}"
        if [[ "$DEVICE_TYPE" != "cpu" && "$DEVICE_TYPE" != "gpu" ]]; then
          echo "Error: --device-type must be 'cpu' or 'gpu'"
          exit 1
        fi
        shift 2
        ;;
      -h|--help)
        print_help
        exit 0
        ;;
      *)
        echo "Unknown option: $1"
        echo "Use -h or --help for usage information"
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

echo "Running tests on Velox adapters..."
echo ""
echo "Device type: ${DEVICE_TYPE}"
TEST_PREAMBLE='if [ -f "/opt/miniforge/etc/profile.d/conda.sh" ]; then
    source "/opt/miniforge/etc/profile.d/conda.sh"
    conda activate adapters
  fi
  export CLASSPATH=$(/usr/local/hadoop/bin/hdfs classpath --glob)'

if [[ "$DEVICE_TYPE" == "cpu" ]]; then
  # disable velox_table_evolution_fuzzer_test pending resolution of too-many-open-files problem
  # seves 1/9/26
  SKIP_TESTS="velox_exec_test|velox_hdfs_file_test|velox_hdfs_insert_test|velox_table_evolution_fuzzer_test"
  TEST_CMD="ctest -j ${NUM_THREADS} --label-exclude cuda_driver --output-on-failure --no-tests=error -E \"${SKIP_TESTS}\""
else
  if [[ "$NUM_THREADS" -gt 2 ]]; then
    echo "Warning: For GPU mode, setting NUM_THREADS to 2 to avoid possible OOM errors."
    NUM_THREADS=2
  fi
  # disable velox_cudf_s3_read_test pending inheritance of RMM with shutdown error-avoidance (PR 2202)
  # seves 1/9/26
  SKIP_TESTS="velox_exec_test|velox_hdfs_file_test|velox_hdfs_insert_test|velox_s3|velox_cudf_s3_read_test"
  TEST_CMD="ctest -j ${NUM_THREADS} -L cuda_driver --output-on-failure --no-tests=error -E \"${SKIP_TESTS}\""
fi
if docker compose -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" bash -c "set -euo pipefail; cd ${EXPECTED_OUTPUT_DIR} && ${TEST_PREAMBLE} && ${TEST_CMD}"; then
  echo ""
  echo "  Tests passed successfully!"
  echo ""
  exit 0
else
  TEST_EXIT_CODE=$?
  echo ""
  echo "  ERROR: Tests failed with exit code $TEST_EXIT_CODE"
  echo ""
  echo "  To debug, you can run:"
  echo "    docker compose -f $COMPOSE_FILE run --rm ${CONTAINER_NAME} bash"
  echo "    # Then inside the container:"
  echo "    # cd ${EXPECTED_OUTPUT_DIR}"
  echo "    # ${TEST_CMD}"
  echo ""
  exit $TEST_EXIT_CODE
fi
