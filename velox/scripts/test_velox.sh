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

set -euo pipefail

source "config.sh"

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
  TEST_CMD="ctest -j ${NUM_THREADS} --label-exclude cuda_driver --output-on-failure --no-tests=error -E \"velox_exec_test|velox_hdfs_file_test|velox_hdfs_insert_test\""
else
  # Run cuda_driver tests with 1 thread
  TEST_CMD="ctest -j 1 -L cuda_driver --output-on-failure --no-tests=error -E \"velox_exec_test|velox_hdfs_file_test|velox_hdfs_insert_test|velox_s3\""
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
