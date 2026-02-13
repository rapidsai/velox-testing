#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the root of the git repository
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"

# Source benchmark-specific libraries
source "${SCRIPT_DIR}/../benchmarks/tpch.sh"

# Default values
BENCHMARK_TYPE="tpch"
QUERIES=""  # Will be set to benchmark-specific defaults if not provided
DEVICE_TYPE="cpu gpu"
BENCHMARK_RESULTS_OUTPUT="./benchmark-results"
PROFILE="false"
ENABLE_GPU_METRICS="false"
DATA_DIR="${REPO_ROOT}/../velox-benchmark-data/tpch"  # Default to TPC-H, will be adjusted per benchmark type
NUM_REPEATS=2

# Docker compose configuration
COMPOSE_FILE="${SCRIPT_DIR}/../docker/docker-compose.adapters.benchmark.yml"
CONTAINER_NAME="velox-benchmark"  # Uses dedicated benchmark service with pre-configured volumes


print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Runs Velox benchmarks with CPU and/or GPU execution engines.
Validates that Velox is built and benchmark data is available before running.
Uses the velox-benchmark Docker service with pre-configured volumes and environment.

Benchmark Options:
  -b, --benchmark-type TYPE               Type of benchmark to run (default: tpch)
  -q, --queries "1 2 ..."                 Query numbers to run, specified as a space-separated list of query numbers (default: all queries for benchmark type)
  -d, --device-type "cpu gpu"             Devices to test: cpu, gpu, or "cpu gpu" (default: "cpu gpu")
  -p, --profile BOOL                      Enable profiling: true or false (default: false)
  --enable-gpu-metrics                    Enable GPU metrics collection during profiling (default: disabled)
  --data-dir DIR                          Path to benchmark data directory (default: ../../../velox-benchmark-data/tpch)
  --num-repeats NUM                       Number of times to repeat each query (default: 2)

General Options:
  -o, --output DIR                        Save benchmark results to DIR (default: ./benchmark-results)
  -h, --help                              Show this help message and exit

$(get_tpch_help)

Examples:
  $(basename "$0")                                      # Run all queries on CPU and GPU (defaults)
  $(basename "$0") --queries 6 --device-type cpu        # Run Q6 on CPU only
  $(basename "$0") --queries "1 6" --device-type "cpu gpu"  # Run Q1 and Q6 on both CPU and GPU
  $(basename "$0") --queries 6 --device-type gpu --profile true  # Run Q6 on GPU with profiling
  $(basename "$0") --queries 6 --device-type gpu --profile true --enable-gpu-metrics  # Run Q6 with GPU metrics enabled
  $(basename "$0") --queries 6 --device-type gpu -o /tmp/results  # Custom output directory
  $(basename "$0") --queries 6 --device-type cpu --data-dir /path/to/data  # Custom data directory
  $(basename "$0") --queries 6 --device-type cpu --num-repeats 5  # Run Q6 with 5 repetitions

Prerequisites:
  1. Velox must be built using: ./build_velox.sh
  2. Benchmark data must exist and location can be specified with --data-dir option
  3. Docker and docker-compose must be available
  4. Uses velox-benchmark Docker service (pre-configured with volumes and environment)

Output:
  Benchmark results (text output and nsys profiles) are automatically saved to the specified output directory via Docker volume mounts.

EOF
}

parse_args() {

  # Parse general arguments
  while [[ $# -gt 0 ]]; do
    case $1 in
      -b|--benchmark-type)
        if [[ -n "${2:-}" ]]; then
          BENCHMARK_TYPE="$2"
          shift 2
        else
          echo "ERROR: --benchmark-type requires a benchmark type argument" >&2
          exit 1
        fi
        ;;
      -q|--queries)
        if [[ -n "${2:-}" ]]; then
          QUERIES="$2"
          shift 2
        else
          echo "ERROR: --queries requires a query list argument" >&2
          exit 1
        fi
        ;;
      -d|--device-type)
        if [[ -n "${2:-}" ]]; then
          DEVICE_TYPE="$2"
          shift 2
        else
          echo "ERROR: --device-type requires a device type argument" >&2
          exit 1
        fi
        ;;
      -p|--profile)
        if [[ -n "${2:-}" ]]; then
          PROFILE="$2"
          shift 2
        else
          echo "ERROR: --profile requires true or false argument" >&2
          exit 1
        fi
        ;;
      -o|--output)
        if [[ -n "${2:-}" ]]; then
          BENCHMARK_RESULTS_OUTPUT="$2"
          shift 2
        else
          echo "ERROR: --output requires a directory argument" >&2
          exit 1
        fi
        ;;
      --data-dir)
        if [[ -n "${2:-}" ]]; then
          DATA_DIR="$2"
          shift 2
        else
          echo "ERROR: --data-dir requires a directory argument" >&2
          exit 1
        fi
        ;;
      --num-repeats)
        if [[ -n "${2:-}" ]]; then
          NUM_REPEATS="$2"
          shift 2
        else
          echo "ERROR: --num-repeats requires a number argument" >&2
          exit 1
        fi
        ;;
      --enable-gpu-metrics)
        ENABLE_GPU_METRICS="true"
        shift
        ;;
      -h|--help)
        print_help
        exit 0
        ;;
      *)
        echo "ERROR: Unknown option: $1" >&2
        echo "Use --help for usage information" >&2
        exit 1
        ;;
    esac
  done

  # Set benchmark-specific defaults for queries if not provided, NOTE: changes with `RUN` commands do not persist
  if [[ -z "$QUERIES" ]]; then
    case "$BENCHMARK_TYPE" in
      "tpch")
        QUERIES="$(get_tpch_default_queries)"
        ;;
      *)
        echo "ERROR: Unknown benchmark type: $BENCHMARK_TYPE" >&2
        echo "Supported benchmark types: tpch" >&2
        exit 1
        ;;
    esac
  fi

}

# Helper function to run commands in the Velox benchmark container
run_in_container() {
  local cmd="$1"

  docker compose -f "$COMPOSE_FILE" --env-file "${SCRIPT_DIR}/.env" run --rm \
    --cap-add=SYS_ADMIN \
    -e ENABLE_GPU_METRICS="${ENABLE_GPU_METRICS}" \
    "$CONTAINER_NAME" bash -c "$cmd"
}


# Helper function to create/update environment file for Docker Compose
create_docker_env_file() {
  local env_file="${SCRIPT_DIR}/.env"

  # Always override the environment file
  cat > "$env_file" << EOF
USER_ID=$(id -u)
GROUP_ID=$(id -g)
BENCHMARK_RESULTS_HOST_PATH=$(realpath "$BENCHMARK_RESULTS_OUTPUT")
BENCHMARK_DATA_HOST_PATH=$(realpath "$DATA_DIR")
EOF

}

prepare_benchmark_results_dir() {
  local output_dir="$1"

  # Create output directory if it doesn't exist
  mkdir -p "$output_dir"
}

check_velox_build() {
  echo "Checking Velox build..."

  # Check if velox-adapters-build image exists
  if ! docker image inspect velox-adapters-build:latest &> /dev/null; then
    echo "ERROR: velox-adapters-build Docker image not found." >&2
    echo "Please build Velox first by running: ./build_velox.sh" >&2
    exit 1
  fi

  # Check if the build output exists in the container
  EXPECTED_OUTPUT_DIR="/opt/velox-build/${BUILD_TYPE}"

  if ! run_in_container "test -d ${EXPECTED_OUTPUT_DIR}" 2>/dev/null; then
    echo "ERROR: Velox build output not found in container at ${EXPECTED_OUTPUT_DIR}" >&2
    echo "Please rebuild Velox by running: ./build_velox.sh" >&2
    exit 1
  fi

  # Check benchmark executables based on benchmark type
  case "$BENCHMARK_TYPE" in
    "tpch")
      check_tpch_benchmark_executable "run_in_container" "$DEVICE_TYPE"
      ;;
    *)
      echo "ERROR: Unknown benchmark type: $BENCHMARK_TYPE" >&2
      exit 1
      ;;
  esac

  echo "Velox build verification passed"
}

check_benchmark_data() {
  echo "Checking benchmark data..."

  case "$BENCHMARK_TYPE" in
    "tpch")
      check_tpch_data "$DATA_DIR"
      ;;
    *)
      echo "ERROR: Unknown benchmark type: $BENCHMARK_TYPE" >&2
      echo "Supported benchmark types: tpch" >&2
      exit 1
      ;;
  esac
}

run_benchmark() {
  local benchmark_type="$1"
  local queries="$2"
  local device_type="$3"
  local profile="$4"

  echo "Running $benchmark_type benchmark..."
  echo "Queries: $queries"
  echo "Device types: $device_type"
  echo "Profile: $profile"

  # Run all query/device combinations
  for query_number in $queries; do
    for device in $device_type; do
      # Dispatch to benchmark-specific implementation
      local exit_code=0

      case "$benchmark_type" in
        "tpch")
          run_tpch_single_benchmark "$query_number" "$device" "$profile" "run_in_container" "$NUM_REPEATS"
          local exit_code=$?
          ;;
        *)
          echo "ERROR: Unknown benchmark type: $benchmark_type" >&2
          exit 1
          ;;
      esac

      if [[ $exit_code -ne 0 ]]; then
        echo "ERROR: Benchmark execution failed for query $query_number on $device" >&2
        exit 1
      fi
    done
  done
}

# Parse arguments
parse_args "$@"

echo ""
echo "Velox Benchmark Runner"
echo "====================="
echo ""
echo "Benchmark type: $BENCHMARK_TYPE"
echo "Results output: $BENCHMARK_RESULTS_OUTPUT"
echo ""

# Validate repo layout
"${REPO_ROOT}/scripts/validate_directories_exist.sh" "${REPO_ROOT}/../velox"

# Create environment file for Docker Compose
create_docker_env_file

# Get BUILD_TYPE from container environment
export BUILD_TYPE=$(run_in_container "echo \$BUILD_TYPE")

# Check benchmark data
check_benchmark_data

# Check Velox build
check_velox_build

# Prepare benchmark results directory
prepare_benchmark_results_dir "$BENCHMARK_RESULTS_OUTPUT"

echo ""
echo "Starting benchmarks..."
echo ""

# Run benchmarks
run_benchmark "$BENCHMARK_TYPE" "$QUERIES" "$DEVICE_TYPE" "$PROFILE"

echo ""
echo "Benchmarks completed successfully!"
echo "Results available in: $BENCHMARK_RESULTS_OUTPUT"
