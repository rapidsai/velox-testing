#!/bin/bash
set -euo pipefail

# Default values
BENCHMARK_TYPE="tpch"
QUERIES=""  # Will be set to benchmark-specific defaults if not provided
DEVICE_TYPE="cpu gpu"
DATA_DIR="../../../velox-benchmark-data"
BENCHMARK_RESULTS_OUTPUT="./benchmark-results"
PROFILE="false"

# Docker compose configuration
COMPOSE_FILE="../docker/docker-compose.adapters.yml"
CONTAINER_NAME="velox-benchmark"  # Uses dedicated benchmark service with pre-configured volumes

# Source benchmark-specific libraries
source "../benchmarks/tpch.sh"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Runs Velox benchmarks with CPU and/or GPU execution engines.
Validates that Velox is built and benchmark data is available before running.
Uses the velox-benchmark Docker service with pre-configured volumes and environment.

Benchmark Options:
  -b, --benchmark-type TYPE               Type of benchmark to run (default: tpch)
  -q, --queries "Q1 Q2 ..."               Query numbers to run (default: all queries for benchmark type)
  -d, --device-type "cpu gpu"             Devices to test: cpu, gpu, or "cpu gpu" (default: "cpu gpu")  
  -p, --profile BOOL                      Enable profiling: true or false (default: false)

General Options:
  -D, --data-dir DIR                      Path to benchmark data directory (default: ../../../velox-benchmark-data)
  -o, --output DIR                    Save benchmark results to DIR (default: ./benchmark-results)
  -h, --help                          Show this help message and exit

$(get_tpch_help)

Prerequisites:
  1. Velox must be built using: ./build_velox.sh
  2. Benchmark data must exist at: <data-dir>/[benchmark_type]/ (e.g., ../../../velox-benchmark-data/tpch/)
  3. Docker and docker-compose must be available
  4. Uses velox-benchmark Docker service (pre-configured with volumes and environment)

Output:
  Benchmark results (text output and nsys profiles) are automatically saved to the specified output directory via Docker volume mounts.

EOF
}

parse_args() {
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
      -D|--data-dir)
        if [[ -n "${2:-}" ]]; then
          DATA_DIR="$2"
          shift 2
        else
          echo "ERROR: --data-dir requires a directory argument" >&2
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
  
  # Set benchmark-specific defaults for queries if not provided
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
  
  BENCHMARK_RESULTS_HOST_PATH="$(realpath "$BENCHMARK_RESULTS_OUTPUT")" \
  docker compose -f "$COMPOSE_FILE" run --rm "$CONTAINER_NAME" bash -c "$cmd"
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
  EXPECTED_OUTPUT_DIR="/opt/velox-build/release"
  
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
  
  # Use the validation script to check base data directory
  ../../scripts/validate_directories_exist.sh "$DATA_DIR"
  
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
  
  # Generic orchestration: run all query/device combinations
  for query_number in $queries; do
    for device in $device_type; do
      # Dispatch to benchmark-specific implementation
      case "$benchmark_type" in
        "tpch")
          run_tpch_single_benchmark "$query_number" "$device" "$profile" "$DATA_DIR/tpch" "run_in_container"
          ;;
        *)
          echo "ERROR: Unknown benchmark type: $benchmark_type" >&2
          exit 1
          ;;
      esac
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
echo "Data directory: $DATA_DIR"
echo "Results output: $BENCHMARK_RESULTS_OUTPUT"
echo ""

# Validate repo layout
../../scripts/validate_directories_exist.sh "../../../velox"

# Check Velox build
check_velox_build

# Check benchmark data 
check_benchmark_data

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
