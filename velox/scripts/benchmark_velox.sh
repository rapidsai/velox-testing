#!/bin/bash
set -euo pipefail

# Default values
BENCHMARK_TYPE="tpch"
DATA_DIR="../../../velox-benchmark-data"
TPCH_FIX_METADATA=false
BENCHMARK_RESULTS_OUTPUT="./benchmark-results"

# Docker compose configuration
COMPOSE_FILE="../docker/docker-compose.adapters.yml"
CONTAINER_NAME="velox-adapters-build"

# Source benchmark-specific libraries
source "../benchmarks/tpch.sh"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [BENCHMARK_TYPE] [QUERIES] [DEVICES] [PROFILE] [OPTIONS]

Runs Velox benchmarks with CPU and/or GPU execution engines.
Validates that Velox is built and benchmark data is available before running.

Arguments:
  BENCHMARK_TYPE  Type of benchmark to run (default: tpch)
  QUERIES         Query numbers to run (default: all queries for benchmark type)
  DEVICES         Devices to test: cpu, gpu, or "cpu gpu" (default: "cpu gpu")
  PROFILE         Enable profiling: true or false (default: false)

General Options:
  --benchmark-results-output DIR  Copy benchmark results to DIR (default: ./benchmark-results)
  -h, --help                      Show this help message and exit

$(get_tpch_help)

Prerequisites:
  1. Velox must be built using: ./build_velox.sh
  2. Benchmark data must exist at: $DATA_DIR/[benchmark_type]/ (e.g., tpch/)
  3. Docker must be available

Output:
  Benchmark results (text output and nsys profiles) are copied to the specified output directory.

EOF
}

parse_args() {
  local args=()
  
  while [[ $# -gt 0 ]]; do
    case $1 in
      --fix-metadata)
        TPCH_FIX_METADATA=true
        shift
        ;;
      --benchmark-results-output)
        if [[ -n "${2:-}" ]]; then
          BENCHMARK_RESULTS_OUTPUT="$2"
          shift 2
        else
          echo "ERROR: --benchmark-results-output requires a directory argument" >&2
          exit 1
        fi
        ;;
      -h|--help)
        print_help
        exit 0
        ;;
      *)
        args+=("$1")
        shift
        ;;
    esac
  done
  
  BENCHMARK_TYPE="${args[0]:-tpch}"
  
  # Validate --fix-metadata is only used with tpch
  if [[ "$TPCH_FIX_METADATA" == "true" ]] && [[ "$BENCHMARK_TYPE" != "tpch" ]]; then
    echo "ERROR: --fix-metadata is only supported for TPC-H benchmarks" >&2
    exit 1
  fi
  
  case "$BENCHMARK_TYPE" in
    "tpch")
      QUERIES="${args[1]:-$(get_tpch_default_queries)}"
      DEVICES="${args[2]:-"cpu gpu"}"
      PROFILE="${args[3]:-"false"}"
      ;;
    *)
      echo "ERROR: Unknown benchmark type: $BENCHMARK_TYPE" >&2
      echo "Supported benchmark types: tpch" >&2
      exit 1
      ;;
  esac
}

# Helper function to run commands in the Velox container
run_in_container() {
  local cmd="$1"
  local extra_args="${2:-}"
  
  if [[ -n "$extra_args" ]]; then
    docker compose -f "$COMPOSE_FILE" run --rm $extra_args "$CONTAINER_NAME" bash -c "$cmd"
  else
    docker compose -f "$COMPOSE_FILE" run --rm "$CONTAINER_NAME" bash -c "$cmd"
  fi
}

copy_benchmark_results() {
  local output_dir="$1"
  
  echo "Copying benchmark results to $output_dir..."
  
  # Create output directory if it doesn't exist
  mkdir -p "$output_dir"
  
  # Copy all files from container benchmark_results to host output directory
  run_in_container "cp -r /workspace/velox/benchmark_results/* /host_output/ 2>/dev/null || true" "-v $(realpath $output_dir):/host_output"
  
}

check_velox_build() {
  echo "Checking Velox build..."
  
  # Check if docker is available
  if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not available. Please install Docker." >&2
    exit 1
  fi
  
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
      check_tpch_benchmark_executable "run_in_container"
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
      check_tpch_data "$DATA_DIR" "$TPCH_FIX_METADATA"
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
  local devices="$3" 
  local profile="$4"
  
  # Run benchmarks based on type
  case "$benchmark_type" in
    "tpch")
      # Setup TPC-H container environment
      setup_tpch_container_environment
      run_tpch_benchmark "$queries" "$devices" "$profile" "$DATA_DIR" "run_in_container"
      ;;
    *)
      echo "ERROR: Unknown benchmark type: $benchmark_type" >&2
      exit 1
      ;;
  esac
  
  # Copy results to host after all benchmarks complete
  copy_benchmark_results "$BENCHMARK_RESULTS_OUTPUT"
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

echo ""
echo "Starting benchmarks..."
echo ""

# Run benchmarks
run_benchmark "$BENCHMARK_TYPE" "$QUERIES" "$DEVICES" "$PROFILE"

echo ""
echo "Benchmarks completed successfully!"
echo "Results available in: $BENCHMARK_RESULTS_OUTPUT" 