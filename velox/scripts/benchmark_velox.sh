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

# Default values
BENCHMARK_TYPE="tpch"
QUERIES=""  # Will be set to benchmark-specific defaults if not provided
DEVICE_TYPE="cpu gpu"
BENCHMARK_RESULTS_OUTPUT="./benchmark-results"
PROFILE="false"
DATA_DIR="../../../velox-benchmark-data/tpch"  # Default to TPC-H, will be adjusted per benchmark type
NUM_REPEATS=2
VERBOSE_LOGGING="false"
CALL_SITE_COLLECTION="false"
SYNC_CALL_SITES_FILE=""
BISECTION_MIDPOINT=""
BISECTION_TOTAL_ROWS=""

# Docker compose configuration
COMPOSE_FILE="../docker/docker-compose.adapters.benchmark.yml"
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
  --data-dir DIR                          Path to benchmark data directory (default: ../../../velox-benchmark-data/tpch)
  --num-repeats NUM                       Number of times to repeat each query (default: 2)
  --verbose-logging BOOL                  Enable RMM memory event logging to CSV file for detailed allocation/deallocation tracking (default: false)

Bisection Search Options (for debugging cuDF memory race conditions):
  --call-site-collection                  Enable call site collection mode: sync ALL deallocation call sites and log unique IDs
                                          Generates CSV file with call site data for analysis
  --sync-call-sites-file FILE             Enable bisection mode: only sync call sites listed in FILE
                                          Supports both single-level and multi-level stack trace formats
  --bisection-midpoint FLOAT              Row-based bisection midpoint (0.0 to 1.0): sync deallocations up to this fraction
  --bisection-total-rows NUM              Total number of deallocation rows expected (for calculating sync threshold)

General Options:
  -o, --output DIR                        Save benchmark results to DIR (default: ./benchmark-results)
  -h, --help                              Show this help message and exit

$(get_tpch_help)

Examples:
  $(basename "$0")                                      # Run all queries on CPU and GPU (defaults)
  $(basename "$0") --queries 6 --device-type cpu        # Run Q6 on CPU only
  $(basename "$0") --queries "1 6" --device-type "cpu gpu"  # Run Q1 and Q6 on both CPU and GPU
  $(basename "$0") --queries 6 --device-type gpu --profile true  # Run Q6 on GPU with profiling
  $(basename "$0") --queries 6 --device-type gpu -o /tmp/results  # Custom output directory
  $(basename "$0") --queries 6 --device-type cpu --data-dir /path/to/data  # Custom data directory
  $(basename "$0") --queries 6 --device-type cpu --num-repeats 5  # Run Q6 with 5 repetitions
  $(basename "$0") --queries 15 --device-type gpu --verbose-logging true  # Run Q15 on GPU with RMM memory event logging

Bisection Search Examples:
  $(basename "$0") --queries 6 --device-type gpu --call-site-collection  # Collect all unique call site IDs (sync all)
  $(basename "$0") --queries 6 --device-type gpu --sync-call-sites-file /tmp/sync_sites.txt  # Test specific call sites only
  $(basename "$0") --queries 6 --device-type gpu --bisection-midpoint 0.5 --bisection-total-rows 117502  # Sync first half of deallocations
  $(basename "$0") --queries 6 --device-type gpu --bisection-midpoint 0.25 --bisection-total-rows 117502  # Sync first quarter

Call Site File Format (/tmp/sync_sites.txt):
  # Lines starting with # are comments
  # Single-level format (basic):
  velox_cudf_tpch_benchmark+0x127060
  libcudf.so+0x3c0160
  
  # Multi-level format (more precise, from backtrace):
  velox_cudf_tpch_benchmark+0x149ee8->0x14af18->0x1273a0->0x6624->0x66f4->0x186e040->0x18d22dc->0xb9eb20
  
  # Each line: module_name+0xoffset[->0xoffset2->...] (from call site analysis CSV)

Environment Variables (for advanced debugging):
  RMM_SYNC_DEBUG=1                       Enable debug output showing sync matches and events (default: disabled)
  RMM_STACK_TRACE_DEPTH=N                Control stack trace depth for call site uniqueness (default: 8, max: 32)
  RMM_SYNC_DISABLE=1                     Completely disable all synchronization (default: disabled)
  RMM_POISON_MEMORY=1                    Poison memory before deallocation to catch use-after-free (default: disabled)
  RMM_POISON_PATTERN=HH                  Hex byte pattern for poisoning (default: DE for 0xDEADBEEF pattern)
  
  # These are automatically set when using --verbose-logging or bisection modes:
  RMM_LOG_FILE=path                      RMM memory event logging (CSV format)
  RMM_DEBUG_LOG_FILE=path                RMM general debug logging (text format)
  RMM_STACK_TRACE_FILE=path              Call site capture logging (CSV format)
  RMM_SYNC_CALL_SITES_FILE=path          File containing call sites to synchronize

Advanced Debugging Examples:
  # Collect call sites with debug output and shallow stack traces (faster):
  RMM_SYNC_DEBUG=1 RMM_STACK_TRACE_DEPTH=4 $(basename "$0") --queries 6 --device-type gpu --call-site-collection
  
  # Test specific call site with deep stack traces for maximum uniqueness:
  RMM_SYNC_DEBUG=1 RMM_STACK_TRACE_DEPTH=16 $(basename "$0") --queries 6 --device-type gpu --sync-call-sites-file /tmp/sync_sites.txt
  
  # Run without any synchronization (baseline performance):
  RMM_SYNC_DISABLE=1 $(basename "$0") --queries 6 --device-type gpu --call-site-collection
  
  # Aggressive memory poisoning to catch use-after-free (will cause immediate crashes):
  RMM_POISON_MEMORY=1 RMM_SYNC_DEBUG=1 $(basename "$0") --queries 6 --device-type gpu --bisection-midpoint 0.125 --bisection-total-rows 117502
  
  # Custom poison pattern (0xFF fills memory with 255):
  RMM_POISON_MEMORY=1 RMM_POISON_PATTERN=FF $(basename "$0") --queries 6 --device-type gpu --bisection-midpoint 0.125 --bisection-total-rows 117502

Prerequisites:
  1. Velox must be built using: ./build_velox.sh (with -fno-omit-frame-pointer for better stack traces)
  2. Benchmark data must exist and location can be specified with --data-dir option
  3. Docker and docker-compose must be available
  4. Uses velox-benchmark Docker service (pre-configured with volumes and environment)

Output Files (when using bisection search or verbose logging):
  benchmark_results/qNN_gpu_N_drivers_rmm.csv           # RMM memory event log
  benchmark_results/qNN_gpu_N_drivers_debug.log         # RMM debug log  
  benchmark_results/qNN_gpu_N_drivers_stacktrace.csv    # Call site capture log (for analysis)
  benchmark_results/qNN_gpu_N_drivers.nsys-rep          # NVIDIA Nsight Systems profile (if --profile true)

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
      --verbose-logging)
        if [[ -n "${2:-}" ]]; then
          VERBOSE_LOGGING="$2"
          shift 2
        else
          echo "ERROR: --verbose-logging requires true or false" >&2
          exit 1
        fi
        ;;
      --call-site-collection)
        CALL_SITE_COLLECTION="true"
        shift
        ;;
      --sync-call-sites-file)
        if [[ -n "${2:-}" ]]; then
          SYNC_CALL_SITES_FILE="$2"
          shift 2
        else
          echo "ERROR: --sync-call-sites-file requires a file path" >&2
          exit 1
        fi
        ;;
      --bisection-midpoint)
        if [[ -n "${2:-}" ]]; then
          BISECTION_MIDPOINT="$2"
          shift 2
        else
          echo "ERROR: --bisection-midpoint requires a float argument (0.0 to 1.0)" >&2
          exit 1
        fi
        ;;
      --bisection-total-rows)
        if [[ -n "${2:-}" ]]; then
          BISECTION_TOTAL_ROWS="$2"
          shift 2
        else
          echo "ERROR: --bisection-total-rows requires a number argument" >&2
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
  
  # Validate bisection search options
  if [[ "$CALL_SITE_COLLECTION" == "true" && -n "$SYNC_CALL_SITES_FILE" ]]; then
    echo "ERROR: Cannot use both --call-site-collection and --sync-call-sites-file" >&2
    echo "Use --call-site-collection for data collection mode (sync all call sites)" >&2
    echo "Use --sync-call-sites-file for bisection mode (sync only specific call sites)" >&2
    exit 1
  fi
  
  # Validate sync call sites file exists if specified
  if [[ -n "$SYNC_CALL_SITES_FILE" && ! -f "$SYNC_CALL_SITES_FILE" ]]; then
    echo "ERROR: Sync call sites file not found: $SYNC_CALL_SITES_FILE" >&2
    exit 1
  fi
  
  # Validation: Row-based bisection parameters must be used together
  if [[ -n "$BISECTION_MIDPOINT" && -z "$BISECTION_TOTAL_ROWS" ]]; then
    echo "ERROR: --bisection-midpoint requires --bisection-total-rows" >&2
    exit 1
  fi
  if [[ -z "$BISECTION_MIDPOINT" && -n "$BISECTION_TOTAL_ROWS" ]]; then
    echo "ERROR: --bisection-total-rows requires --bisection-midpoint" >&2
    exit 1
  fi
  
  # Validation: Midpoint must be between 0.0 and 1.0
  if [[ -n "$BISECTION_MIDPOINT" ]]; then
    if ! awk "BEGIN {exit !($BISECTION_MIDPOINT >= 0.0 && $BISECTION_MIDPOINT <= 1.0)}"; then
      echo "ERROR: --bisection-midpoint must be between 0.0 and 1.0" >&2
      exit 1
    fi
  fi
  
}

# Helper function to run commands in the Velox benchmark container
run_in_container() {
  local cmd="$1"
  
  docker compose -f "$COMPOSE_FILE" --env-file ./.env run --rm \
    --cap-add=SYS_ADMIN \
    "$CONTAINER_NAME" bash -c "$cmd"
}


# Helper function to create/update environment file for Docker Compose
create_docker_env_file() {
  local env_file="./.env"
  
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
  local verbose_logging="$5"
  local call_site_collection="$6"
  local sync_call_sites_file="$7"
  local bisection_midpoint="$8"
  local bisection_total_rows="$9"
  
  echo "Running $benchmark_type benchmark..."
  echo "Queries: $queries"
  echo "Device types: $device_type"
  echo "Profile: $profile"
  echo "Verbose logging: $verbose_logging"
  
  # Show bisection search mode
  if [[ "$call_site_collection" == "true" ]]; then
    echo "Bisection mode: Call site collection (sync ALL call sites)"
  elif [[ -n "$sync_call_sites_file" ]]; then
    echo "Bisection mode: Sync specific call sites from: $sync_call_sites_file"
  elif [[ -n "$bisection_midpoint" ]]; then
    echo "Bisection mode: Row-based bisection (midpoint=$bisection_midpoint, total_rows=$bisection_total_rows)"
  else
    echo "Bisection mode: Disabled"
  fi
  
  # Run all query/device combinations
  for query_number in $queries; do
    for device in $device_type; do
      # Dispatch to benchmark-specific implementation
      local exit_code=0

      case "$benchmark_type" in
        "tpch")
          run_tpch_single_benchmark "$query_number" "$device" "$profile" "run_in_container" "$NUM_REPEATS" "$verbose_logging" "$call_site_collection" "$sync_call_sites_file" "$bisection_midpoint" "$bisection_total_rows"
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
../../scripts/validate_directories_exist.sh "../../../velox"

# Create environment file for Docker Compose
create_docker_env_file

# Get BUILD_TYPE from container environment
export BUILD_TYPE=$(run_in_container "echo \$BUILD_TYPE")

# Source benchmark-specific libraries
source "../benchmarks/tpch.sh"

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
run_benchmark "$BENCHMARK_TYPE" "$QUERIES" "$DEVICE_TYPE" "$PROFILE" "$VERBOSE_LOGGING" "$CALL_SITE_COLLECTION" "$SYNC_CALL_SITES_FILE" "$BISECTION_MIDPOINT" "$BISECTION_TOTAL_ROWS"

echo ""
echo "Benchmarks completed successfully!"
echo "Results available in: $BENCHMARK_RESULTS_OUTPUT" 
