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

# Source config (same as benchmark script)
source ./config.sh

# Default values (following benchmark_velox.sh pattern)
MEMORY_RESOURCE="async"
PARQUET_PATH=""
THREADS=8
ITERATIONS=5
BENCHMARK_RESULTS_OUTPUT="./reproducer-results"
DATA_DIR="../../../velox-benchmark-data/tpch"  # Same default as benchmark script

# Docker compose configuration (same as benchmark script)
COMPOSE_FILE="../docker/docker-compose.adapters.benchmark.yml"
CONTAINER_NAME="velox-benchmark"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Runs the cuDF memory race reproducer using the benchmark container infrastructure.
This reproducer tests for memory allocation race conditions in cuDF with multiple threads.

Options:
  --memory-resource RESOURCE  Memory resource type: cuda, pool, async, arena, managed, etc. (default: async)
  --parquet-path PATH         Path to parquet file or directory relative to data directory
  --data-dir DIR              Path to benchmark data directory (default: ../../../velox-benchmark-data/tpch)
  --threads NUM               Number of concurrent threads (default: 8)
  --iterations NUM            Iterations per thread (default: 5)
  -o, --output DIR            Output directory for results (default: ./reproducer-results)
  -h, --help                  Show this help message and exit

Examples:
  $(basename "$0") --parquet-path lineitem/lineitem.parquet --memory-resource pool
  $(basename "$0") --parquet-path lineitem --memory-resource cuda --threads 4
  $(basename "$0") --data-dir /datasets/misiug/sf500 --parquet-path lineitem --memory-resource async

Memory Resource Types:
  cuda                        Direct CUDA malloc/free (most thread-safe)
  pool                        Memory pool (known to have race conditions)
  async                       Async memory allocator (occasional race conditions)
  arena                       Arena allocator
  managed                     CUDA Unified Memory
  prefetch_managed            Managed memory with prefetching
  managed_pool                Pool allocator using managed memory
  prefetch_managed_pool       Pool allocator with managed memory and prefetching

Prerequisites:
  1. Velox must be built with GPU support and benchmarks: ./build_velox.sh --gpu --benchmarks true
  2. TPC-H benchmark data must be available
  3. Docker and docker-compose must be available

EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      --memory-resource)
        if [[ -n "${2:-}" ]]; then
          MEMORY_RESOURCE="$2"
          shift 2
        else
          echo "ERROR: --memory-resource requires a value" >&2
          exit 1
        fi
        ;;
      --parquet-path)
        if [[ -n "${2:-}" ]]; then
          PARQUET_PATH="$2"
          shift 2
        else
          echo "ERROR: --parquet-path requires a value" >&2
          exit 1
        fi
        ;;
      --data-dir)
        if [[ -n "${2:-}" ]]; then
          DATA_DIR="$2"
          shift 2
        else
          echo "ERROR: --data-dir requires a directory" >&2
          exit 1
        fi
        ;;
      --threads)
        if [[ -n "${2:-}" ]]; then
          THREADS="$2"
          shift 2
        else
          echo "ERROR: --threads requires a value" >&2
          exit 1
        fi
        ;;
      --iterations)
        if [[ -n "${2:-}" ]]; then
          ITERATIONS="$2"
          shift 2
        else
          echo "ERROR: --iterations requires a value" >&2
          exit 1
        fi
        ;;
      -o|--output)
        if [[ -n "${2:-}" ]]; then
          BENCHMARK_RESULTS_OUTPUT="$2"
          shift 2
        else
          echo "ERROR: --output requires a directory" >&2
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
}

# Create environment file for Docker Compose (same as benchmark script)
create_docker_env_file() {
  local env_file="./.env"
  
  # Same pattern as benchmark_velox.sh
  BENCHMARK_DATA_HOST_PATH=$(realpath "$DATA_DIR")
  BENCHMARK_RESULTS_HOST_PATH=$(realpath "$BENCHMARK_RESULTS_OUTPUT")
  
  cat > "$env_file" << EOF
USER_ID=$(id -u)
GROUP_ID=$(id -g)
BENCHMARK_RESULTS_HOST_PATH=$BENCHMARK_RESULTS_HOST_PATH
BENCHMARK_DATA_HOST_PATH=$BENCHMARK_DATA_HOST_PATH
EOF
}

# Helper function to run commands in the benchmark container (same as benchmark script)
run_in_container() {
  local cmd="$1"
  
  docker compose -f "$COMPOSE_FILE" --env-file ./.env run --rm \
    --cap-add=SYS_ADMIN \
    "$CONTAINER_NAME" bash -c "$cmd"
}

parse_args "$@"

if [[ -z "$PARQUET_PATH" ]]; then
  echo "ERROR: --parquet-path is required" >&2
  echo "Example: --parquet-path lineitem/lineitem.parquet" >&2
  echo "Example: --parquet-path lineitem" >&2
  exit 1
fi

echo "cuDF Memory Race Reproducer"
echo "==========================="
echo ""
echo "Configuration:"
echo "  Memory Resource: $MEMORY_RESOURCE"
echo "  Data Directory: $DATA_DIR"
echo "  Parquet Path: $PARQUET_PATH"
echo "  Threads: $THREADS"
echo "  Iterations: $ITERATIONS"
echo "  Output Directory: $BENCHMARK_RESULTS_OUTPUT"
echo ""

# Create output directory
mkdir -p "$BENCHMARK_RESULTS_OUTPUT"

# Create environment file (same as benchmark script)
create_docker_env_file

# Check if reproducer executable exists
echo "Checking if reproducer is built..."
if ! run_in_container "test -f /opt/velox-build/*/velox/experimental/cudf/reproducer/velox_cudf_memory_race_reproducer" 2>/dev/null; then
  echo "ERROR: Reproducer not found. Please build with: ./build_velox.sh --gpu --benchmarks true" >&2
  exit 1
fi

echo "Running reproducer..."
echo ""

# Run the reproducer (same container pattern as benchmark script)
set +e
run_in_container "
  set -euo pipefail
  
  # Find the reproducer executable
  REPRODUCER=\$(find /opt/velox-build -name velox_cudf_memory_race_reproducer -type f | head -1)
  if [[ -z \"\$REPRODUCER\" ]]; then
    echo \"ERROR: Reproducer executable not found\"
    exit 1
  fi
  
  echo \"Using reproducer: \$REPRODUCER\"
  echo \"Data directory: /workspace/velox/velox-benchmark-data\"
  echo \"Parquet path: /workspace/velox/velox-benchmark-data/$PARQUET_PATH\"
  echo \"Memory resource: $MEMORY_RESOURCE\"
  echo \"\"
  
  # Set memory resource environment variable
  export VELOX_CUDF_MEMORY_RESOURCE=\"$MEMORY_RESOURCE\"
  
  # Run the reproducer
  \"\$REPRODUCER\" \"/workspace/velox/velox-benchmark-data/$PARQUET_PATH\" $THREADS $ITERATIONS 2>&1 | tee \"/workspace/velox/benchmark_results/reproducer_${MEMORY_RESOURCE}_${THREADS}threads_${ITERATIONS}iter.log\"
  
  # Fix ownership
  chown \${USER_ID}:\${GROUP_ID} \"/workspace/velox/benchmark_results/reproducer_${MEMORY_RESOURCE}_${THREADS}threads_${ITERATIONS}iter.log\"
"

EXIT_CODE=$?
set -e

echo ""
echo "=========================================="
if [[ $EXIT_CODE -eq 0 ]]; then
  echo "Reproducer completed successfully!"
  echo "No race condition detected with '$MEMORY_RESOURCE' memory resource"
else
  echo "Reproducer detected issues!"
  echo "Race condition found with '$MEMORY_RESOURCE' memory resource"
fi
echo "=========================================="
echo ""
echo "Results saved to: $BENCHMARK_RESULTS_OUTPUT/reproducer_${MEMORY_RESOURCE}_${THREADS}threads_${ITERATIONS}iter.log"
echo ""
echo "To test other memory resources:"
echo "  $0 --data-dir $DATA_DIR --parquet-path $PARQUET_PATH --memory-resource cuda    # Should work"
echo "  $0 --data-dir $DATA_DIR --parquet-path $PARQUET_PATH --memory-resource pool    # Should fail"
echo "  $0 --data-dir $DATA_DIR --parquet-path $PARQUET_PATH --memory-resource async   # May fail occasionally"

exit $EXIT_CODE