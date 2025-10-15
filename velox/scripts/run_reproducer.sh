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

# Source config
source ./config.sh

# Default values
MEMORY_RESOURCE="async"
PARQUET_FILE=""
THREADS=8
ITERATIONS=5
OUTPUT_DIR="./reproducer-results"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Runs the cuDF memory race reproducer using the benchmark container infrastructure.
This reproducer tests for memory allocation race conditions in cuDF with multiple threads.

Options:
  --memory-resource RESOURCE  Memory resource type: cuda, pool, async, arena, managed, etc. (default: async)
  --parquet-file FILE         Path to parquet file (relative to benchmark data directory)
  --threads NUM               Number of concurrent threads (default: 8)
  --iterations NUM            Iterations per thread (default: 5)
  -o, --output DIR            Output directory for results (default: ./reproducer-results)
  -h, --help                  Show this help message and exit

Examples:
  $(basename "$0") --parquet-file lineitem/part-00000.parquet --memory-resource pool
  $(basename "$0") --parquet-file lineitem/part-00000.parquet --memory-resource cuda --threads 4
  $(basename "$0") --parquet-file lineitem/part-00000.parquet --memory-resource async --iterations 10

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
      --parquet-file)
        if [[ -n "${2:-}" ]]; then
          PARQUET_FILE="$2"
          shift 2
        else
          echo "ERROR: --parquet-file requires a value" >&2
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
          OUTPUT_DIR="$2"
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

# Helper function to run commands in the benchmark container
run_in_container() {
  local cmd="$1"
  
  docker compose -f "$COMPOSE_FILE" --env-file ./.env run --rm \
    --cap-add=SYS_ADMIN \
    velox-benchmark bash -c "$cmd"
}

# Create environment file for Docker Compose
create_docker_env_file() {
  local env_file="./.env"
  
  cat > "$env_file" << EOF
USER_ID=$(id -u)
GROUP_ID=$(id -g)
BENCHMARK_RESULTS_HOST_PATH=$(realpath "$OUTPUT_DIR")
BENCHMARK_DATA_HOST_PATH=$(realpath "../../velox-benchmark-data/tpch")
EOF
}

parse_args "$@"

if [[ -z "$PARQUET_FILE" ]]; then
  echo "ERROR: --parquet-file is required" >&2
  echo "Example: --parquet-file lineitem/part-00000.parquet" >&2
  exit 1
fi

echo "cuDF Memory Race Reproducer"
echo "==========================="
echo ""
echo "Configuration:"
echo "  Memory Resource: $MEMORY_RESOURCE"
echo "  Parquet File: $PARQUET_FILE"
echo "  Threads: $THREADS"
echo "  Iterations: $ITERATIONS"
echo "  Output Directory: $OUTPUT_DIR"
echo ""

# Validate repo layout
../../scripts/validate_directories_exist.sh "../../velox"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Create environment file
create_docker_env_file

# Check if reproducer executable exists
echo "Checking if reproducer is built..."
if ! run_in_container "test -f /opt/velox-build/*/velox/experimental/cudf/reproducer/velox_cudf_memory_race_reproducer" 2>/dev/null; then
  echo "ERROR: Reproducer not found. Please build with: ./build_velox.sh --gpu --benchmarks true" >&2
  exit 1
fi

echo "Running reproducer..."
echo ""

# Run the reproducer
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
  echo \"Parquet file: /workspace/velox/velox-benchmark-data/$PARQUET_FILE\"
  echo \"Memory resource: $MEMORY_RESOURCE\"
  echo \"\"
  
  # Set memory resource environment variable
  export VELOX_CUDF_MEMORY_RESOURCE=\"$MEMORY_RESOURCE\"
  
  # Run the reproducer
  \"\$REPRODUCER\" \"/workspace/velox/velox-benchmark-data/$PARQUET_FILE\" $THREADS $ITERATIONS 2>&1 | tee \"/workspace/velox/benchmark_results/reproducer_${MEMORY_RESOURCE}_${THREADS}threads_${ITERATIONS}iter.log\"
  
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
echo "Results saved to: $OUTPUT_DIR/reproducer_${MEMORY_RESOURCE}_${THREADS}threads_${ITERATIONS}iter.log"
echo ""
echo "To test other memory resources:"
echo "  $0 --parquet-file $PARQUET_FILE --memory-resource cuda    # Should work"
echo "  $0 --parquet-file $PARQUET_FILE --memory-resource pool    # Should fail"
echo "  $0 --parquet-file $PARQUET_FILE --memory-resource async   # May fail occasionally"

exit $EXIT_CODE
