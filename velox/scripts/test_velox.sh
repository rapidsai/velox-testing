#!/bin/bash
set -euo pipefail

source ./config.sh

<<<<<<< HEAD
BUILD_TARGET="gpu"  # Default to GPU build

=======
>>>>>>> main
print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Runs tests on the Velox adapters Docker container, with options to control which build target to test.

Options:
  --cpu              Test CPU-only build (excludes CUDA tests).
  --gpu              Test GPU build (includes CUDA tests if available) [default].
  -h, --help         Show this help message and exit.

Examples:
  $(basename "$0")
  $(basename "$0") --cpu
  $(basename "$0") --gpu
  $(basename "$0") -j 8
  $(basename "$0") --num-threads 8

By default, tests the GPU build target, uses 3/4 of available CPU cores for parallel test execution.
Runs tests on the Velox adapters using ctest with parallel execution.

EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --cpu)
        BUILD_TARGET="cpu"
        shift
        ;;
      --gpu)
        BUILD_TARGET="gpu"
        shift
        ;;
      -j|--num-threads)
        if [[ -z "${2:-}" || "${2}" =~ ^- ]]; then
          echo "Error: --num-threads requires a value"
          exit 1
        fi
        NUM_THREADS="$2"
        shift 2
        ;;
      -h|--help)
        print_help
        exit 0
        ;;
      *)
        echo "Unrecognized argument: $1"
        echo "Use -h or --help for usage information."
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

# Set container name based on build target
if [[ "$BUILD_TARGET" == "cpu" ]]; then
  CONTAINER_NAME="$CPU_CONTAINER_NAME"
  echo "Running tests on Velox adapters (CPU-only build)..."
  # Exclude CUDA tests for CPU builds
  test_cmd="LD_LIBRARY_PATH=${EXPECTED_OUTPUT_LIB_DIR} ctest -j ${NUM_THREADS} --label-exclude cuda_driver --output-on-failure --no-tests=error --stop-on-failure"
else
  CONTAINER_NAME="$GPU_CONTAINER_NAME"
  echo "Running tests on Velox adapters (GPU build)..."
  # Include all tests for GPU builds (CUDA tests may be available)
  test_cmd="LD_LIBRARY_PATH=${EXPECTED_OUTPUT_LIB_DIR} ctest -j ${NUM_THREADS} --output-on-failure --no-tests=error --stop-on-failure"
fi

echo ""
if docker compose -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" bash -c "cd ${EXPECTED_OUTPUT_DIR} && ${test_cmd}"; then
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
  echo "    # ${test_cmd}"
  echo ""
  exit $TEST_EXIT_CODE
fi
