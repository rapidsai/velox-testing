#!/bin/bash
set -euo pipefail

source ./config.sh

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Runs tests on the Velox adapters using ctest with parallel execution.

Options:
  -j, --num-threads NUM  Number of threads to use for testing (default: 3/4 of CPU cores).
  -h, --help            Show this help message and exit.

Examples:
  $(basename "$0")
  $(basename "$0") -j 8
  $(basename "$0") --num-threads 4

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
test_cmd="ctest -j ${NUM_THREADS} -R cudf -V"
if docker compose --env-file ./.env-build-velox -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" bash -c "cd ${EXPECTED_OUTPUT_DIR} && ${test_cmd}"; then
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
  echo "    docker compose --env-file ./.env-build-velox -f $COMPOSE_FILE run --rm ${CONTAINER_NAME} bash"
  echo "    # Then inside the container:"
  echo "    # cd ${EXPECTED_OUTPUT_DIR}"
  echo "    # ${test_cmd}"
  echo ""
  exit $TEST_EXIT_CODE
fi
