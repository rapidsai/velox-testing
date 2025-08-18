#!/bin/bash
set -euo pipefail

source ./config.sh

echo "Running tests on Velox adapters..."
echo ""

# Check if the build output directory exists in the container
if ! docker compose -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" test -d "${EXPECTED_OUTPUT_DIR}" 2>/dev/null; then
  echo "  ERROR: Build output directory ${EXPECTED_OUTPUT_DIR} not found in the container."
  echo "  Please run build_velox.sh first to build the project."
  echo ""
  exit 1
fi

# Run ctest with cudf tests
echo "  Running: ctest -R cudf -V --output-on-failure"
echo ""

if docker compose -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" bash -c "cd ${EXPECTED_OUTPUT_DIR} && ctest -R cudf -V --output-on-failure"; then
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
  echo "    # ctest -R cudf -V --output-on-failure"
  echo ""
  exit $TEST_EXIT_CODE
fi
