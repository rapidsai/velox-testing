#!/bin/bash
set -euo pipefail

source ./config.sh

echo "Running tests on Velox adapters..."
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
