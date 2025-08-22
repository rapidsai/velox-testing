#!/bin/bash
set -euo pipefail

source ./config.sh

echo "Running tests on Velox adapters..."
echo ""
test_cmd="LD_LIBRARY_PATH=${EXPECTED_OUTPUT_LIB_DIR} ctest -j ${NUM_THREADS} --label-exclude cuda_driver --output-on-failure --no-tests=error --stop-on-failure"
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
