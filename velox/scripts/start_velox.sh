#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker/docker-compose.adapters.yml"

"${SCRIPT_DIR}/stop_velox.sh" || true

docker compose -f "$COMPOSE_FILE" build --pull
docker compose -f "$COMPOSE_FILE" up -d

# Check and report build/test status, following Dockerfile logic
BUILD_STATUS=""
if docker exec velox-adapters test -f /tmp/build_status 2>/dev/null; then
    BUILD_STATUS="$(docker exec velox-adapters cat /tmp/build_status 2>/dev/null || true)"
    if echo "$BUILD_STATUS" | grep -q "VELOX_BUILD_STARTED"; then
        echo ""
        echo "  INFO: Velox build started but not completed, something went wrong."
        echo ""
    elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_INCOMPLETE_AND_BUILD_COMPLETED"; then
        echo ""
        echo "  INFO: Velox tests incomplete, but build completed."
        echo ""
    elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_FAILED_BUT_BUILD_COMPLETED"; then
        echo ""
        echo "  WARNING: Velox build completed but tests failed."
        echo ""
    elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_PASSED_AND_BUILD_COMPLETED"; then
        echo ""
        echo "  INFO: Velox build and tests completed successfully."
        echo ""
    elif echo "$BUILD_STATUS" | grep -q "VELOX_TESTS_SKIPPED_AND_BUILD_COMPLETED"; then
        echo ""
        echo "  INFO: Velox build completed, tests skipped."
        echo ""
    else
        echo ""
        echo "  ERROR: Unknown build status in /tmp/build_status: $BUILD_STATUS"
        echo ""
    fi
else
    echo ""
    echo "  ERROR: /tmp/build_status not found, something went wrong."
    echo ""
fi

echo "  Started velox-adapters in detached mode. View logs with:"
echo "    docker compose -f $COMPOSE_FILE logs -f velox-adapters"

echo ""
echo "  The Velox build output is located in the container at:"
echo "    /opt/velox-build/release"
echo ""
echo "  To access the build output, you can run:"
echo "    docker exec -it velox-adapters ls /opt/velox-build/release"
echo ""
echo "  View build log with:"
echo "    docker exec -it velox-adapters cat /tmp/adapters_build.log"
echo ""
