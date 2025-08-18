#!/bin/bash
set -euo pipefail

source ./config.sh

echo "Stopping Velox containers..."

if docker compose -f "$COMPOSE_FILE" down; then
  echo "  Velox containers stopped successfully."
else
  STOP_EXIT_CODE=$?
  echo "  ERROR: Failed to stop containers with exit code $STOP_EXIT_CODE"
  exit $STOP_EXIT_CODE
fi
