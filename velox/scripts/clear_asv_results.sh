#!/bin/bash
# Helper script to clear ASV results with proper permissions
# This handles Docker-created files that may have root ownership

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RESULTS_PATH="${1:-$SCRIPT_DIR/../asv_benchmarks/results}"

echo "Clearing ASV results in: $RESULTS_PATH"

# Try normal deletion first
if rm -rf "$RESULTS_PATH"/* 2>/dev/null; then
    echo "✓ Results cleared successfully"
    exit 0
fi

# If that fails, use Docker with proper permissions
echo "Permission denied - using Docker to clear results..."
docker run --rm \
    -v "$RESULTS_PATH:/results" \
    alpine sh -c "rm -rf /results/* && echo '✓ Results cleared via Docker'"

echo "Done!"
