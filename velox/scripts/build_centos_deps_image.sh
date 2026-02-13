#!/bin/bash

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the root of the git repository
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"

echo "Building Velox dependencies/run-time container image..."

pushd "${REPO_ROOT}/../velox"
docker compose -f docker-compose.yml --progress plain build adapters-cpp
popd

echo "Velox dependencies/run-time container image built!"
