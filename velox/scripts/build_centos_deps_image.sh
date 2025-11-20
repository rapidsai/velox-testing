#!/bin/bash

set -e

echo "Building Velox dependencies/run-time container image..."

pushd ../../../velox
docker compose -f docker-compose.yml --progress plain build adapters-cpp
popd

echo "Velox dependencies/run-time container image built!"
