#!/bin/bash

set -e

# Validate repo layout using shared script
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../scripts/validate_layout.sh"
validate_repo_layout presto 3

./stop_presto.sh
./build_centos_deps_image.sh
docker compose -f ../docker/docker-compose.native-gpu.yml build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --progress plain
docker compose -f ../docker/docker-compose.native-gpu.yml up -d
