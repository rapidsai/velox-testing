#!/bin/bash

set -e

# Load common environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
setup_presto_env

./stop_presto.sh
./build_centos_deps_image.sh

# Build with ccache support
echo "Building with ccache support (cache dir: $CCACHE_DIR)..."
DOCKER_BUILDKIT=1 docker compose -f ../docker/docker-compose.native-gpu.yml build \
  --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) \
  --build-arg CCACHE_DIR="$CCACHE_DIR" \
  --progress plain

docker compose -f ../docker/docker-compose.native-gpu.yml up -d
