#!/bin/bash

set -e

# Parse command line arguments
CCACHE_DIR=""
NO_SUBMODULES=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --ccache-dir)
      CCACHE_DIR="$2"
      shift 2
      ;;
    --no-submodules)
      NO_SUBMODULES="--no-submodules"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--ccache-dir <path>] [--no-submodules]"
      exit 1
      ;;
  esac
done

./stop_presto.sh
./build_centos_deps_image.sh $NO_SUBMODULES

# Build with ccache support if cache directory is provided
if [ -n "$CCACHE_DIR" ]; then
  echo "Building with ccache support (cache dir: $CCACHE_DIR)..."
  DOCKER_BUILDKIT=1 docker compose -f ../docker/docker-compose.native-cpu.yml build \
    --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) \
    --build-arg CCACHE_DIR=/ccache \
    --progress plain
else
  echo "Building without ccache..."
  docker compose -f ../docker/docker-compose.native-cpu.yml build \
    --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) \
    --progress plain
fi

docker compose -f ../docker/docker-compose.native-cpu.yml up -d
