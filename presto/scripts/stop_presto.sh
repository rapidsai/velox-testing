#!/bin/bash

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GPU_FILE="${SCRIPT_DIR}/../docker/docker-compose/generated/docker-compose.native-gpu.rendered.yml"
GPU_DEV_FILE="${SCRIPT_DIR}/../docker/docker-compose.native-gpu-dev.yml"
JAVA_FILE="${SCRIPT_DIR}/../docker/docker-compose.java.yml"
CPU_FILE="${SCRIPT_DIR}/../docker/docker-compose.native-cpu.yml"

# Bring down each variant independently to avoid path resolution issues when combining files
docker compose -f "$JAVA_FILE" down
docker compose -f "$CPU_FILE" down
if [ -f "$GPU_FILE" ]; then
  docker compose -f "$GPU_FILE" down
fi
if [ -f "$GPU_DEV_FILE" ]; then
  docker compose -f "$GPU_DEV_FILE" down
fi
