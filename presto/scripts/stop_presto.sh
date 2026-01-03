#!/bin/bash

set -e

GPU_FILE="../docker/docker-compose/generated/docker-compose.native-gpu.rendered.yml"
JAVA_FILE="../docker/docker-compose.java.yml"
CPU_FILE="../docker/docker-compose.native-cpu.yml"
GPU_DEV_FILE="../docker/docker-compose.native-gpu-dev.yml"

# Bring down each variant independently to avoid path resolution issues when combining files
docker compose -f "$JAVA_FILE" down
docker compose -f "$CPU_FILE" down
if [ -f "$GPU_FILE" ]; then
  docker compose -f "$GPU_FILE" down
fi
docker compose -f "$GPU_DEV_FILE" down
