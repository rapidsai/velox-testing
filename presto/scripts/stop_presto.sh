#!/bin/bash

set -e

GPU_FILE="../docker/.generated/docker-compose.native-gpu.rendered.yml"
JAVA_FILE="../docker/docker-compose.java.yml"
CPU_FILE="../docker/docker-compose.native-cpu.yml"

# Bring down each variant independently to avoid path resolution issues when combining files
docker compose -f "$JAVA_FILE" down
docker compose -f "$CPU_FILE" down
[ -f "$GPU_FILE" ] && docker compose -f "$GPU_FILE" down
