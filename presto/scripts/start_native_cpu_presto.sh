#!/bin/bash
#
# Start Native CPU Presto variant
# Uses Velox-powered native execution engine (CPU-only)
# Requires building from source with cmake and ninja
#
# Prerequisites:
# - Docker and Docker Compose
# - Valid repository structure with presto and velox directories
# - Sufficient build resources (memory and CPU)
#
# Usage: ./start_native_cpu_presto.sh

# Enable strict error handling
set -euo pipefail

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh
./build_centos_deps_image.sh
docker compose -f ../docker/docker-compose.native-cpu.yml build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --progress plain
docker compose -f ../docker/docker-compose.native-cpu.yml up -d
