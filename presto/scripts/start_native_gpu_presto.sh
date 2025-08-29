#!/bin/bash
#
# Start Native GPU Presto variant
# Uses Velox-powered native execution engine with CUDF GPU acceleration
# Requires building from source with CUDA support
#
# Prerequisites:
# - Docker and Docker Compose with NVIDIA runtime
# - NVIDIA GPU with compute capability 7.0+
# - Valid repository structure with presto and velox directories
# - Sufficient build resources (memory, CPU, and GPU)
#
# Usage: ./start_native_gpu_presto.sh

# Enable strict error handling
set -euo pipefail

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh
./build_centos_deps_image.sh
docker compose -f ../docker/docker-compose.native-gpu.yml build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --progress plain
docker compose -f ../docker/docker-compose.native-gpu.yml up -d
