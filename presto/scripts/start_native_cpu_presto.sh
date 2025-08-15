#!/bin/bash

set -e

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh
./build_centos_deps_image.sh
docker compose -f ../docker/docker-compose.native-cpu.yml build --build-arg NUM_THREADS=$(($(nproc) * 3 / 4)) --progress plain
docker compose -f ../docker/docker-compose.native-cpu.yml up -d
