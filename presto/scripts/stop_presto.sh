#!/bin/bash

set -e

# Change to the script's directory to ensure correct relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

docker compose -f ../docker/docker-compose.java.yml -f ../docker/docker-compose.native-cpu.yml -f ../docker/docker-compose.native-gpu.yml down
