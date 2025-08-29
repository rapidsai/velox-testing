#!/bin/bash
#
# Stop all running Presto containers and cleanup
# Safely terminates all Presto variants and removes containers/networks
#
# Prerequisites:
# - Docker and Docker Compose
#
# Usage: ./stop_presto.sh

# Enable strict error handling
set -euo pipefail

docker compose -f ../docker/docker-compose.java.yml -f ../docker/docker-compose.native-cpu.yml -f ../docker/docker-compose.native-gpu.yml down
