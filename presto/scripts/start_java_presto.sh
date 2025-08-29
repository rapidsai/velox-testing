#!/bin/bash
#
# Start Java-based Presto variant
# Uses prestodb/presto:latest Docker image with Java execution engine
#
# Prerequisites:
# - Docker and Docker Compose
# - Valid repository structure with presto and velox directories
#
# Usage: ./start_java_presto.sh

# Enable strict error handling
set -euo pipefail

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh
docker compose -f ../docker/docker-compose.java.yml up -d
