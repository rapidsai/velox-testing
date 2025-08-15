#!/bin/bash

set -e

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"

./stop_presto.sh
docker compose -f ../docker/docker-compose.java.yml up -d
