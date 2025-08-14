#!/bin/bash

set -e

# Validate repo layout using shared script
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../scripts/validate_layout.sh"
validate_repo_layout presto 3

./stop_presto.sh
docker compose -f ../docker/docker-compose.java.yml up -d
