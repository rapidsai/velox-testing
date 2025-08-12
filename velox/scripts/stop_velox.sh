#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker compose -f "${SCRIPT_DIR}/../docker/docker-compose.adapters.yml" down 