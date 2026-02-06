#!/bin/bash

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${COMPOSE_PROJECT_NAME:-}" ]; then
  project_user="${USER:-user}"
  project_user="${project_user//[^a-zA-Z0-9]/-}"
  project_user="$(printf "%s" "${project_user}" | tr '[:upper:]' '[:lower:]')"
  COMPOSE_PROJECT_NAME="presto-${project_user}"
  export COMPOSE_PROJECT_NAME
fi

GPU_FILE="${SCRIPT_DIR}/../docker/docker-compose/generated/docker-compose.native-gpu.rendered.yml"
JAVA_FILE="${SCRIPT_DIR}/../docker/docker-compose.java.yml"
CPU_FILE="${SCRIPT_DIR}/../docker/docker-compose.native-cpu.yml"

# Bring down each variant independently to avoid path resolution issues when combining files
docker compose -f "$JAVA_FILE" down
docker compose -f "$CPU_FILE" down
if [ -f "$GPU_FILE" ]; then
  docker compose -f "$GPU_FILE" down
fi
