#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Tears down the Spark Connect server started by start_spark_connect.sh.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

COMPOSE_FILE="${REPO_ROOT}/spark_gluten/docker/docker-compose.spark-connect.yml"

docker compose -f "${COMPOSE_FILE}" down 2>/dev/null || true

# `docker compose down` does not remove containers created by `docker compose run`.
# Find and remove any leftover run containers owned by the current user.
for container_id in $(docker ps -aq \
  --filter "label=com.nvidia.spark-connect.user=${USER}" 2>/dev/null); do
  docker rm -f "${container_id}" 2>/dev/null || true
done
