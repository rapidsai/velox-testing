#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Common utility functions for Docker and S3 operations

validate_docker_image() {
  local IMAGE_NAME=$1
  echo "Validating Docker image ${IMAGE_NAME}..."
  if [[ -z $(docker images -q ${IMAGE_NAME}) ]]; then
    echo "ERROR: Docker image ${IMAGE_NAME} does not exist"
    exit 1
  fi
  echo "✓ Docker image exists"
}

# Retry `docker pull` with exponential backoff. Useful for shrugging off
# transient registry errors (ghcr.io 502s, brief network blips).
#
# Usage: docker_pull_with_retry IMAGE
# Override attempt count / initial backoff via DOCKER_PULL_ATTEMPTS and
# DOCKER_PULL_BACKOFF_INITIAL (defaults: 5 attempts, 10s initial, doubling).
docker_pull_with_retry() {
  local image=$1
  local attempts=${DOCKER_PULL_ATTEMPTS:-5}
  local delay=${DOCKER_PULL_BACKOFF_INITIAL:-10}
  local i
  for ((i = 1; i <= attempts; i++)); do
    if docker pull "${image}"; then
      return 0
    fi
    if (( i < attempts )); then
      echo "docker pull '${image}' failed (attempt ${i}/${attempts}); retrying in ${delay}s..."
      sleep "${delay}"
      delay=$((delay * 2))
    fi
  done
  echo "ERROR: docker pull '${image}' failed after ${attempts} attempts" >&2
  return 1
}
