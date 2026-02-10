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
