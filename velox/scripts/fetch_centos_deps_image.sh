#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/config.sh"
source "${SCRIPT_DIR}/../../scripts/fetch_docker_image_from_s3.sh"

IMAGE_NAME="ghcr.io/facebookincubator/velox-dev:adapters"

ARCH=$(uname -m)
BUCKET_SUBDIR="velox-docker-images"
IMAGE_FILE="velox_adapters_deps_image_centos9_${ARCH}.tar.gz"

#
# check for existing container image
#

validate_docker_image "${IMAGE_NAME}"

echo "Velox dependencies/run-time container image not found"

#
# try to pull container image from our S3 bucket
#

if fetch_docker_image_from_s3 "${IMAGE_NAME}" "${BUCKET_SUBDIR}" "${IMAGE_FILE}"; then
  echo "Successfully fetched pre-built Velox dependencies/run-time container image"
  exit 0
else
  echo "Failed to fetch pre-built Velox dependencies/run-time container image"
  exit 1
fi
