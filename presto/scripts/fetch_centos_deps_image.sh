#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/../../scripts/fetch_docker_image_from_s3.sh"

IMAGE_NAME="presto/prestissimo-dependency:centos9"

ARCH=$(uname -m)
BUCKET_SUBDIR="presto-docker-images"
IMAGE_FILE="presto_deps_container_image_centos9_${ARCH}.tar.gz"

#
# check for existing container image
#

validate_docker_image ${IMAGE_NAME}

echo "Presto dependencies/run-time container image not found"

#
# try to pull container image from our S3 bucket
#

fetch_docker_image_from_s3 ${IMAGE_NAME} ${BUCKET_SUBDIR} ${IMAGE_FILE}

# tag with the user-specific name to avoid conflicts between multiple users on the same host
USER_IMAGE_NAME="presto/prestissimo-dependency:centos9-${USER:-latest}"
if [[ "${USER_IMAGE_NAME}" != "${IMAGE_NAME}" ]]; then
  echo "Tagging image as ${USER_IMAGE_NAME}..."
  docker tag ${IMAGE_NAME} ${USER_IMAGE_NAME}
fi

echo "Failed to fetch pre-built Presto dependencies/run-time container image"
