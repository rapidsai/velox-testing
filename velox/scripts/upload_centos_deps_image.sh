#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

source ./config.sh
source ../../scripts/upload_docker_image_to_s3.sh

IMAGE_NAME="ghcr.io/facebookincubator/velox-dev:adapters"

ARCH=$(uname -m)
BUCKET_SUBDIR="velox-docker-images"
IMAGE_FILE="velox_adapters_deps_image_centos9_${ARCH}.tar.gz"

#
# validate that the container image exists
#

validate_docker_image ${IMAGE_NAME}

#
# upload container image to S3 bucket
#

upload_docker_image_to_s3 ${IMAGE_NAME} ${BUCKET_SUBDIR} ${IMAGE_FILE}

if [[ $? -eq 0 ]]; then
  echo "Successfully uploaded Velox dependencies/run-time container image to S3"
  exit 0
else
  echo "Failed to upload Velox dependencies/run-time container image to S3"
  exit 1
fi
