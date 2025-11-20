#!/bin/bash

set -e

source ./config.sh
source ../../scripts/fetch_docker_image_from_s3.sh

IMAGE_NAME="ghcr.io/facebookincubator/velox-dev:adapters"

ARCH=$(uname -m)
BUCKET_SUBDIR="velox-docker-images"
IMAGE_FILE="velox_adapters_deps_image_centos9_${ARCH}.tar.gz"

#
# check for existing container image
#

validate_docker_image ${IMAGE_NAME}

echo "Velox dependencies/run-time container image not found"

#
# try to pull container image from our S3 bucket
#

fetch_docker_image_from_s3 ${IMAGE_NAME} ${BUCKET_SUBDIR} ${IMAGE_FILE}

if [[ $? -eq 0 ]]; then
  echo "Successfully fetched pre-built Velox dependencies/run-time container image"
  exit 0
else
  echo "Failed to fetch pre-built Velox dependencies/run-time container image"
  exit 1
fi
