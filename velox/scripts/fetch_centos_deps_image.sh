#!/bin/bash

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
# check for existing container image - skip download if already present
#
echo "Checking for existing Docker image ${IMAGE_NAME}..."
if [[ -n $(docker images -q ${IMAGE_NAME}) ]]; then
  echo "✓ Docker image already exists, skipping download"
  exit 0
fi

echo "Docker image not found locally, fetching from S3..."

#
# try to pull container image from our S3 bucket
#

fetch_docker_image_from_s3 ${IMAGE_NAME} ${BUCKET_SUBDIR} ${IMAGE_FILE}

if [[ $? -eq 0 ]]; then
  echo "✓ Successfully fetched Velox dependencies image"
  exit 0
else
  echo "ERROR: Failed to fetch Velox dependencies image"
  exit 1
fi
