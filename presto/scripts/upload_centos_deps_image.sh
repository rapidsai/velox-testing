#!/bin/bash

set -e

#
# Upload Presto dependencies image to S3
#
# Usage:
#   ./upload_centos_deps_image.sh [upstream|pinned]
#
# The variant determines the S3 filename:
#   upstream -> presto_deps_upstream_centos9_<arch>.tar.gz
#   pinned   -> presto_deps_pinned_centos9_<arch>.tar.gz
#
# Default: upstream
#

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/../../scripts/upload_docker_image_to_s3.sh"

# Parse variant argument (default: upstream)
VARIANT="${1:-upstream}"

if [[ "${VARIANT}" != "upstream" && "${VARIANT}" != "pinned" ]]; then
  echo "ERROR: Invalid variant '${VARIANT}'. Must be 'upstream' or 'pinned'."
  exit 1
fi

IMAGE_NAME="presto/prestissimo-dependency:centos9"

ARCH=$(uname -m)
BUCKET_SUBDIR="presto-docker-images"
IMAGE_FILE="presto_deps_${VARIANT}_centos9_${ARCH}.tar.gz"

echo "Uploading Presto deps image (variant: ${VARIANT})"

#
# validate that the container image exists
#

validate_docker_image ${IMAGE_NAME}

#
# upload container image to S3 bucket
#

upload_docker_image_to_s3 ${IMAGE_NAME} ${BUCKET_SUBDIR} ${IMAGE_FILE}

if [[ $? -eq 0 ]]; then
  echo "Successfully uploaded Presto dependencies/run-time container image to S3"
  echo "  Variant: ${VARIANT}"
  echo "  File: ${IMAGE_FILE}"
  exit 0
else
  echo "Failed to upload Presto dependencies/run-time container image to S3"
  exit 1
fi
