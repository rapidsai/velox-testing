#!/bin/bash

set -e

#
# Fetch Presto dependencies image from S3
#
# Usage:
#   ./fetch_centos_deps_image.sh [upstream|pinned]
#
# The variant determines which S3 file to fetch:
#   upstream -> presto_deps_upstream_centos9_<arch>.tar.gz
#   pinned   -> presto_deps_pinned_centos9_<arch>.tar.gz
#
# Default: upstream
#

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/../../scripts/fetch_docker_image_from_s3.sh"

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

#
# check for existing container image - skip download if already present
#
echo "Checking for existing Docker image ${IMAGE_NAME}..."
if [[ -n $(docker images -q ${IMAGE_NAME}) ]]; then
  echo "✓ Presto dependencies/run-time (Variant: ${VARIANT}) Docker image already exists, skipping download"
  exit 0
fi

echo "Presto dependencies/run-time (Variant: ${VARIANT}) container image not found, fetching from S3..."

#
# fetch container image from S3 bucket
#
fetch_docker_image_from_s3 ${IMAGE_NAME} ${BUCKET_SUBDIR} ${IMAGE_FILE}

if [[ $? -eq 0 ]]; then
  echo "✓ Successfully fetched Presto dependencies image"
  exit 0
else
  echo "ERROR: Failed to fetch Presto dependencies image"
  exit 1
fi
