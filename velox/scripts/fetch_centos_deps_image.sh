#!/bin/bash

set -e

source ./config.sh
source ../../scripts/fetch_docker_image_from_s3.sh

#
# check for existing container image
#

validate_docker_image ${IMAGE_NAME}

echo "Velox dependencies/run-time container image not found"

#
# try to pull container image from our S3 bucket
#

fetch_docker_image_from_s3 ${DEPS_IMAGE_NAME} ${S3_BUCKET_SUBDIR} ${S3_IMAGE_FILE}

echo "Failed to fetch pre-built Velox dependencies/run-time container image"
