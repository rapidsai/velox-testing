#!/bin/bash

set -e

source ../../scripts/fetch_docker_image_from_s3.sh

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

echo "Failed to fetch pre-built Presto dependencies/run-time container image"
