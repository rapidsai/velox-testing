#!/bin/bash

set -e

source ../../scripts/fetch_docker_image_from_s3.sh

#
# check for existing container image
#

DEPS_IMAGE="presto/prestissimo-dependency:centos9"

validate_docker_image ${DEPS_IMAGE}

echo "Presto dependencies/run-time container image not found, attempting to fetch pre-built image file..."

#
# try to pull container image from our S3 bucket
#

ARCH=$(uname -m)
BUCKET_SUBDIR="presto-docker-images"
IMAGE_FILE="presto_deps_container_image_centos9_${ARCH}.tar.gz"

fetch_docker_image_from_s3 ${DEPS_IMAGE} ${BUCKET_SUBDIR} ${IMAGE_FILE}

echo "Failed to fetch pre-built Presto dependencies/run-time container image, building locally..."

#
# build deps container image
#

echo "Building Presto dependencies/run-time image..."

# for this simpler version, report this but continue
echo "WARNING: Build patches will not be applied, local build will likely fail"

pushd ../../../presto/presto-native-execution
docker compose --progress plain build centos-native-dependency
popd

echo "Presto dependencies/run-time container image built!"
