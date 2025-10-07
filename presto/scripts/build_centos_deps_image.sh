#!/bin/bash

set -e

#
# check for existing container image
#

# the generic image name
DEPS_IMAGE="presto/prestissimo-dependency:centos9"

# if that image already exists, we assume it's valid and we're done
echo "Checking for existing image..."
if [ ! -z $(docker images -q ${DEPS_IMAGE}) ]; then
	echo "Found existing Presto dependencies/run-time container image, using..."
	exit 0
fi

echo "Presto dependencies/run-time container image not found, attempting to fetch image file..."

#
# try to pull container image from our S3 bucket
#

# the image file name on S3
ARCH=$(uname -m)
BUCKET_URL="s3://rapidsai-velox-testing-artifacts-bucket-that-does-not-exist-yet"
DEPS_IMAGE_FILE="presto_deps_container_image_centos9_${ARCH}.tar.gz"
DEPS_IMAGE_PATH="${BUCKET_URL}/${DEPS_IMAGE_FILE}"

# pull the repo image
echo "Fetching image file..."
aws s3 cp ${DEPS_IMAGE_PATH} /tmp/${DEPS_IMAGE_FILE}

# load the image into docker
echo "Loading image file..."
docker load < /tmp/${DEPS_IMAGE_FILE}

# re-tag the resulting image
# @TODO remove this after generating new images with the correct tag!
docker tag $(docker images --quiet) ${DEPS_IMAGE}

# clean up
rm -f /tmp/${DEPS_IMAGE_FILE}

# validate that the image was loaded correctly
echo "Validating image..."
if [[ ! -z $(docker images -q ${DEPS_IMAGE}) ]]; then
	echo "Pulled Presto dependencies/run-time container image from repo"
	exit 0
fi

echo "Failed to pull Presto dependencies/run-time container image from repo, building locally..."

# for this simpler version, report this but continue
echo "WARNING: Build patches will not be applied, local build will likely fail"

#
# build deps container image
#

echo "Building Presto dependencies/run-time image..."

pushd ../../../presto/presto-native-execution
docker compose --progress plain build centos-native-dependency
popd

echo "Presto dependencies/run-time container image built!"
