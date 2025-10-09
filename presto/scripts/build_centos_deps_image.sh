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
BUCKET_URL="s3://rapidsai-velox-testing/presto-docker-images"
DEPS_IMAGE_FILE="presto_deps_container_image_centos9_${ARCH}.tar.gz"
DEPS_IMAGE_PATH="${BUCKET_URL}/${DEPS_IMAGE_FILE}"

# ask for temporary credentials for file access
# expects AWS_ARN_STRING, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to be in the environment
TEMP_CREDS_JSON=$(aws sts assume-role \
	--role-arn ${AWS_ARN_STRING} \
	--role-session-name "GetPrestoContainerImage" \
	--query 'Credentials' \
	--output json)

# override environment with full temporary credentials
export AWS_ACCESS_KEY_ID=$(echo "$TEMP_CREDS_JSON" | jq -r '.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo "$TEMP_CREDS_JSON" | jq -r '.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo "$TEMP_CREDS_JSON" | jq -r '.SessionToken')

# pull the repo image
echo "Fetching image file..."
aws s3 cp ${DEPS_IMAGE_PATH} /tmp/${DEPS_IMAGE_FILE}

# load the image into docker
echo "Loading image file..."
docker load < /tmp/${DEPS_IMAGE_FILE}

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
