#!/bin/bash

set -e

#
# check for existing container image
#

# the generic image name
DEPS_IMAGE="velox-adapters-deps:centos9"

# if that image already exists, we assume it's valid and we're done
echo "Checking for existing image..."
if [ ! -z $(docker images -q ${DEPS_IMAGE}) ]; then
	echo "Found existing Velox dependencies/run-time container image, using..."
	exit 0
fi

echo "Velox dependencies/run-time container image not found, attempting to fetch image file..."

#
# try to pull container image from our S3 bucket
#

# the image file name on S3
ARCH=$(uname -m)
BUCKET_URL="s3://rapidsai-velox-testing/velox-docker-images"
DEPS_IMAGE_FILE="velox_adapters_build_image_centos9_${ARCH}.tar.gz"
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
aws s3 cp --no-progress ${DEPS_IMAGE_PATH} /tmp/${DEPS_IMAGE_FILE}

# load the image into docker
echo "Loading image file..."
docker load < /tmp/${DEPS_IMAGE_FILE}

# clean up
rm -f /tmp/${DEPS_IMAGE_FILE}

# validate that the image was loaded correctly
echo "Validating image..."
if [[ ! -z $(docker images -q ${DEPS_IMAGE}) ]]; then
	echo "Pulled Velox dependencies/run-time container image from repo"
	exit 0
fi

echo "Failed to pull Velox dependencies/run-time container image from repo, building locally..."

# continue with local build; attempt to apply patches if present
echo "Proceeding with local build; will apply patches if present"

#
# build deps container image
#

echo "Building Velox dependencies/run-time image..."
CONTAINER_NAME="velox-adapters-deps"
COMPOSE_FILE="../docker/docker-compose.adapters.build.yml"

# apply patches to the velox if there's any
PATCHES_DIR="$(dirname "$0")/../patches"
VELOX_DIR="$(dirname "$0")/../../../velox"

if [ -d "$PATCHES_DIR" ] && [ -d "$VELOX_DIR" ]; then
    # check if any .patch or .diff files exist in the patches directory
    shopt -s nullglob
    patch_files=("$PATCHES_DIR"/*.patch "$PATCHES_DIR"/*.diff)
    shopt -u nullglob

    if [ ${#patch_files[@]} -gt 0 ]; then
        echo "Applying patches from $PATCHES_DIR to $VELOX_DIR ..."
        for patch_file in "${patch_files[@]}"; do
            patch_name=$(basename "$patch_file")
            echo "Applying $patch_name ..."
            if ! git -C "$VELOX_DIR" apply --index --whitespace=nowarn "$patch_file"; then
                echo "git apply failed for $patch_name, attempting with --reject ..."
                git -C "$VELOX_DIR" apply --reject --whitespace=fix "$patch_file"
            fi
        done
    fi
fi

docker compose --progress plain build ${CONTAINER_NAME} -f ${COMPOSE_FILE}

echo "Velox dependencies/run-time container image built!"
