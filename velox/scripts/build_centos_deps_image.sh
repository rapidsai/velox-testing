#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# check for existing container image
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

# ----- helper functions -----

image_exists() {
	# returns 0 if image exists locally
	[ -n "$(docker images -q ${DEPS_IMAGE})" ]
}

ensure_aws_cli() {
	if ! command -v aws >/dev/null 2>&1; then
		echo "aws CLI not found; skipping S3 fetch and building locally..."
		return 1
	fi
	return 0
}

assume_role() {
	if [ -z "${AWS_ARN_STRING:-}" ]; then
		echo "AWS_ARN_STRING is not set."
		return 1
	fi
	local creds
	if ! creds=$(aws sts assume-role \
		--role-arn ${AWS_ARN_STRING} \
		--role-session-name "GetPrestoContainerImage" \
		--query 'Credentials' \
		--output json); then
		echo "aws sts assume-role failed."
		return 1
	fi
	AWS_ACCESS_KEY_ID=$(echo "$creds" | jq -r '.AccessKeyId') || return 1
	AWS_SECRET_ACCESS_KEY=$(echo "$creds" | jq -r '.SecretAccessKey') || return 1
	AWS_SESSION_TOKEN=$(echo "$creds" | jq -r '.SessionToken') || return 1
	if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ] || [ -z "$AWS_SESSION_TOKEN" ]; then
		echo "Failed to parse STS credentials."
		return 1
	fi
	export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
	return 0
}

s3_copy_image() {
	echo "Fetching image file from S3..."
	aws s3 cp --no-progress ${DEPS_IMAGE_PATH} /tmp/${DEPS_IMAGE_FILE}
}

docker_load_image() {
	echo "Loading image file into Docker..."
	docker load < /tmp/${DEPS_IMAGE_FILE} || return 1
	rm -f /tmp/${DEPS_IMAGE_FILE} || true
	return 0
}

validate_loaded_image() {
	echo "Validating image..."
	image_exists
}

fetch_image_from_s3() {
	ensure_aws_cli || return 1
	assume_role || return 1
	s3_copy_image || return 1
	docker_load_image || return 1
	validate_loaded_image || return 1
	return 0
}

apply_patches_if_any() {
	# apply patches to the velox repo if present
	local patches_dir
	local velox_dir
	patches_dir="$(realpath "$(dirname "$0")/../patches")"
	velox_dir="$(realpath "$(dirname "$0")/../../../velox")"

	echo "PATCHES_DIR: $patches_dir"
	echo "VELOX_DIR: $velox_dir"

	if [ -d "$patches_dir" ] && [ -d "$velox_dir" ]; then
		shopt -s nullglob
		local patch_files=("$patches_dir"/*.patch "$patches_dir"/*.diff)
		if [ ${#patch_files[@]} -gt 0 ]; then
			echo "Applying patches from $patches_dir to $velox_dir ..."
			for patch_file in "${patch_files[@]}"; do
				local patch_name
				patch_name=$(basename "$patch_file")
				echo "Applying $patch_name ..."
				if ! git -C "$velox_dir" apply --whitespace=nowarn "$patch_file"; then
					echo "git apply failed for $patch_name; skipping without writing rejects."
					return 1
				fi
			done
		fi
	fi
}

build_image_locally() {
	echo "Proceeding with local build; will apply patches if present"
	local compose_file
	local container_name
	container_name="velox-adapters-deps"
	compose_file="../docker/docker-compose.adapters.build.yml"
	apply_patches_if_any
	docker compose -f "${compose_file}" --progress plain build "${container_name}"
	echo "Velox dependencies/run-time container image built!"
}

echo "Attempting to fetch dependency image from S3 and load into Docker..."
if fetch_image_from_s3; then
	echo "Pulled Velox dependencies/run-time container image from repo"
	exit 0
fi

echo "Failed to pull Velox dependencies/run-time container image from repo, building locally..."

build_image_locally
