#!/usr/bin/env bash

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

set -euo pipefail

#
# fetch_docker_image_from_s3 <imagename> <subdir> <filename>
#
# fetches s3://rapidsai-velox-testing/<subdir>/<filename>
# and attempts to load it as a Docker image
# 

validate_docker_image() {
  local IMAGE_NAME=$1
  echo "Validating Docker image ${IMAGE_NAME}..."
  if [[ ! -z $(docker images -q ${IMAGE_NAME}) ]]; then
    echo "Docker image exists"
    exit 0
  fi
}

fetch_docker_image_from_s3() {
  # validate parameter count
  if [[ "$#" -ne 3 ]]; then
    echo "Usage: fetch_docker_image_from_s3 <imagename> <subdir> <filename>" >&2
    exit 2
  fi

  # expected parameters
  local IMAGE_NAME=$1
  local BUCKET_SUBDIR=$2
  local IMAGE_FILE_NAME=$3

  # the bucket
  local BUCKET_URL="s3://rapidsai-velox-testing"
  local BUCKET_REGION="us-west-1"

  # construct full S3 path
  local IMAGE_FILE_PATH="${BUCKET_URL}/${BUCKET_SUBDIR}/${IMAGE_FILE_NAME}"

  # validate the provided environment
  echo "Validating incoming environment for temporary S3 credentials request..."
  if [ ! -v AWS_ARN_STRING ] || [ ! -v AWS_ACCESS_KEY_ID ] || [ ! -v AWS_SECRET_ACCESS_KEY ]; then
    echo "ERROR: AWS_ARN_STRING, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment"
    exit 1
  fi

  # ensure region is set
  export AWS_REGION=${BUCKET_REGION}

  # ask for temporary credentials for file access
  # expects AWS_ARN_STRING, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to be in the environment
  echo "Requesting temporary S3 credentials..."
  local TEMP_CREDS_JSON=$(aws sts assume-role \
    --role-arn ${AWS_ARN_STRING} \
    --role-session-name "GetPrestoContainerImage" \
    --query "Credentials" \
    --output json)

  # override environment with full temporary credentials
  export AWS_ACCESS_KEY_ID=$(echo "$TEMP_CREDS_JSON" | jq -r '.AccessKeyId')
  export AWS_SECRET_ACCESS_KEY=$(echo "$TEMP_CREDS_JSON" | jq -r '.SecretAccessKey')
  export AWS_SESSION_TOKEN=$(echo "$TEMP_CREDS_JSON" | jq -r '.SessionToken')

  # pull the repo image
  echo "Fetching image file from S3..."
  aws s3 cp --no-progress ${IMAGE_FILE_PATH} /tmp/${IMAGE_FILE_NAME}

  # load the image into docker
  echo "Loading image file into Docker..."
  docker load < /tmp/${IMAGE_FILE_NAME}

  # clean up
  rm -f /tmp/${IMAGE_FILE_NAME}

  # validate image
  validate_docker_image ${IMAGE_NAME}
}

# if executed directly, run with provided args
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  fetch_docker_image_from_s3 "$@"
fi
