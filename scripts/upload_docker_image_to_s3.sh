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

set -e

#
# upload_docker_image_to_s3 <imagename> <subdir> <filename>
#
# saves Docker image to a tar.gz file
# and uploads it to s3://rapidsai-velox-testing/<subdir>/<filename>
# 

validate_docker_image() {
  local IMAGE_NAME=$1
  echo "Validating Docker image ${IMAGE_NAME}..."
  if [[ -z $(docker images -q ${IMAGE_NAME}) ]]; then
    echo "ERROR: Docker image ${IMAGE_NAME} does not exist"
    exit 1
  fi
  echo "✓ Docker image exists"
}

upload_docker_image_to_s3() {
  # validate parameter count
  if [[ "$#" -ne 3 ]]; then
    echo "Usage: upload_docker_image_to_s3 <imagename> <subdir> <filename>" >&2
    exit 2
  fi

  # expected parameters
  local IMAGE_NAME=$1
  local BUCKET_SUBDIR=$2
  local IMAGE_FILE_NAME=$3

  # these env vars are required regardless of what creds are used
  echo "Validating incoming environment..."
  if [ -z "${AWS_ACCESS_KEY_ID}" ] || [ -z "${AWS_SECRET_ACCESS_KEY}" ] || [ -z "${S3_BUCKET_NAME}" ] || [ -z "${S3_BUCKET_REGION}" ]; then
    echo "ERROR: The following values must be set in the environment:"
    echo "  AWS_ARN_STRING (optional)"
    echo "  AWS_ACCESS_KEY_ID"
    echo "  AWS_SECRET_ACCESS_KEY"
    echo "  S3_BUCKET_NAME"
    echo "  S3_BUCKET_REGION"
    echo "Keys must either be valid for direct access to the bucket, or valid for an assume-role operation if AWS_ARN_STRING is set"
    exit 1
  fi

  # validate image exists before proceeding
  validate_docker_image ${IMAGE_NAME}

  # construct full S3 path
  local IMAGE_FILE_PATH="s3://${S3_BUCKET_NAME}/${BUCKET_SUBDIR}/${IMAGE_FILE_NAME}"

  # ensure region is set
  export AWS_REGION=${S3_BUCKET_REGION}

  # if AWS_ARN_STRING is set in the environment, use environment creds to request new
  # temporary rolling creds for the private bucket, otherwise use environment creds directly
  if [ ! -z "${AWS_ARN_STRING}" ]; then
    # ask for temporary credentials for file access
    echo "Requesting temporary S3 credentials..."
    local TEMP_CREDS_JSON=$(aws sts assume-role \
      --role-arn ${AWS_ARN_STRING} \
      --role-session-name "UploadVeloxContainerImage" \
      --query "Credentials" \
      --output json)

    # override environment with full temporary credentials
    export AWS_ACCESS_KEY_ID=$(echo "$TEMP_CREDS_JSON" | jq -r '.AccessKeyId')
    export AWS_SECRET_ACCESS_KEY=$(echo "$TEMP_CREDS_JSON" | jq -r '.SecretAccessKey')
    export AWS_SESSION_TOKEN=$(echo "$TEMP_CREDS_JSON" | jq -r '.SessionToken')
  fi

  # save the Docker image to a tar.gz file
  echo "Saving Docker image to file..."
  docker save ${IMAGE_NAME} | gzip > /tmp/${IMAGE_FILE_NAME}

  # get file size for progress reporting
  local FILE_SIZE=$(du -h /tmp/${IMAGE_FILE_NAME} | cut -f1)
  echo "Image file size: ${FILE_SIZE}"

  # upload to S3
  echo "Uploading image file to S3..."
  echo "Destination: ${IMAGE_FILE_PATH}"
  aws s3 cp --no-progress /tmp/${IMAGE_FILE_NAME} ${IMAGE_FILE_PATH}

  # clean up
  echo "Cleaning up temporary file..."
  rm -f /tmp/${IMAGE_FILE_NAME}

  echo "✓ Successfully uploaded Docker image to S3"
}

# if executed directly, run with provided args
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  upload_docker_image_to_s3 "$@"
fi

