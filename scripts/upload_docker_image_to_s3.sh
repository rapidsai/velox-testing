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

# Source common utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

#
# upload_docker_image_to_s3 <imagename> <subdir> <filename>
#
# Saves Docker image to a tar.gz file and uploads it to S3.
# Overwrites existing image at the same path (no versioning).
#
# Example:
#   upload_docker_image_to_s3 "ghcr.io/facebookincubator/velox-dev:adapters" \
#     "velox-docker-images" "velox_adapters_deps_image_centos9_x86_64.tar.gz"
#
# Result:
#   s3://${S3_BUCKET_NAME}/velox-docker-images/velox_adapters_deps_image_centos9_x86_64.tar.gz
#

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

  # validate required environment variables
  echo "Validating environment..."
  if [ -z "${AWS_ACCESS_KEY_ID}" ] || [ -z "${AWS_SECRET_ACCESS_KEY}" ] || [ -z "${S3_BUCKET_NAME}" ] || [ -z "${S3_BUCKET_REGION}" ]; then
    echo "ERROR: The following values must be set in the environment:"
    echo "  AWS_ARN_STRING (optional - for assume-role)"
    echo "  AWS_ACCESS_KEY_ID"
    echo "  AWS_SECRET_ACCESS_KEY"
    echo "  S3_BUCKET_NAME"
    echo "  S3_BUCKET_REGION"
    exit 1
  fi

  # validate image exists before proceeding
  validate_docker_image ${IMAGE_NAME}

  # construct full S3 path
  local IMAGE_FILE_PATH="s3://${S3_BUCKET_NAME}/${BUCKET_SUBDIR}/${IMAGE_FILE_NAME}"

  # ensure region is set
  export AWS_REGION=${S3_BUCKET_REGION}

  # if AWS_ARN_STRING is set, assume role for temporary credentials
  if [ ! -z "${AWS_ARN_STRING}" ]; then
    echo "Requesting temporary S3 credentials via assume-role..."
    local TEMP_CREDS_JSON=$(aws sts assume-role \
      --role-arn ${AWS_ARN_STRING} \
      --role-session-name "UploadVeloxContainerImage" \
      --query "Credentials" \
      --output json)

    export AWS_ACCESS_KEY_ID=$(echo "$TEMP_CREDS_JSON" | jq -r '.AccessKeyId')
    export AWS_SECRET_ACCESS_KEY=$(echo "$TEMP_CREDS_JSON" | jq -r '.SecretAccessKey')
    export AWS_SESSION_TOKEN=$(echo "$TEMP_CREDS_JSON" | jq -r '.SessionToken')
    echo "✓ Obtained temporary credentials"
  fi

  # save Docker image to tar.gz
  echo "Saving Docker image to file..."
  if ! docker save ${IMAGE_NAME} | gzip > /tmp/${IMAGE_FILE_NAME}; then
    echo "ERROR: Failed to save Docker image"
    exit 1
  fi

  # report file size
  local FILE_SIZE=$(du -h /tmp/${IMAGE_FILE_NAME} | cut -f1)
  echo "✓ Image saved (${FILE_SIZE})"

  # upload to S3 (overwrites existing)
  echo "Uploading to S3..."
  echo "  Destination: ${IMAGE_FILE_PATH}"
  if ! aws s3 cp --no-progress /tmp/${IMAGE_FILE_NAME} ${IMAGE_FILE_PATH}; then
    echo "ERROR: Upload failed"
    rm -f /tmp/${IMAGE_FILE_NAME}
    exit 1
  fi

  echo "✓ Upload successful"

  # clean up local file
  rm -f /tmp/${IMAGE_FILE_NAME}
  echo "✓ Cleanup complete"

  echo ""
  echo "Image available at: ${IMAGE_FILE_PATH}"
}

# if executed directly, run with provided args
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  upload_docker_image_to_s3 "$@"
fi
