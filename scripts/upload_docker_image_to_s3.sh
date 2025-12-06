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
# saves Docker image to a tar.gz file
# and uploads it to s3://rapidsai-velox-testing/<subdir>/<filename>
#
# VERSION HISTORY MANAGEMENT:
# - Maintains the current version with the original filename (no suffix)
# - Keeps up to 3 historical versions with datetime suffix (YYYYMMDD_HHMMSS)
# - On upload: renames current file with datetime -> uploads new as current
# - On failure: rolls back the rename operation
# - Auto-deletes versions older than the 3 most recent historical versions
#
# Example S3 structure after multiple uploads:
#   myimage.tar.gz                    <- current/latest version
#   myimage_20251206_143022.tar.gz    <- historical version 1
#   myimage_20251205_120000.tar.gz    <- historical version 2  
#   myimage_20251204_100000.tar.gz    <- historical version 3
#   (older versions are automatically deleted)
#

# Helper function to rollback S3 rename operation
rollback_s3_rename() {
  local OLD_PATH=$1
  local NEW_PATH=$2
  echo "Rolling back rename: ${NEW_PATH} -> ${OLD_PATH}"
  aws s3 mv --no-progress "${NEW_PATH}" "${OLD_PATH}" || echo "WARNING: Rollback failed for ${NEW_PATH}"
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

  # save the Docker image to a tar.gz file first (before any S3 operations)
  echo "Saving Docker image to file..."
  if ! docker save ${IMAGE_NAME} | gzip > /tmp/${IMAGE_FILE_NAME}; then
    echo "ERROR: Failed to save Docker image"
    exit 1
  fi

  # get file size for progress reporting
  local FILE_SIZE=$(du -h /tmp/${IMAGE_FILE_NAME} | cut -f1)
  echo "Image file size: ${FILE_SIZE}"

  # generate datetime suffix for versioning
  local DATETIME_SUFFIX=$(date -u +"%Y%m%d_%H%M%S")
  local RENAMED_FILE_PATH=""
  local ROLLBACK_NEEDED=false
  
  # check if current version exists on S3 and rename it with datetime suffix
  echo "Checking for existing image on S3..."
  if aws s3 ls "${IMAGE_FILE_PATH}" >/dev/null 2>&1; then
    echo "Found existing image, renaming with datetime suffix..."
    # extract base name and extension
    local BASE_NAME="${IMAGE_FILE_NAME%.tar.gz}"
    RENAMED_FILE_PATH="s3://${S3_BUCKET_NAME}/${BUCKET_SUBDIR}/${BASE_NAME}_${DATETIME_SUFFIX}.tar.gz"
    
    # rename the current file
    if ! aws s3 mv --no-progress "${IMAGE_FILE_PATH}" "${RENAMED_FILE_PATH}"; then
      echo "ERROR: Failed to rename existing file"
      rm -f /tmp/${IMAGE_FILE_NAME}
      exit 1
    fi
    echo "✓ Renamed existing file to: ${RENAMED_FILE_PATH}"
    ROLLBACK_NEEDED=true
  else
    echo "No existing image found, proceeding with upload"
  fi

  # upload to S3
  echo "Uploading image file to S3..."
  echo "Destination: ${IMAGE_FILE_PATH}"
  if ! aws s3 cp --no-progress /tmp/${IMAGE_FILE_NAME} ${IMAGE_FILE_PATH}; then
    echo "ERROR: Upload failed"
    # clean up local file
    rm -f /tmp/${IMAGE_FILE_NAME}
    # rollback the rename if we renamed a file
    if [[ "${ROLLBACK_NEEDED}" == "true" ]]; then
      rollback_s3_rename "${IMAGE_FILE_PATH}" "${RENAMED_FILE_PATH}"
    fi
    exit 1
  fi

  echo "✓ Upload successful"

  # clean up local temporary file
  echo "Cleaning up temporary file..."
  rm -f /tmp/${IMAGE_FILE_NAME}

  # now rotate old versions - keep only the 3 most recent
  echo "Rotating old versions (keeping only 3 most recent)..."
  local BASE_NAME="${IMAGE_FILE_NAME%.tar.gz}"
  local S3_PREFIX="s3://${S3_BUCKET_NAME}/${BUCKET_SUBDIR}/${BASE_NAME}"
  
  # list all versions (files with datetime suffix), sorted by date (newest first)
  local OLD_VERSIONS=$(aws s3 ls "s3://${S3_BUCKET_NAME}/${BUCKET_SUBDIR}/" | \
    grep "${BASE_NAME}_[0-9]\{8\}_[0-9]\{6\}\.tar\.gz" | \
    awk '{print $4}' | \
    sort -r)
  
  if [[ ! -z "${OLD_VERSIONS}" ]]; then
    local COUNT=0
    while IFS= read -r OLD_FILE; do
      COUNT=$((COUNT + 1))
      if [[ ${COUNT} -gt 3 ]]; then
        local OLD_FILE_PATH="s3://${S3_BUCKET_NAME}/${BUCKET_SUBDIR}/${OLD_FILE}"
        echo "Deleting old version: ${OLD_FILE}"
        aws s3 rm --no-progress "${OLD_FILE_PATH}"
      fi
    done <<< "${OLD_VERSIONS}"
    echo "✓ Rotation complete - kept ${COUNT} version(s)"
  else
    echo "No old versions to rotate"
  fi

  echo "✓ Successfully uploaded Docker image to S3 with version history"
}

# if executed directly, run with provided args
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  upload_docker_image_to_s3 "$@"
fi

