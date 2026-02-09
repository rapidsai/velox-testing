#!/bin/bash

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/../../scripts/fetch_docker_image_from_s3.sh"

IMAGE_NAME_BASE="presto/prestissimo-dependency"
IMAGE_TAG="${USER:-latest}"
IMAGE_NAME="${IMAGE_NAME_BASE}:centos9-${IMAGE_TAG}"

print_help() {
  cat << EOF

Usage: fetch_centos_deps_image.sh [OPTIONS]

This script fetches a pre-built Presto dependencies/run-time container image from S3.

OPTIONS:
    -h, --help           Show this help message
    -t, --tag            Docker image tag to use (default: current username from \$USER)

EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -t|--tag)
        if [[ -n $2 ]]; then
          IMAGE_TAG=$2
          IMAGE_NAME="${IMAGE_NAME_BASE}:centos9-${IMAGE_TAG}"
          shift 2
        else
          echo "Error: --tag requires a value"
          exit 1
        fi
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

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
