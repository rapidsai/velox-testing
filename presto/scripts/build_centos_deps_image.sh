#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

IMAGE_NAME="presto/prestissimo-dependency:centos9-${USER:-latest}"
NO_CACHE_ARG=''

print_help() {
  cat << EOF

Usage: build_centos_deps_image.sh [OPTIONS]

This script does a local build of a Presto dependencies/run-time container to a Docker image.
It expects sibling Presto and Velox clones, and will override the Presto Velox dependencies
scripts and CMake config to be those of the sibling Velox.

WARNING: If an image of the given name already exists, it will be removed prior to the build.

OPTIONS:
    -h, --help           Show this help message
    -i, --image-name     Desired Docker Image name (default: presto/prestissimo-dependency:centos9-\${USER:-latest})
    -n, --no-cache       Do not use Docker build cache (default: use cache)

EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -i|--image-name)
        if [[ -n $2 ]]; then
          IMAGE_NAME=$2
          shift 2
        else
          echo "Error: --image-name requires a value"
          exit 1
        fi
        ;;
      -n|--no-cache)
        NO_CACHE_ARG="--no-cache"
        shift
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

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the root of the git repository
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"

# verify sibling Presto and Velox clones
if [[ ! -d "${REPO_ROOT}/../presto/presto-native-execution" || ! -d "${REPO_ROOT}/../velox" ]]; then
  echo "Error: Sibling Presto and/or Velox clone not found"
  exit 1
fi

# remove any existing image?
if [[ ! -z $(docker images -q ${IMAGE_NAME}) ]]; then
	echo "Removing existing Presto dependencies/run-time image..."
	docker rmi -f ${IMAGE_NAME}
fi

# restore original Presto Velox on exit
# on a clean exit, this happens before the automatic popd
function cleanup {
  echo "Restoring original Presto Velox..."
  rm -rf velox
  mv velox.bak velox
}
trap cleanup EXIT

# move to Presto Velox
pushd "${REPO_ROOT}/../presto/presto-native-execution" > /dev/null

# override Presto Velox build config
echo "Overriding Presto Velox build config from sibling Velox clone..."
mv velox velox.bak
mkdir -p velox
cp -r ../../velox/scripts velox
cp -r ../../velox/CMake velox

# Capture build provenance from sibling repos for embedding in Docker labels.
PRESTO_SHA=$(git -C "${REPO_ROOT}/../presto" rev-parse HEAD 2>/dev/null || echo "")
PRESTO_BRANCH=$(git -C "${REPO_ROOT}/../presto" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
PRESTO_REPO=$(git -C "${REPO_ROOT}/../presto" remote get-url origin 2>/dev/null || echo "")
VELOX_SHA=$(git -C "${REPO_ROOT}/../velox" rev-parse HEAD 2>/dev/null || echo "")
VELOX_BRANCH=$(git -C "${REPO_ROOT}/../velox" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
VELOX_REPO=$(git -C "${REPO_ROOT}/../velox" remote get-url origin 2>/dev/null || echo "")

# now build
echo "Building..."
docker compose --progress plain build ${NO_CACHE_ARG} centos-native-dependency

# tag with the user-specific name to avoid conflicts between multiple users on the same host
COMPOSE_IMAGE_NAME='presto/prestissimo-dependency:centos9'

# Apply provenance labels; docker compose build --label is not universally supported.
echo "Applying provenance labels..."
echo "FROM ${COMPOSE_IMAGE_NAME}" | docker build --no-cache \
  --label "velox-testing.presto.sha=${PRESTO_SHA}" \
  --label "velox-testing.presto.branch=${PRESTO_BRANCH}" \
  --label "velox-testing.presto.repository=${PRESTO_REPO}" \
  --label "velox-testing.velox.sha=${VELOX_SHA}" \
  --label "velox-testing.velox.branch=${VELOX_BRANCH}" \
  --label "velox-testing.velox.repository=${VELOX_REPO}" \
  -t "${COMPOSE_IMAGE_NAME}" -

if [[ "${IMAGE_NAME}" != "${COMPOSE_IMAGE_NAME}" ]]; then
  echo "Tagging image as ${IMAGE_NAME}..."
  docker tag ${COMPOSE_IMAGE_NAME} ${IMAGE_NAME}
fi

# done (will cleanup on exit)
echo "Presto dependencies/run-time container image built!"
