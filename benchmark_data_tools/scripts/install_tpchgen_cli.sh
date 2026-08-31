#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/../.local_installs/bin"

REPO_URL="https://github.com/TomAugspurger/tpchgen-rs.git"
REPO_BRANCH="tom/sync-upstream-clean"
IMAGE_NAME="tpchgen-cli-builder"
REGISTRY_IMAGE="ghcr.io/rapidsai/velox-testing-images:tpchgen-cli"
PUSH=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --push)
            PUSH=true
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

TEMP_DIR=$(mktemp -d)
trap 'rm -rf $TEMP_DIR' EXIT

echo "Cloning tpchgen-rs ($REPO_BRANCH)..."
git clone --depth 1 --single-branch --branch "$REPO_BRANCH" "$REPO_URL" "$TEMP_DIR/tpchgen-rs"

echo "Building Docker image..."
docker build -t "$IMAGE_NAME" "$TEMP_DIR/tpchgen-rs"

echo "Extracting tpchgen-cli binary..."
CONTAINER_ID=$(docker create "$IMAGE_NAME")
mkdir -p "$INSTALL_DIR"
docker cp "$CONTAINER_ID:/usr/local/bin/tpchgen-cli" "$INSTALL_DIR/tpchgen-cli"
docker rm "$CONTAINER_ID"

if [[ "$PUSH" == true ]]; then
    echo "Tagging image as $REGISTRY_IMAGE..."
    docker tag "$IMAGE_NAME" "$REGISTRY_IMAGE"
    docker rmi "$IMAGE_NAME"
    echo "Pushing $REGISTRY_IMAGE..."
    docker push "$REGISTRY_IMAGE"
    echo "Cleaning up..."
    docker rmi "$REGISTRY_IMAGE"
else
    echo "Cleaning up..."
    docker rmi "$IMAGE_NAME"
fi

echo "tpchgen-cli installed at $INSTALL_DIR/tpchgen-cli"
