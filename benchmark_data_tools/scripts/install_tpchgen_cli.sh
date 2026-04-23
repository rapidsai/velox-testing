#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/../.local_installs/bin"

REPO_URL="https://github.com/TomAugspurger/tpchgen-rs.git"
REPO_BRANCH="tom/sync-upstream-clean"
IMAGE_NAME="tpchgen-cli-builder"

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

echo "Cleaning up..."
docker rm "$CONTAINER_ID"
docker rmi "$IMAGE_NAME"

echo "tpchgen-cli installed at $INSTALL_DIR/tpchgen-cli"
