#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$SCRIPT_DIR/../.local_installs/bin"

REPO_URL="https://github.com/TomAugspurger/tpchgen-rs.git"
REPO_COMMIT="9f0071fee879a90504a36e0188384b3dcf2e112e"
IMAGE_NAME="tpchgen-cli-builder"

TEMP_DIR=$(mktemp -d)
trap 'rm -rf $TEMP_DIR' EXIT

echo "Fetching tpchgen-rs ($REPO_COMMIT)..."
git -C "$TEMP_DIR" init -q tpchgen-rs
git -C "$TEMP_DIR/tpchgen-rs" remote add origin "$REPO_URL"
git -C "$TEMP_DIR/tpchgen-rs" fetch --depth 1 origin "$REPO_COMMIT"
git -C "$TEMP_DIR/tpchgen-rs" checkout --detach FETCH_HEAD
test "$(git -C "$TEMP_DIR/tpchgen-rs" rev-parse HEAD)" = "$REPO_COMMIT"

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
