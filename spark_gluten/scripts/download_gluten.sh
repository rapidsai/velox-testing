#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INSTALL_PATH="$SCRIPT_DIR/../testing/spark-gluten-install"

FILE_NAME="apache-gluten-1.5.0-incubating-bin-spark-3.5.tar.gz"
DOWNLOAD_URL="https://downloads.apache.org/incubator/gluten/1.5.0-incubating/$FILE_NAME"

echo "Downloading Gluten from $DOWNLOAD_URL"
wget $DOWNLOAD_URL
echo "Download complete"

mkdir -p "$INSTALL_PATH"
mv "$FILE_NAME" "$INSTALL_PATH"

pushd "$INSTALL_PATH"
tar -xzf $FILE_NAME
rm $FILE_NAME
popd

echo "Installed Gluten at $INSTALL_PATH/$FILE_NAME"
