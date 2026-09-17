#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_ROOT="$SCRIPT_DIR/../.local_installs"
INSTALL_DIR="$INSTALL_ROOT/bin"
METADATA_FILE="$INSTALL_ROOT/tpchgen-cli.json"

REPO_URL="https://github.com/qbacpey/tpchgen-rs.git"
REPO_BRANCH="tom/upstream-sync-pr1"
IMAGE_NAME="tpchgen-cli-builder"

usage() {
    cat <<'EOF'
Install tpchgen-cli from a tpchgen-rs git repository via Docker.

USAGE:
    install_tpchgen_cli.sh [--repo-url URL] [--repo-branch BRANCH]

OPTIONS:
    --repo-url URL       Git repository URL (default: qbacpey/tpchgen-rs fork)
    --repo-branch NAME   Branch to build (default: tom/upstream-sync-pr1)
    --help               Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-url)
            REPO_URL="$2"
            shift 2
            ;;
        --repo-branch)
            REPO_BRANCH="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

echo "Cloning tpchgen-rs ($REPO_BRANCH) from $REPO_URL..."
git clone --depth 1 --single-branch --branch "$REPO_BRANCH" "$REPO_URL" "$TEMP_DIR/tpchgen-rs"
COMMIT_SHA=$(git -C "$TEMP_DIR/tpchgen-rs" rev-parse HEAD)

echo "Building Docker image..."
docker build -t "$IMAGE_NAME" "$TEMP_DIR/tpchgen-rs"

echo "Extracting tpchgen-cli binary..."
CONTAINER_ID=$(docker create "$IMAGE_NAME")
mkdir -p "$INSTALL_DIR"
docker cp "$CONTAINER_ID:/usr/local/bin/tpchgen-cli" "$INSTALL_DIR/tpchgen-cli"
chmod +x "$INSTALL_DIR/tpchgen-cli"

echo "Cleaning up..."
docker rm "$CONTAINER_ID"
docker rmi "$IMAGE_NAME"

SUPPORTS_FORK_PARQUET_FLAGS=false
if "$INSTALL_DIR/tpchgen-cli" parquet --help 2>&1 | grep -q -- '--decimal-column-type'; then
    SUPPORTS_FORK_PARQUET_FLAGS=true
fi

mkdir -p "$INSTALL_ROOT"
cat >"$METADATA_FILE" <<EOF
{
  "repo_url": "$REPO_URL",
  "branch": "$REPO_BRANCH",
  "commit": "$COMMIT_SHA",
  "supports_fork_parquet_flags": $SUPPORTS_FORK_PARQUET_FLAGS
}
EOF

echo "Wrote install metadata to $METADATA_FILE"
echo "tpchgen-cli installed at $INSTALL_DIR/tpchgen-cli"
