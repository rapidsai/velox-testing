#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Extract deploy libraries (bundle JAR + libs/) from a runtime Docker image
# to a local directory.
#
# Uses docker create + docker cp pattern (no running container needed).

set -euo pipefail

PROG_NAME="${GGBUILD_CMD:-$(basename "$0")}"
IMAGE=""
OUTPUT_DIR=""
DEPLOY_PATH="/opt/gluten-deploy"

usage() {
  cat <<EOF
Usage: ${PROG_NAME} [options]

Extract deploy libraries from a runtime Docker image to a local directory.

Required:
  --image=IMAGE       Runtime Docker image tag
  --output_dir=PATH   Local directory to extract artifacts to

Options:
  --deploy_path=PATH  Path inside the image (default: /opt/gluten-deploy)
  -h, --help          Show this help
EOF
}

for arg in "$@"; do
  case $arg in
    --image=*)        IMAGE="${arg#*=}" ;;
    --output_dir=*)   OUTPUT_DIR="${arg#*=}" ;;
    --deploy_path=*)  DEPLOY_PATH="${arg#*=}" ;;
    -h|--help)        usage; exit 0 ;;
    *)                echo "Unknown option: $arg" >&2; usage >&2; exit 1 ;;
  esac
done

if [ -z "$IMAGE" ]; then
  echo "ERROR: --image is required." >&2
  usage >&2
  exit 1
fi
if [ -z "$OUTPUT_DIR" ]; then
  echo "ERROR: --output_dir is required." >&2
  usage >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR=$(realpath "$OUTPUT_DIR")

echo "Extracting deploy artifacts from image: $IMAGE"
echo "  Image deploy path: $DEPLOY_PATH"
echo "  Local output dir:  $OUTPUT_DIR"
echo ""

# Create a temporary container (not started).
CONTAINER_ID=$(docker create "$IMAGE" /bin/true)
trap 'docker rm "$CONTAINER_ID" >/dev/null 2>&1 || true' EXIT

echo "Created temporary container: ${CONTAINER_ID:0:12}"

# Copy the deploy directory out.
docker cp "${CONTAINER_ID}:${DEPLOY_PATH}/." "${OUTPUT_DIR}/"

echo ""
echo "Extracted artifacts:"
# shellcheck disable=SC2012
ls -1 "$OUTPUT_DIR"/*.jar 2>/dev/null | while read -r f; do
  echo "  $(basename "$f") ($(du -h "$f" | cut -f1))"
done
if [ -d "$OUTPUT_DIR/libs" ]; then
  LIB_COUNT=$(ls -1 "$OUTPUT_DIR/libs/" 2>/dev/null | wc -l)
  echo "  libs/ ($LIB_COUNT files)"
fi
echo ""
echo "Done. Artifacts extracted to: $OUTPUT_DIR"
