#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Pull a Docker image from GitHub Container Registry (ghcr.io) and save it
# as a .sqsh file using pyxis/enroot on a compute node.
#
# Requires ~/.config/enroot/.credentials to contain ghcr.io credentials.
#
# Usage:
#   ./pull_ghcr_image.sh <ghcr.io/org/image:tag> [--output <path/to/image.sqsh>]
#
# Examples:
#   ./pull_ghcr_image.sh ghcr.io/myorg/presto-worker:latest
#   ./pull_ghcr_image.sh ghcr.io/myorg/presto-worker:v1.2.3 --output /scratch/prestouser/images/presto/worker.sqsh

set -e

usage() {
    echo "Usage: $0 <ghcr.io/org/image:tag> [--output <path/to/image.sqsh>]"
    echo ""
    echo "Environment variables:"
    echo "  IMAGES_DIR  Output directory when --output is not specified (default: \$HOME/Misiu/Images)"
    exit 1
}

# Parse arguments
IMAGE_REF=""
OUTPUT_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output|-o)
            [[ -n "${2:-}" ]] || { echo "Error: --output requires a value"; usage; }
            OUTPUT_PATH="$2"
            shift 2
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            ;;
        *)
            [[ -z "$IMAGE_REF" ]] || { echo "Error: unexpected argument '$1'"; usage; }
            IMAGE_REF="$1"
            shift
            ;;
    esac
done

[[ -n "$IMAGE_REF" ]] || { echo "Error: image reference is required"; usage; }

# Validate it looks like a ghcr.io reference
if [[ "$IMAGE_REF" != ghcr.io/* ]]; then
    echo "Error: image reference must start with ghcr.io/ (got: $IMAGE_REF)"
    exit 1
fi

# Convert ghcr.io/org/image:tag  ->  docker://ghcr.io#org/image:tag  (enroot import URI)
ENROOT_URI="docker://${IMAGE_REF/ghcr.io\//ghcr.io#}"

# Derive default output path from image name and tag
if [[ -z "$OUTPUT_PATH" ]]; then
    IMAGES_DIR="${IMAGES_DIR:-$HOME/Misiu/Images}"
    mkdir -p "$IMAGES_DIR"

    # Extract image name and tag: ghcr.io/org/image:tag -> image-tag
    IMAGE_SLUG="${IMAGE_REF#ghcr.io/}"    # org/image:tag
    IMAGE_SLUG="${IMAGE_SLUG##*/}"        # image:tag
    IMAGE_SLUG="${IMAGE_SLUG//:/-}"       # image-tag
    OUTPUT_PATH="$IMAGES_DIR/${IMAGE_SLUG}.sqsh"
fi

echo "Image:  $IMAGE_REF"
echo "Output: $OUTPUT_PATH"
echo ""

if [[ -f "$OUTPUT_PATH" ]]; then
    echo "Warning: output file already exists and will be overwritten: $OUTPUT_PATH"
fi

# Run enroot import directly as the job so it inherits ENROOT_GZIP_PROGRAM.
# Pyxis (--container-image) runs enroot inside slurmstepd and ignores --export,
# so we bypass pyxis entirely here.
ENROOT_DECOMPRESS="$(dirname "${BASH_SOURCE[0]}")/enroot-decompress.sh"

srun --export="ALL,PMIX_MCA_gds=^ds12,ENROOT_GZIP_PROGRAM=${ENROOT_DECOMPRESS}" \
    --nodes=1 --mem=0 --ntasks-per-node=1 \
    --mpi=pmix_v4 \
    enroot import --output "${OUTPUT_PATH}" "${ENROOT_URI}"

echo ""
echo "Saved: $(ls -lh "$OUTPUT_PATH")"
