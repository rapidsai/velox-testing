#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Pull a Docker image from GitHub Container Registry (ghcr.io) and save it
# as a .sqsh file using pyxis/enroot on a compute node.
#
# Requires ~/.config/enroot/.credentials to contain ghcr.io credentials.
#
# Usage:
#   ./pull_ghcr_image.sh <ghcr.io/org/image:tag> [--output <path/to/image.sqsh>] [--overwrite]
#
# Examples:
#   ./pull_ghcr_image.sh ghcr.io/myorg/presto-worker:latest
#   ./pull_ghcr_image.sh ghcr.io/myorg/presto-worker:v1.2.3 --output /tmp/worker.sqsh
#   ./pull_ghcr_image.sh ghcr.io/myorg/presto-worker:v1.2.3 --overwrite

set -e

source "$(dirname "${BASH_SOURCE[0]}")/defaults.env"

usage() {
    echo "Usage: $0 <ghcr.io/org/image:tag> [--output <path/to/image.sqsh>] [--overwrite]"
    echo ""
    echo "Options:"
    echo "  --output, -o   Write the image to this exact path (overrides IMAGE_DIR)."
    echo "  --overwrite    Re-pull even when the target .sqsh already exists."
    echo ""
    echo "Environment variables:"
    echo "  IMAGE_DIR   Output directory when --output is not specified (default: \$IMAGE_DIR from defaults.env)"
    exit 1
}

# Parse arguments
IMAGE_REF=""
OUTPUT_PATH=""
OVERWRITE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output|-o)
            [[ -n "${2:-}" ]] || { echo "Error: --output requires a value"; usage; }
            OUTPUT_PATH="$2"
            shift 2
            ;;
        --overwrite)
            OVERWRITE=1
            shift
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
    [[ -n "${IMAGE_DIR:-}" ]] || { echo "Error: IMAGE_DIR is not set (check defaults.env)"; exit 1; }
    # Extract image name and tag: ghcr.io/org/image:tag -> image-tag
    IMAGE_SLUG="${IMAGE_REF#ghcr.io/}"    # org/image:tag
    IMAGE_SLUG="${IMAGE_SLUG##*/}"        # image:tag
    IMAGE_SLUG="${IMAGE_SLUG//:/-}"       # image-tag
    OUTPUT_PATH="$IMAGE_DIR/${IMAGE_SLUG}.sqsh"
fi

echo "Image:      $IMAGE_REF"
echo "Output:     $OUTPUT_PATH"
echo "Overwrite:  $([[ $OVERWRITE -eq 1 ]] && echo yes || echo no)"
echo ""

# Run enroot import directly as the job so it inherits ENROOT_GZIP_PROGRAM.
# Pyxis (--container-image) runs enroot inside slurmstepd and ignores --export,
# so we bypass pyxis entirely here.
#
# The existence check, mkdir, and enroot import all run on the compute node
# because the default IMAGE_DIR (/scratch/$USER/images/presto) is not mounted
# on the head node; checking or creating it from here would be inconsistent
# with what the compute node sees.
ENROOT_DECOMPRESS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/enroot-decompress.sh"
export OUTPUT_PATH ENROOT_URI OVERWRITE

srun --export="ALL,PMIX_MCA_gds=^ds12,ENROOT_GZIP_PROGRAM=${ENROOT_DECOMPRESS}" \
    --nodes=1 --mem=0 --ntasks-per-node=1 \
    --mpi=pmix_v4 \
    bash -c '
set -e
if [[ -f "$OUTPUT_PATH" ]]; then
    size=$(ls -lh "$OUTPUT_PATH" | awk "{print \$5}")
    if [[ "$OVERWRITE" == "1" ]]; then
        echo "Image already exists: $OUTPUT_PATH ($size)"
        echo "--overwrite was passed; removing and re-pulling."
        rm -f "$OUTPUT_PATH"
    else
        echo "Image already exists: $OUTPUT_PATH ($size)"
        echo "Skipping pull.  Pass --overwrite to re-pull, or --output <path> to write elsewhere."
        exit 0
    fi
fi
mkdir -p "$(dirname "$OUTPUT_PATH")"
enroot import --output "$OUTPUT_PATH" "$ENROOT_URI"
echo ""
echo "Saved: $(ls -lh "$OUTPUT_PATH")"
'
