#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Delete a container image tag from GHCR via the OCI distribution API.
#
# This resolves the tag to its digest, then deletes the manifest by digest.
# This is the same mechanism used by `crane delete`.
#
# Usage:
#   ./scripts/ghcr-delete-tags.sh TAG
#
# Example:
#   IMAGE_NAME=rapidsai/velox-testing-images GITHUB_TOKEN=ghp_... \
#     ./scripts/ghcr-delete-tags.sh velox-deps-abc123-cuda12.9-20260319-amd64
#
# Environment:
#   IMAGE_NAME   - Full image name without registry prefix (required)
#   GITHUB_TOKEN - GitHub token with delete:packages scope (required)
#   REGISTRY     - Container registry hostname (default: ghcr.io)

REGISTRY="${REGISTRY:-ghcr.io}"

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 TAG" >&2
  exit 1
fi
TAG="$1"

if [[ -z "${IMAGE_NAME:-}" ]]; then
  echo "Error: IMAGE_NAME is required" >&2
  exit 1
fi
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "Error: GITHUB_TOKEN is required" >&2
  exit 1
fi

# Exchange GITHUB_TOKEN for a GHCR bearer token with delete scope
GHCR_TOKEN=$(curl -sf -u "USERNAME:${GITHUB_TOKEN}" \
  "https://${REGISTRY}/token?service=${REGISTRY}&scope=repository:${IMAGE_NAME}:delete" \
  | jq -r '.token')

if [[ -z "${GHCR_TOKEN}" || "${GHCR_TOKEN}" == "null" ]]; then
  echo "Error: Failed to obtain GHCR bearer token" >&2
  exit 1
fi

BASE_URL="https://${REGISTRY}/v2/${IMAGE_NAME}"

# Step 1: Resolve tag to digest
echo "Resolving tag '${TAG}' to digest..."
digest=$(curl -s -I \
  -H "Authorization: Bearer ${GHCR_TOKEN}" \
  -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json" \
  "${BASE_URL}/manifests/${TAG}" \
  | grep -i '^docker-content-digest:' \
  | tr -d '\r' \
  | awk '{print $2}')

if [[ -z "${digest}" ]]; then
  echo "  Tag '${TAG}' not found or could not resolve digest."
  exit 0
fi
echo "  Resolved to: ${digest}"

# Step 2: Delete manifest by digest
echo "Deleting manifest ${digest}..."
http_code=$(curl -s -o /dev/null -w '%{http_code}' \
  -X DELETE \
  -H "Authorization: Bearer ${GHCR_TOKEN}" \
  "${BASE_URL}/manifests/${digest}")

if [[ "${http_code}" == "202" ]]; then
  echo "  -> Deleted tag '${TAG}' (${digest})"
elif [[ "${http_code}" == "404" || "${http_code}" == "405" ]]; then
  echo "  -> Not found or not allowed (HTTP ${http_code}). Tag may already be gone."
else
  echo "  -> Failed to delete. HTTP status: ${http_code}" >&2
  exit 1
fi
