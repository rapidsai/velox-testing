#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Delete a container image tag from GHCR via the GitHub Packages REST API.
#
# The OCI distribution API DELETE endpoint returns 405 on GHCR; the Packages
# REST API is the supported way to remove tags/versions.
#
# Usage:
#   ./ci/delete-arch-tags.sh TAG [TAG ...]
#
# Example:
#   IMAGE_NAME=rapidsai/velox-testing-images GITHUB_TOKEN=ghp_... \
#     ./ci/delete-arch-tags.sh velox-deps-abc123-cuda12.9-20260319-amd64 velox-deps-abc123-cuda12.9-20260319-arm64
#
# Environment:
#   IMAGE_NAME   - Full image name without registry prefix (e.g. rapidsai/velox-testing-images)
#   GITHUB_TOKEN - GitHub token with write:packages scope
#   REGISTRY     - Unused; kept for compatibility with the action that sets it

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 TAG [TAG ...]" >&2
  exit 1
fi

if [[ -z "${IMAGE_NAME:-}" ]]; then
  echo "Error: IMAGE_NAME is required" >&2
  exit 1
fi
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "Error: GITHUB_TOKEN is required" >&2
  exit 1
fi

# Split IMAGE_NAME into org and package name (e.g. rapidsai / velox-testing-images)
ORG="${IMAGE_NAME%%/*}"
PACKAGE="${IMAGE_NAME#*/}"

versions_file=$(mktemp)
trap 'rm -f "${versions_file}"' EXIT

echo "Looking up ${#} tag(s) in ${ORG}/${PACKAGE}..."
GH_TOKEN="${GITHUB_TOKEN}" gh api --paginate \
  "orgs/${ORG}/packages/container/${PACKAGE}/versions?per_page=100" \
  > "${versions_file}"

for tag in "$@"; do
  VERSION_ID=$(jq -r --arg tag "${tag}" \
    'first(.[] | select((.metadata.container.tags // []) | index($tag)) | .id) // empty' \
    "${versions_file}")

  if [[ -z "${VERSION_ID}" ]]; then
    echo "  Tag '${tag}' not found. Skipping."
    continue
  fi

  echo "  Found version ID for '${tag}': ${VERSION_ID}. Deleting..."
  GH_TOKEN="${GITHUB_TOKEN}" gh api --method DELETE \
    "orgs/${ORG}/packages/container/${PACKAGE}/versions/${VERSION_ID}"
  echo "  -> Deleted tag '${tag}' (version ${VERSION_ID})"
done
