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
page=1
while :; do
  page_file=$(mktemp)
  response_file=$(mktemp)
  # Fetch one page at a time so cleanup can stop once all requested tags are found.
  GH_TOKEN="${GITHUB_TOKEN}" gh api -i \
    "orgs/${ORG}/packages/container/${PACKAGE}/versions?per_page=100&page=${page}" \
    > "${response_file}"
  awk 'body { print } /^\r?$/ { body=1 }' "${response_file}" > "${page_file}"
  # Keep the existing jq lookup below simple by accumulating the pages already fetched.
  jq -s 'add' "${versions_file}" "${page_file}" > "${versions_file}.next"
  mv "${versions_file}.next" "${versions_file}"
  rm -f "${page_file}"

  missing=0
  for tag in "$@"; do
    if ! jq -e --arg tag "${tag}" \
      'any(.[]; (.metadata.container.tags // []) | index($tag))' \
      "${versions_file}" >/dev/null; then
      missing=1
      break
    fi
  done
  if [[ "${missing}" -eq 0 ]]; then
    rm -f "${response_file}"
    break
  fi

  # The GitHub Packages API exposes the next page only through the Link header.
  if ! grep -qi 'rel="next"' "${response_file}"; then
    rm -f "${response_file}"
    break
  fi
  rm -f "${response_file}"
  page=$((page + 1))
done

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
