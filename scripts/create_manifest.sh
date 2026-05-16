#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

create_manifest_alias() {
  local tag="$1"
  local final_tag="$2"

  echo "Creating multi-arch manifest: ${final_tag}"
  docker buildx imagetools create -t "${REGISTRY}/${IMAGE_NAME}:${final_tag}" \
    "${REGISTRY}/${IMAGE_NAME}:${tag}-${BUILD_VARIANT}-${GITHUB_RUN_ID}-amd64" \
    "${REGISTRY}/${IMAGE_NAME}:${tag}-${BUILD_VARIANT}-${GITHUB_RUN_ID}-arm64"
}

resolve_run_id_suffix() {
  if [[ -z "${GITHUB_RUN_ID:-}" ]]; then
    echo "GITHUB_RUN_ID must be set" >&2
    return 1
  fi
  if [[ "${BUILD_VARIANT:-}" != "nightly" && -n "${BUILD_VARIANT:-}" ]]; then
    printf -- '-%s-%s' "${BUILD_VARIANT}" "${GITHUB_RUN_ID}"
  fi
}

create_manifest() {
  local tag="$1"

  create_manifest_alias "${tag}" "${tag}$(resolve_run_id_suffix)"
}
