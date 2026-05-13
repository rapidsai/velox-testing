#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

create_manifest_from_arch_tags() {
  local tag="$1"
  local final_tag="$2"

  echo "Creating multi-arch manifest: ${final_tag}"
  docker buildx imagetools create -t "${REGISTRY}/${IMAGE_NAME}:${final_tag}" \
    "${REGISTRY}/${IMAGE_NAME}:${tag}-${JOB_VARIANT_IDENTIFIER}-amd64" \
    "${REGISTRY}/${IMAGE_NAME}:${tag}-${JOB_VARIANT_IDENTIFIER}-arm64"
}

resolve_run_id_suffix() {
  if [[ "${BUILD_TYPE}" != "nightly" ]]; then
    printf -- '-%s' "${JOB_VARIANT_IDENTIFIER%%-*}"
  fi
}

create_manifest() {
  local tag="$1"

  create_manifest_from_arch_tags "${tag}" "${tag}$(resolve_run_id_suffix)"
}
