#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

create_manifest_from_arch_tags() {
  local tag="$1"
  local final_tag="$2"

  echo "Creating multi-arch manifest: ${final_tag}"
  docker buildx imagetools create -t "${REGISTRY}/${IMAGE_NAME}:${final_tag}" \
    "${REGISTRY}/${IMAGE_NAME}:${tag}${INTERMEDIATE_TAG_SUFFIX}-amd64" \
    "${REGISTRY}/${IMAGE_NAME}:${tag}${INTERMEDIATE_TAG_SUFFIX}-arm64"
}

create_manifest() {
  local tag="$1"
  local run_id_suffix="${2:-}"

  create_manifest_from_arch_tags "${tag}" "${tag}${run_id_suffix}"
}
