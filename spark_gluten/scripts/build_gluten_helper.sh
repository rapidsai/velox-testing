#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

BUILD_HELPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$BUILD_HELPER_DIR/../.."

# Validate that the required sibling repositories exist.
"${REPO_ROOT}/scripts/validate_directories_exist.sh" \
  "${REPO_ROOT}/../incubator-gluten" \
  "${REPO_ROOT}/../velox"

WORKSPACE_ROOT="${REPO_ROOT}/.."
DOCKERFILE="${REPO_ROOT}/spark_gluten/docker/gluten_build.dockerfile"

# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/cuda_helper.sh"

run_gluten_build() {
  local image="$1"
  local build_type="$2"
  local base_image="$3"
  local num_threads="$4"
  local device_type="$5"
  local no_cache="$6"
  local cuda_arch="$7"

  local build_args=(
    -t "$image"
    -f "$DOCKERFILE"
    --progress=plain
    --build-arg BUILD_TYPE="$build_type"
    --build-arg DEVICE_TYPE="$device_type"
    --build-arg BASE_IMAGE="$base_image"
    --build-arg NO_CACHE="$no_cache"
    --build-arg NUM_THREADS="$num_threads"
  )

  if [[ "${no_cache}" == "true" ]]; then
    build_args+=(--no-cache)
  fi

  if [[ -n "${cuda_arch}" ]]; then
    build_args+=(--build-arg "CUDA_ARCHITECTURES=${cuda_arch}")
  fi

  docker build "${build_args[@]}" "$WORKSPACE_ROOT"
}
