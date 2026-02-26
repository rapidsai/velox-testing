#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Shared helpers for Gluten build scripts.
#
# Source this file to get:
#   - REPO_ROOT, WORKSPACE_ROOT, DOCKERFILE variables
#   - Sibling-repository validation (incubator-gluten, velox)
#   - run_gluten_build function

set -e

BUILD_HELPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$BUILD_HELPER_DIR/../../.."

# Validate that the required sibling repositories exist.
"${REPO_ROOT}/scripts/validate_directories_exist.sh" \
  "${REPO_ROOT}/../incubator-gluten" \
  "${REPO_ROOT}/../velox"

WORKSPACE_ROOT="${REPO_ROOT}/.."
DOCKERFILE="${REPO_ROOT}/spark_gluten/docker/gluten_build.dockerfile"

# ---------------------------------------------------------------------------
# run_gluten_build <image_tag> <build_type> <base_image> <gcc_toolset>
#
# Runs docker build with the unified gluten_build.dockerfile.
# ---------------------------------------------------------------------------
run_gluten_build() {
  local image_tag="$1"
  local build_type="$2"
  local base_image="$3"
  local gcc_toolset="$4"

  docker build \
    -t "$image_tag" \
    -f "$DOCKERFILE" \
    --build-arg BUILD_TYPE="$build_type" \
    --build-arg BASE_IMAGE="$base_image" \
    --build-arg GCC_TOOLSET="$gcc_toolset" \
    "$WORKSPACE_ROOT"
}
