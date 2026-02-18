#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

print_help() {
  cat << EOF

Usage: $(basename "$0") [OPTIONS]

Build a standalone Docker image that contains only the
velox_cudf_hashagg_replay binary, using the same dependency image as native
GPU builds.

OPTIONS:
    -h, --help               Show this help message.
    -n, --no-cache           Do not use Docker build cache.
    -j, --num-threads N      Number of build threads (default: nproc / 2).
    --build-type TYPE        Build type: release|relwithdebinfo|debug (default: release).
    --cuda-archs LIST        CUDA architectures (default: auto-detect).
    --deps-image IMAGE       Dependency image (default: presto/prestissimo-dependency:centos9).
    --tag TAG                Docker image tag (default: velox-hashagg-replay-\$USER).
    --extra-cmake-flags STR  Extra CMake flags appended to defaults.

EXAMPLES:
    $0
    $0 --no-cache --build-type relwithdebinfo
    $0 --cuda-archs "90;100" --tag velox-hashagg-replay:latest

EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

NUM_THREADS=$(($(nproc) / 2))
if (( NUM_THREADS < 1 )); then
  NUM_THREADS=1
fi
BUILD_TYPE=release
CUDA_ARCHS=""
DEPS_IMAGE="${PRESTO_DEPS_IMAGE:-presto/prestissimo-dependency:centos9}"
IMAGE_TAG="velox-hashagg-replay-${USER:-latest}"
SKIP_CACHE_ARG=""
EXTRA_CMAKE_FLAGS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_help
      exit 0
      ;;
    -n|--no-cache)
      SKIP_CACHE_ARG="--no-cache"
      shift
      ;;
    -j|--num-threads)
      NUM_THREADS="$2"
      shift 2
      ;;
    --build-type)
      BUILD_TYPE="${2@L}"
      shift 2
      ;;
    --cuda-archs)
      CUDA_ARCHS="$2"
      shift 2
      ;;
    --deps-image)
      DEPS_IMAGE="$2"
      shift 2
      ;;
    --tag)
      IMAGE_TAG="$2"
      shift 2
      ;;
    --extra-cmake-flags)
      EXTRA_CMAKE_FLAGS="$2"
      shift 2
      ;;
    *)
      echo "Error: unknown argument $1"
      print_help
      exit 1
      ;;
  esac
done

if [[ ! ${BUILD_TYPE} =~ ^(release|relwithdebinfo|debug)$ ]]; then
  echo "Error: invalid --build-type value."
  exit 1
fi

if (( NUM_THREADS <= 0 )); then
  echo "Error: --num-threads must be a positive integer."
  exit 1
fi

if [[ -z "$CUDA_ARCHS" ]]; then
  if ! command -v nvidia-smi &> /dev/null; then
    echo "ERROR: nvidia-smi could not be found. Please ensure NVIDIA drivers are installed."
    exit 1
  fi
  CUDA_ARCHS="$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -n 1 | sed 's/\.//g')"
fi

if [[ ! -d "${WORKSPACE_ROOT}/velox" ]]; then
  echo "ERROR: expected sibling velox repo at ${WORKSPACE_ROOT}/velox"
  exit 1
fi

DOCKERFILE_PATH="${REPO_ROOT}/presto/docker/hashagg_replay_build.dockerfile"

BUILD_ARGS=(
  --build-arg PRESTO_DEPS_IMAGE="${DEPS_IMAGE}"
  --build-arg BUILD_TYPE="${BUILD_TYPE}"
  --build-arg NUM_THREADS="${NUM_THREADS}"
  --build-arg CUDA_ARCHITECTURES="${CUDA_ARCHS}"
)

if [[ -n "${EXTRA_CMAKE_FLAGS}" ]]; then
  BUILD_ARGS+=(--build-arg EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS}")
fi

echo "Building hashagg replay image: ${IMAGE_TAG}"
docker build ${SKIP_CACHE_ARG} \
  -f "${DOCKERFILE_PATH}" \
  "${BUILD_ARGS[@]}" \
  -t "${IMAGE_TAG}" \
  "${WORKSPACE_ROOT}"

echo "Built image: ${IMAGE_TAG}"
echo "Binary path in image: /usr/bin/velox_cudf_hashagg_replay"
