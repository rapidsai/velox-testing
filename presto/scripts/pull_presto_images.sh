#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Pulls prebuilt Presto images and retags them with the local image names used
# by the start_*_presto.sh scripts.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel 2>/dev/null || realpath "${SCRIPT_DIR}/../..")"

source "${REPO_ROOT}/scripts/common.sh"

SOURCE="prestodb"
IMAGE_TAG=""
LOCAL_TAG="${PRESTO_IMAGE_TAG:-${USER:-latest}}"
CUDA_VERSION="13.1"
CUDA_VERSION_SET=false
DRY_RUN=false
INCLUDE_COORDINATOR=true

REGISTRY="${REGISTRY:-ghcr.io}"
PACKAGE_REPO="${PACKAGE_REPO:-rapidsai}"
PACKAGE_NAME="${PACKAGE_NAME:-velox-testing-images}"
CI_IMAGE_BASE="${CI_IMAGE_BASE:-${REGISTRY}/${PACKAGE_REPO}/${PACKAGE_NAME}}"

COORDINATOR_IMAGE=""
GPU_WORKER_IMAGE=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Pull Presto images from a supported source and retag them for local use by the
start_*_presto.sh scripts.

Sources:
  prestodb  Pull PrestoDB Docker Hub native GPU worker and coordinator images.
  ci        Pull RAPIDS CI native GPU worker and coordinator images from GHCR.

Options:
  --source SOURCE              Image source: prestodb or ci (default: prestodb).
  --tag TAG                    Source image tag to pull.
                               prestodb default: gpu-nightly.
                               ci default: latest.
  --local-tag TAG              Local tag to create (default: PRESTO_IMAGE_TAG or USER).
                               Start scripts use this tag through PRESTO_IMAGE_TAG.
  --cuda-version VERSION       CI GPU CUDA version to pull (default: 13.1).
  --no-coordinator             Do not pull or retag the coordinator image.
  --dry-run                    Print the pull/tag actions without running Docker.
  -h, --help                   Show this help.

Examples:
  # PrestoDB defaults:
  #   prestodb/presto:coordinator-gpu-nightly
  #   prestodb/presto-native:gpu-nightly
  $(basename "$0")

  # Use a different PrestoDB tag.
  $(basename "$0") --tag latest

  # RAPIDS CI stable latest GPU images.
  $(basename "$0") --source ci

  # Use a specific RAPIDS CI Presto/Velox tag body.
  $(basename "$0") --source ci --tag c0de72d-velox-f374779-20260616-manual-27588892754

  # Pull a CI worker built with a different CUDA version.
  $(basename "$0") --source ci --cuda-version 12.8

Local image names created:
  presto-coordinator:<tag>
  presto-native-worker-gpu:<tag>
EOF
}

require_value() {
  local option=$1
  local value=${2:-}
  if [[ -z "${value}" ]]; then
    echo "ERROR: ${option} requires a value." >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      require_value "$1" "${2:-}"
      SOURCE="$2"
      shift 2
      ;;
    --tag)
      require_value "$1" "${2:-}"
      IMAGE_TAG="$2"
      shift 2
      ;;
    --local-tag)
      require_value "$1" "${2:-}"
      LOCAL_TAG="$2"
      shift 2
      ;;
    --cuda-version)
      require_value "$1" "${2:-}"
      CUDA_VERSION="$2"
      CUDA_VERSION_SET=true
      shift 2
      ;;
    --no-coordinator)
      INCLUDE_COORDINATOR=false
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

case "${SOURCE}" in
  prestodb|ci) ;;
  *)
    echo "ERROR: --source must be one of: prestodb, ci." >&2
    exit 1
    ;;
esac

if [[ -z "${LOCAL_TAG}" ]]; then
  echo "ERROR: local tag cannot be empty." >&2
  exit 1
fi

if [[ "${SOURCE}" == "prestodb" && "${CUDA_VERSION_SET}" == true ]]; then
  echo "ERROR: --cuda-version is only valid with --source ci." >&2
  exit 1
fi

want_role() {
  local role=$1
  case "${role}" in
    coordinator) [[ "${INCLUDE_COORDINATOR}" == true ]] ;;
    gpu_worker) return 0 ;;
    *) return 1 ;;
  esac
}

set_preset_images() {
  case "${SOURCE}" in
    prestodb)
      IMAGE_TAG="${IMAGE_TAG:-gpu-nightly}"
      if [[ "${IMAGE_TAG}" == "gpu-nightly" ]]; then
        COORDINATOR_IMAGE="prestodb/presto:coordinator-gpu-nightly"
      else
        COORDINATOR_IMAGE="prestodb/presto:${IMAGE_TAG}"
      fi
      GPU_WORKER_IMAGE="prestodb/presto-native:${IMAGE_TAG}"
      ;;
    ci)
      IMAGE_TAG="${IMAGE_TAG:-latest}"
      if [[ "${IMAGE_TAG}" == "latest" ]]; then
        COORDINATOR_IMAGE="${CI_IMAGE_BASE}:presto-coordinator-latest"
        GPU_WORKER_IMAGE="${CI_IMAGE_BASE}:presto-latest-gpu-cuda${CUDA_VERSION}"
      else
        local presto_short_sha velox_short_sha tag_suffix coordinator_suffix worker_suffix
        if [[ ! "${IMAGE_TAG}" =~ ^([0-9a-fA-F]+)-velox-([0-9a-fA-F]+)(-(.+))?$ ]]; then
          echo "ERROR: custom CI tags must use '<presto_sha>-velox-<velox_sha>[-<suffix>]'." >&2
          exit 1
        fi
        presto_short_sha="${BASH_REMATCH[1]}"
        velox_short_sha="${BASH_REMATCH[2]}"
        tag_suffix="${BASH_REMATCH[4]:-}"
        coordinator_suffix=""
        worker_suffix=""
        if [[ -n "${tag_suffix}" ]]; then
          coordinator_suffix="-${tag_suffix}"
          worker_suffix="-${tag_suffix}"
        fi
        COORDINATOR_IMAGE="${CI_IMAGE_BASE}:presto-coordinator-${presto_short_sha}${coordinator_suffix}"
        GPU_WORKER_IMAGE="${CI_IMAGE_BASE}:presto-${presto_short_sha}-velox-${velox_short_sha}-gpu-cuda${CUDA_VERSION}${worker_suffix}"
      fi
      ;;
  esac
}

require_source_image() {
  local role=$1
  local image=$2
  if [[ -z "${image}" ]]; then
    echo "ERROR: source '${SOURCE}' does not define an image for ${role}." >&2
    exit 1
  fi
}

pull_and_tag() {
  local source_image=$1
  local local_repo=$2
  local local_image="${local_repo}:${LOCAL_TAG}"

  if [[ "${DRY_RUN}" == true ]]; then
    echo "Would pull ${source_image}"
    echo "Would tag  ${source_image} -> ${local_image}"
    return
  fi

  echo "Pulling ${source_image}..."
  docker_pull_with_retry "${source_image}"
  echo "Tagging ${source_image} -> ${local_image}"
  docker tag "${source_image}" "${local_image}"
}

validate_requested_images() {
  if want_role coordinator; then
    require_source_image "coordinator" "${COORDINATOR_IMAGE}"
  fi

  if want_role gpu_worker; then
    require_source_image "gpu-worker" "${GPU_WORKER_IMAGE}"
  fi
}

set_preset_images
validate_requested_images

echo "Source:    ${SOURCE}"
echo "Tag:       ${IMAGE_TAG}"
if [[ "${SOURCE}" == "ci" ]]; then
  echo "CUDA:      ${CUDA_VERSION}"
fi
echo "Local tag: ${LOCAL_TAG}"
echo ""

if want_role coordinator; then
  pull_and_tag "${COORDINATOR_IMAGE}" "presto-coordinator"
fi

if want_role gpu_worker; then
  pull_and_tag "${GPU_WORKER_IMAGE}" "presto-native-worker-gpu"
fi

echo ""
echo "Done. Start scripts will use these images when PRESTO_IMAGE_TAG=${LOCAL_TAG}."
