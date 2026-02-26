#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Build Gluten with Velox backend (dynamic linking) and produce a Docker
# image containing the Gluten JARs.
#
# Expects the following sibling directories alongside the velox-testing repo:
#   ../incubator-gluten   – Gluten source tree
#   ../velox              – checked-out Velox source tree
#
# Usage:
#   ./build_gluten_dynamic.sh -d cpu [--image-tag TAG]
#   ./build_gluten_dynamic.sh -d gpu [--image-tag TAG]

DEVICE_TYPE=""
IMAGE_TAG=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--device-type)
      if [[ -n $2 ]]; then
        DEVICE_TYPE=$2
        shift 2
      else
        echo "Error: --device-type requires a value"
        exit 1
      fi
      ;;
    --image-tag)
      if [[ -n $2 ]]; then
        IMAGE_TAG=$2
        shift 2
      else
        echo "Error: --image-tag requires a value"
        exit 1
      fi
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [[ -z ${DEVICE_TYPE} || ! ${DEVICE_TYPE} =~ ^(cpu|gpu)$ ]]; then
  echo "Error: --device-type (-d) is required and must be 'cpu' or 'gpu'."
  exit 1
fi

# Set defaults based on device type.
if [[ "${DEVICE_TYPE}" == "gpu" ]]; then
  IMAGE_TAG="${IMAGE_TAG:-apache/gluten:dynamic_gpu_${USER:-latest}}"
  BASE_IMAGE="apache/gluten:centos-9-jdk8-cudf"
  GCC_TOOLSET="gcc-toolset-14"
else
  IMAGE_TAG="${IMAGE_TAG:-apache/gluten:dynamic_cpu_${USER:-latest}}"
  BASE_IMAGE="apache/gluten:centos-9-jdk17"
  GCC_TOOLSET="gcc-toolset-12"
fi

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/build_gluten_helper.sh"

echo "Building image ${IMAGE_TAG} (${DEVICE_TYPE} dynamic) ..."
run_gluten_build "$IMAGE_TAG" "$DEVICE_TYPE" "$BASE_IMAGE" "$GCC_TOOLSET"
echo "Image ${IMAGE_TAG} built successfully."
