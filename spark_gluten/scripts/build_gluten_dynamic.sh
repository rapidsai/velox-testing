#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script builds Gluten with the Velox backend (dynamic linking) and produces a docker
image containing the Gluten JARs and linked libraries.

OPTIONS:
    -h, --help              Show this help message.
    -d, --device-type       Device type to build for. Must be "cpu" or "gpu".
    -i, --image-tag         Tag for the resulting docker image. The full image reference is
                            "apache/gluten:{image-tag}". Default values are
                            "dynamic_cpu_\${USER:-latest}" for cpu and "dynamic_gpu_\${USER:-latest}"
                            for gpu.
    -j, --num-threads       Number of threads to use for the build (default is \$(nproc) / 2).
    -n, --no-cache          Clear the cached build directory and force a full rebuild.
                            Without this flag, C++ build artifacts are reused across
                            rebuilds for faster incremental compilation.
    --cuda-arch             Semicolon-separated CUDA SM architectures for GPU builds.
                            Use "all" to target all supported architectures.By default,
                            the native architecture is auto-detected from the host GPU using
                            nvidia-smi. Ignored for CPU builds.

EXAMPLES:
    $0 -d cpu
    $0 -d gpu
    $0 -d cpu --image-tag my_cpu_image
    $0 -d gpu -i my_gpu_image
    $0 -d cpu -j 8
    $0 -d gpu --no-cache
    $0 -d gpu --cuda-arch all
    $0 -h

EOF
}

NO_CACHE=false

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -d|--device-type)
        if [[ -n $2 ]]; then
          DEVICE_TYPE=$2
          shift 2
        else
          echo "Error: --device-type requires a value"
          exit 1
        fi
        ;;
      -i|--image-tag)
        if [[ -n $2 ]]; then
          IMAGE_TAG=$2
          shift 2
        else
          echo "Error: --image-tag requires a value"
          exit 1
        fi
        ;;
      -j|--num-threads)
        if [[ -n $2 ]]; then
          NUM_THREADS=$2
          shift 2
        else
          echo "Error: --num-threads requires a value"
          exit 1
        fi
        ;;
      -n|--no-cache)
        NO_CACHE=true
        shift
        ;;
      --cuda-arch)
        if [[ -n $2 ]]; then
          CUDA_ARCH=$2
          shift 2
        else
          echo "Error: --cuda-arch requires a value"
          exit 1
        fi
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

if [[ -z ${DEVICE_TYPE} || ! ${DEVICE_TYPE} =~ ^(cpu|gpu)$ ]]; then
  echo "Error: A valid device type (cpu or gpu) is required. Use the -d or --device-type argument."
  print_help
  exit 1
fi

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/build_gluten_helper.sh"

# All supported CUDA SM architectures (T4 through B-series).
ALL_CUDA_ARCHS="75;80;86;90;100;120"

# Expand the "all" alias.
if [[ "${CUDA_ARCH}" == "all" ]]; then
  CUDA_ARCH="${ALL_CUDA_ARCHS}"
fi

# Set defaults based on device type.
if [[ "${DEVICE_TYPE}" == "gpu" ]]; then
  IMAGE_TAG="${IMAGE_TAG:-dynamic_gpu_${USER:-latest}}"
  BASE_IMAGE="apache/gluten:centos-9-jdk8-cudf"
  # Auto-detect the native CUDA architecture if not explicitly provided.
  if [[ -z "${CUDA_ARCH}" ]]; then
    CUDA_ARCH=$(detect_cuda_architecture)
    echo "Auto-detected CUDA architecture: ${CUDA_ARCH}"
  fi
else
  IMAGE_TAG="${IMAGE_TAG:-dynamic_cpu_${USER:-latest}}"
  BASE_IMAGE="apache/gluten:centos-9-jdk17"
fi

NUM_THREADS="${NUM_THREADS:-$(($(nproc) / 2))}"

IMAGE="apache/gluten:${IMAGE_TAG}"
echo "Building image ${IMAGE} (${DEVICE_TYPE} dynamic, ${NUM_THREADS} threads) ..."
run_gluten_build "$IMAGE" "${DEVICE_TYPE}_dynamic" "$BASE_IMAGE" "$NUM_THREADS" "$DEVICE_TYPE" "$NO_CACHE" "${CUDA_ARCH:-}"
echo "Image ${IMAGE} built successfully."
