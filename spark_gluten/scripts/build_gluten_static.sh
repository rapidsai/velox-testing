#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

OUTPUT_DIR="build_artifacts/cpu_static"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script builds Gluten with the Velox CPU backend (static linking via vcpkg) and
copies the output JAR to a host directory.

OPTIONS:
    -h, --help                   Show this help message.
    -o, --output-gluten-jar-dir  Directory path for the output JAR file. By default,
                                 the "${OUTPUT_DIR}" path is used.
    -j, --num-threads            Number of threads to use for the build (default is \$(nproc) / 2).
    -n, --no-cache               Clear the cached build directory and force a full
                                 rebuild. Without this flag, C++ build artifacts are
                                 reused across rebuilds for faster incremental
                                 compilation.

EXAMPLES:
    $0
    $0 -o my_gluten_jar_dir
    $0 --output-gluten-jar-dir my_gluten_jar_dir
    $0 -j 8
    $0 --no-cache
    $0 -h

EOF
}

NUM_THREADS=$(($(nproc) / 2))
NO_CACHE=false

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -o|--output-gluten-jar-dir)
        if [[ -n $2 ]]; then
          OUTPUT_DIR=$2
          shift 2
        else
          echo "Error: --output-gluten-jar-dir requires a value"
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
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/build_gluten_helper.sh"

BUILD_JAR_IMAGE="gluten-static-build-staging:latest"

echo "Building Gluten (static CPU, ${NUM_THREADS} threads) ..."
run_gluten_build "$BUILD_JAR_IMAGE" cpu_static apache/gluten:vcpkg-centos-9 "$NUM_THREADS" cpu "$NO_CACHE"

# Extract JARs from the image to the host output directory.
mkdir -p "$OUTPUT_DIR"
docker run --rm \
  -v "$(readlink -f "$OUTPUT_DIR"):/output" \
  "$BUILD_JAR_IMAGE" \
  bash -c "cp /opt/gluten/jars/* /output/ && chown -R $(id -u):$(id -g) /output"

# Clean up the temporary image (force-remove in case stale containers reference it).
docker rmi -f "$BUILD_JAR_IMAGE" > /dev/null

echo "Static build complete. Output JAR can be found at ${OUTPUT_DIR}/"
