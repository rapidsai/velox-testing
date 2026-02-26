#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Build Gluten with Velox CPU backend (static linking via vcpkg) and
# copy the output JAR(s) to a host directory.
#
# Expects the following sibling directories alongside the velox-testing repo:
#   ../incubator-gluten   – Gluten source tree
#   ../velox              – checked-out Velox source tree
#
# Usage:
#   ./build_gluten_static.sh [-o OUTPUT_DIR]

OUTPUT_DIR=".build_artifacts/cpu_static"

while [[ $# -gt 0 ]]; do
  case $1 in
    -o|--output-gluten-jar-dir)
      if [[ -n $2 ]]; then
        OUTPUT_DIR=$2
        shift 2
      else
        echo "Error: --output-gluten-jar-dir requires a value"
        exit 1
      fi
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/build_gluten_helper.sh"

TEMP_IMAGE="gluten-static-build-tmp:$$"

echo "Building Gluten (static CPU) ..."
run_gluten_build "$TEMP_IMAGE" static apache/gluten:vcpkg-centos-9 gcc-toolset-12

# Extract JARs from the image to the host output directory.
mkdir -p "$OUTPUT_DIR"
CONTAINER_ID=$(docker create "$TEMP_IMAGE")
docker cp "$CONTAINER_ID:/opt/gluten/jars/." "$OUTPUT_DIR/"
docker rm "$CONTAINER_ID" > /dev/null

# Set ownership to the current user.
chown -R "$(id -u):$(id -g)" "$OUTPUT_DIR"

# Clean up the temporary image.
docker rmi "$TEMP_IMAGE" > /dev/null

echo "Static build complete. Output JAR(s) in ${OUTPUT_DIR}/"
ls -lh "$OUTPUT_DIR"/gluten-velox-bundle-*.jar
