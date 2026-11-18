#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Single script to build CentOS dependencies
# Run this INSIDE the container after mounting presto-build
#
# Then inside container:
#   /presto-build/build-deps-in-container.sh

set -e

PRESTO_BUILD_DIR=${PRESTO_BUILD_DIR:-/presto-build}

echo "============================================"
echo "Building CentOS Dependencies"
echo "============================================"
echo "Presto build mount: ${PRESTO_BUILD_DIR}"
echo "============================================"
echo ""

echo ls -l "${PRESTO_BUILD_DIR}/"
ls -l "${PRESTO_BUILD_DIR}/"

# Verify presto-build directory is mounted
if [[ ! -d "${PRESTO_BUILD_DIR}" ]]; then
    echo "ERROR: Presto build directory not found at ${PRESTO_BUILD_DIR}"
    echo ""
    echo "Please mount it when starting the container:"
    echo "    --container-mount=/mnt/data/$USER/src/velox-testing/presto/slurm/build:/presto-build:ro \\"
    echo ""
    echo "Or set PRESTO_BUILD_DIR to the correct mount point."
    exit 1
fi

# Verify required source directories exist
if [[ ! -d "${PRESTO_BUILD_DIR}/presto/presto-native-execution/scripts" ]]; then
    echo "ERROR: Presto scripts not found at ${PRESTO_BUILD_DIR}/presto/presto-native-execution/scripts"
    exit 1
fi

if [[ ! -d "${PRESTO_BUILD_DIR}/presto/presto-native-execution/velox/scripts" ]]; then
    echo "ERROR: Velox scripts not found at ${PRESTO_BUILD_DIR}/velox/scripts"
    exit 1
fi

if [[ ! -f "${PRESTO_BUILD_DIR}/presto/presto-native-execution/velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch" ]]; then
    echo "ERROR: CMake patch not found at ${PRESTO_BUILD_DIR}/presto/presto-native-execution/velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch"
    exit 1
fi

echo "Step 1/4: Copying Presto scripts to /scripts..."
cp -r "${PRESTO_BUILD_DIR}/presto/presto-native-execution/scripts" /scripts

echo "Step 2/4: Copying Velox scripts to /velox/scripts..."
mkdir -p /velox
cp -r "${PRESTO_BUILD_DIR}/presto/presto-native-execution/velox/scripts" /velox/scripts

echo "Step 3/4: Copying Velox CMake patch to /velox/cmake-compatibility.patch..."
cp "${PRESTO_BUILD_DIR}/presto/presto-native-execution/velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch" \
   /velox/cmake-compatibility.patch

echo ls -l "/scripts/"
ls -l "/scripts/"
echo ls -l "/velox/scripts/"
ls -l "/velox/scripts/"

echo "Step 4/4: Running dependency build..."
echo "============================================"
echo "This will take 15-20 minutes..."
echo "============================================"
echo ""

# Run the build script directly
"${PRESTO_BUILD_DIR}/build-centos-deps.sh"

echo ""
echo "============================================"
echo "Build Complete!"
echo "============================================"
echo ""
echo "To commit this container to an image, run from the host:"
echo "  docker commit <container-id> presto/prestissimo-dependency:centos9"
echo ""
echo "To verify the image:"
echo "  docker images | grep prestissimo-dependency"
echo "============================================"
