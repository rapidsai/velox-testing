#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Build script for CentOS dependency image
# Based on presto/presto-native-execution/scripts/dockerfiles/centos-dependency.dockerfile
# Run this inside a quay.io/centos/centos:stream9 container

set -e

echo "============================================"
echo "Building Presto/Velox Dependencies"
echo "============================================"

# Set environment variables (matching the Dockerfile)
export PROMPT_ALWAYS_RESPOND=y
export CC=/opt/rh/gcc-toolset-12/root/bin/gcc
export CXX=/opt/rh/gcc-toolset-12/root/bin/g++
export ARM_BUILD_TARGET=${ARM_BUILD_TARGET:-""}
export VELOX_ARROW_CMAKE_PATCH=/velox/cmake-compatibility.patch

# Override /root/.local/share/uv/tools to workaround --container-remap-root
# /mnt/home/$USER/.config/uv/uv-receipt.json will be created regardless and there's no way to configure that
export UV_TOOL_DIR=/uv/tools
export UV_NO_CACHE=1
export UV_NO_CONFIG=1

# Verify required directories exist
echo "Verifying required scripts and patches..."
if [[ ! -d /scripts ]]; then
    echo "ERROR: /scripts directory not found!"
    echo "You need to copy presto-native-execution/scripts into the container"
    exit 1
fi

if [[ ! -d /velox/scripts ]]; then
    echo "ERROR: /velox/scripts directory not found!"
    echo "You need to copy velox/scripts into the container"
    exit 1
fi

if [[ ! -f /velox/cmake-compatibility.patch ]]; then
    echo "ERROR: /velox/cmake-compatibility.patch not found!"
    echo "You need to copy velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch"
    exit 1
fi

echo "All required files present. Starting build..."
echo ""

# Run all setup commands in a single bash session (like the Dockerfile does)
# This ensures environment variables and functions persist across all steps
echo "Starting build process (this runs as a single bash session)..."
echo ""

bash -c "
    set -e

    # Ensure uv-installed tools are in PATH
    export UV_TOOL_BIN_DIR=/usr/local/bin
    export PATH=/usr/local/bin:\$PATH

    # Create build directory
    mkdir -p /build
    cd /build

    echo '============================================'
    echo 'Step 1/7: Running setup-centos.sh'
    echo '============================================'
    /scripts/setup-centos.sh

    # After gcc-toolset-12/enable, /usr/local/bin gets pushed back in PATH
    # Re-export to ensure uv-installed tools (like cmake) are found
    export PATH=/usr/local/bin:\$PATH

    # Rehash PATH to pick up newly installed binaries
    hash -r

    # Verify cmake is available after setup-centos.sh
    echo ''
    echo 'Verifying cmake installation...'
    if ! command -v cmake &> /dev/null; then
        echo 'ERROR: cmake not found in PATH after installation'
        echo 'PATH='\$PATH
        echo 'Checking /usr/local/bin:'
        ls -la /usr/local/bin/ | grep cmake || echo 'No cmake found'
        exit 1
    fi
    cmake --version
    echo ''

    echo ''
    echo '============================================'
    echo 'Step 2/7: Running setup-adapters.sh'
    echo '============================================'
    /scripts/setup-adapters.sh

    echo ''
    echo '============================================'
    echo 'Step 3/7: Sourcing setup-centos9.sh'
    echo '============================================'
    source /velox/scripts/setup-centos9.sh

    echo ''
    echo '============================================'
    echo 'Step 4/8: Sourcing setup-centos-adapters.sh'
    echo '============================================'
    source /velox/scripts/setup-centos-adapters.sh

    echo ''
    echo '============================================'
    echo 'Step 5/8: Installing clang15'
    echo '============================================'
    install_clang15

    echo ''
    echo '============================================'
    echo 'Step 6/8: Installing CUDA 13.0'
    echo '============================================'
    install_cuda 13.0

    echo ''
    echo '============================================'
    echo 'Step 7/8: Installing UCX (required by UCXX/cuDF)'
    echo '============================================'
    install_ucx

    echo ''
    echo '============================================'
    echo 'Step 8/8: Installing adapters'
    echo '============================================'
    install_adapters
"

# Clean up build directory (like the Dockerfile does)
rm -rf /build

echo ""
echo "============================================"
echo "Build Complete!"
echo "============================================"
echo "Environment configured:"
echo "  CC: $CC"
echo "  CXX: $CXX"
echo "  PATH: $PATH"
echo "  CUDA: /usr/local/cuda/bin"
echo ""
echo "You can now commit this container to create the dependency image:"
echo "  docker commit <container-id> presto/prestissimo-dependency:centos9"
echo "============================================"
