#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Build Gluten C++ (libgluten.so + libvelox.so) with GPU-hardcoded defaults.
# Direct cmake + make, modeled after build-tpch-gpu.sh.
#
# Required env vars:
#   GLUTEN_DIR   — path to gluten source root
#   VELOX_HOME   — path to velox source root
#
# Optional env vars:
#   NUM_THREADS  — parallel jobs (default: nproc)
#   ENABLE_HDFS  — ON|OFF (default: ON)
#   ENABLE_S3    — ON|OFF (default: OFF)
#   REBUILD_GLUTEN_CPP — true|false — if true, rm -rf build/ first (default: false)

set -exu

: "${GLUTEN_DIR:?ERROR: GLUTEN_DIR is not set}"
: "${VELOX_HOME:?ERROR: VELOX_HOME is not set}"

NUM_THREADS="${NUM_THREADS:-$(nproc)}"
ENABLE_HDFS="${ENABLE_HDFS:-ON}"
ENABLE_S3="${ENABLE_S3:-OFF}"
REBUILD_GLUTEN_CPP="${REBUILD_GLUTEN_CPP:-false}"

echo "=== Building Gluten C++ ==="

# ── Fix nvcomp lib path mismatch ──────────────────────────────────────────
# The nvcomp cmake config (nvcomp-targets-dynamic.cmake) references
# /usr/local/lib/libnvcomp.so.* but the actual libraries live in
# /usr/local/lib64/.  Symlink them so find_package(cudf) succeeds.
if [ -d /usr/local/lib64 ] && ls /usr/local/lib64/libnvcomp* >/dev/null 2>&1; then
  if ! ls /usr/local/lib/libnvcomp* >/dev/null 2>&1; then
    echo "  Symlinking nvcomp libs from lib64/ → lib/ ..."
    sudo ln -sf /usr/local/lib64/libnvcomp* /usr/local/lib/
  fi
fi

cd "$GLUTEN_DIR/cpp"

if [ "$REBUILD_GLUTEN_CPP" = true ] && [ -d build ]; then
  echo "  Clearing build/ (--rebuild_gluten_cpp)..."
  rm -rf build
fi

mkdir -p build
cd build

cmake .. \
  -DBUILD_VELOX_BACKEND=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DVELOX_HOME="$VELOX_HOME" \
  -DENABLE_GPU=ON \
  -DBUILD_TESTS=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DBUILD_BENCHMARKS=OFF \
  -DENABLE_JEMALLOC_STATS=OFF \
  -DENABLE_QAT=OFF \
  -DENABLE_S3="$ENABLE_S3" \
  -DENABLE_GCS=OFF \
  -DENABLE_HDFS="$ENABLE_HDFS" \
  -DENABLE_ABFS=OFF \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

make -j "$NUM_THREADS"

echo "=== Gluten C++ build complete ==="
echo "  Output: $GLUTEN_DIR/cpp/build/releases/"
ls -lh "$GLUTEN_DIR/cpp/build/releases/"*.so 2>/dev/null || true
