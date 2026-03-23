#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Build Velox with GPU-hardcoded defaults.
# Frozen copy of the compile logic from gluten's ep/build-velox/src/build-velox.sh,
# stripped to GPU-only with no runtime dependency on gluten's build system.
#
# Source: gluten/ep/build-velox/src/build-velox.sh
#
# Required env vars:
#   VELOX_HOME   — path to velox source root
#
# Optional env vars:
#   CUDA_ARCH    — CUDA architectures (default: native)
#   CUDF_SOURCE  — SYSTEM|BUNDLED (default: SYSTEM)
#   NUM_THREADS  — parallel jobs (default: nproc)
#   ENABLE_HDFS  — ON|OFF (default: ON)
#   ENABLE_S3    — ON|OFF (default: OFF)
#   REBUILD_VELOX — true|false — if true, rm -rf _build/release/ first (default: false)

set -exu

: "${VELOX_HOME:?ERROR: VELOX_HOME is not set}"

CUDA_ARCH="${CUDA_ARCH:-native}"
CUDF_SOURCE="${CUDF_SOURCE:-SYSTEM}"
NUM_THREADS="${NUM_THREADS:-$(nproc)}"
ENABLE_HDFS="${ENABLE_HDFS:-ON}"
ENABLE_S3="${ENABLE_S3:-OFF}"
REBUILD_VELOX="${REBUILD_VELOX:-false}"

echo "=== Building Velox (cudf_SOURCE=${CUDF_SOURCE}, GPU) ==="
echo "VELOX_HOME=${VELOX_HOME}"
echo "CUDA_ARCH=${CUDA_ARCH}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "ENABLE_S3=${ENABLE_S3}"

# ── Compile flags (GPU-only, stripped from upstream build-velox.sh) ────────
CXX_FLAGS='-Wno-error=stringop-overflow -Wno-error=cpp -Wno-missing-field-initializers \
    -Wno-error=uninitialized -Wno-unknown-warning-option'

COMPILE_OPTION="-DCMAKE_CXX_FLAGS=\"$CXX_FLAGS\" \
    -DVELOX_ENABLE_PARQUET=ON \
    -DVELOX_BUILD_TESTING=OFF \
    -DVELOX_MONO_LIBRARY=ON \
    -DVELOX_BUILD_RUNNER=OFF \
    -DVELOX_SIMDJSON_SKIPUTF8VALIDATION=ON \
    -DVELOX_ENABLE_GEO=ON"

# Connectors.
if [ "$ENABLE_HDFS" == "ON" ]; then
  COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_HDFS=ON"
fi
if [ "$ENABLE_S3" == "ON" ]; then
  COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_S3=ON"
fi

# GPU support (always ON).
COMPILE_OPTION="$COMPILE_OPTION \
    -DVELOX_ENABLE_GPU=ON \
    -DVELOX_ENABLE_CUDF=ON \
    -DCMAKE_CUDA_COMPILER=/usr/local/cuda/bin/nvcc \
    -Dcudf_SOURCE=${CUDF_SOURCE}"

# CUDA_ARCHITECTURES is passed as a separate make variable (not in
# EXTRA_CMAKE_FLAGS) so the Velox Makefile can quote it correctly.
# Semicolons in EXTRA_CMAKE_FLAGS would be interpreted as shell command
# separators when make's recipe runs cmake.
CUDA_ARCH_MAKE_VAR="CUDA_ARCHITECTURES=${CUDA_ARCH}"

COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_BUILD_TYPE=Release"

echo "COMPILE_OPTION: $COMPILE_OPTION"

# ── Thread opts ────────────────────────────────────────────────────────────
NUM_THREADS_OPTS="NUM_THREADS=$NUM_THREADS MAX_HIGH_MEM_JOBS=$NUM_THREADS MAX_LINK_JOBS=$NUM_THREADS"
echo "NUM_THREADS_OPTS: $NUM_THREADS_OPTS"

# ── Build ──────────────────────────────────────────────────────────────────
export simdjson_SOURCE=AUTO
export Arrow_SOURCE=SYSTEM

cd "$VELOX_HOME"

if [ "$REBUILD_VELOX" = true ] && [ -d _build/release ]; then
  echo "  Clearing _build/release/ (--rebuild_velox)..."
  rm -rf _build/release
fi

ARCH=$(uname -m)
if [ "$ARCH" == 'x86_64' ]; then
  make release $NUM_THREADS_OPTS ${CUDA_ARCH_MAKE_VAR} EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
elif [[ "$ARCH" == 'arm64' || "$ARCH" == 'aarch64' ]]; then
  CPU_TARGET=$ARCH make release $NUM_THREADS_OPTS ${CUDA_ARCH_MAKE_VAR} EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
else
  echo "Unsupported arch: $ARCH"
  exit 1
fi

# ── Install deps built by FetchContent ─────────────────────────────────────
if [ -d "_build/release/_deps" ]; then
  cd _build/release/_deps
  if [ -d xsimd-build ]; then
    echo "INSTALL xsimd."
    sudo cmake --install xsimd-build/
  fi
  if [ -d cudf-build ]; then
    echo "INSTALL cudf."
    sudo cmake --install cudf-build/
  fi
fi

echo "=== Velox build complete ==="
