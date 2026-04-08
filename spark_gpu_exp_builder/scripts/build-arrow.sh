#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Build Arrow C++ and Java for Gluten.
# Frozen copy from gluten/dev/build-arrow.sh, adapted for standalone use.
#
# Required env vars:
#   GLUTEN_DIR  — path to gluten source root
#
# Optional env vars:
#   INSTALL_PREFIX — where to install Arrow C++ (default: /usr/local)
#   SUDO           — prefix for install commands (default: "")

set -exu

SCRIPT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
SUDO="${SUDO:-""}"
source "${SCRIPT_DIR}/build-helper-functions.sh"

# GLUTEN_DIR must be set by the caller (docker-build-cudf.sh or similar).
: "${GLUTEN_DIR:?ERROR: GLUTEN_DIR is not set}"

VELOX_ARROW_BUILD_VERSION=15.0.0
ARROW_PREFIX=$GLUTEN_DIR/ep/_ep/arrow_ep
BUILD_TYPE=Release
INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}

function prepare_arrow_build() {
  mkdir -p ${ARROW_PREFIX}/../ && pushd ${ARROW_PREFIX}/../ && ${SUDO} rm -rf arrow_ep/
  wget_and_untar https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${VELOX_ARROW_BUILD_VERSION}.tar.gz arrow_ep
  cd arrow_ep
  patch -p1 < $GLUTEN_DIR/ep/build-velox/src/modify_arrow.patch
  patch -p1 < $GLUTEN_DIR/ep/build-velox/src/modify_arrow_dataset_scan_option.patch
  patch -p1 < $GLUTEN_DIR/ep/build-velox/src/cmake-compatibility.patch
  patch -p1 < $GLUTEN_DIR/ep/build-velox/src/support_ibm_power.patch
  popd
}

function build_arrow_cpp() {
  pushd $ARROW_PREFIX/cpp
  ARROW_WITH_ZLIB=ON
  cmake_install \
       -DARROW_PARQUET=OFF \
       -DARROW_FILESYSTEM=ON \
       -DARROW_PROTOBUF_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_SOURCE=BUNDLED \
       -DARROW_WITH_THRIFT=ON \
       -DARROW_WITH_LZ4=ON \
       -DARROW_WITH_SNAPPY=ON \
       -DARROW_WITH_ZLIB=${ARROW_WITH_ZLIB} \
       -DARROW_WITH_ZSTD=ON \
       -DARROW_JEMALLOC=OFF \
       -DARROW_SIMD_LEVEL=NONE \
       -DARROW_RUNTIME_SIMD_LEVEL=NONE \
       -DARROW_WITH_UTF8PROC=OFF \
       -DARROW_TESTING=ON \
       -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
       -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
       -DARROW_BUILD_SHARED=OFF \
       -DARROW_BUILD_STATIC=ON

 # Install thrift.
 cd _build/thrift_ep-prefix/src/thrift_ep-build
 ${SUDO} cmake --install ./ --prefix "${INSTALL_PREFIX}"/
 popd
}

function build_arrow_java() {
    ARROW_INSTALL_DIR="${ARROW_PREFIX}/install"

    # Use Gluten's Maven wrapper
    MVN_CMD="${GLUTEN_DIR}/build/mvn"

    NPROC=${NPROC:-$(nproc --ignore=2)}
    echo "set cmake build level to ${NPROC}"
    export CMAKE_BUILD_PARALLEL_LEVEL=$NPROC

    pushd $ARROW_PREFIX/java
    # Because arrow-bom module need the -DprocessAllModules
    ${MVN_CMD} versions:set -DnewVersion=15.0.0-gluten -DprocessAllModules

    ${MVN_CMD} clean install -pl bom,maven/module-info-compiler-maven-plugin,vector -am \
          -DskipTests -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly

    # Arrow C Data Interface CPP libraries
    ${MVN_CMD} generate-resources -P generate-libs-cdata-all-os -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow JNI Date Interface CPP libraries
    export PKG_CONFIG_PATH="${INSTALL_PREFIX}"/lib64/pkgconfig:"${INSTALL_PREFIX}"/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}
    ${MVN_CMD} generate-resources -Pgenerate-libs-jni-macos-linux -N -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR \
      -DARROW_GANDIVA=OFF -DARROW_JAVA_JNI_ENABLE_GANDIVA=OFF -DARROW_ORC=OFF -DARROW_JAVA_JNI_ENABLE_ORC=OFF \
	    -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow Java libraries
    ${MVN_CMD} install -Parrow-jni -P arrow-c-data -pl c,dataset -am \
      -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly
    popd
}

echo "=== Building Arrow ==="
prepare_arrow_build
build_arrow_cpp
echo "Finished building Arrow C++"
build_arrow_java
echo "Finished building Arrow Java"
