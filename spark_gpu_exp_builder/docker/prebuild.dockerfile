# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# All-in-one prebuild image for Gluten GPU builds (CentOS Stream 9).
#
# Builds and installs:
#   1. Arrow C++ (static) + Arrow Java + JNI bridges
#   2. Gluten Maven dependency cache (go-offline)
#   3. libcudf + RAPIDS stack (rmm, kvikio, rapids-cmake)
#   4. protobuf 3.21.8 (shared lib)
#   5. curl upgrade (kvikio needs >= 7.80)
#
# The resulting image is used by builder.sh (both docker and direct modes)
# and by runtime.dockerfile as the build stage base.
#
# Build:
#   docker build \
#     --build-context gluten=/path/to/gluten \
#     --build-context velox=/path/to/velox \
#     --build-arg CUDA_ARCH="75;80;86;89;90" \
#     -t gluten:centos-9-prebuild \
#     -f docker/prebuild.dockerfile .
#
# To target a single arch (e.g. A100):
#   docker build --build-arg CUDA_ARCH=80 ...

ARG BASE_IMAGE=apache/gluten:centos-9-jdk8-cudf
FROM ${BASE_IMAGE}

ARG SPARK_VERSION=3.5
ARG ARROW_VERSION=15.0.0
ARG CUDA_ARCH="75;80;86;89;90"
ARG CURL_VERSION=8.12.1

ENV CUDA_ARCHITECTURES=${CUDA_ARCH}

# ══════════════════════════════════════════════════════════════════════════════
# Phase 0: Maven setup (decoupled from gluten/build/mvn)
# ══════════════════════════════════════════════════════════════════════════════
# Install Maven independently so we don't depend on gluten/build/mvn wrapper.
# Reads maven.version from Gluten's pom.xml to stay in sync, or falls back to
# a default version. Also installs settings.xml if provided.
ARG MAVEN_VERSION=""
COPY --from=gluten pom.xml /tmp/_gluten_pom.xml

RUN set -ex \
  && MVN_VER="${MAVEN_VERSION}" \
  # If no explicit version, read from Gluten's pom.xml.
  && if [ -z "$MVN_VER" ]; then \
       MVN_VER=$(grep '<maven.version>' /tmp/_gluten_pom.xml | head -1 \
         | sed 's/.*<maven.version>\(.*\)<\/maven.version>.*/\1/') ; \
     fi \
  # Fallback default.
  && MVN_VER="${MVN_VER:-3.9.9}" \
  && echo "Installing Maven ${MVN_VER}..." \
  && curl -fsSL "https://repo1.maven.org/maven2/org/apache/maven/apache-maven/${MVN_VER}/apache-maven-${MVN_VER}-bin.tar.gz" \
     | tar xz -C /opt \
  && ln -s /opt/apache-maven-${MVN_VER} /opt/maven \
  && rm -f /tmp/_gluten_pom.xml

ENV MAVEN_HOME=/opt/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"
ENV MAVEN_OPTS="-Xss128m -Xmx4g -XX:ReservedCodeCacheSize=2g"

# ── Maven settings (optional) ─────────────────────────────────────────────
# Stage custom settings.xml if provided. All mvn calls below use the `mvns`
# wrapper which auto-appends `-s /opt/maven-settings/settings.xml` when the
# file exists. If not provided, mvn uses its default (Maven Central).
COPY .docker-maven-settings/ /opt/maven-settings/
RUN printf '#!/bin/sh\nif [ -f /opt/maven-settings/settings.xml ]; then\n  exec mvn -s /opt/maven-settings/settings.xml "$@"\nelse\n  exec mvn "$@"\nfi\n' \
      > /usr/local/bin/mvns \
  && chmod +x /usr/local/bin/mvns

RUN mvns --version

# Copy the Gluten source tree (patches, pom files).
# Requires: --build-context gluten=/path/to/gluten
COPY --from=gluten . /tmp/gluten-src

# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Arrow C++ (static)
# ══════════════════════════════════════════════════════════════════════════════
# Download release tarball, apply Gluten patches, build with cmake/ninja.
# Installs headers + static libs to /usr/local; also installs thrift.
# ARROW_SIMD_LEVEL=NONE keeps the binaries portable across CPU micro-archs.
RUN source /opt/rh/gcc-toolset-14/enable \
  && set -ex \
  && ARROW_PREFIX=/tmp/gluten-src/ep/_ep/arrow_ep \
  && INSTALL_PREFIX=/usr/local \
  && mkdir -p ${ARROW_PREFIX} \
  && curl -sL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz" \
     | tar xz --strip-components=1 -C ${ARROW_PREFIX} \
  && cd ${ARROW_PREFIX} \
  && patch -p1 < /tmp/gluten-src/ep/build-velox/src/modify_arrow.patch \
  && patch -p1 < /tmp/gluten-src/ep/build-velox/src/modify_arrow_dataset_scan_option.patch \
  && patch -p1 < /tmp/gluten-src/ep/build-velox/src/cmake-compatibility.patch \
  && patch -p1 < /tmp/gluten-src/ep/build-velox/src/support_ibm_power.patch \
  && cd ${ARROW_PREFIX}/cpp \
  && cmake -Wno-dev -B_build \
       -GNinja \
       -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
       -DCMAKE_CXX_STANDARD=20 \
       -DCMAKE_PREFIX_PATH=${INSTALL_PREFIX} \
       -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
       -DCMAKE_BUILD_TYPE=Release \
       -DBUILD_TESTING=OFF \
       -DARROW_PARQUET=OFF \
       -DARROW_FILESYSTEM=ON \
       -DARROW_PROTOBUF_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_SOURCE=BUNDLED \
       -DARROW_WITH_THRIFT=ON \
       -DARROW_WITH_LZ4=ON \
       -DARROW_WITH_SNAPPY=ON \
       -DARROW_WITH_ZLIB=ON \
       -DARROW_WITH_ZSTD=ON \
       -DARROW_JEMALLOC=OFF \
       -DARROW_SIMD_LEVEL=NONE \
       -DARROW_RUNTIME_SIMD_LEVEL=NONE \
       -DARROW_WITH_UTF8PROC=OFF \
       -DARROW_TESTING=ON \
       -DARROW_BUILD_SHARED=OFF \
       -DARROW_BUILD_STATIC=ON \
  && cmake --build _build -j$(nproc) \
  && cmake --install _build \
  && cmake --install _build/thrift_ep-prefix/src/thrift_ep-build \
       --prefix ${INSTALL_PREFIX}

# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: Arrow Java + JNI
# ══════════════════════════════════════════════════════════════════════════════
RUN source /opt/rh/gcc-toolset-14/enable \
  && set -ex \
  && GLUTEN_DIR=/tmp/gluten-src \
  && ARROW_PREFIX=${GLUTEN_DIR}/ep/_ep/arrow_ep \
  && INSTALL_PREFIX=/usr/local \
  && ARROW_INSTALL_DIR=${ARROW_PREFIX}/install \
  && NPROC=$(nproc --ignore=2) \
  && export CMAKE_BUILD_PARALLEL_LEVEL=${NPROC} \
  && cd ${ARROW_PREFIX}/java \
  && mvns versions:set \
       -DnewVersion=${ARROW_VERSION}-gluten -DprocessAllModules \
  && mvns clean install \
       -pl bom,maven/module-info-compiler-maven-plugin,vector -am \
       -DskipTests -Drat.skip -Dmaven.gitcommitid.skip \
       -Dcheckstyle.skip -Dassembly.skipAssembly \
  && mkdir -p ${ARROW_INSTALL_DIR} \
  && mvns generate-resources \
       -Pgenerate-libs-cdata-all-os \
       -Darrow.c.jni.dist.dir=${ARROW_INSTALL_DIR} \
       -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip \
       -Dcheckstyle.skip -N \
  && export PKG_CONFIG_PATH=${INSTALL_PREFIX}/lib64/pkgconfig:${INSTALL_PREFIX}/lib/pkgconfig \
  && mvns generate-resources \
       -Pgenerate-libs-jni-macos-linux -N \
       -Darrow.dataset.jni.dist.dir=${ARROW_INSTALL_DIR} \
       -DARROW_GANDIVA=OFF -DARROW_JAVA_JNI_ENABLE_GANDIVA=OFF \
       -DARROW_ORC=OFF -DARROW_JAVA_JNI_ENABLE_ORC=OFF \
       -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip \
       -Dcheckstyle.skip -N \
  && mvns install \
       -Parrow-jni -Parrow-c-data \
       -pl c,dataset -am \
       -Darrow.c.jni.dist.dir=${ARROW_INSTALL_DIR}/lib \
       -Darrow.dataset.jni.dist.dir=${ARROW_INSTALL_DIR}/lib \
       -Darrow.cpp.build.dir=${ARROW_INSTALL_DIR}/lib \
       -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip \
       -Dcheckstyle.skip -Dassembly.skipAssembly

# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Gluten Maven dependency resolution
# ══════════════════════════════════════════════════════════════════════════════
RUN set -ex \
  && cd /tmp/gluten-src \
  && mvns dependency:go-offline \
       -Pbackends-velox -Pspark-${SPARK_VERSION} \
       -DskipTests -fn \
  && rm -rf /tmp/gluten-src

# ══════════════════════════════════════════════════════════════════════════════
# Phase 4: cuDF stack
# ══════════════════════════════════════════════════════════════════════════════

# Copy Velox cmake files needed by build-cudf-standalone.cmake.
COPY --from=velox CMake/ResolveDependency.cmake \
                  /opt/velox_setup/CMake/ResolveDependency.cmake
COPY --from=velox CMake/resolve_dependency_modules/cudf.cmake \
                  /opt/velox_setup/CMake/resolve_dependency_modules/cudf.cmake

# Copy the standalone cmake project.
COPY docker/build-cudf-standalone.cmake /opt/velox_setup/CMakeLists.txt

# Record cudf commit info for downstream verification.
RUN set -e \
  && CUDF_CMAKE=/opt/velox_setup/CMake/resolve_dependency_modules/cudf.cmake \
  && grep -E '^set\(VELOX_cudf_(COMMIT|VERSION) ' "$CUDF_CMAKE" \
    | sed 's/set(\(VELOX_cudf_[A-Z]*\) \([^ )]*\).*/\1=\2/' \
    > /home/cudf-version-info \
  && echo "=== cudf-version-info ===" \
  && cat /home/cudf-version-info \
  && COMMIT=$(grep '^VELOX_cudf_COMMIT=' /home/cudf-version-info | cut -d= -f2) \
  && VERSION=$(grep '^VELOX_cudf_VERSION=' /home/cudf-version-info | cut -d= -f2) \
  && [ -n "$COMMIT" ]  || { echo "ERROR: VELOX_cudf_COMMIT not extracted"; exit 1; } \
  && [ -n "$VERSION" ] || { echo "ERROR: VELOX_cudf_VERSION not extracted"; exit 1; } \
  && grep -q "set(VELOX_cudf_COMMIT ${COMMIT}" "$CUDF_CMAKE" \
     || { echo "ERROR: COMMIT '${COMMIT}' not found in cudf.cmake"; exit 1; } \
  && grep -q "set(VELOX_cudf_VERSION ${VERSION} " "$CUDF_CMAKE" \
     || { echo "ERROR: VERSION '${VERSION}' not found in cudf.cmake"; exit 1; } \
  && echo "=== verified OK ==="

# Upgrade libcurl (>= 7.80 needed by kvikio for curl_url_strerror).
RUN source /opt/rh/gcc-toolset-14/enable \
  && set -ex \
  && curl -sL "https://curl.se/download/curl-${CURL_VERSION}.tar.gz" \
     | tar xz -C /tmp \
  && cd /tmp/curl-${CURL_VERSION} \
  && cmake -B _build -GNinja \
       -DCMAKE_BUILD_TYPE=Release \
       -DCMAKE_INSTALL_PREFIX=/usr/local \
       -DBUILD_SHARED_LIBS=ON \
       -DCURL_USE_OPENSSL=ON \
       -DBUILD_TESTING=OFF \
       -DCURL_DISABLE_LDAP=ON \
       -DCURL_USE_LIBPSL=OFF \
  && cmake --build _build -j$(nproc) \
  && cmake --install _build \
  && ldconfig \
  && rm -rf /tmp/curl-${CURL_VERSION}

# Build the cuDF stack and install to /usr/local.
RUN source /opt/rh/gcc-toolset-14/enable \
  && cmake -B /tmp/cudf-build \
          -GNinja \
          -DCMAKE_CUDA_ARCHITECTURES="${CUDA_ARCH}" \
          -DCMAKE_CUDA_COMPILER=/usr/local/cuda/bin/nvcc \
          -DCMAKE_C_COMPILER="$(which gcc)" \
          -DCMAKE_CXX_COMPILER="$(which g++)" \
          /opt/velox_setup \
  && cmake --build /tmp/cudf-build -j$(nproc) \
  && cmake --install /tmp/cudf-build --prefix /usr/local \
  && printf '/usr/local/lib\n/usr/local/lib64\n' > /etc/ld.so.conf.d/cudf.conf \
  && ldconfig \
  && rm -rf /tmp/cudf-build /opt/velox_setup

# ══════════════════════════════════════════════════════════════════════════════
# Phase 5: protobuf 3.21.8 (shared library)
# ══════════════════════════════════════════════════════════════════════════════
# The base image has only libprotobuf.a (static).  Gluten's JNI loader needs
# libprotobuf.so.32 at runtime.
RUN source /opt/rh/gcc-toolset-14/enable \
  && curl -sL https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protobuf-all-21.8.tar.gz \
     | tar xz -C /tmp \
  && cmake -B /tmp/protobuf-build -GNinja \
          -S /tmp/protobuf-21.8/cmake \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
          -Dprotobuf_BUILD_SHARED_LIBS=ON \
          -Dprotobuf_BUILD_TESTS=OFF \
          -DCMAKE_INSTALL_PREFIX=/usr/local \
  && cmake --build /tmp/protobuf-build -j$(nproc) \
  && cmake --install /tmp/protobuf-build \
  && ldconfig \
  && rm -rf /tmp/protobuf-21.8 /tmp/protobuf-build

# ══════════════════════════════════════════════════════════════════════════════
# Phase 6: patchelf (used by collect-deploy-libs.sh to fix RPATH)
# ══════════════════════════════════════════════════════════════════════════════
RUN dnf install -y patchelf && dnf clean all
