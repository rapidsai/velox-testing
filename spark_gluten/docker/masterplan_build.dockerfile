# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Dockerfile: builds the "master plan" variant of Gluten+Velox.
#
# Build context must be the workspace root containing
# spark-gluten/, velox/, and velox-testing/ as sibling directories.
#
# Each major build phase is a separate layer so that successful phases
# are cached independently (e.g. velox C++ survives a gluten failure).
#
# Build args:
#   BASE_IMAGE          – base Docker image (default: adapters image)
#   CUDA_ARCHITECTURES  – semicolon-separated SM architectures
#   NUM_THREADS         – compilation threads
#   NO_CACHE            – "true" to wipe BuildKit cache

ARG BASE_IMAGE=ghcr.io/facebookincubator/velox-dev:adapters

FROM ${BASE_IMAGE}

ARG NO_CACHE=false
ARG CUDA_ARCHITECTURES="89"
ARG NUM_THREADS=16

# Install build-time and runtime deps not in the base image.
RUN dnf -y install \
      java-17-openjdk \
      java-17-openjdk-devel \
      patch \
      python3.12 \
      python3.12-pip \
    && dnf clean all

# Install nsys for profiling.
RUN dnf config-manager --add-repo \
      https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo \
    && dnf -y install nsight-systems-2025.6.3 \
    && dnf clean all

ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# =============================================================================
# Phase 1: Build Velox C++
# =============================================================================
RUN --mount=type=bind,source=velox,target=/src/velox \
    --mount=type=cache,target=/opt/velox-build,id=masterplan-velox \
    set -eux && \
    \
    for v in 14 13 12; do \
        if [ -f /opt/rh/gcc-toolset-${v}/enable ]; then \
            . /opt/rh/gcc-toolset-${v}/enable && break; \
        fi; \
    done && \
    export CC=gcc CXX=g++ && \
    \
    # Copy source (preserve build artifacts in cache)
    if [ ! -d /opt/velox-build/velox ]; then \
        cp -a /src/velox /opt/velox-build/velox; \
    else \
        # Update source files only, keep _build
        find /src/velox -maxdepth 1 -mindepth 1 ! -name '_build' -exec cp -a {} /opt/velox-build/velox/ \; ; \
    fi && \
    \
    cd /opt/velox-build/velox && \
    if [ ! -f _build/release/lib/libvelox.a ]; then \
        echo "=== Phase 1: Building Velox C++ ===" && \
        cmake -B _build/release -GNinja \
            -DCMAKE_BUILD_TYPE=Release \
            -DVELOX_ENABLE_PARQUET=ON \
            -DVELOX_BUILD_TESTING=OFF \
            -DVELOX_ENABLE_CUDF=ON \
            -DCMAKE_CUDA_ARCHITECTURES="${CUDA_ARCHITECTURES}" && \
        cmake --build _build/release -j"${NUM_THREADS}"; \
    else \
        echo "=== Phase 1: Velox already built — skipping ==="; \
    fi && \
    \
    # Copy runtime shared libs to a persistent image location (cache mount
    # is not available at runtime).
    mkdir -p /opt/velox-runtime-libs && \
    for lib in libcudf.so librmm.so librapids_logger.so libkvikio.so; do \
        find /opt/velox-build/velox/_build/release/_deps -name "$lib" -exec cp {} /opt/velox-runtime-libs/ \; 2>/dev/null; \
    done && \
    # Also grab nvcomp
    find /opt/velox-build/velox/_build/release/_deps -name "libnvcomp.so*" -exec cp -P {} /opt/velox-runtime-libs/ \; 2>/dev/null; \
    ls /opt/velox-runtime-libs/

ENV LD_LIBRARY_PATH="/opt/velox-runtime-libs:/usr/local/lib64:/usr/local/cuda/lib64"

# =============================================================================
# Phase 2: Build spark-gluten C++
# =============================================================================
RUN --mount=type=bind,source=spark-gluten,target=/src/spark-gluten \
    --mount=type=cache,target=/opt/velox-build,id=masterplan-velox \
    --mount=type=cache,target=/opt/gluten-cpp-build,id=masterplan-gluten-cpp \
    set -eux && \
    \
    for v in 14 13 12; do \
        if [ -f /opt/rh/gcc-toolset-${v}/enable ]; then \
            . /opt/rh/gcc-toolset-${v}/enable && break; \
        fi; \
    done && \
    export CC=gcc CXX=g++ && \
    \
    VELOX_HOME=/opt/velox-build/velox && \
    VELOX_BUILD_PATH=$VELOX_HOME/_build/release && \
    CUDF_CMAKE_DIR=$VELOX_BUILD_PATH/_deps/cudf-build && \
    \
    # Copy spark-gluten source (preserve cpp/build in cache)
    if [ ! -d /opt/gluten-cpp-build/spark-gluten ]; then \
        cp -a /src/spark-gluten /opt/gluten-cpp-build/spark-gluten; \
    else \
        find /src/spark-gluten -maxdepth 1 -mindepth 1 ! -name 'cpp' -exec cp -a {} /opt/gluten-cpp-build/spark-gluten/ \; && \
        cp -a /src/spark-gluten/cpp/CMake /opt/gluten-cpp-build/spark-gluten/cpp/ && \
        cp -a /src/spark-gluten/cpp/core /opt/gluten-cpp-build/spark-gluten/cpp/ && \
        cp -a /src/spark-gluten/cpp/velox /opt/gluten-cpp-build/spark-gluten/cpp/ && \
        cp -a /src/spark-gluten/cpp/CMakeLists.txt /opt/gluten-cpp-build/spark-gluten/cpp/; \
    fi && \
    \
    cd /opt/gluten-cpp-build/spark-gluten && \
    echo "=== Phase 2: Building spark-gluten C++ ===" && \
    if [ ! -f cpp/build/CMakeCache.txt ]; then \
        mkdir -p cpp/build && cd cpp/build && \
        cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_PREFIX_PATH="/usr/local;/usr/local/lib64;$CUDF_CMAKE_DIR" \
            -DVELOX_HOME=$VELOX_HOME \
            -DVELOX_BUILD_PATH=$VELOX_BUILD_PATH \
            -Dcudf_DIR=$CUDF_CMAKE_DIR \
            -DThrift_ROOT=/usr/local \
            -DENABLE_GLUTEN_VCPKG=OFF \
            -DBUILD_TESTS=OFF \
            -DBUILD_BENCHMARKS=OFF \
            -DENABLE_GPU=ON \
            -DCMAKE_CUDA_ARCHITECTURES="${CUDA_ARCHITECTURES}" && \
        cd /opt/gluten-cpp-build/spark-gluten; \
    fi && \
    cmake --build cpp/build --target velox -j"${NUM_THREADS}" && \
    \
    # Copy .so to a stable location for later phases
    mkdir -p /opt/gluten-releases && \
    cp cpp/build/releases/libvelox.so /opt/gluten-releases/ && \
    cp cpp/build/releases/libgluten.so /opt/gluten-releases/

# =============================================================================
# Phase 3: Build Arrow Java JARs (15.0.0-gluten)
# =============================================================================
RUN --mount=type=bind,source=spark-gluten,target=/src/spark-gluten \
    --mount=type=cache,target=/root/.m2,id=masterplan-maven \
    set -eux && \
    \
    ARROW_CHECK="/root/.m2/repository/org/apache/arrow/arrow-vector/15.0.0-gluten/arrow-vector-15.0.0-gluten.jar" && \
    if [ ! -f "$ARROW_CHECK" ]; then \
        echo "=== Phase 3: Building Arrow Java ===" && \
        # Need writable copy for Maven wrapper download
        cp -a /src/spark-gluten /tmp/sg-arrow && \
        MVN=/tmp/sg-arrow/build/mvn && \
        ARROW_PREFIX=/tmp/arrow_ep && \
        mkdir -p "$ARROW_PREFIX" && cd "$ARROW_PREFIX" && \
        curl -sL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-15.0.0.tar.gz" | tar xz && \
        ARROW_SRC=$ARROW_PREFIX/arrow-apache-arrow-15.0.0 && \
        cd $ARROW_SRC && \
        for p in /tmp/sg-arrow/ep/build-velox/src/*.patch; do \
            [ -f "$p" ] && patch -p1 --forward < "$p" || true; \
        done && \
        cd $ARROW_SRC/java && \
        $MVN versions:set -DnewVersion=15.0.0-gluten -DprocessAllModules && \
        $MVN clean install \
            -pl bom,maven/module-info-compiler-maven-plugin,vector -am \
            -DskipTests -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip \
            -Dassembly.skipAssembly -Dspotless.check.skip=true && \
        mkdir -p $ARROW_PREFIX/install/lib && \
        $MVN install -Parrow-jni -Parrow-c-data -pl c,dataset -am \
            -Darrow.c.jni.dist.dir=$ARROW_PREFIX/install/lib \
            -Darrow.dataset.jni.dist.dir=$ARROW_PREFIX/install/lib \
            -Darrow.cpp.build.dir=$ARROW_PREFIX/install/lib \
            -DskipTests -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip \
            -Dassembly.skipAssembly -Dspotless.check.skip=true || true && \
        rm -rf /tmp/arrow_ep /tmp/sg-arrow && \
        # Verify Arrow JARs were installed
        ls /root/.m2/repository/org/apache/arrow/arrow-vector/15.0.0-gluten/arrow-vector-15.0.0-gluten.jar; \
    else \
        echo "=== Phase 3: Arrow Java already in Maven cache — skipping ==="; \
    fi

# =============================================================================
# Phase 4: Maven build (Java/Scala) + bundle JAR
# =============================================================================
RUN --mount=type=bind,source=spark-gluten,target=/src/spark-gluten \
    --mount=type=cache,target=/root/.m2,id=masterplan-maven \
    set -eux && \
    \
    echo "=== Phase 4: Maven build ===" && \
    # Work in /tmp to avoid bind-mount read-only issues
    cp -a /src/spark-gluten /tmp/spark-gluten && \
    cd /tmp/spark-gluten && \
    ./build/mvn install \
        -Pbackends-velox -Pspark-3.4 -Pjava-17 \
        -DVELOX_HOME=/opt/velox-build/velox \
        -Denable_gpu=ON \
        -DskipTests \
        -Dmaven.javadoc.skip=true \
        -Dcheckstyle.skip=true \
        -Dscalastyle.skip=true \
        -Denforcer.skip=true \
        -Dspotless.check.skip=true \
        -pl package -am && \
    \
    # Save the JAR
    mkdir -p /opt/gluten/jars && \
    cp package/target/gluten-velox-bundle-*.jar /opt/gluten/jars/ && \
    rm -rf /tmp/spark-gluten

# =============================================================================
# Phase 5: Inject native libraries + Arrow JNI into JAR
# =============================================================================
RUN --mount=type=bind,source=spark-gluten,target=/src/spark-gluten \
    --mount=type=bind,source=velox-testing/spark_gluten/scripts,target=/src/scripts \
    set -eux && \
    \
    echo "=== Phase 5: Injecting .so + JNI libs ===" && \
    JAR=$(ls /opt/gluten/jars/gluten-velox-bundle-*.jar | head -1) && \
    \
    # Build Arrow C Data JNI lib from source (download Arrow if needed)
    ARROW_SRC=/tmp/arrow_jni_src && \
    mkdir -p $ARROW_SRC && \
    curl -sL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-15.0.0.tar.gz" \
        | tar xz -C $ARROW_SRC --strip-components=1 \
            arrow-apache-arrow-15.0.0/java/c/src/main/java/org/apache/arrow/c/jni \
            arrow-apache-arrow-15.0.0/java/c/src/main/cpp && \
    JAVA_SRC=$ARROW_SRC/java/c/src/main/java/org/apache/arrow/c/jni && \
    mkdir -p /tmp/arrow_jni_classes /tmp/arrow_jni_headers && \
    javac -d /tmp/arrow_jni_classes -h /tmp/arrow_jni_headers \
        $JAVA_SRC/JniWrapper.java $JAVA_SRC/CDataJniException.java \
        $JAVA_SRC/JniLoader.java $JAVA_SRC/PrivateData.java && \
    . /opt/rh/gcc-toolset-14/enable && \
    g++ -shared -fPIC -O2 \
        -I$JAVA_HOME/include -I$JAVA_HOME/include/linux \
        -I/tmp/arrow_jni_headers \
        -o /tmp/libarrow_cdata_jni.so \
        $ARROW_SRC/java/c/src/main/cpp/jni_wrapper.cc && \
    \
    python3 /src/scripts/inject_native_libs.py \
        "$JAR" /opt/gluten-releases \
        --jni-lib /tmp/libarrow_cdata_jni.so && \
    \
    rm -rf /tmp/arrow_jni_* /tmp/arrow_jni_src

ENV GLUTEN_JAR_DIR=/opt/gluten/jars
ENV GLUTEN_DEVICE_TYPE=gpu
