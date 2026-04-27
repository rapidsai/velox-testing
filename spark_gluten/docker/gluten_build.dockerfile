# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Dockerfile: builds Gluten with Velox backend using a checked-out Velox
# repository. The resulting JARs are placed in /opt/gluten/jars/.
#
# Supports static (vcpkg) CPU builds, dynamic CPU builds, and dynamic GPU
# builds via the BUILD_TYPE argument.
#
# Build context must be the workspace root containing both
# incubator-gluten/ and velox/ as sibling directories.
#
# Build args:
#   BUILD_TYPE   – "gpu_dynamic" (default), "cpu_dynamic", or "cpu_static"
#   DEVICE_TYPE  – "cpu" or "gpu" (stored as GLUTEN_DEVICE_TYPE env var in the image)
#   BASE_IMAGE   – base Docker image (default: apache/gluten:centos-9-jdk8-cudf)
#   NO_CACHE     – "true" to clear the BuildKit build cache and force a full rebuild
#   CUDA_ARCHITECTURES  – semicolon-separated CUDA SM architectures for GPU builds.
#   NUM_THREADS  – number of threads for compilation

ARG SPARK_VERSION=3.5.5
ARG BASE_IMAGE=apache/gluten:centos-9-jdk8-cudf

FROM ${BASE_IMAGE} AS spark-download
ARG SPARK_VERSION
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        | tar -xz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark && \
    curl -fsSL -o "/opt/spark/jars/spark-connect_2.12-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/spark-connect_2.12-${SPARK_VERSION}.jar"

FROM ${BASE_IMAGE}

ARG BUILD_TYPE=gpu_dynamic
ARG DEVICE_TYPE=gpu
ARG NO_CACHE=false
ARG CUDA_ARCHITECTURES="75;80;86;90;100;120"
ARG NUM_THREADS

ENV NUM_THREADS=${NUM_THREADS}

# Install Python 3.12 for running tests/benchmarks and patchelf for fixing
# ELF NEEDED entries after the C++ build.
RUN dnf install -y python3.12 python3.12-pip patchelf && dnf clean all

RUN if [ "${DEVICE_TYPE}" = "gpu" ]; then \
        rpm --import https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub && \
        dnf install -y 'dnf-command(config-manager)' && \
        dnf config-manager --add-repo "https://developer.download.nvidia.com/devtools/repos/rhel$(source /etc/os-release; echo ${VERSION_ID%%.*})/$(rpm --eval '%{_arch}' | sed s/aarch/arm/)/" && \
        dnf install -y nsight-systems-cli && dnf clean all && \
        NSYS_BIN="$(compgen -G '/opt/nvidia/nsight-systems-cli/*/target-linux-x64/nsys' | sort -V | tail -1)" && \
        ln -sf "${NSYS_BIN}" /usr/local/bin/nsys && \
        ln -sf "${NSYS_BIN}" /usr/local/cuda/bin/nsys; \
    fi

COPY --from=spark-download /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY velox-testing/spark_gluten/scripts/launch_spark_connect_server.sh /opt/spark/

# Bind-mount the Gluten and Velox source trees from the build context.
# A BuildKit cache mount at /build_staging persists C++ build artifacts
# across rebuilds so that incremental compilation is possible.  Pass
# NO_CACHE=true to wipe the cache and force a full rebuild.
RUN --mount=type=bind,source=incubator-gluten,target=/workspace/gluten \
    --mount=type=bind,source=velox,target=/workspace/velox \
    --mount=type=cache,target=/build_staging,id=gluten-build-${BUILD_TYPE} \
    if [[ "${NO_CACHE}" == "true" ]]; then \
        echo "Clearing build cache..." && \
        rm -rf /build_staging/* && \
        rm -rf /root/.m2/repository/org/apache/gluten/ /root/.sbt/1.0/zinc/; \
    fi && \
    mkdir -p /build_staging/gluten && \
    # Remove cached source dirs (but keep cpp/ and ep/ for C++ build caching) \
    # so that files deleted from the repo don't persist as stale sources. \
    find /build_staging/gluten -mindepth 1 -maxdepth 1 ! -name cpp ! -name ep -exec rm -rf {} + && \
    cp -a /workspace/gluten/. /build_staging/gluten/ && \
    rm -rf /build_staging/gluten/.git && \
    mkdir -p /build_staging/gluten/ep/build-velox/build/velox_ep && \
    # Same cleanup for Velox: remove stale sources but keep build/ artifacts. \
    find /build_staging/gluten/ep/build-velox/build/velox_ep -mindepth 1 -maxdepth 1 ! -name build ! -name _build -exec rm -rf {} + && \
    cp -a /workspace/velox/. /build_staging/gluten/ep/build-velox/build/velox_ep/ && \
    rm -rf /build_staging/gluten/ep/build-velox/build/velox_ep/.git && \
    cd /build_staging/gluten && \
    for v in 14 13 12; do \
        if [[ -f /opt/rh/gcc-toolset-${v}/enable ]]; then \
            source /opt/rh/gcc-toolset-${v}/enable && \
            echo "Using gcc-toolset-${v}: $(gcc --version | head -1)" && \
            break; \
        fi; \
    done && \
    # Arrow's bundled helpers.h uses assert() without including <cassert>.
    export CXXFLAGS="${CXXFLAGS:-} -include cassert" && \
    if [[ "$(uname -m)" = "aarch64" ]]; then \
        export CPU_TARGET="aarch64"; \
        if [[ "${BUILD_TYPE}" = "cpu_static" ]]; then \
            export VCPKG_FORCE_SYSTEM_BINARIES=1; \
        fi; \
    fi && \
    if [[ "${BUILD_TYPE}" = "gpu_dynamic" ]]; then \
        ./dev/buildbundle-veloxbe.sh \
            --run_setup_script=OFF \
            --build_arrow=OFF \
            --enable_gpu=ON \
            --cuda_arch="${CUDA_ARCHITECTURES}" \
            --num_threads=${NUM_THREADS} \
            --spark_version=3.5 \
            --velox_home=/build_staging/gluten/ep/build-velox/build/velox_ep ; \
    elif [[ "${BUILD_TYPE}" = "cpu_static" ]]; then \
        ./dev/builddeps-veloxbe.sh \
            --enable_vcpkg=ON \
            --build_arrow=OFF \
            --enable_s3=OFF \
            --enable_gcs=OFF \
            --enable_hdfs=OFF \
            --enable_abfs=OFF \
            --num_threads=${NUM_THREADS} \
            --velox_home=/build_staging/gluten/ep/build-velox/build/velox_ep && \
        mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests ; \
    else \
        ./dev/builddeps-veloxbe.sh \
            --run_setup_script=ON \
            --enable_vcpkg=OFF \
            --build_arrow=OFF \
            --enable_hdfs=OFF \
            --num_threads=${NUM_THREADS} \
            --velox_home=/build_staging/gluten/ep/build-velox/build/velox_ep && \
        # Fix missing NEEDED OpenSSL symbol. Arrow bundled deps leak OpenSSL refs into libgluten.so.
        patchelf --add-needed libcrypto.so.3 cpp/build/releases/libgluten.so && \
        ./build/mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests ; \
    fi && \
    if [[ "${BUILD_TYPE}" != "cpu_static" ]]; then \
        ./dev/build-thirdparty.sh ; \
    fi && \
    mkdir -p /opt/gluten/jars && \
    cp package/target/gluten-velox-bundle-*.jar /opt/gluten/jars/

ENV GLUTEN_JAR_DIR=/opt/gluten/jars
ENV GLUTEN_DEVICE_TYPE=${DEVICE_TYPE}
