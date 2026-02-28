# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Dockerfile: builds Gluten with Velox backend using a checked-out Velox
# source tree.  The resulting JARs are placed in /opt/gluten/jars/.
#
# Supports static (vcpkg) CPU builds, dynamic CPU builds, and GPU builds via
# the BUILD_TYPE argument.
#
# Build context must be the workspace root containing both
# incubator-gluten/ and velox/ as sibling directories.
#
# Usage (via wrapper scripts):
#   ./build_gluten_static.sh  [-o OUTPUT_DIR]
#   ./build_gluten_dynamic.sh -d cpu [--image-tag TAG]
#   ./build_gluten_dynamic.sh -d gpu [--image-tag TAG]
#
# Build args:
#   BUILD_TYPE   – "cpu" (default), "gpu", or "static"
#   BASE_IMAGE   – base Docker image (must be provided)
#   GCC_TOOLSET  – gcc-toolset version to enable (default: gcc-toolset-12)

ARG BUILD_TYPE=cpu
ARG BASE_IMAGE
ARG GCC_TOOLSET=gcc-toolset-12

FROM ${BASE_IMAGE}

ARG BUILD_TYPE
ARG GCC_TOOLSET

SHELL ["/bin/bash", "-c"]

# Bind-mount the Gluten and Velox source trees from the build context so
# that they are available during compilation without creating large
# intermediate image layers.  The source is copied into /work (writable)
# and the checked-out Velox tree is placed where Gluten's EP expects it.
RUN --mount=type=bind,source=incubator-gluten,target=/src \
    --mount=type=bind,source=velox,target=/velox \
    cp -a /src /work && \
    mkdir -p /work/ep/build-velox/build && \
    cp -a /velox /work/ep/build-velox/build/velox_ep && \
    cd /work && \
    source /opt/rh/${GCC_TOOLSET}/enable && \
    if [ "$(uname -m)" = "aarch64" ]; then \
        export CPU_TARGET="aarch64"; \
        if [ "${BUILD_TYPE}" = "static" ]; then export VCPKG_FORCE_SYSTEM_BINARIES=1; fi; \
    fi && \
    rm -rf ep/build-velox/build/velox_ep/_build && \
    if [ "${BUILD_TYPE}" = "gpu" ]; then \
        ./dev/buildbundle-veloxbe.sh \
            --run_setup_script=OFF \
            --build_arrow=OFF \
            --enable_gpu=ON \
            --spark_version=3.5 \
            --velox_home=/work/ep/build-velox/build/velox_ep ; \
    elif [ "${BUILD_TYPE}" = "static" ]; then \
        ./dev/builddeps-veloxbe.sh \
            --enable_vcpkg=ON \
            --build_arrow=OFF \
            --enable_s3=OFF \
            --enable_gcs=OFF \
            --enable_hdfs=OFF \
            --enable_abfs=OFF \
            --velox_home=/work/ep/build-velox/build/velox_ep && \
        mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests ; \
    else \
        ./dev/builddeps-veloxbe.sh \
            --run_setup_script=ON \
            --enable_vcpkg=OFF \
            --build_arrow=OFF \
            --enable_hdfs=OFF \
            --velox_home=/work/ep/build-velox/build/velox_ep && \
        ./build/mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests ; \
    fi && \
    if [ "${BUILD_TYPE}" != "static" ]; then \
        ./dev/build-thirdparty.sh ; \
    fi && \
    mkdir -p /opt/gluten/jars && \
    cp package/target/gluten-velox-bundle-*.jar /opt/gluten/jars/ && \
    if [ "${BUILD_TYPE}" != "static" ]; then \
        cp package/target/thirdparty-lib/gluten-thirdparty-lib-*.jar /opt/gluten/jars/ ; \
    fi

ENV GLUTEN_JAR_DIR=/opt/gluten/jars
