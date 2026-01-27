ARG TARGETARCH

# Install latest ninja
FROM --platform=$TARGETPLATFORM alpine:latest AS ninja-amd64
RUN apk add --no-cache unzip
ADD https://github.com/ninja-build/ninja/releases/latest/download/ninja-linux.zip /tmp

FROM --platform=$TARGETPLATFORM alpine:latest AS ninja-arm64
RUN apk add --no-cache unzip
ADD https://github.com/ninja-build/ninja/releases/latest/download/ninja-linux-aarch64.zip /tmp
RUN mv /tmp/ninja-linux-aarch64.zip /tmp/ninja-linux.zip

FROM ninja-${TARGETARCH} AS ninja
RUN unzip -d /usr/bin -o /tmp/ninja-linux.zip

FROM ghcr.io/facebookincubator/velox-dev:adapters
ARG TARGETARCH

# Do this separate so changing unrelated build args doesn't invalidate nsys installation layer
ARG VELOX_ENABLE_BENCHMARKS=ON
ARG INSTALL_CCLS=OFF

# Base packages for dev container runtime
RUN dnf install -y sudo cmake ninja-build git \
    && dnf clean all \
    && rm -rf /var/cache/dnf

# Install NVIDIA Nsight Systems (nsys) for profiling - only if benchmarks are enabled
RUN \ 
<<EOF
if [ "$VELOX_ENABLE_BENCHMARKS" = "ON" ]; then 
      set -euxo pipefail 
      # Detect architecture and set appropriate repo
      ARCH=$(uname -m)
      if [ "$ARCH" = "aarch64" ]; then
        CUDA_ARCH="sbsa"
      else \
        CUDA_ARCH="x86_64"
      fi
      # Add NVIDIA CUDA repository with proper GPG key
      dnf install -y dnf-plugins-core
      dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel9/${CUDA_ARCH}/cuda-rhel9.repo
      # Import NVIDIA GPG key
      rpm --import https://developer.download.nvidia.com/compute/cuda/repos/rhel9/${CUDA_ARCH}/D42D0685.pub
      # Install nsys from CUDA repository
      dnf install -y nsight-systems
      # Verify nsys installation
      which nsys && nsys --version
    else
      echo "Skipping nsys installation (VELOX_ENABLE_BENCHMARKS=OFF)"
    fi
EOF

# Optional ccls build (cached in image)
RUN if [ "$INSTALL_CCLS" = "ON" ]; then \
      set -eux; \
      dnf install -y clang clang-devel clang-tools-extra llvm-devel llvm-static; \
      CCLS_DIR=/opt/ccls; \
      git clone --recursive https://github.com/MaskRay/ccls.git "${CCLS_DIR}"; \
      cd "${CCLS_DIR}"; \
      git submodule update --init --recursive; \
      CLANG_DIR=$(rpm -ql clang clang-devel 2>/dev/null | grep -m1 'ClangConfig.cmake' | xargs dirname || true); \
      if [ -z "${CLANG_DIR}" ]; then \
        if [ -f /usr/lib64/cmake/clang/ClangConfig.cmake ]; then \
          CLANG_DIR=/usr/lib64/cmake/clang; \
        elif [ -f /usr/lib/cmake/clang/ClangConfig.cmake ]; then \
          CLANG_DIR=/usr/lib/cmake/clang; \
        fi; \
      fi; \
      cmake -S . -B Release \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH=/usr/lib/llvm-18 \
        -DLLVM_INCLUDE_DIR=/usr/lib/llvm-18/include \
        -DLLVM_BUILD_INCLUDE_DIR=/usr/include/llvm-18/ \
        ${CLANG_DIR:+-DClang_DIR=${CLANG_DIR}}; \
      cmake --build Release; \
      ln -sf "${CCLS_DIR}/Release/ccls" /usr/local/bin/ccls; \
      dnf clean all; \
      rm -rf /var/cache/dnf; \
    else \
      echo "Skipping ccls install (INSTALL_CCLS=${INSTALL_CCLS})"; \
    fi

# Build-time configuration, these may be overridden in the docker compose yaml,
# environment variables, or via the docker build command
ARG NUM_THREADS=8
ARG MAX_HIGH_MEM_JOBS=4
ARG MAX_LINK_JOBS=4
ARG CUDA_VERSION=12.8
ARG CUDA_ARCHITECTURES=70
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON
ARG BUILD_WITH_VELOX_ENABLE_WAVE=OFF
ARG TREAT_WARNINGS_AS_ERRORS=1
ARG BUILD_BASE_DIR=/opt/velox-build
ARG BUILD_TYPE=release
ARG ENABLE_SCCACHE=OFF
ARG SCCACHE_SERVER_LOG="sccache=info"
ARG SCCACHE_VERSION=latest
ARG UPDATE_NINJA=true
# Don't read from cache, but do put/replace entries
ARG SCCACHE_RECACHE
# Don't read from cache and don't write new entries
ARG SCCACHE_NO_CACHE
# Always compile locally (even if the build cluster is configured/available)
ARG SCCACHE_NO_DIST_COMPILE

# Environment mirroring upstream CI defaults and incorporating build args
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    GTest_SOURCE=BUNDLED \
    cudf_SOURCE=BUNDLED \
    faiss_SOURCE=BUNDLED \
    CUDA_VERSION=${CUDA_VERSION} \
    TREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS} \
    MAKEFLAGS="NUM_THREADS=${NUM_THREADS}" \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    CUDA_COMPILER=/usr/local/cuda-${CUDA_VERSION}/bin/nvcc \
    CUDA_FLAGS="-ccbin /opt/rh/gcc-toolset-14/root/usr/bin" \
    BUILD_BASE_DIR=${BUILD_BASE_DIR} \
    BUILD_TYPE=${BUILD_TYPE} \
    NUM_THREADS=${NUM_THREADS} \
    EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_BENCHMARKS=${VELOX_ENABLE_BENCHMARKS} \
                      -DVELOX_ENABLE_EXAMPLES=ON \
                      -DVELOX_ENABLE_ARROW=ON \
                      -DVELOX_ENABLE_GEO=ON \
                      -DVELOX_ENABLE_PARQUET=ON \
                      -DVELOX_ENABLE_HDFS=ON \
                      -DVELOX_ENABLE_S3=ON \
                      -DVELOX_ENABLE_GCS=ON \
                      -DVELOX_ENABLE_ABFS=ON \
                      -DVELOX_ENABLE_WAVE=${BUILD_WITH_VELOX_ENABLE_WAVE} \
                      -DVELOX_MONO_LIBRARY=ON \
                      -DVELOX_BUILD_SHARED=ON \
                      -DVELOX_ENABLE_CUDF=${BUILD_WITH_VELOX_ENABLE_CUDF} \
                      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
                      -DVELOX_ENABLE_FAISS=ON" \
    LD_LIBRARY_PATH="${BUILD_BASE_DIR}/${BUILD_TYPE}/lib:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/cudf-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rmm-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rapids_logger-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/kvikio-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/nvcomp_proprietary_binary-src/lib64" \
    ENABLE_SCCACHE="${ENABLE_SCCACHE}" \
    BUILD_WITH_VELOX_ENABLE_CUDF="${BUILD_WITH_VELOX_ENABLE_CUDF}" \
    BUILD_WITH_VELOX_ENABLE_WAVE="${BUILD_WITH_VELOX_ENABLE_WAVE}" \
    MAX_HIGH_MEM_JOBS="${MAX_HIGH_MEM_JOBS}" \
    MAX_LINK_JOBS="${MAX_LINK_JOBS}" \
    SCCACHE_NO_DIST_COMPILE="${SCCACHE_NO_DIST_COMPILE}" \
    SCCACHE_RECACHE="${SCCACHE_RECACHE}" \
    SCCACHE_NO_CACHE="${SCCACHE_NO_CACHE}" \
    SCCACHE_VERSION="${SCCACHE_VERSION}" \
    SCCACHE_SERVER_LOG="${SCCACHE_SERVER_LOG}" \
    SCCACHE_ERROR_LOG=/tmp/sccache.log \
    SCCACHE_CACHE_SIZE=107374182400 \
    SCCACHE_BUCKET=rapids-sccache-devs \
    SCCACHE_REGION=us-east-2 \
    SCCACHE_S3_NO_CREDENTIALS=false \
    # disable shutdown-on-idle
    SCCACHE_IDLE_TIMEOUT=0 \
    SCCACHE_DIST_AUTH_TYPE=token \
    SCCACHE_DIST_REQUEST_TIMEOUT=7140 \
    SCCACHE_DIST_SCHEDULER_URL="https://${TARGETARCH}.linux.sccache.rapids.nvidia.com" \
    SCCACHE_DIST_MAX_RETRIES=4 \
    SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE=true \
    UPDATE_NINJA="${UPDATE_NINJA}"

WORKDIR /workspace/velox

# Print environment variables for debugging
RUN printenv | sort

# Install latest ninja (conditionally based on UPDATE_NINJA)
RUN --mount=from=ninja,source=/usr/bin/ninja,target=/tmp/ninja \
    if [ "$UPDATE_NINJA" = "true" ]; then \
      echo "Installing ninja..."; \
      cp /tmp/ninja /usr/bin/ninja && chmod +x /usr/bin/ninja; \
    else \
      echo "Skipping ninja installation"; \
    fi

# Defer build to dev container
