FROM ghcr.io/facebookincubator/velox-dev:adapters

# Build-time configuration, these may be overridden in the docker compose yaml,
# environment variables, or via the docker build command
ARG NUM_THREADS=8
ARG CUDA_VERSION=12.8
ARG CUDA_ARCHITECTURES=70
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON
ARG BUILD_WITH_VELOX_ENABLE_WAVE=OFF
ARG TREAT_WARNINGS_AS_ERRORS=1
ARG VELOX_ENABLE_BENCHMARKS=ON
ARG BUILD_BASE_DIR=/opt/velox-build
ARG BUILD_TYPE=release
ARG ENABLE_SCCACHE=OFF
ARG SCCACHE_DISABLE_DIST=ON

# Environment mirroring upstream CI defaults and incorporating build args
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    GTest_SOURCE=BUNDLED \
    cudf_SOURCE=BUNDLED \
    faiss_SOURCE=BUNDLED \
    CUDA_VERSION=${CUDA_VERSION} \
    TREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS} \
    MAKEFLAGS="NUM_THREADS=${NUM_THREADS} MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=4" \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    CUDA_COMPILER=/usr/local/cuda-${CUDA_VERSION}/bin/nvcc \
    CUDA_FLAGS="-ccbin /opt/rh/gcc-toolset-12/root/usr/bin" \
    BUILD_TYPE=${BUILD_TYPE} \
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
                      -DVELOX_ENABLE_FAISS=ON \
                      -DCMAKE_CXX_FLAGS=-fno-omit-frame-pointer" \
    LD_LIBRARY_PATH="${BUILD_BASE_DIR}/${BUILD_TYPE}/lib:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/cudf-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rmm-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rapids_logger-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/kvikio-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/nvcomp_proprietary_binary-src/lib64" \
    ENABLE_SCCACHE=${ENABLE_SCCACHE} \
    SCCACHE_DISABLE_DIST=${SCCACHE_DISABLE_DIST}

WORKDIR /workspace/velox

RUN dnf install -y libnvjitlink-devel-$(echo ${CUDA_VERSION} | tr '.' '-')

# Install CUDA Sanitizer (compute-sanitizer) for memory debugging
RUN dnf install -y cuda-sanitizer-$(echo ${CUDA_VERSION} | tr '.' '-') || \
    echo "WARNING: CUDA Sanitizer not available for CUDA ${CUDA_VERSION}, trying alternative installation" && \
    dnf install -y cuda-sanitizer-12-* || \
    echo "WARNING: CUDA Sanitizer installation failed - --cuda-sanitizer option will not work"

# Install GDB for manual debugging (used in interactive mode)
RUN dnf install -y gdb


# Build and install newer curl to replace system version
RUN set -euxo pipefail && \
    # Install build dependencies
    dnf install -y wget tar make gcc openssl-devel zlib-devel libnghttp2-devel && \
    # Download and build curl 7.88.1 with curl_url_strerror support
    cd /tmp && \
    wget https://curl.se/download/curl-7.88.1.tar.gz && \
    tar -xzf curl-7.88.1.tar.gz && \
    cd curl-7.88.1 && \
    ./configure --prefix=/usr \
                --libdir=/usr/lib64 \
                --with-openssl \
                --with-zlib \
                --with-nghttp2 \
                --enable-shared \
                --disable-static && \
    make -j$(nproc) && \
    # Install with new curl
    make install && \
    # Update library cache
    ldconfig && \
    # Verify the new curl works and has the required symbol
    curl --version && \
    nm -D /usr/lib64/libcurl.so | grep curl_url_strerror && \
    # Clean up build files
    cd / && rm -rf /tmp/curl-7.88.1*

# Print environment variables for debugging
RUN printenv | sort

# Install sccache if enabled
RUN if [ "$ENABLE_SCCACHE" = "ON" ]; then \
      set -euxo pipefail && \
      # Install RAPIDS sccache fork
      wget --no-hsts -q -O- "https://github.com/rapidsai/sccache/releases/download/v0.10.0-rapids.68/sccache-v0.10.0-rapids.68-$(uname -m)-unknown-linux-musl.tar.gz" | \
      tar -C /usr/bin -zf - --wildcards --strip-components=1 -x '*/sccache' 2>/dev/null && \
      chmod +x /usr/bin/sccache && \
      # Verify installation
      sccache --version; \
    else \
      echo "Skipping sccache installation (ENABLE_SCCACHE=OFF)"; \
    fi

# Install NVIDIA Nsight Systems (nsys) for profiling - only if benchmarks are enabled
RUN if [ "$VELOX_ENABLE_BENCHMARKS" = "ON" ]; then \
      set -euxo pipefail && \
      # Add NVIDIA CUDA repository with proper GPG key
      dnf install -y dnf-plugins-core && \
      dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel8/x86_64/cuda-rhel8.repo && \
      # Import NVIDIA GPG key
      rpm --import https://developer.download.nvidia.com/compute/cuda/repos/rhel8/x86_64/D42D0685.pub && \
      # Install nsys from CUDA repository
      dnf install -y nsight-systems && \
      # Verify nsys installation
      which nsys && nsys --version; \
    else \
      echo "Skipping nsys installation (VELOX_ENABLE_BENCHMARKS=OFF)"; \
    fi

# Copy sccache setup script (if sccache enabled)
COPY velox-testing/velox/docker/sccache/sccache_setup.sh /sccache_setup.sh
RUN if [ "$ENABLE_SCCACHE" = "ON" ]; then chmod +x /sccache_setup.sh; fi

# Copy sccache auth files (note source of copy must be within the docker build context)
COPY velox-testing/velox/docker/sccache/sccache_auth/ /sccache_auth/

# Create build directory that will be cached in Docker layers
RUN mkdir -p ${BUILD_BASE_DIR}

# Build reproducer first (fail fast) - this layer will be cached
RUN --mount=type=bind,source=velox,target=/workspace/velox,ro \
    set -euxo pipefail && \
    # Configure sccache if enabled
    if [ "$ENABLE_SCCACHE" = "ON" ]; then \
      /sccache_setup.sh && \
      EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache" && \
      echo "sccache distributed status:" && \
      sccache --dist-status && \
      echo "Pre-build sccache (zeroed out) statistics:" && \
      sccache --show-stats; \
    fi && \
    # Configure CMake
    make cmake BUILD_DIR="${BUILD_TYPE}" BUILD_TYPE="${BUILD_TYPE}" EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS}" BUILD_BASE_DIR="${BUILD_BASE_DIR}" && \
    # Build ONLY the benchmark first (fail fast)
    echo "Building benchmark first for fast failure detection..." && \
    cd ${BUILD_BASE_DIR}/${BUILD_TYPE} && \
    ninja velox_cudf_tpch_benchmark

# Build everything else - this layer will be cached separately
# RUN --mount=type=bind,source=velox,target=/workspace/velox,ro \
#     set -euxo pipefail && \
#     # Configure sccache if enabled
#     if [ "$ENABLE_SCCACHE" = "ON" ]; then \
#       /sccache_setup.sh && \
#       EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache" && \
#       echo "sccache distributed status:" && \
#       sccache --dist-status && \
#       echo "Pre-build sccache (zeroed out) statistics:" && \
#       sccache --show-stats; \
#     fi && \
#     # Build everything else
#     make build BUILD_DIR="${BUILD_TYPE}" BUILD_BASE_DIR="${BUILD_BASE_DIR}" && \
#     # Show final sccache stats if enabled
#     if [ "$ENABLE_SCCACHE" = "ON" ]; then \
#       echo "Post-build sccache statistics:" && \
#       sccache --show-stats; \
#     fi
