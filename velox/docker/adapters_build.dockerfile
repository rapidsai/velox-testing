FROM ghcr.io/facebookincubator/velox-dev:adapters

# Build-time configuration, these may be overridden in the docker compose yaml,
# environment variables, or via the docker build command
ARG USE_CLANG=false
ARG NUM_THREADS=8
ARG CUDA_VERSION=12.8
ARG CUDA_ARCHITECTURES=70
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON

# Environment mirroring upstream CI defaults
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    GTest_SOURCE=BUNDLED \
    cudf_SOURCE=BUNDLED \
    faiss_SOURCE=BUNDLED \
    USE_CLANG=${USE_CLANG} \
    CUDA_VERSION=${CUDA_VERSION} \
    MAKEFLAGS="NUM_THREADS=${NUM_THREADS} MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=4" \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    CUDA_COMPILER=/usr/local/cuda-${CUDA_VERSION}/bin/nvcc \
    BUILD_WITH_VELOX_ENABLE_CUDF=${BUILD_WITH_VELOX_ENABLE_CUDF} \
    CUDA_FLAGS="-ccbin /opt/rh/gcc-toolset-12/root/usr/bin" \
    EXTRA_CMAKE_FLAG_BENCHMARKS="-DVELOX_ENABLE_BENCHMARKS=ON" \
    EXTRA_CMAKE_FLAG_EXAMPLES="-DVELOX_ENABLE_EXAMPLES=ON" \
    EXTRA_CMAKE_FLAG_ARROW="-DVELOX_ENABLE_ARROW=ON" \
    EXTRA_CMAKE_FLAG_GEO="-DVELOX_ENABLE_GEO=ON" \
    EXTRA_CMAKE_FLAG_PARQUET="-DVELOX_ENABLE_PARQUET=ON" \
    EXTRA_CMAKE_FLAG_HDFS="-DVELOX_ENABLE_HDFS=ON" \
    EXTRA_CMAKE_FLAG_S3="-DVELOX_ENABLE_S3=ON" \
    EXTRA_CMAKE_FLAG_GCS="-DVELOX_ENABLE_GCS=ON" \
    EXTRA_CMAKE_FLAG_ABFS="-DVELOX_ENABLE_ABFS=ON" \
    EXTRA_CMAKE_FLAG_WAVE="-DVELOX_ENABLE_WAVE=ON" \
    EXTRA_CMAKE_FLAG_MONO_LIBRARY="-DVELOX_MONO_LIBRARY=ON" \
    EXTRA_CMAKE_FLAG_BUILD_SHARED="-DVELOX_BUILD_SHARED=ON" \
    EXTRA_CMAKE_FLAG_CUDF="-DVELOX_ENABLE_CUDF=${BUILD_WITH_VELOX_ENABLE_CUDF}" \
    EXTRA_CMAKE_FLAG_FAISS="-DVELOX_ENABLE_FAISS=ON"


WORKDIR /workspace/velox

# Print environment variables for debugging
RUN printenv | sort

RUN --mount=type=bind,source=velox,target=/workspace/velox,rw \
    --mount=type=cache,target=/buildcache,sharing=locked,rw \
    # Set up shell environment
    set -euxo pipefail && \
    # Zero ccache stats if available (uncomment when ccache is available)
    #ccache -sz && \
    # Build release into /buildcache
    make release EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS[*]}" BUILD_BASE_DIR="/buildcache" && \
    # Show ccache stats (uncomment when ccache is available)
    #ccache -s && \
    # Copy release to /opt/velox-build/release
    mkdir -p /opt/velox-build/release && \
    cp -a "/buildcache/release/." "/opt/velox-build/release/"