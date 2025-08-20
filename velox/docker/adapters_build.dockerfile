FROM ghcr.io/facebookincubator/velox-dev:adapters

# Build-time configuration, these may be overridden in the docker compose yaml,
# environment variables, or via the docker build command
ARG NUM_THREADS=8
ARG CUDA_VERSION=12.8
ARG CUDA_ARCHITECTURES=70
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON
ARG SHOW_CCACHE_STATS=OFF

# Environment mirroring upstream CI defaults and incorporating build args
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    GTest_SOURCE=BUNDLED \
    cudf_SOURCE=BUNDLED \
    faiss_SOURCE=BUNDLED \
    CUDA_VERSION=${CUDA_VERSION} \
    TREAT_WARNINGS_AS_ERRORS=0 \
    MAKEFLAGS="NUM_THREADS=${NUM_THREADS} MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=4" \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    CUDA_COMPILER=/usr/local/cuda-${CUDA_VERSION}/bin/nvcc \
    CUDA_FLAGS="-ccbin /opt/rh/gcc-toolset-12/root/usr/bin" \
    EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_BENCHMARKS=ON \
                      -DVELOX_ENABLE_EXAMPLES=ON \
                      -DVELOX_ENABLE_ARROW=ON \
                      -DVELOX_ENABLE_GEO=ON \
                      -DVELOX_ENABLE_PARQUET=ON \
                      -DVELOX_ENABLE_HDFS=ON \
                      -DVELOX_ENABLE_S3=ON \
                      -DVELOX_ENABLE_GCS=ON \
                      -DVELOX_ENABLE_ABFS=ON \
                      -DVELOX_ENABLE_WAVE=ON \
                      -DVELOX_MONO_LIBRARY=ON \
                      -DVELOX_BUILD_SHARED=ON \
                      -DVELOX_ENABLE_CUDF=${BUILD_WITH_VELOX_ENABLE_CUDF} \
                      -DVELOX_ENABLE_FAISS=ON"


WORKDIR /workspace/velox

# Print environment variables for debugging
RUN printenv | sort

RUN --mount=type=bind,source=velox,target=/workspace/velox,ro \
    --mount=type=cache,target=/buildcache,sharing=locked,rw \
    # Set up shell environment
    set -euxo pipefail && \
    # Zero ccache stats if enabled
    if [ "${SHOW_CCACHE_STATS}" = "ON" ]; then ccache -sz; fi && \
    # Build release into /buildcache
    make release EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS[*]}" BUILD_BASE_DIR="/buildcache" && \
    # Show ccache stats if enabled
    if [ "${SHOW_CCACHE_STATS}" = "ON" ]; then ccache -s; fi && \
    # Copy release to /opt/velox-build/release
    mkdir -p /opt/velox-build/release && \
    cp -a "/buildcache/release/." "/opt/velox-build/release/"
