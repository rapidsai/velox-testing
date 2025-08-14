FROM ghcr.io/facebookincubator/velox-dev:adapters

# Build-time configuration, these may be overridden in the docker compose yaml,
# environment variables, or via the docker build command
ARG USE_CLANG=false
ARG NUM_THREADS=8
ARG CUDA_VERSION=12.8
ARG CUDA_ARCHITECTURES=70
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON
ARG CCACHE_DIR=/velox_ccache

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
    CCACHE_DIR=${CCACHE_DIR}

COPY velox-testing/velox/docker/scripts/build_adapters.sh /build_adapters.sh
RUN chmod +x /build_adapters.sh

RUN --mount=type=bind,source=velox,target=/workspace/velox,rw \
    --mount=type=cache,target=/velox_ccache,id=velox-ccache,sharing=locked,rw \
    --mount=type=cache,target=/buildcache,sharing=locked,rw \
   /bin/bash -c "/build_adapters.sh |& tee /workspace/adapters_build.log"