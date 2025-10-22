FROM velox-adapters-deps:centos9

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

# Environment mirroring upstream CI defaults and incorporating build args
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    GTest_SOURCE=BUNDLED \
    cudf_SOURCE=BUNDLED \
    simdjson_SOURCE=BUNDLED \
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
                      -DVELOX_ENABLE_FAISS=ON" \
    CCACHE_DIR=/ccache

WORKDIR /workspace/velox

# Print environment variables for debugging
RUN printenv | sort

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

# Build using the specified build type and directory
RUN --mount=type=bind,source=velox,target=/workspace/velox,ro \
    --mount=type=cache,target=/ccache \
    set -euxo pipefail && \
    make cmake BUILD_DIR="${BUILD_TYPE}" BUILD_TYPE="${BUILD_TYPE}" EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS[*]}" BUILD_BASE_DIR="${BUILD_BASE_DIR}" && \
    make build BUILD_DIR="${BUILD_TYPE}" BUILD_BASE_DIR="${BUILD_BASE_DIR}"
