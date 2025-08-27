FROM ghcr.io/facebookincubator/velox-dev:adapters

# Build-time configuration, these may be overridden in the docker compose yaml,
# environment variables, or via the docker build command
ARG NUM_THREADS=8
ARG CUDA_VERSION=12.8
ARG CUDA_ARCHITECTURES=70
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON
ARG VELOX_ENABLE_BENCHMARKS=ON

# Environment mirroring upstream CI defaults and incorporating build args
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    GTest_SOURCE=BUNDLED \
    cudf_SOURCE=BUNDLED \
    faiss_SOURCE=BUNDLED \
    CUDA_VERSION=${CUDA_VERSION} \
    MAKEFLAGS="NUM_THREADS=${NUM_THREADS} MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=4" \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    CUDA_COMPILER=/usr/local/cuda-${CUDA_VERSION}/bin/nvcc \
    CUDA_FLAGS="-ccbin /opt/rh/gcc-toolset-12/root/usr/bin" \
    EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_BENCHMARKS=${VELOX_ENABLE_BENCHMARKS} \
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
                      -DVELOX_ENABLE_FAISS=ON" \
    LD_LIBRARY_PATH="/opt/velox-build/release/lib:\
/opt/velox-build/release/_deps/cudf-build:\
/opt/velox-build/release/_deps/rapids_logger-build:\
/opt/velox-build/release/_deps/kvikio-build:\
/opt/velox-build/release/_deps/nvcomp_proprietary_binary-src/lib64"



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

RUN --mount=type=bind,source=velox,target=/workspace/velox,ro \
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
