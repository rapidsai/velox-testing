ARG TARGETARCH

# Install latest ninja
FROM alpine:latest AS ninja-amd64
RUN apk add --no-cache unzip
ADD https://github.com/ninja-build/ninja/releases/latest/download/ninja-linux.zip /tmp

FROM alpine:latest AS ninja-arm64
RUN apk add --no-cache unzip
ADD https://github.com/ninja-build/ninja/releases/latest/download/ninja-linux-aarch64.zip /tmp
RUN mv /tmp/ninja-linux-aarch64.zip /tmp/ninja-linux.zip

FROM ninja-${TARGETARCH} AS ninja
RUN unzip -d /usr/bin -o /tmp/ninja-linux.zip

FROM ghcr.io/facebookincubator/velox-dev:adapters
ARG TARGETARCH

# Do this separate so changing unrelated build args doesn't invalidate nsys installation layer
ARG VELOX_ENABLE_BENCHMARKS=ON

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
    simdjson_SOURCE=BUNDLED \
    CUDA_VERSION=${CUDA_VERSION} \
    TREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS} \
    MAKEFLAGS="NUM_THREADS=${NUM_THREADS}" \
    CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES} \
    CUDA_COMPILER=/usr/local/cuda-${CUDA_VERSION}/bin/nvcc \
    CUDA_FLAGS="-ccbin /opt/rh/gcc-toolset-14/root/usr/bin" \
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
    LD_LIBRARY_PATH="${BUILD_BASE_DIR}/${BUILD_TYPE}/lib:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/cudf-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rmm-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/rapids_logger-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/kvikio-build:\
${BUILD_BASE_DIR}/${BUILD_TYPE}/_deps/nvcomp_proprietary_binary-src/lib64" \
    ENABLE_SCCACHE="${ENABLE_SCCACHE}" \
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
    SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE=true

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

# Build into ${BUILD_BASE_DIR}
RUN \
    # Mount velox source dir
    --mount=type=bind,source=velox,target=/workspace/velox,ro \
    # Mount sccache preprocessor and toolchain caches
    --mount=type=cache,target=/root/.cache/sccache/preprocessor \
    --mount=type=cache,target=/root/.cache/sccache-dist-client \
    # Mount sccache auth secrets
    --mount=type=secret,id=github_token,env=SCCACHE_DIST_AUTH_TOKEN \
    --mount=type=secret,id=aws_credentials,target=/root/.aws/credentials \
    # Mount sccache setup script
    --mount=type=bind,source=velox-testing/velox/docker/sccache/sccache_setup.sh,target=/sccache_setup.sh,ro \
<<EOF
set -euxo pipefail;

# Enable gcc-toolset-14 and set compilers
# Reference: https://github.com/facebookincubator/velox/pull/15427
source /opt/rh/gcc-toolset-14/enable;
export CC=gcc CXX=g++;
# Verify gcc version
echo "Using GCC version:";
gcc --version | head -1;

# Install and configure sccache if enabled
if [ "$ENABLE_SCCACHE" = "ON" ]; then
  # Run sccache setup script
  bash /sccache_setup.sh;
  # Add sccache CMake flags
  EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache";
  export NVCC_APPEND_FLAGS="${NVCC_APPEND_FLAGS:+$NVCC_APPEND_FLAGS }-t=100";
fi

if test -n "${MAX_HIGH_MEM_JOBS:-}"; then
  MAKEFLAGS="${MAKEFLAGS} MAX_HIGH_MEM_JOBS=${MAX_HIGH_MEM_JOBS}";
fi
if test -n "${MAX_LINK_JOBS:-}"; then
  MAKEFLAGS="${MAKEFLAGS} MAX_LINK_JOBS=${MAX_LINK_JOBS}";
fi

# Disable sccache-dist for CMake configuration's test compiles
SCCACHE_NO_DIST_COMPILE=1 \
make cmake BUILD_DIR="${BUILD_TYPE}" BUILD_TYPE="${BUILD_TYPE}" EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS}" BUILD_BASE_DIR="${BUILD_BASE_DIR}";

# Run the build with timings
time make build BUILD_DIR="${BUILD_TYPE}" BUILD_BASE_DIR="${BUILD_BASE_DIR}";

# Show final sccache stats if enabled
if [ "$ENABLE_SCCACHE" = "ON" ]; then
  echo "Post-build sccache statistics:";
  sccache --show-stats;
fi

EOF
