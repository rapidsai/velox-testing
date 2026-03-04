# check=skip=SecretsUsedInArgOrEnv
#
# Velox build image.
#
# Compiles Velox from source on top of a Velox deps image.
# Adapted from velox/docker/adapters_build.dockerfile to use COPY instead of
# bind mounts so it works with docker/build-push-action.
#
# Build context must contain:
#   velox/                  – Velox source checkout
#   velox-testing/          – velox-testing repository checkout
#
ARG DEPS_IMAGE=ghcr.io/facebookincubator/velox-dev:adapters
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

FROM ${DEPS_IMAGE}
ARG TARGETARCH

ARG VELOX_ENABLE_BENCHMARKS=OFF

# Install NVIDIA Nsight Systems (nsys) for profiling - only if benchmarks are enabled
RUN \
<<EOF
if [ "$VELOX_ENABLE_BENCHMARKS" = "ON" ]; then
      set -euxo pipefail
      ARCH=$(uname -m)
      if [ "$ARCH" = "aarch64" ]; then
        CUDA_ARCH="sbsa"
      else
        CUDA_ARCH="x86_64"
      fi
      dnf install -y dnf-plugins-core
      dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel9/${CUDA_ARCH}/cuda-rhel9.repo
      rpm --import https://developer.download.nvidia.com/compute/cuda/repos/rhel9/${CUDA_ARCH}/D42D0685.pub
      dnf install -y nsight-systems
      which nsys && nsys --version
    else
      echo "Skipping nsys installation (VELOX_ENABLE_BENCHMARKS=OFF)"
    fi
EOF

ARG NUM_THREADS=8
ARG MAX_HIGH_MEM_JOBS=4
ARG MAX_LINK_JOBS=4
ARG CUDA_VERSION=12.9
ARG CUDA_ARCHITECTURES=70
ARG BUILD_WITH_VELOX_ENABLE_CUDF=ON
ARG BUILD_WITH_VELOX_ENABLE_WAVE=OFF
ARG TREAT_WARNINGS_AS_ERRORS=0
ARG BUILD_BASE_DIR=/opt/velox-build
ARG BUILD_TYPE=release
ARG ENABLE_SCCACHE=ON
ARG SCCACHE_SERVER_LOG="sccache=info"
ARG SCCACHE_VERSION=latest
ARG UPDATE_NINJA=true
ARG SCCACHE_RECACHE
ARG SCCACHE_NO_CACHE
ARG SCCACHE_NO_DIST_COMPILE

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
    SCCACHE_IDLE_TIMEOUT=0 \
    SCCACHE_DIST_AUTH_TYPE=token \
    SCCACHE_DIST_REQUEST_TIMEOUT=7140 \
    SCCACHE_DIST_SCHEDULER_URL="https://${TARGETARCH}.linux.sccache.rapids.nvidia.com" \
    SCCACHE_DIST_MAX_RETRIES=4 \
    SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE=true

WORKDIR /workspace/velox

# Install latest ninja (conditionally based on UPDATE_NINJA)
RUN --mount=from=ninja,source=/usr/bin/ninja,target=/tmp/ninja \
    if [ "$UPDATE_NINJA" = "true" ]; then \
      echo "Installing ninja..."; \
      cp /tmp/ninja /usr/bin/ninja && chmod +x /usr/bin/ninja; \
    else \
      echo "Skipping ninja installation"; \
    fi

# Copy sccache setup script
COPY velox-testing/scripts/sccache/sccache_setup.sh /sccache_setup.sh

# Copy Velox source
COPY velox/ /workspace/velox/

# Build into ${BUILD_BASE_DIR}
RUN \
    --mount=type=cache,target=/root/.cache/sccache/preprocessor \
    --mount=type=cache,target=/root/.cache/sccache-dist-client \
    --mount=type=secret,id=github_token,env=SCCACHE_DIST_AUTH_TOKEN \
    --mount=type=secret,id=aws_credentials,target=/root/.aws/credentials \
<<EOF
set -euxo pipefail;

source /opt/rh/gcc-toolset-14/enable;
export CC=gcc CXX=g++;
echo "Using GCC version:";
gcc --version | head -1;

if [ "$ENABLE_SCCACHE" = "ON" ]; then
  if [ -n "${SCCACHE_NO_DIST_COMPILE:-}" ]; then
    export SCCACHE_NO_DIST_COMPILE=1;
  fi
  bash /sccache_setup.sh;
  sccache --zero-stats;
  EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache";
  export NVCC_APPEND_FLAGS="${NVCC_APPEND_FLAGS:+$NVCC_APPEND_FLAGS }-t=100";
fi

if test -n "${MAX_HIGH_MEM_JOBS:-}"; then
  MAKEFLAGS="${MAKEFLAGS} MAX_HIGH_MEM_JOBS=${MAX_HIGH_MEM_JOBS}";
fi
if test -n "${MAX_LINK_JOBS:-}"; then
  MAKEFLAGS="${MAKEFLAGS} MAX_LINK_JOBS=${MAX_LINK_JOBS}";
fi

SCCACHE_NO_DIST_COMPILE=1 \
make cmake BUILD_DIR="${BUILD_TYPE}" BUILD_TYPE="${BUILD_TYPE}" EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS}" BUILD_BASE_DIR="${BUILD_BASE_DIR}";

time make build BUILD_DIR="${BUILD_TYPE}" BUILD_BASE_DIR="${BUILD_BASE_DIR}";

if [ "$ENABLE_SCCACHE" = "ON" ]; then
  echo "Post-build sccache statistics:";
  sccache --show-stats;
fi

EOF
