# Velox development container based on RAPIDS devcontainers
#
# Uses RAPIDS devcontainer base image (Ubuntu) for compatibility with
# RAPIDS build utilities and cuDF development.

ARG CUDA_VERSION=12.9
ARG BASE=rapidsai/devcontainers:latest-cpp-cuda12.9-ucx1.19.0-openmpi5.0.7

FROM ${BASE}

ARG TARGETARCH
ARG CUDA_VERSION

# Install velox build dependencies
RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    # GCC 14 (avoids GCC 13 false-positive -Wstringop-overflow in fmt)
    gcc-14 \
    g++-14 \
    # Velox dependencies not in RAPIDS base
    libc-ares-dev \
    libboost-all-dev \
    libdouble-conversion-dev \
    libevent-dev \
    libfmt-dev \
    libgflags-dev \
    libgoogle-glog-dev \
    liblz4-dev \
    liblzo2-dev \
    libprotobuf-dev \
    libprotoc-dev \
    protobuf-compiler \
    librdkafka-dev \
    libre2-dev \
    libsnappy-dev \
    libsodium-dev \
    libstemmer-dev \
    libthrift-dev \
    libzstd-dev \
    # Build tools
    bison \
    flex \
    ninja-build \
    ccache \
    && rm -rf /tmp/* /var/tmp/* /var/cache/apt/* /var/lib/apt/lists/*

# Set GCC 14 as default (GCC 13 has false-positive -Wstringop-overflow in fmt)
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 140 \
                        --slave /usr/bin/g++ g++ /usr/bin/g++-14 \
                        --slave /usr/bin/gcov gcov /usr/bin/gcov-14 && \
    update-alternatives --set gcc /usr/bin/gcc-14

# Create build directories
RUN mkdir -p /opt/velox-build /opt/presto-build && \
    chown -R coder:coder /opt/velox-build /opt/presto-build

# Install velox helper scripts
COPY velox-testing/scripts/devcontainer/* /usr/local/bin/
RUN chmod 755 /usr/local/bin/build-* /usr/local/bin/configure-* \
              /usr/local/bin/test-* /usr/local/bin/clean-* /usr/local/bin/post-create

# Environment for velox builds
# VELOX_DEPENDENCY_SOURCE is read from env by CMake
# Individual _SOURCE overrides must be passed as -D flags in configure-velox
ENV VELOX_DEPENDENCY_SOURCE=SYSTEM \
    CUDA_VERSION=${CUDA_VERSION} \
    CUDAARCHS="RAPIDS" \
    DEFAULT_VIRTUAL_ENV=rapids \
    PYTHON_PACKAGE_MANAGER=pip

# Python environment
ENV PYTHONSAFEPATH="1" \
    PYTHONUNBUFFERED="1" \
    PYTHONDONTWRITEBYTECODE="1"

# Persist bash history to cache
ENV HISTFILE="/home/coder/.cache/._bash_history"

###
# sccache configuration
###
ENV AWS_ROLE_ARN="arn:aws:iam::279114543810:role/nv-gha-token-sccache-devs" \
    SCCACHE_REGION="us-east-2" \
    SCCACHE_BUCKET="rapids-sccache-devs" \
    SCCACHE_S3_USE_PREPROCESSOR_CACHE_MODE=true \
    SCCACHE_IDLE_TIMEOUT=0

###
# sccache-dist configuration
###
# Enable sccache-dist by default
ENV DEVCONTAINER_UTILS_ENABLE_SCCACHE_DIST=1
# Compile locally if max retries exceeded
ENV SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE=true
# Retry transient errors 4 times (for a total of 5 attempts)
ENV SCCACHE_DIST_MAX_RETRIES=4
# 1hr 59min (to accommodate debug builds)
ENV SCCACHE_DIST_REQUEST_TIMEOUT=7140
ENV SCCACHE_DIST_URL="https://${TARGETARCH}.linux.sccache.rapids.nvidia.com"

# Build as much in parallel as possible
ENV INFER_NUM_DEVICE_ARCHITECTURES=1 \
    MAX_DEVICE_OBJ_TO_COMPILE_IN_PARALLEL=20

# Add velox environment to bashrc
RUN echo "export CUDA_VERSION=${CUDA_VERSION}" >> /home/coder/.bashrc && \
    echo 'export CMAKE_C_COMPILER_LAUNCHER=sccache' >> /home/coder/.bashrc && \
    echo 'export CMAKE_CXX_COMPILER_LAUNCHER=sccache' >> /home/coder/.bashrc && \
    echo 'export CMAKE_CUDA_COMPILER_LAUNCHER=sccache' >> /home/coder/.bashrc

USER coder
WORKDIR /home/coder
