#!/usr/bin/env bash
#
# Development build helper intended to be run *inside* the velox-adapters-dev
# container. This is adapted from the commented BuildKit RUN block in
# velox/docker/adapters_dev.dockerfile.

# Build into ${BUILD_BASE_DIR}
#RUN \
#    # Mount velox source dir
#    --mount=type=bind,source=velox,target=/workspace/velox,ro \
#    # Mount sccache preprocessor and toolchain caches
#    --mount=type=cache,target=/root/.cache/sccache/preprocessor \
#    --mount=type=cache,target=/root/.cache/sccache-dist-client \
#    # Mount sccache auth secrets
#    --mount=type=secret,id=github_token,env=SCCACHE_DIST_AUTH_TOKEN \
#    --mount=type=secret,id=aws_credentials,target=/root/.aws/credentials \
#    # Mount sccache setup script
#    --mount=type=bind,source=velox-testing/velox/docker/sccache/sccache_setup.sh,target=/sccache_setup.sh,ro \
#<<EOF
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

#EOF
