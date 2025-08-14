#!/usr/bin/env bash
set -euxo pipefail

export CCACHE_DIR VELOX_DEPENDENCY_SOURCE GTest_SOURCE cudf_SOURCE \
  faiss_SOURC CUDA_ARCHITECTURES


REPO_DIR="/workspace/velox"
cd "${REPO_DIR}"

# Ensure _build is a symlink to /buildcache for build caching, if it exists
# error out and inform user to remove it
if [[ -e "_build" ]]; then
  if [[ ! -L "_build" || "$(readlink -f _build)" != "/buildcache" ]]; then
    echo "ERROR: the path '_build' exists in the velox repo checkout, please remove it and try rebuilding." >&2
    exit 1
  fi
else
  ln -s /buildcache _build
fi

# Show environment for debugging
printenv | sort || true

# Zero ccache stats if available
ccache -sz || true

# Build environment similar to upstream CI
export MAKEFLAGS="NUM_THREADS=${NUM_THREADS} MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=4"
export CUDA_COMPILER="/usr/local/cuda-${CUDA_VERSION}/bin/nvcc"
CUDA_FLAGS="-ccbin /opt/rh/gcc-toolset-12/root/usr/bin"

# Compose EXTRA_CMAKE_FLAGS mirroring upstream CI
EXTRA_CMAKE_FLAGS=(
  "-DVELOX_ENABLE_BENCHMARKS=ON"
  "-DVELOX_ENABLE_EXAMPLES=ON"
  "-DVELOX_ENABLE_ARROW=ON"
  "-DVELOX_ENABLE_GEO=ON"
  "-DVELOX_ENABLE_PARQUET=ON"
  "-DVELOX_ENABLE_HDFS=ON"
  "-DVELOX_ENABLE_S3=ON"
  "-DVELOX_ENABLE_GCS=ON"
  "-DVELOX_ENABLE_ABFS=ON"
  "-DVELOX_ENABLE_WAVE=ON"
  "-DVELOX_MONO_LIBRARY=ON"
  "-DVELOX_BUILD_SHARED=ON"
)

# Mirror upstream CI handling of CLANG
if [[ "${USE_CLANG}" == "true" ]]; then
  # Install clang15 toolchain if image provides the helper (as in upstream CI)
  if [[ -x scripts/setup-centos9.sh ]]; then
    scripts/setup-centos9.sh install_clang15 || true
  fi
  export CC=/usr/bin/clang-15
  export CXX=/usr/bin/clang++-15
  CUDA_FLAGS="-ccbin /usr/lib64/llvm15/bin/clang++-15"
else
  EXTRA_CMAKE_FLAGS+=("-DVELOX_ENABLE_CUDF=${BUILD_WITH_VELOX_ENABLE_CUDF}")
  EXTRA_CMAKE_FLAGS+=("-DVELOX_ENABLE_FAISS=ON")
fi

export CUDA_FLAGS

# Build release
make release EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS[*]}"

# Show ccache stats
if command -v ccache >/dev/null 2>&1; then
  ccache -s || true
fi

# Copy built artifacts to /opt/velox-build/release inside the container
mkdir -p /opt/velox-build/release
cp -a "${REPO_DIR}/_build/release/." "/opt/velox-build/release/"
