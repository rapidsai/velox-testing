#!/usr/bin/env bash
set -euxo pipefail

CCACHE_DIR="/ccache"


# Mirror key upstream CI environment defaults; allow overrides via env.
: "${VELOX_DEPENDENCY_SOURCE:=SYSTEM}"
: "${GTest_SOURCE:=BUNDLED}"
: "${cudf_SOURCE:=BUNDLED}"
: "${CUDA_VERSION:=12.8}"
: "${faiss_SOURCE:=BUNDLED}"
: "${USE_CLANG:=false}"
: "${ENABLE_TESTS:=true}"
: "${BUILD_WITH_VELOX_ENABLE_CUDF:=ON}"

# Optional: threads and CUDA arch override
: "${NUM_THREADS:=$(nproc)}"
: "${CUDA_ARCHITECTURES:=70}"

export CCACHE_DIR VELOX_DEPENDENCY_SOURCE GTest_SOURCE cudf_SOURCE CUDA_VERSION \
  faiss_SOURCE USE_CLANG ENABLE_TESTS BUILD_WITH_VELOX_ENABLE_CUDF \
  CUDA_ARCHITECTURES NUM_THREADS


# Ensure we run in the velox repo root (mounted at /workspace/velox)
REPO_DIR="/workspace/velox"


if [[ ! -d "$REPO_DIR" || ! -f "$REPO_DIR/Makefile" ]]; then
  echo "ERROR: Expected Velox checkout at build context path ./velox (same level as velox-testing)." >&2
  echo "Please clone velox next to velox-testing:"
  echo "  /path/to/parent/"
  echo "    velox-testing/"
  echo "    velox/" >&2
  exit 1
fi
cd "$REPO_DIR"


echo "VELOX_BUILD_STARTED" > /tmp/build_status

# Ensure _build is a symlink to /buildcache for build caching, if it exists
# error out and inform user to remove it
if [[ -e "_build" ]]; then
  if [[ ! -L "_build" || "$(readlink -f _build)" != "/buildcache" ]]; then
    echo "ERROR: the path '_build' exists, please remove it and try rebuilding." >&2
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
  # Comment below taken from upstream CI:
  # Investigate issues with remote function service: Issue #13897
  EXTRA_CMAKE_FLAGS+=("-DVELOX_ENABLE_REMOTE_FUNCTIONS=ON")
fi

export CUDA_ARCHITECTURES
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

# Skip tests if ENABLE_TESTS is not set to "true"
if [[ "${ENABLE_TESTS:-true}" != "true" ]]; then
  echo "VELOX_TESTS_SKIPPED_AND_BUILD_COMPLETED" > /tmp/build_status
  exit 0
fi

echo "VELOX_TESTS_INCOMPLETE_AND_BUILD_COMPLETED" > /tmp/build_status

# Run tests similar to upstream CI adapters job
export LIBHDFS3_CONF="${REPO_DIR}/scripts/ci/hdfs-client.xml"
(
  cd _build/release
  # Conda activation (image may provide adapters env)
  if [ -f "/opt/miniforge/etc/profile.d/conda.sh" ]; then
    source "/opt/miniforge/etc/profile.d/conda.sh"
    conda activate adapters || true
  fi
  export CLASSPATH=$(/usr/local/hadoop/bin/hdfs classpath --glob || true)

  
  # Run tests with explicit handling to log a warning if tests fail
  echo "Starting tests..."

  set +e
  
  # Run ctest with timeout
  timeout 1800 ctest -j "${NUM_THREADS}" --label-exclude cuda_driver --output-on-failure --no-tests=error --stop-on-failure
  CTEST_EXIT_CODE=$?
  
  if [ $CTEST_EXIT_CODE -ne 0 ]; then
    
    # Kill any remaining processes aggressively, for some reason ctest leaks processes
    pkill -9 -f ctest || true
    
    # Set a flag to indicate that tests failed but build completed
    echo "VELOX_TESTS_FAILED_BUT_BUILD_COMPLETED" > /tmp/build_status
    exit 1
  fi

  echo "VELOX_TESTS_PASSED_AND_BUILD_COMPLETED" > /tmp/build_status

  set -e

)
