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
  # Run sccache setup script (needs root to place binaries/certs)
  sudo -E bash /sccache_setup.sh;
  # Ensure sccache cache and temp dirs are writable
  export SCCACHE_DIR="${SCCACHE_DIR:-${HOME}/.cache/sccache}";
  export SCCACHE_TMPDIR="${SCCACHE_TMPDIR:-/tmp/.sccache_temp}";
  export SCCACHE_TEMPDIR="${SCCACHE_TEMPDIR:-${SCCACHE_TMPDIR}}";
  mkdir -p "${SCCACHE_DIR}";
  if [ ! -w "${SCCACHE_TMPDIR}" ]; then
    sudo mkdir -p "${SCCACHE_TMPDIR}";
    sudo chown -R "$(id -u):$(id -g)" "${SCCACHE_TMPDIR}";
  fi
  # Enable verbose sccache logging if requested
  if [ -n "${SCCACHE_LOG:-}" ] || [ -n "${SCCACHE_LOG_FILE:-}" ] || [ "${SCCACHE_DEBUG:-0}" = "1" ]; then
    export SCCACHE_LOG="${SCCACHE_LOG:-trace}";
    export SCCACHE_LOG_FILE="${SCCACHE_LOG_FILE:-/tmp/sccache.log}";
    log_dir="$(dirname "${SCCACHE_LOG_FILE}")";
    if [ ! -d "${log_dir}" ]; then
      mkdir -p "${log_dir}" || sudo mkdir -p "${log_dir}";
      sudo chown -R "$(id -u):$(id -g)" "${log_dir}" || true;
    fi
    if [ -e "${SCCACHE_LOG_FILE}" ] && [ ! -w "${SCCACHE_LOG_FILE}" ]; then
      sudo rm -f "${SCCACHE_LOG_FILE}";
    fi
    # Restart server so it picks up env vars. Prefer running as root for S3 creds.
    sccache_cmd="sccache";
    if [ -n "${SCCACHE_BUCKET:-}" ] && [ "${SCCACHE_S3_NO_CREDENTIALS:-false}" != "true" ]; then
      export AWS_SHARED_CREDENTIALS_FILE="${AWS_SHARED_CREDENTIALS_FILE:-/root/.aws/credentials}";
      sccache_cmd="sudo -E sccache";
      export SCCACHE_DIR="/root/.cache/sccache";
      sudo mkdir -p "${SCCACHE_DIR}" "${SCCACHE_TMPDIR}";
      export SCCACHE_LOG_FILE="${SCCACHE_DIR}/sccache.log";
    fi
    if ! touch "${SCCACHE_LOG_FILE}" 2>/dev/null; then
      sudo touch "${SCCACHE_LOG_FILE}";
      sudo chown "$(id -u):$(id -g)" "${SCCACHE_LOG_FILE}" || true;
    fi
    ${sccache_cmd} --stop-server || true;
    ${sccache_cmd} --start-server;
  fi
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
(
  cd /workspace/velox
  build_dir="${BUILD_BASE_DIR}/${BUILD_TYPE}"
  cache_file="${build_dir}/CMakeCache.txt"

  # Skip reconfigure if CMakeCache.txt already exists unless FORCE_CMAKE=1
  if [[ "${FORCE_CMAKE:-0}" != "1" && -f "${cache_file}" ]]; then
    echo "CMake cache found at ${cache_file}; skipping configure (set FORCE_CMAKE=1 to reconfigure)"
  else
    SCCACHE_NO_DIST_COMPILE=1 \
    make cmake BUILD_DIR="${BUILD_TYPE}" BUILD_TYPE="${BUILD_TYPE}" EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS}" BUILD_BASE_DIR="${BUILD_BASE_DIR}";
  fi

  # Run the build with timings
  time make build BUILD_DIR="${BUILD_TYPE}" BUILD_BASE_DIR="${BUILD_BASE_DIR}";
)

# Show final sccache stats if enabled
if [ "$ENABLE_SCCACHE" = "ON" ]; then
  echo "Post-build sccache statistics:";
  sccache --show-stats;
fi

# Rewrite compile_commands.json paths to host-absolute locations for ccls/IDE use
BUILD_DIR_PATH="${BUILD_BASE_DIR}/${BUILD_TYPE}"
CCDB="${BUILD_DIR_PATH}/compile_commands.json"
HOST_SRC_ABS="${HOST_VELOX_ABS:-/workspace/velox}"
HOST_BUILD_ABS="${BUILD_BASE_DIR}"
if [ -f "${CCDB}" ]; then
  CCDB="${CCDB}" HOST_SRC_ABS="${HOST_SRC_ABS}" HOST_BUILD_ABS="${HOST_BUILD_ABS}" python - <<'PYCODE'
import json, os, sys
ccdb = os.environ["CCDB"]
src_abs = os.environ["HOST_SRC_ABS"]
bld_abs = os.environ["HOST_BUILD_ABS"]
with open(ccdb, "r", encoding="utf-8") as f:
    data = json.load(f)
changed = False
for entry in data:
    for k in ("directory", "file", "command", "arguments"):
        if k in entry:
            val = entry[k]
            if isinstance(val, str):
                new = val.replace("/workspace/velox", src_abs).replace("/opt/velox-build", bld_abs)
                if new != val:
                    entry[k] = new
                    changed = True
            elif isinstance(val, list):
                new_list = []
                for item in val:
                    if isinstance(item, str):
                        new_item = item.replace("/workspace/velox", src_abs).replace("/opt/velox-build", bld_abs)
                        new_list.append(new_item)
                        if new_item != item:
                            changed = True
                    else:
                        new_list.append(item)
                entry[k] = new_list
if changed:
    with open(ccdb, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
PYCODE
  echo "Rewrote paths in ${CCDB} to ${HOST_SRC_ABS} and ${HOST_BUILD_ABS}"
fi

#EOF
