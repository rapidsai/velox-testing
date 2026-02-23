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

# Configure ccache if sccache is not enabled
if [ "$ENABLE_SCCACHE" != "ON" ]; then
  if command -v ccache >/dev/null 2>&1; then
    echo "ccache is available: $(ccache --version | head -1)";
    # Ensure env vars are set (may already be set via docker compose environment)
    export CCACHE_DIR="${CCACHE_DIR:-${HOME}/.cache/ccache}";
    export CCACHE_BASEDIR="${CCACHE_BASEDIR:-/workspace/velox}";
    export CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-400G}";
    export CCACHE_COMPRESSLEVEL="${CCACHE_COMPRESSLEVEL:-1}";
    export CCACHE_DEPEND="${CCACHE_DEPEND:-1}";
    export CCACHE_SLOPPINESS="${CCACHE_SLOPPINESS:-include_file_mtime,include_file_ctime,time_macros,pch_defines,system_headers,locale,random_seed,file_stat_matches,modules}";
    # Compiler binary content check (not mtime) - prevents false invalidation after image rebuild
    export CCACHE_COMPILERCHECK="${CCACHE_COMPILERCHECK:-content}";
    # C++20 module flags + diagnostic flags that don't affect codegen
    export CCACHE_IGNOREOPTIONS="${CCACHE_IGNOREOPTIONS:--fmodules-ts -fmodule-mapper=* -fdeps-format=* -fdiagnostics-color=* -fmessage-length=*}";
    mkdir -p "${CCACHE_DIR}";
    # Add ccache CMake flags (mirrors sccache pattern above) so ALL targets
    # (including early-resolved deps like faiss/curl) use ccache
    EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_CUDA_COMPILER_LAUNCHER=ccache";
    echo "Pre-build ccache statistics:";
    ccache -s;
  else
    echo "WARNING: ccache not found in PATH; builds will not be cached.";
  fi
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

# Show final ccache stats if sccache is not active
if [ "$ENABLE_SCCACHE" != "ON" ]; then
  if command -v ccache >/dev/null 2>&1; then
    echo "Post-build ccache statistics:";
    ccache -s;
  fi
fi

# Rewrite compile_commands.json paths to host-absolute locations for ccls/IDE use
BUILD_DIR_PATH="${BUILD_BASE_DIR}/${BUILD_TYPE}"
CCDB="${BUILD_DIR_PATH}/compile_commands.json"
HOST_SRC_ABS="${HOST_VELOX_ABS:-/workspace/velox}"
HOST_BUILD_ABS="${BUILD_BASE_DIR}"
if [ -f "${CCDB}" ]; then
  CCDB="${CCDB}" HOST_SRC_ABS="${HOST_SRC_ABS}" HOST_BUILD_ABS="${HOST_BUILD_ABS}" python - <<'PYCODE'
import json
import os
import shlex
import sys
ccdb = os.environ["CCDB"]
src_abs = os.environ["HOST_SRC_ABS"]
bld_abs = os.environ["HOST_BUILD_ABS"]
force_std_raw = os.environ.get("FORCE_CXX_STANDARD", "").strip()
std_flag = None
if force_std_raw:
    if force_std_raw.startswith("-std="):
        std_flag = force_std_raw
    elif force_std_raw.startswith("c++") or force_std_raw.startswith("gnu++"):
        std_flag = f"-std={force_std_raw}"
    else:
        std_flag = f"-std=c++{force_std_raw}"

def replace_paths(value: str) -> str:
    return value.replace("/workspace/velox", src_abs).replace("/opt/velox-build", bld_abs)

def ensure_std(args):
    if not std_flag:
        return args, False
    changed = False
    has_std = False
    new_args = []
    for arg in args:
        if isinstance(arg, str) and arg.startswith("-std="):
            has_std = True
            if arg != std_flag:
                new_args.append(std_flag)
                changed = True
            else:
                new_args.append(arg)
        else:
            new_args.append(arg)
    if not has_std:
        new_args.append(std_flag)
        changed = True
    return new_args, changed

with open(ccdb, "r", encoding="utf-8") as f:
    data = json.load(f)
changed = False
for entry in data:
    if "directory" in entry and isinstance(entry["directory"], str):
        new = replace_paths(entry["directory"])
        if new != entry["directory"]:
            entry["directory"] = new
            changed = True
    if "file" in entry and isinstance(entry["file"], str):
        new = replace_paths(entry["file"])
        if new != entry["file"]:
            entry["file"] = new
            changed = True
    if "command" in entry and isinstance(entry["command"], str):
        cmd = entry["command"]
        cmd_replaced = replace_paths(cmd)
        cmd_changed = cmd_replaced != cmd
        cmd = cmd_replaced
        std_changed = False
        if std_flag:
            try:
                args = shlex.split(cmd)
            except ValueError:
                args = None
            if args is not None:
                args, std_changed = ensure_std(args)
                if std_changed:
                    cmd = shlex.join(args)
            elif "-std=" not in cmd and std_flag not in cmd:
                cmd = f"{cmd} {std_flag}"
                std_changed = True
        if cmd_changed or std_changed:
            entry["command"] = cmd
            changed = True
    if "arguments" in entry and isinstance(entry["arguments"], list):
        new_args = []
        args_changed = False
        for arg in entry["arguments"]:
            if isinstance(arg, str):
                new_arg = replace_paths(arg)
                if new_arg != arg:
                    args_changed = True
                new_args.append(new_arg)
            else:
                new_args.append(arg)
        new_args, std_changed = ensure_std(new_args)
        if args_changed or std_changed:
            entry["arguments"] = new_args
            changed = True
if changed:
    with open(ccdb, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
PYCODE
  if [ -n "${FORCE_CXX_STANDARD:-}" ]; then
    echo "Rewrote paths in ${CCDB} and enforced -std=c++${FORCE_CXX_STANDARD}"
  else
    echo "Rewrote paths in ${CCDB} to ${HOST_SRC_ABS} and ${HOST_BUILD_ABS}"
  fi
fi

#EOF
