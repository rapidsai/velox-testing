#!/bin/bash

set -euo pipefail

PRESTO_EXTRA_CMAKE_FLAGS=${PRESTO_EXTRA_CMAKE_FLAGS:-"-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_CUDF=ON -DVELOX_BUILD_TESTING=OFF -DCMAKE_CUDA_COMPILER_LAUNCHER=ccache -DCMAKE_EXPORT_COMPILE_COMMANDS=ON"}
PRESTO_BUILD_BASE=/workspace/build
PRESTO_BUILD_TYPE=${PRESTO_BUILD_TYPE:-RelWithDebInfo}
PRESTO_BUILD_DIR_NAME=${PRESTO_BUILD_DIR_NAME:-relwithdebinfo}
PRESTO_REBUILD=${PRESTO_REBUILD:-0}
PRESTO_FORCE_REBUILD=${PRESTO_FORCE_REBUILD:-0}
PRESTO_SKIP_SERVER=${PRESTO_SKIP_SERVER:-0}
PRESTO_NUM_THREADS=${NUM_THREADS:-12}
PRESTO_SRC=/workspace/presto/presto-native-execution
PRESTO_ETC_DIR=/opt/presto-server/etc

PRESTO_BIN="${PRESTO_BUILD_BASE}/${PRESTO_BUILD_DIR_NAME}/presto_cpp/main/presto_server"

function log() {
  echo "[presto-dev] $*"
}

function ensure_dir() {
  mkdir -p "$1"
}

ensure_dir "$HOME"
ensure_dir "$HOME/.ccache"
ensure_dir "$HOME/.cache/clangd"

if [[ -f /opt/rh/gcc-toolset-14/enable ]]; then
  # shellcheck disable=SC1091
  source /opt/rh/gcc-toolset-14/enable
fi

function resolve_cuda_archs() {
  if [[ -n "${CUDA_ARCHITECTURES:-}" ]]; then
    echo "$CUDA_ARCHITECTURES"
    return
  fi
  if command -v nvidia-smi >/dev/null 2>&1; then
    local cuda_cap
    cuda_cap=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -n 1 | sed 's/\.//g')
    if [[ -n "$cuda_cap" ]]; then
      echo "$cuda_cap"
      return
    fi
  fi
  echo ""
}

function build_presto_if_needed() {
  ensure_dir "$PRESTO_BUILD_BASE"
  if [[ "$PRESTO_FORCE_REBUILD" == "1" ]]; then
    rm -rf "${PRESTO_BUILD_BASE:?}/${PRESTO_BUILD_DIR_NAME}"
  fi
  if [[ -x "$PRESTO_BIN" && "$PRESTO_FORCE_REBUILD" != "1" && "$PRESTO_REBUILD" != "1" ]]; then
    return 0
  fi

  local cuda_archs
  cuda_archs=$(resolve_cuda_archs)
  local extra_flags="$PRESTO_EXTRA_CMAKE_FLAGS"
  if [[ -n "$cuda_archs" ]]; then
    extra_flags="${extra_flags} -DCMAKE_CUDA_ARCHITECTURES=${cuda_archs}"
  fi

  local build_dir="${PRESTO_BUILD_BASE}/${PRESTO_BUILD_DIR_NAME}"
  if [[ "$PRESTO_REBUILD" == "1" && -f "${build_dir}/CMakeCache.txt" ]]; then
    log "Incremental build (no reconfigure) (${PRESTO_BUILD_TYPE}/${PRESTO_BUILD_DIR_NAME})"
    (cd "$build_dir" && ninja -j "$PRESTO_NUM_THREADS" presto_server)
  else
    log "Configuring + building (${PRESTO_BUILD_TYPE}/${PRESTO_BUILD_DIR_NAME})"
    (cd "$PRESTO_SRC" && \
      make cmake-and-build \
        BUILD_BASE_DIR="$PRESTO_BUILD_BASE" \
        BUILD_DIR="$PRESTO_BUILD_DIR_NAME" \
        BUILD_TYPE="$PRESTO_BUILD_TYPE" \
        NUM_THREADS="$PRESTO_NUM_THREADS" \
        EXTRA_CMAKE_FLAGS="$extra_flags")
  fi
}

if [[ ! -d "$PRESTO_SRC" ]]; then
  log "ERROR: PRESTO_SRC directory '$PRESTO_SRC' not found."
  exit 1
fi

if ! build_presto_if_needed; then
  log "Container is running without presto_server. Attach with 'docker exec -it $HOSTNAME bash' to build manually."
  PRESTO_SKIP_SERVER=1
fi

if [[ "$PROFILE" == "ON" ]]; then
  ensure_dir /presto_profiles
  if [[ -z "$PROFILE_ARGS" ]]; then
    PROFILE_ARGS="-t nvtx,cuda,osrt \
                  --cuda-memory-usage=true \
                  --cuda-um-cpu-page-faults=true \
                  --cuda-um-gpu-page-faults=true \
                  --cudabacktrace=true"
  fi
  PROFILE_CMD=(nsys launch $PROFILE_ARGS)
else
  PROFILE_CMD=()
fi

# Fix for libboost_iostreams.so.1.84.0 not found
if [[ $(id -u) -eq 0 ]]; then
  ldconfig /usr/local/lib
else
  ldconfig /usr/local/lib >/dev/null 2>&1 || true
fi

if [[ "$PRESTO_SKIP_SERVER" == "1" ]]; then
  log "PRESTO_SKIP_SERVER=1 - container is idling for interactive development."
  tail -f /dev/null
else
  if [[ ! -x "$PRESTO_BIN" ]]; then
    log "ERROR: presto_server binary missing at ${PRESTO_BIN}"
    exit 1
  fi
  log "Starting presto_server from ${PRESTO_BIN}"
  exec "${PROFILE_CMD[@]}" "$PRESTO_BIN" --etc-dir="$PRESTO_ETC_DIR"
fi

