#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
# load common variables and functions
source ./config.sh

ALL_CUDA_ARCHS=false
NO_CACHE=false
PLAIN_OUTPUT=false
BUILD_WITH_VELOX_ENABLE_CUDF="ON"
VELOX_ENABLE_BENCHMARKS="ON"
BUILD_TYPE="Release"
LOG_ENABLED=false
TREAT_WARNINGS_AS_ERRORS="${TREAT_WARNINGS_AS_ERRORS:-1}"
LOGFILE="./build_velox.log"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Builds the Velox adapters Docker image using docker compose, with options to control CUDA architectures, cache usage, output style, and CPU/GPU build.

Options:
  --all-cuda-archs            Build for all supported CUDA architectures (default: false).
  --no-cache                  Build without using Docker cache (default: false).
  --plain                     Use plain output for Docker build logs (default: false).
  --log [LOGFILE]             Capture build process to log file, enables --plain, by default LOGFILE='./build_velox.log' (default: false).
  --cpu                       Build for CPU only (disables CUDF; sets BUILD_WITH_VELOX_ENABLE_CUDF=OFF).
  --gpu                       Build with GPU support (enables CUDF; sets BUILD_WITH_VELOX_ENABLE_CUDF=ON) [default].
  -j|--num-threads            NUM Number of threads to use for building (default: 3/4 of CPU cores).
  --benchmarks true|false     Enable benchmarks and nsys profiling tools (default: true).
  --build-type TYPE           Build type: Release, Debug, or RelWithDebInfo (default: Release).
  -h, --help                  Show this help message and exit.

Examples:
  $(basename "$0") --all-cuda-archs --no-cache
  $(basename "$0") --plain
  $(basename "$0") --cpu
  $(basename "$0") --benchmarks true   # Build with benchmarks/nsys (default)
  $(basename "$0") --benchmarks false  # Build without benchmarks/nsys
  $(basename "$0") --cpu --benchmarks false  # CPU-only build without benchmarks
  $(basename "$0") --log
  $(basename "$0") --log mybuild.log --all-cuda-archs
  $(basename "$0") -j 8 --gpu
  $(basename "$0") --num-threads 16 --no-cache
  $(basename "$0") --build-type Debug
  $(basename "$0") --build-type RelWithDebInfo --gpu

By default, the script builds for the Native CUDA architecture (detected on host), uses Docker cache, standard build output, GPU support (CUDF enabled), and benchmarks enabled.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --all-cuda-archs)
        ALL_CUDA_ARCHS=true
        shift
        ;;
      --no-cache)
        NO_CACHE=true
        shift
        ;;
      --plain)
        PLAIN_OUTPUT=true
        shift
        ;;
      --log)
        LOG_ENABLED=true
        PLAIN_OUTPUT=true
        if [[ -n "${2:-}" && ! "${2}" =~ ^- ]]; then
          LOGFILE="$2"
          shift 2
        else
          shift
        fi
        ;;
      --cpu)
        BUILD_WITH_VELOX_ENABLE_CUDF="OFF"
        shift
        ;;
      --gpu)
        BUILD_WITH_VELOX_ENABLE_CUDF="ON"
        shift
        ;;
      -j|--num-threads)
        if [[ -z "${2:-}" || "${2}" =~ ^- ]]; then
          echo "Error: --num-threads requires a value"
          exit 1
        fi
        NUM_THREADS="$2"
        shift 2
        ;;
      --benchmarks)
        if [[ -n "${2:-}" && ! "${2}" =~ ^- ]]; then
          case "${2,,}" in
            true)
              VELOX_ENABLE_BENCHMARKS="ON"
              shift 2
              ;;
            false)
              VELOX_ENABLE_BENCHMARKS="OFF"
              shift 2
              ;;
            *)
              echo "ERROR: --benchmarks accepts 'true' or 'false', got: $2" >&2
              exit 1
              ;;
          esac
        else
          echo "ERROR: --benchmarks requires a value: 'true' or 'false'" >&2
          exit 1
        fi
        ;;
      --build-type)
        if [[ -n "${2:-}" && ! "${2}" =~ ^- ]]; then
          case "${2}" in
            "Release"|"Debug"|"RelWithDebInfo")
              BUILD_TYPE="$2"
              shift 2
              ;;
            *)
              echo "ERROR: --build-type must be one of: Release, Debug, RelWithDebInfo (got: $2)" >&2
              exit 1
              ;;
          esac
        else
          echo "ERROR: --build-type requires a value: Release, Debug, or RelWithDebInfo" >&2
          exit 1
        fi
        ;;
      -h|--help)
        print_help
        exit 0
        ;;
      *)
        echo "Unrecognized argument: $1"
        echo "Use -h or --help for usage information."
        exit 1
        ;;
    esac
  done
}


# Detect CUDA architecture since native architecture detection doesn't work
# inside Docker containers
detect_cuda_architecture() {
  if ! command -v nvidia-smi >/dev/null 2>&1; then
    return 0
  fi
  
  local compute_cap
  if compute_cap=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader,nounits 2>/dev/null | head -1); then
    if [[ -n "$compute_cap" && "$compute_cap" =~ ^[0-9]+\.[0-9]+$ ]]; then
      local cuda_arch=$(echo "$compute_cap" | tr -d '.')
      DOCKER_BUILD_OPTS+=(--build-arg CUDA_ARCHITECTURES="${cuda_arch}")
      echo "Using CUDA architecture: ${cuda_arch}"
    fi
  fi
}



parse_args "$@"

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../velox"

# Compose docker build command options
DOCKER_BUILD_OPTS=(--pull)
if [[ "$NO_CACHE" == true ]]; then
  DOCKER_BUILD_OPTS+=(--no-cache)
fi
if [[ "$PLAIN_OUTPUT" == true ]]; then
  DOCKER_BUILD_OPTS+=(--progress=plain)
fi
if [[ "$ALL_CUDA_ARCHS" == true ]]; then
  DOCKER_BUILD_OPTS+=(--build-arg CUDA_ARCHITECTURES="70;75;80;86;89;90;100;120")
else
  # Only detect native architecture if not building for all architectures
  detect_cuda_architecture
fi
# Determine build directory name based on build type
case "${BUILD_TYPE}" in
  "Debug") BUILD_DIR_NAME="debug" ;;
  "RelWithDebInfo") BUILD_DIR_NAME="relwithdebinfo" ;;
  "Release") BUILD_DIR_NAME="release" ;;
  *) BUILD_DIR_NAME="release" ;;
esac

DOCKER_BUILD_OPTS+=(--build-arg BUILD_WITH_VELOX_ENABLE_CUDF="${BUILD_WITH_VELOX_ENABLE_CUDF}")
DOCKER_BUILD_OPTS+=(--build-arg NUM_THREADS="${NUM_THREADS}")
DOCKER_BUILD_OPTS+=(--build-arg VELOX_ENABLE_BENCHMARKS="${VELOX_ENABLE_BENCHMARKS}")
DOCKER_BUILD_OPTS+=(--build-arg TREAT_WARNINGS_AS_ERRORS="${TREAT_WARNINGS_AS_ERRORS}")
DOCKER_BUILD_OPTS+=(--build-arg BUILD_TYPE="${BUILD_TYPE}")
DOCKER_BUILD_OPTS+=(--build-arg BUILD_DIR_NAME="${BUILD_DIR_NAME}")

if [[ "$LOG_ENABLED" == true ]]; then
  echo "Logging build output to $LOGFILE"
  docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}" | tee "$LOGFILE"
  BUILD_EXIT_CODE=${PIPESTATUS[0]}
else
  docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}"
  BUILD_EXIT_CODE=$?
fi

if [[ "$BUILD_EXIT_CODE" == "0" ]]; then
  # Update EXPECTED_OUTPUT_DIR to use the correct build directory
  EXPECTED_OUTPUT_DIR="/opt/velox-build/${BUILD_DIR_NAME}"
  
  if docker compose  -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" test -d "${EXPECTED_OUTPUT_DIR}" 2>/dev/null; then
    echo "  Built velox-adapters (${BUILD_TYPE} build). View logs with:"
    echo "    docker compose -f $COMPOSE_FILE logs -f ${CONTAINER_NAME}"
    echo ""
    echo "  The Velox build output is located in the container at:"
    echo "    ${EXPECTED_OUTPUT_DIR}"
    echo ""
    echo "  Build directory name: ${BUILD_DIR_NAME}"
    echo "  Downstream tasks will auto-detect this build directory."
    echo ""
    echo "  To access the build output, you can run:"
    echo "    docker compose -f $COMPOSE_FILE run --rm ${CONTAINER_NAME} ls ${EXPECTED_OUTPUT_DIR}"
    echo ""
    if [[ "$VELOX_ENABLE_BENCHMARKS" == "ON" ]]; then
      echo "  Benchmarks and nsys profiling are enabled in this build."
    else
      echo "  Benchmarks and nsys profiling are disabled in this build."
    fi
    echo ""
  else
    echo "  ERROR: Build succeeded but ${EXPECTED_OUTPUT_DIR} not found in the container."
    echo "  View logs with:"
    echo "    docker compose -f $COMPOSE_FILE logs -f ${CONTAINER_NAME}"
    echo ""
  fi
else
  echo "  ERROR: velox-adapters-build docker compose build failed with exit code $BUILD_EXIT_CODE."
  echo "  Check docker compose output and logs for details."
  echo ""
fi
