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
BUILD_TYPE="release"
LOG_ENABLED=false
TREAT_WARNINGS_AS_ERRORS="${TREAT_WARNINGS_AS_ERRORS:-1}"
LOGFILE="./build_velox.log"
ENABLE_SCCACHE=false
SCCACHE_AUTH_DIR=""
EXPORT_COMPILE_COMMANDS=false
COMPILE_COMMANDS_OUTPUT_DIR=""
SKIP_BUILD=false

# Cleanup function to remove copied sccache auth files
cleanup_sccache_auth() {
    if [[ "$ENABLE_SCCACHE" == true && -d "../docker/sccache/sccache_auth/" ]]; then
        rm -f ../docker/sccache/sccache_auth/github_token ../docker/sccache/sccache_auth/aws_credentials
    fi
}

trap cleanup_sccache_auth EXIT SIGTERM SIGINT SIGQUIT

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
  --sccache                   Enable sccache distributed compilation caching.
  --sccache-auth-dir DIR      Directory containing sccache authentication files (github_token, aws_credentials).
  --export-compile-commands   Export compile commands database (compile_commands.json).
  --compile-commands-dir DIR  Directory to output compile_commands.json (required with --export-compile-commands).
  --skip-build                Skip the actual build, only generate compile commands (requires --export-compile-commands).
  --build-type TYPE           Build type: Release, Debug, or RelWithDebInfo (case insensitive, default: release).
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
  $(basename "$0") --sccache --sccache-auth-dir /auth_dir/      # Build with sccache and use auth files in /auth_dir/
  $(basename "$0") --export-compile-commands --compile-commands-dir ./compile_db/  # Export compile database
  $(basename "$0") --export-compile-commands --compile-commands-dir ./compile_db/ --skip-build  # Only generate compile database
  $(basename "$0") --build-type Debug
  $(basename "$0") --build-type debug --gpu
  $(basename "$0") --build-type RELWITHDEBINFO --gpu

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
      --sccache)
        ENABLE_SCCACHE=true
        shift
        ;;
      --sccache-auth-dir)
        if [[ -z "${2:-}" || "${2}" =~ ^- ]]; then
          echo "Error: --sccache-auth-dir requires a directory path"
          exit 1
        fi
        SCCACHE_AUTH_DIR="$2"
        shift 2
        ;;
      --export-compile-commands)
        EXPORT_COMPILE_COMMANDS=true
        shift
        ;;
      --compile-commands-dir)
        if [[ -z "${2:-}" || "${2}" =~ ^- ]]; then
          echo "Error: --compile-commands-dir requires a directory path"
          exit 1
        fi
        COMPILE_COMMANDS_OUTPUT_DIR="$2"
        shift 2
        ;;
      --skip-build)
        SKIP_BUILD=true
        shift
        ;;
      --build-type)
        if [[ -n "${2:-}" && ! "${2}" =~ ^- ]]; then
          # Convert to lowercase first, then validate
          local build_type_lower="${2@L}"
          case "${build_type_lower}" in
            "release"|"debug"|"relwithdebinfo")
              BUILD_TYPE="${build_type_lower}"
              shift 2
              ;;
            *)
              echo "ERROR: --build-type must be one of: Release, Debug, RelWithDebInfo (case insensitive, got: $2)" >&2
              exit 1
              ;;
          esac
        else
          echo "ERROR: --build-type requires a value: Release, Debug, or RelWithDebInfo (case insensitive)" >&2
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

# Validate sccache authentication
validate_sccache_auth() {
  if [[ "$ENABLE_SCCACHE" == true ]]; then
    
    if [[ -n "$SCCACHE_AUTH_DIR" ]]; then
      if [[ ! -d "$SCCACHE_AUTH_DIR" ]]; then
        echo "ERROR: sccache auth directory not found: $SCCACHE_AUTH_DIR"
        exit 1
      fi
      if [[ ! -f "$SCCACHE_AUTH_DIR/github_token" ]]; then
        echo "ERROR: GitHub token not found: $SCCACHE_AUTH_DIR/github_token"
        exit 1
      fi
      if [[ ! -f "$SCCACHE_AUTH_DIR/aws_credentials" ]]; then
        echo "ERROR: AWS credentials not found: $SCCACHE_AUTH_DIR/aws_credentials"
        exit 1
      fi
      echo "sccache authentication files found in: $SCCACHE_AUTH_DIR"
    else
      echo "ERROR: No sccache auth directory provided but sccache is enabled. Run setup_sccache_auth.sh first."
      exit 1
    fi
  fi
}

# Validate compile commands options
validate_compile_commands() {
  if [[ "$EXPORT_COMPILE_COMMANDS" == true ]]; then
    if [[ -z "$COMPILE_COMMANDS_OUTPUT_DIR" ]]; then
      echo "ERROR: --export-compile-commands requires --compile-commands-dir to be specified"
      exit 1
    fi
    # Create output directory if it doesn't exist
    mkdir -p "$COMPILE_COMMANDS_OUTPUT_DIR"
    if [[ ! -d "$COMPILE_COMMANDS_OUTPUT_DIR" ]]; then
      echo "ERROR: Failed to create compile commands output directory: $COMPILE_COMMANDS_OUTPUT_DIR"
      exit 1
    fi
    echo "Compile commands will be exported to: $COMPILE_COMMANDS_OUTPUT_DIR"
  fi
  
  if [[ "$SKIP_BUILD" == true ]]; then
    if [[ "$EXPORT_COMPILE_COMMANDS" != true ]]; then
      echo "ERROR: --skip-build requires --export-compile-commands to be enabled"
      exit 1
    fi
    echo "Skip build enabled - will only generate compile commands database"
  fi
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

# Validate sccache authentication if sccache is enabled
validate_sccache_auth

# Validate compile commands options
validate_compile_commands

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
DOCKER_BUILD_OPTS+=(--build-arg BUILD_WITH_VELOX_ENABLE_CUDF="${BUILD_WITH_VELOX_ENABLE_CUDF}")
DOCKER_BUILD_OPTS+=(--build-arg NUM_THREADS="${NUM_THREADS}")
DOCKER_BUILD_OPTS+=(--build-arg VELOX_ENABLE_BENCHMARKS="${VELOX_ENABLE_BENCHMARKS}")
DOCKER_BUILD_OPTS+=(--build-arg TREAT_WARNINGS_AS_ERRORS="${TREAT_WARNINGS_AS_ERRORS}")
DOCKER_BUILD_OPTS+=(--build-arg BUILD_TYPE="${BUILD_TYPE}")

# Add sccache build arguments
if [[ "$ENABLE_SCCACHE" == true ]]; then
  DOCKER_BUILD_OPTS+=(--build-arg ENABLE_SCCACHE="ON")
  # Copy auth files to build context
  mkdir -p ../docker/sccache/sccache_auth/
  cp "$SCCACHE_AUTH_DIR/github_token" ../docker/sccache/sccache_auth/
  cp "$SCCACHE_AUTH_DIR/aws_credentials" ../docker/sccache/sccache_auth/
else
  DOCKER_BUILD_OPTS+=(--build-arg ENABLE_SCCACHE="OFF")
fi

# Add compile commands build arguments
if [[ "$EXPORT_COMPILE_COMMANDS" == true ]]; then
  DOCKER_BUILD_OPTS+=(--build-arg EXPORT_COMPILE_COMMANDS="ON")
else
  DOCKER_BUILD_OPTS+=(--build-arg EXPORT_COMPILE_COMMANDS="OFF")
fi

# Add skip build argument
if [[ "$SKIP_BUILD" == true ]]; then
  DOCKER_BUILD_OPTS+=(--build-arg SKIP_BUILD="ON")
else
  DOCKER_BUILD_OPTS+=(--build-arg SKIP_BUILD="OFF")
fi

# Build the Docker image
if [[ "$LOG_ENABLED" == true ]]; then
  echo "Logging build output to $LOGFILE"
  docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}" | tee "$LOGFILE"
  BUILD_EXIT_CODE=${PIPESTATUS[0]}
else
  docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}"
  BUILD_EXIT_CODE=$?
fi

# Copy compile commands if enabled and build succeeded
if [[ "$BUILD_EXIT_CODE" == "0" && "$EXPORT_COMPILE_COMMANDS" == true ]]; then
  echo "Extracting compile_commands.json from container..."
  # Convert relative path to absolute path
  COMPILE_COMMANDS_OUTPUT_DIR=$(realpath "$COMPILE_COMMANDS_OUTPUT_DIR")
  
  # Create a temporary container to copy from
  TEMP_CONTAINER_ID=$(docker create "${CONTAINER_NAME}:latest")
  
  # Copy compile_commands.json from the container to the host using docker cp
  if docker cp "${TEMP_CONTAINER_ID}:/opt/compile_output/compile_commands.json" "${COMPILE_COMMANDS_OUTPUT_DIR}/compile_commands.json" 2>/dev/null; then
    echo "Successfully exported compile_commands.json to: ${COMPILE_COMMANDS_OUTPUT_DIR}/compile_commands.json"
  else
    echo "WARNING: Failed to extract compile_commands.json from container"
    echo "Checking if compile_commands.json exists in build directory..."
    if docker run --rm "${CONTAINER_NAME}:latest" test -f "/opt/velox-build/${BUILD_TYPE}/compile_commands.json"; then
      echo "compile_commands.json found in build directory, but not in output directory"
    else
      echo "compile_commands.json not found in build directory"
    fi
  fi
  
  # Clean up temporary container
  docker rm "${TEMP_CONTAINER_ID}" >/dev/null 2>&1
fi


if [[ "$BUILD_EXIT_CODE" == "0" ]]; then
  # Update EXPECTED_OUTPUT_DIR to use the correct build directory
  EXPECTED_OUTPUT_DIR="/opt/velox-build/${BUILD_TYPE}"
  
  if docker compose  -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" test -d "${EXPECTED_OUTPUT_DIR}" 2>/dev/null; then
    echo "  Built velox-adapters (${BUILD_TYPE} build). View logs with:"
    echo "    docker compose -f $COMPOSE_FILE logs -f ${CONTAINER_NAME}"
    echo ""
    echo "  The Velox build output is located in the container at:"
    echo "    ${EXPECTED_OUTPUT_DIR}"
    echo ""
    echo "  Build type: ${BUILD_TYPE}"
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
    if [[ "$ENABLE_SCCACHE" == true ]]; then
      echo "  sccache distributed compilation caching was enabled for this build."
      if [[ -n "$SCCACHE_AUTH_DIR" ]]; then
        echo "  To check sccache stats, run:"
        echo "    docker compose -f $COMPOSE_FILE run --rm ${CONTAINER_NAME} sccache --show-stats"
      fi
    fi
    if [[ "$EXPORT_COMPILE_COMMANDS" == true ]]; then
      echo "  Compile commands database was exported to: ${COMPILE_COMMANDS_OUTPUT_DIR}/compile_commands.json"
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
