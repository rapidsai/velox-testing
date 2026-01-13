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
source ./config.dev.sh

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
SCCACHE_AUTH_DIR="${SCCACHE_AUTH_DIR:-$HOME/.sccache-auth}"
SCCACHE_ENABLE_DIST=false
SCCACHE_VERSION="${SCCACHE_VERSION:-latest}"
UPDATE_NINJA=true

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
  --sccache                   Enable sccache distributed compilation caching (requires auth files in ~/.sccache-auth/).
  --sccache-version           Install a specific version of rapidsai/sccache, e.g. "0.12.0-rapids.1" (default: latest)
  --sccache-enable-dist       Enable distributed compilation (WARNING: may cause compilation differences like additional warnings that could lead to build failures).
  --update-ninja true|false   Update ninja build tool during build (default: true).
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
  $(basename "$0") --sccache                                   # Build with sccache (remote S3 cache, local compilation)
  $(basename "$0") --sccache --sccache-version 0.12.0-rapids.1 # Build with sccache v0.12.0-rapids.1 (see: https://github.com/rapidsai/sccache/releases)
  $(basename "$0") --sccache --sccache-enable-dist         # Build with sccache including distributed compilation (may cause build differences)
  $(basename "$0") --update-ninja false                   # Build without updating ninja
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
      --sccache-version)
        SCCACHE_VERSION="$2"
        shift 2
        ;;
      --sccache-enable-dist)
        SCCACHE_ENABLE_DIST=true
        shift
        ;;
      --update-ninja)
        if [[ -n "${2:-}" && ! "${2}" =~ ^- ]]; then
          case "${2,,}" in
            true)
              UPDATE_NINJA=true
              shift 2
              ;;
            false)
              UPDATE_NINJA=false
              shift 2
              ;;
            *)
              echo "ERROR: --update-ninja accepts 'true' or 'false', got: $2" >&2
              exit 1
              ;;
          esac
        else
          echo "ERROR: --update-ninja requires a value: 'true' or 'false'" >&2
          exit 1
        fi
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

compose_file() {
  if [[ "$ENABLE_SCCACHE" == true ]]; then
		echo $COMPOSE_FILE_SCCACHE
	else
		echo $COMPOSE_FILE
	fi
}

# Validate sccache authentication
validate_sccache_auth() {
  if [[ "$ENABLE_SCCACHE" == true ]]; then
    echo "Checking for sccache authentication files in: $SCCACHE_AUTH_DIR"

    if [[ ! -d "$SCCACHE_AUTH_DIR" ]]; then
      echo "ERROR: sccache auth directory not found: $SCCACHE_AUTH_DIR" >&2
      echo "Run setup_sccache_auth.sh to set up authentication." >&2
      exit 1
    fi

    if [[ ! -f "$SCCACHE_AUTH_DIR/github_token" ]]; then
      echo "ERROR: GitHub token not found: $SCCACHE_AUTH_DIR/github_token" >&2
      echo "Run setup_sccache_auth.sh to set up authentication." >&2
      exit 1
    fi

    if [[ ! -f "$SCCACHE_AUTH_DIR/aws_credentials" ]]; then
      echo "ERROR: AWS credentials not found: $SCCACHE_AUTH_DIR/aws_credentials" >&2
      echo "Run setup_sccache_auth.sh to set up authentication." >&2
      exit 1
    fi
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

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../velox"

# Compose docker build command options (default: do not force pull; use local images if present)
DOCKER_BUILD_OPTS=()
if [[ "$NO_CACHE" == true ]]; then
  DOCKER_BUILD_OPTS+=(--no-cache)
fi
if [[ "$PLAIN_OUTPUT" == true ]]; then
  DOCKER_BUILD_OPTS+=(--progress=plain)
fi
if [[ "$ALL_CUDA_ARCHS" == true ]]; then
  DOCKER_BUILD_OPTS+=(--build-arg CUDA_ARCHITECTURES="75;80;86;90;100;120")
else
  # Only detect native architecture if not building for all architectures
  detect_cuda_architecture
fi
DOCKER_BUILD_OPTS+=(--build-arg BUILD_WITH_VELOX_ENABLE_CUDF="${BUILD_WITH_VELOX_ENABLE_CUDF}")
DOCKER_BUILD_OPTS+=(--build-arg NUM_THREADS="${NUM_THREADS}")
DOCKER_BUILD_OPTS+=(--build-arg VELOX_ENABLE_BENCHMARKS="${VELOX_ENABLE_BENCHMARKS}")
DOCKER_BUILD_OPTS+=(--build-arg TREAT_WARNINGS_AS_ERRORS="${TREAT_WARNINGS_AS_ERRORS}")
DOCKER_BUILD_OPTS+=(--build-arg BUILD_TYPE="${BUILD_TYPE}")
DOCKER_BUILD_OPTS+=(--build-arg SCCACHE_VERSION="${SCCACHE_VERSION}")
DOCKER_BUILD_OPTS+=(--build-arg UPDATE_NINJA="${UPDATE_NINJA}")

# If these are set (even to empty string), pass them through as-is
if test -v MAX_HIGH_MEM_JOBS; then
    DOCKER_BUILD_OPTS+=(--build-arg MAX_HIGH_MEM_JOBS="${MAX_HIGH_MEM_JOBS:-}")
else
    DOCKER_BUILD_OPTS+=(--build-arg MAX_HIGH_MEM_JOBS=4)
fi
if test -v MAX_LINK_JOBS; then
    DOCKER_BUILD_OPTS+=(--build-arg MAX_LINK_JOBS="${MAX_LINK_JOBS:-}")
else
    DOCKER_BUILD_OPTS+=(--build-arg MAX_LINK_JOBS=4)
fi

# sccache checks the existence of these envvars (not their values) so only set them if they're defined
if test -v SCCACHE_RECACHE; then
    DOCKER_BUILD_OPTS+=(--build-arg SCCACHE_RECACHE="${SCCACHE_RECACHE:-}")
fi
if test -v SCCACHE_NO_CACHE; then
    DOCKER_BUILD_OPTS+=(--build-arg SCCACHE_NO_CACHE="${SCCACHE_NO_CACHE:-}")
fi

# Add sccache build arguments
if [[ "$ENABLE_SCCACHE" == true ]]; then
  DOCKER_BUILD_OPTS+=(--build-arg ENABLE_SCCACHE="ON")
  # Add distributed compilation control (disabled by default)
  if [[ "$SCCACHE_ENABLE_DIST" == true ]]; then
    echo "WARNING: sccache distributed compilation enabled - may cause compilation differences"
  else
    DOCKER_BUILD_OPTS+=(--build-arg SCCACHE_NO_DIST_COMPILE=1)
  fi
else
  DOCKER_BUILD_OPTS+=(--build-arg ENABLE_SCCACHE="OFF")
  DOCKER_BUILD_OPTS+=(--build-arg SCCACHE_NO_DIST_COMPILE=1)
fi

SELECTED_COMPOSE_FILE=$(compose_file)

#if [[ "$LOG_ENABLED" == true ]]; then
#  echo "Logging build output to $LOGFILE"
#  docker compose -f "$SELECTED_COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}" | tee "$LOGFILE"
#  BUILD_EXIT_CODE=${PIPESTATUS[0]}
#else
#  docker compose -f "$SELECTED_COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}"
#  BUILD_EXIT_CODE=$?
#fi

docker compose -f "$SELECTED_COMPOSE_FILE" down  velox-adapters-dev --remove-orphans
docker compose -f "$SELECTED_COMPOSE_FILE" up -d --build velox-adapters-dev
BUILD_EXIT_CODE=$?

if [[ "$BUILD_EXIT_CODE" == "0" ]]; then
  echo "Built dev container you can get a shell with: docker exec -it velox-adapters-dev /bin/bash"
  echo ""
else
  echo "  ERROR: failed to build dev container, exit code: $BUILD_EXIT_CODE."
  echo ""
fi
