#!/bin/bash
set -euo pipefail

ALL_CUDA_ARCHS=false
NO_CACHE=false
PLAIN_OUTPUT=false

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Builds the Velox adapters Docker image using docker compose, with options to control CUDA architectures, cache usage, output style, and CPU/GPU build.

Options:
  --all-cuda-archs   Build for all supported CUDA architectures (default: false).
  --no-cache         Build without using Docker cache (default: false).
  --plain            Use plain output for Docker build logs (default: false).
  --cpu              Build for CPU only (disables CUDF; sets BUILD_WITH_VELOX_ENABLE_CUDF=OFF).
  --gpu              Build with GPU support (enables CUDF; sets BUILD_WITH_VELOX_ENABLE_CUDF=ON) [default].
  -h, --help         Show this help message and exit.

Examples:
  $(basename "$0") --all-cuda-archs --no-cache
  $(basename "$0") --plain
  $(basename "$0") --cpu

By default, the script builds for the default CUDA architecture, uses Docker cache, standard build output, and GPU support (CUDF enabled).
EOF
}

# Default: GPU build (CUDF enabled)
BUILD_WITH_VELOX_ENABLE_CUDF="ON"

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
      --cpu)
        BUILD_WITH_VELOX_ENABLE_CUDF="OFF"
        shift
        ;;
      --gpu)
        BUILD_WITH_VELOX_ENABLE_CUDF="ON"
        shift
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker/docker-compose.adapters.yml"

parse_args "$@"

# Validate repo layout using shared script
source "${SCRIPT_DIR}/../../scripts/validate_layout.sh"
validate_repo_layout velox 3

"${SCRIPT_DIR}/stop_velox.sh" || true

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
fi
DOCKER_BUILD_OPTS+=(--build-arg BUILD_WITH_VELOX_ENABLE_CUDF="${BUILD_WITH_VELOX_ENABLE_CUDF}")

docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}"
BUILD_EXIT_CODE=$?

CONTAINER_NAME="velox-adapters-build"
EXPECTED_OUTPUT_DIR="/opt/velox-build/release"
BUILD_LOG="/workspace/adapters_build.log"

if [[ "$BUILD_EXIT_CODE" == "0" ]]; then
  if docker run "${CONTAINER_NAME}" test -d "${EXPECTED_OUTPUT_DIR}" 2>/dev/null; then
    echo "  Built velox-adapters. View logs with:"
    echo "    docker compose -f $COMPOSE_FILE logs -f ${CONTAINER_NAME}"
    echo ""
    echo "  The Velox build output is located in the container at:"
    echo "    ${EXPECTED_OUTPUT_DIR}"
    echo ""
    echo "  To access the build output, you can run:"
    echo "    docker run ${CONTAINER_NAME} ls ${EXPECTED_OUTPUT_DIR}"
    echo ""
    echo "  View build log with:"
    echo "    docker run ${CONTAINER_NAME} cat ${BUILD_LOG}"
    echo ""
  else
    echo "  ERROR: Build succeeded but ${EXPECTED_OUTPUT_DIR} not found in the container."
    echo "  View build log with:"
    echo "    docker run ${CONTAINER_NAME} cat ${BUILD_LOG}"
    echo ""
  fi
else
  echo "  ERROR: velox-adapters-build docker compose build failed with exit code $BUILD_EXIT_CODE."
  echo "  Check docker compose output and logs for details."
  echo ""
fi
