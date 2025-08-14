#!/bin/bash
set -euo pipefail

ALL_CUDA_ARCHS=false
NO_CACHE=false
PLAIN_OUTPUT=false

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Builds the Velox adapters Docker image using docker compose, with options to control CUDA architectures, cache usage, and output style.

Options:
  --all-cuda-archs   Build for all supported CUDA architectures (default: false).
  --no-cache         Build without using Docker cache (default: false).
  --plain            Use plain output for Docker build logs (default: false).
  -h, --help         Show this help message and exit.

Examples:
  $(basename "$0") --all-cuda-archs --no-cache
  $(basename "$0") --plain

By default, the script builds for the default CUDA architecture, uses Docker cache, and standard build output.
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

docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}"

# Check if the container built successfully by checking its exit code, and expected outputs

CONTAINER_NAME="velox-adapters-build"
EXPECTED_OUTPUT_DIR="/opt/velox-build/release"
BUILD_LOG="/workspace/adapters_build.log"

EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' "${CONTAINER_NAME}" 2>/dev/null || echo "not_found")

if [[ "$EXIT_CODE" == "0" ]]; then
  if docker exec "${CONTAINER_NAME}" test -d "${EXPECTED_OUTPUT_DIR}" 2>/dev/null; then
    echo "  Built velox-adapters. View logs with:"
    echo "    docker compose -f $COMPOSE_FILE logs -f ${CONTAINER_NAME}"
    echo ""
    echo "  The Velox build output is located in the container at:"
    echo "    ${EXPECTED_OUTPUT_DIR}"
    echo ""
    echo "  To access the build output, you can run:"
    echo "    docker exec -it ${CONTAINER_NAME} ls ${EXPECTED_OUTPUT_DIR}"
    echo ""
    echo "  View build log with:"
    echo "    docker exec -it ${CONTAINER_NAME} cat ${BUILD_LOG}"
    echo ""
  else
    echo "  ERROR: Build succeeded but ${EXPECTED_OUTPUT_DIR} not found in the container."
    echo "  View build log with:"
    echo "    docker exec -it ${CONTAINER_NAME} cat ${BUILD_LOG}"
    echo ""
  fi
elif [[ "$EXIT_CODE" == "not_found" ]]; then
  echo "  ERROR: velox-adapters-build container not found. Build may have failed."
  echo "  Check docker compose output and logs for details."
  echo ""
else
  echo "  ERROR: velox-adapters-build container exited with code $EXIT_CODE."
  echo "  View logs with:"
  echo "    docker compose -f $COMPOSE_FILE logs -f ${CONTAINER_NAME}"
  echo ""
fi
