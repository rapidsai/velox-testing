#!/bin/bash
set -euo pipefail

ALL_CUDA_ARCHS=false
NO_CACHE=false
PLAIN_OUTPUT=false
BUILD_WITH_VELOX_ENABLE_CUDF="ON"
VELOX_ENABLE_BENCHMARKS="ON"
LOG_ENABLED=false
LOGFILE="./build_velox.log"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Builds the Velox adapters Docker image using docker compose, with options to control CUDA architectures, cache usage, output style, and CPU/GPU build.

Options:
  --all-cuda-archs   Build for all supported CUDA architectures (default: false).
  --no-cache         Build without using Docker cache (default: false).
  --plain            Use plain output for Docker build logs (default: false).
  --log [LOGFILE]    Capture build process to log file, enables --plain, by default LOGFILE='./build_velox.log' (default: false).
  --cpu              Build for CPU only (disables CUDF; sets BUILD_WITH_VELOX_ENABLE_CUDF=OFF).
  --gpu              Build with GPU support (enables CUDF; sets BUILD_WITH_VELOX_ENABLE_CUDF=ON) [default].
  --benchmarks       Enable benchmarks and nsys profiling tools (sets VELOX_ENABLE_BENCHMARKS=ON) [default].
  --no-benchmarks    Disable benchmarks and skip nsys installation (sets VELOX_ENABLE_BENCHMARKS=OFF).
  -h, --help         Show this help message and exit.

Examples:
  $(basename "$0") --all-cuda-archs --no-cache
  $(basename "$0") --plain
  $(basename "$0") --cpu
  $(basename "$0") --no-benchmarks  # Build without benchmarks/nsys
  $(basename "$0") --cpu --no-benchmarks  # CPU-only build without benchmarks
  $(basename "$0") --log
  $(basename "$0") --log mybuild.log --all-cuda-archs

By default, the script builds for the Volta (7.0) CUDA architecture, uses Docker cache, standard build output, GPU support (CUDF enabled), and benchmarks enabled.
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
      --benchmarks)
        VELOX_ENABLE_BENCHMARKS="ON"
        shift
        ;;
      --no-benchmarks)
        VELOX_ENABLE_BENCHMARKS="OFF"
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

COMPOSE_FILE="../docker/docker-compose.adapters.yml"

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
fi
DOCKER_BUILD_OPTS+=(--build-arg BUILD_WITH_VELOX_ENABLE_CUDF="${BUILD_WITH_VELOX_ENABLE_CUDF}")
DOCKER_BUILD_OPTS+=(--build-arg VELOX_ENABLE_BENCHMARKS="${VELOX_ENABLE_BENCHMARKS}")

if [[ "$LOG_ENABLED" == true ]]; then
  echo "Logging build output to $LOGFILE"
  docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}" | tee "$LOGFILE"
  BUILD_EXIT_CODE=${PIPESTATUS[0]}
else
  docker compose -f "$COMPOSE_FILE" build "${DOCKER_BUILD_OPTS[@]}"
  BUILD_EXIT_CODE=$?
fi

CONTAINER_NAME="velox-adapters-build"
EXPECTED_OUTPUT_DIR="/opt/velox-build/release"

if [[ "$BUILD_EXIT_CODE" == "0" ]]; then
  if docker compose -f "$COMPOSE_FILE" run --rm "${CONTAINER_NAME}" test -d "${EXPECTED_OUTPUT_DIR}" 2>/dev/null; then
    echo "  Built velox-adapters. View logs with:"
    echo "    docker compose -f $COMPOSE_FILE logs -f ${CONTAINER_NAME}"
    echo ""
    echo "  The Velox build output is located in the container at:"
    echo "    ${EXPECTED_OUTPUT_DIR}"
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
