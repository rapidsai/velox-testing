#!/bin/bash

set -e

CUDA_VERSION=''

print_help() {
  cat << EOF

Usage: build_centos_deps_image.sh [OPTIONS]

This script does a local build of a Velox dependencies/run-time container to a Docker image.
It expects a sibling Velox clone.

OPTIONS:
    -h, --help           Show this help message
    --cuda-version       CUDA version to install (default: 12.9)

EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      --cuda-version)
        if [[ -n $2 ]]; then
          CUDA_VERSION=$2
          shift 2
        else
          echo "Error: --cuda-version requires a value"
          exit 1
        fi
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the root of the git repository
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"

echo "Building Velox dependencies/run-time container image..."

pushd "${REPO_ROOT}/../velox"
CUDA_VERSION_ARG=""
if [[ -n "${CUDA_VERSION}" ]]; then
  CUDA_VERSION_ARG="--build-arg CUDA_VERSION=${CUDA_VERSION}"
fi
docker compose -f docker-compose.yml --progress plain build ${CUDA_VERSION_ARG} adapters-cpp
popd

echo "Velox dependencies/run-time container image built!"
