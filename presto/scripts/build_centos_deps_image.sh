#!/bin/bash

set -e

IMAGE_NAME_BASE='presto/prestissimo-dependency'
IMAGE_TAG="${USER:-latest}"
IMAGE_NAME="${IMAGE_NAME_BASE}:centos9-${IMAGE_TAG}"
NO_CACHE_ARG=''

print_help() {
  cat << EOF

Usage: build_centos_deps_image.sh [OPTIONS]

This script does a local build of a Presto dependencies/run-time container to a Docker image.
It expects sibling Presto and Velox clones, and will override the Presto Velox dependencies
scripts and CMake config to be those of the sibling Velox.

WARNING: If an image of the given name already exists, it will be removed prior to the build.

OPTIONS:
    -h, --help           Show this help message
    -i, --image-name     Desired Docker Image name (default: presto/prestissimo-dependency:centos9-\$USER).
                         This option overrides the --tag option and provides full control over the image name.
    -t, --tag            Docker image tag to use (default: current username from \$USER).
                         Creates image name: presto/prestissimo-dependency:centos9-<tag>
    -n, --no-cache       Do not use Docker build cache (default: use cache)

EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -i|--image-name)
        if [[ -n $2 ]]; then
          IMAGE_NAME=$2
          shift 2
        else
          echo "Error: --image-name requires a value"
          exit 1
        fi
        ;;
      -t|--tag)
        if [[ -n $2 ]]; then
          IMAGE_TAG=$2
          IMAGE_NAME="${IMAGE_NAME_BASE}:centos9-${IMAGE_TAG}"
          shift 2
        else
          echo "Error: --tag requires a value"
          exit 1
        fi
        ;;
      -n|--no-cache)
        NO_CACHE_ARG="--no-cache"
        shift
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

# verify sibling Presto and Velox clones
if [[ ! -d "${REPO_ROOT}/../presto/presto-native-execution" || ! -d "${REPO_ROOT}/../velox" ]]; then
  echo "Error: Sibling Presto and/or Velox clone not found"
  exit 1
fi

# remove any existing image?
if [[ ! -z $(docker images -q ${IMAGE_NAME}) ]]; then
	echo "Removing existing Presto dependencies/run-time image..."
	docker rmi -f ${IMAGE_NAME}
fi

# restore original Presto Velox on exit
# on a clean exit, this happens before the automatic popd
function cleanup {
  echo "Restoring original Presto Velox..."
  rm -rf velox
  mv velox.bak velox
}
trap cleanup EXIT

# move to Presto Velox
pushd "${REPO_ROOT}/../presto/presto-native-execution" > /dev/null

# override Presto Velox build config
echo "Overriding Presto Velox build config from sibling Velox clone..."
mv velox velox.bak
mkdir -p velox
cp -r ../../velox/scripts velox
cp -r ../../velox/CMake velox

# now build
echo "Building..."
docker compose --progress plain build ${NO_CACHE_ARG} centos-native-dependency

# Tag the built image with the desired name
echo "Tagging image as ${IMAGE_NAME}..."
docker tag centos-native-dependency ${IMAGE_NAME}

# done (will cleanup on exit)
echo "Presto dependencies/run-time container image built!"
