#!/bin/bash

set -e

IMAGE_NAME='presto/prestissimo-dependency:centos9'
VELOX_REPO='rapidsai/velox'
VELOX_COMMIT='merged-prs'
NO_CACHE_ARG=''

print_help() {
  cat << EOF

Usage: build_centos_deps_image.sh [OPTIONS]

This script does a local build of a Presto dependencies/run-time container to a Docker image.
It expects a sibling Presto clone, and will override the Velox sub-module to the given repo
and commit prior to the build.

WARNING: If an image of the given name already exists, it will be removed prior to the build.

WARNING: The Presto clone will be reset to the current commit, and the Velox sub-module will
         be reset to the given commit and therefore any local changes to either will be lost.

OPTIONS:
    -h, --help           Show this help message
    -i, --image-name     Desired Docker Image name (default: presto/prestissimo-dependency:centos9)
    -r, --velox-repo     Velox repo to use (default: rapidsai/velox) (full URL prefix not required)
    -c, --velox-commit   Velox repo commit/branch to use (default: merged-prs)
    -n, --no-cache       Do not use Docker build cache (default: use cache)

EXAMPLES:
    build_centos_deps_image.sh --velox-repo developer/velox --velox-commit my-test-branch

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
      -r|--velox-repo)
        if [[ -n $2 ]]; then
          VELOX_REPO=$2
          shift 2
        else
          echo "Error: --velox-repo requires a value"
          exit 1
        fi
        ;;
      -c|--velox-commit)
        if [[ -n $2 ]]; then
          VELOX_COMMIT=$2
          shift 2
        else
          echo "Error: --velox-commit requires a value"
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

#
# remove any existing image?
#

if [[ ! -z $(docker images -q ${IMAGE_NAME}) ]]; then
	echo "Removing existing Presto dependencies/run-time image..."
	docker rmi -f ${IMAGE_NAME}
fi

#
# try to build deps container image locally
#

echo "Building Presto dependencies/run-time image..."

# move to Presto clone
pushd ../../../presto

# reset submodule
echo "Resetting Velox submodule to specified version"
git submodule set-url presto-native-execution/velox http://github.com/${VELOX_REPO}
git submodule set-branch --branch ${VELOX_COMMIT} presto-native-execution/velox
git submodule sync
git submodule update --init --remote presto-native-execution/velox

# apply patches here if needed
# @TODO extend this to read from a directory of files
echo "No patches currently required"

# now build
echo "Building..."
pushd presto-native-execution
docker compose --progress plain build ${NO_CACHE_ARG} centos-native-dependency
popd

# return
popd

# done
echo "Presto dependencies/run-time container image built!"
