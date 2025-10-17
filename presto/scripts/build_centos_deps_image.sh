#!/bin/bash

set -e

IMAGE_NAME='presto/prestissimo-dependency:centos9'
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
    -i, --image-name     Desired Docker Image name (default: presto/prestissimo-dependency:centos9)
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

# remove any existing image?
if [[ ! -z $(docker images -q ${IMAGE_NAME}) ]]; then
	echo "Removing existing Presto dependencies/run-time image..."
	docker rmi -f ${IMAGE_NAME}
fi

# move to Presto Velox
pushd ../../../presto/presto-native-execution > /dev/null

# override Presto Velox build config
echo "Overriding Presto Velox build config from sibling Velox clone..."
mv velox velox.bak
mkdir -p velox
cp -r ../../velox/scripts velox
cp -r ../../velox/CMake velox

# now build
echo "Building..."
docker compose --progress plain build ${NO_CACHE_ARG} centos-native-dependency

# restore Presto Velox
echo "Restoring original Presto Velox..."
rm -rf velox
mv velox.bak velox

# return
popd > /dev/null

# done
echo "Presto dependencies/run-time container image built!"
