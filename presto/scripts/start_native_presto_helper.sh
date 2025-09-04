#!/bin/bash

set -e

# Validate repo layout using shared script
../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"


print_help() {
  cat << EOF

Usage: $SCRIPT_NAME [OPTIONS]

This script deploys a Presto server cluster (one coordinator node and one $DEVICE_TYPE worker node).

OPTIONS:
    -h, --help           Show this help message.
    -n, --no-cache       Do not use the builder cache when building the image.
    -t, --thread-count   Number of threads to use when building the image (default is `nproc` / 2).

EXAMPLES:
    $SCRIPT_NAME --no-cache
    $SCRIPT_NAME -t 8
    $SCRIPT_NAME -h

EOF
}


SKIP_CACHE_ARG=""
NUM_THREADS=$(($(nproc) / 2))

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -n|--no-cache)
        SKIP_CACHE_ARG="--no-cache"
        shift
        ;;
      -t|--thread-count)
        if [[ -n $2 ]]; then
          NUM_THREADS=$2
          shift 2
        else
          echo "Error: --thread-count requires a value"
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

if [[ -z ${DEVICE_TYPE} || ! ${DEVICE_TYPE} =~ ^(cpu|gpu)$ ]]; then
  echo "Error: A valid device type (cpu or gpu) is required. Use the -d or --device-type argument."
  print_help
  exit 1
fi

if (( NUM_THREADS <= 0 )); then
  echo "Error: --thread-count must be a positive integer."
  print_help
  exit 1
fi

./stop_presto.sh
./build_centos_deps_image.sh
docker compose -f ../docker/docker-compose.native-$DEVICE_TYPE.yml build $SKIP_CACHE_ARG --build-arg NUM_THREADS=$NUM_THREADS --progress plain
docker compose -f ../docker/docker-compose.native-$DEVICE_TYPE.yml up -d
