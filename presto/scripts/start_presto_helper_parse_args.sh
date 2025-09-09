#!/bin/bash

set -e

print_help() {
  cat << EOF

Usage: $SCRIPT_NAME [OPTIONS]

This script deploys a Presto server cluster (one coordinator node and one $VARIANT_TYPE worker node).

OPTIONS:
    -h, --help           Show this help message.
    -n, --no-cache       Do not use the builder cache when building the image.
    -b, --build          Service type to build from source. Possible values are 
                         "coordinator" or "c", "worker" or "w", and "all" or "a".
                         By default, services will be lazily built i.e. a build 
                         will only occur if there is no local image for the service.
    -j, --num-threads    Number of threads to use when building the image (default is `nproc` / 2).

EXAMPLES:
    $SCRIPT_NAME --no-cache
    $SCRIPT_NAME -b worker
    $SCRIPT_NAME --build c
    $SCRIPT_NAME -t 8
    $SCRIPT_NAME -h

EOF
}

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
      -b|--build)
        if [[ -n $2 ]]; then
          BUILD_TARGET=$2
          shift 2
        else
          echo "Error: --build requires a value"
          exit 1
        fi
        ;;
      -j|--num-threads)
        if [[ -n $2 ]]; then
          NUM_THREADS=$2
          shift 2
        else
          echo "Error: --num-threads requires a value"
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

if [[ -n ${BUILD_TARGET} && ! ${BUILD_TARGET} =~ ^(coordinator|c|worker|w|all|a)$ ]]; then
  echo "Error: invalid --build value."
  print_help
  exit 1
fi

if (( NUM_THREADS <= 0 )); then
  echo "Error: --num-threads must be a positive integer."
  print_help
  exit 1
fi
