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
    $SCRIPT_NAME -j 8
    $SCRIPT_NAME -h

EOF
}

NUM_THREADS=$(($(nproc) / 2))

source ../../scripts/helper_functions.sh
declare -A OPTION_MAP=( ["-b"]="--build-target" ["-j"]="--num-threads" )
make_options "OPTION_MAP"

custom_parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help) print_help; exit 0 ;;
      -n|--no-cache) SKIP_CACHE_ARG="--no-cache"; shift ;;
      $OPTIONS) parse_option $1 $2; shift 2 ;;
      *) echo "Error: Unknown argument $1"; print_help; exit 1 ;;
    esac
  done
}

custom_parse_args "$@"

if [[ -n ${BUILD_TARGET} && ! ${BUILD_TARGET} =~ ^(coordinator|c|worker|w|all|a)$ ]]; then
  echo "Error: invalid --build-target value."
  print_help
  exit 1
fi

if (( NUM_THREADS <= 0 )); then
  echo "Error: --num-threads must be a positive integer."
  print_help
  exit 1
fi
