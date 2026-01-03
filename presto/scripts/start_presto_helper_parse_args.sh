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
    -w, --num-workers    Number of GPU workers to start (GPU variant only).
    -g, --gpu-ids        Comma-delimited list of GPU device IDs to use (e.g., "0,1,3,5").
                         Must be used with --num-workers. If not specified, defaults to "0,1,...,N-1"
                         where N is the value from --num-workers (GPU variant only).
    --single-container   Launch multiple Presto servers in a single container (GPU variant only).
    --build-type         Build type for native CPU and GPU image builds. Possible values are "release",
                         "relwithdebinfo", or "debug". Values are case insensitive. The default value
                         is "release".
    --all-cuda-archs     Build for all supported CUDA architectures (GPU only) (default: false).
    -p, --profile        Launch the Presto server with profiling enabled.
    --profile-args       Arguments to pass to the profiler when it launches the Presto server.
                         This will override the default arguments.
    --overwrite-config   Force config to be regenerated (will overwrite local changes).
    -s, --skip-server    (gpu-dev only) Keep the dev worker container running but do not start presto_server.

EXAMPLES:
    $SCRIPT_NAME --no-cache
    $SCRIPT_NAME -b worker
    $SCRIPT_NAME --build c
    $SCRIPT_NAME -j 8
    $SCRIPT_NAME -w 4
    $SCRIPT_NAME -w 4 -g 4,5,6,7
    $SCRIPT_NAME --profile
    $SCRIPT_NAME --skip-server
    $SCRIPT_NAME -h

EOF
}

NUM_THREADS=$(($(nproc) / 2))
BUILD_TYPE=release
ALL_CUDA_ARCHS=false
export SINGLE_CONTAINER=false
export OVERWRITE_CONFIG=false
export PROFILE=OFF
export NUM_WORKERS=1
export KVIKIO_THREADS=8
export VCPU_PER_WORKER=""
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
      --kvikio-threads)
        if [[ -n $2 ]]; then
          export KVIKIO_THREADS=$2
          shift 2
        else
          echo "Error: --kvikio-threads requires a value"
          exit 1
        fi
        ;;
      --num-drivers)
        if [[ -n $2 ]]; then
          export VCPU_PER_WORKER=$2
          shift 2
        else
          echo "Error: --num-drivers requires a value"
          exit 1
        fi
        ;;
      -w|--num-workers)
        if [[ -n $2 ]]; then
          export NUM_WORKERS=$2
          shift 2
        else
          echo "Error: --num-workers requires a value"
          exit 1
        fi
        ;;
      -g|--gpu-ids)
        if [[ -n $2 ]]; then
          export GPU_IDS=$2
          shift 2
        else
          echo "Error: --gpu-ids requires a value"
          exit 1
        fi
        ;;
      --single-container)
        export SINGLE_CONTAINER=true
        shift
        ;;
      --build-type)
        if [[ -n $2 ]]; then
          # Convert value to lowercase using the "L" transformation operator.
          BUILD_TYPE=${2@L}
          shift 2
        else
          echo "Error: --build-type requires a value"
          exit 1
        fi
        ;;
      -p|--profile)
        PROFILE=ON
        shift
        ;;
      --profile-args)
        if [[ -n $2 ]]; then
          export PROFILE_ARGS=$2
          shift 2
        else
          echo "Error: --profile-args requires a value"
          exit 1
        fi
        ;;
      --all-cuda-archs)
        ALL_CUDA_ARCHS=true
        shift
        ;;
      --overwrite-config)
        OVERWRITE_CONFIG=true
        shift
        ;;
      -s|--skip-server)
        export PRESTO_SKIP_SERVER=1
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

if [[ ! ${BUILD_TYPE} =~ ^(release|relwithdebinfo|debug)$ ]]; then
  echo "Error: invalid --build-type value."
  print_help
  exit 1
fi

if [[ -n $PROFILE_ARGS && "$PROFILE" == "OFF" ]]; then
  echo "Error: the --profile-args argument should only be set when --profile is enabled"
  exit 1
fi

# Validation for GPU IDs
if [[ -n $GPU_IDS ]]; then
  # If GPU_IDS is set, NUM_WORKERS must also be set
  if [[ -z $NUM_WORKERS ]]; then
    echo "Error: --gpu-ids requires --num-workers to be set"
    exit 1
  fi
  
  # Validate that GPU_IDS is a comma-delimited list of integers
  if [[ ! $GPU_IDS =~ ^[0-9]+(,[0-9]+)*$ ]]; then
    echo "Error: --gpu-ids must be a comma-delimited list of integers (e.g., '0,1,2,3')"
    exit 1
  fi
  
  # Count the number of GPU IDs provided
  IFS=',' read -ra GPU_ID_ARRAY <<< "$GPU_IDS"
  GPU_ID_COUNT=${#GPU_ID_ARRAY[@]}
  
  # Validate that the count matches NUM_WORKERS
  if [[ $GPU_ID_COUNT -ne $NUM_WORKERS ]]; then
    echo "Error: number of GPU IDs ($GPU_ID_COUNT) must match --num-workers ($NUM_WORKERS)"
    exit 1
  fi
fi
