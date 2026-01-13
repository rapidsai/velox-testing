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

function to_abs_path() {
  local target="$1"
  realpath "$target"
}

function prepare_gpu_dev_environment() {
  local default_state_root="../devstate"
  local configured_root="${PRESTO_DEV_STATE_ROOT:-$default_state_root}"

  mkdir -p "$configured_root"
  PRESTO_DEV_STATE_ROOT=$(to_abs_path "$configured_root")

  export PRESTO_DEV_STATE_ROOT
}

function is_gpu_variant() {
  [[ "$VARIANT_TYPE" == "gpu" || "$VARIANT_TYPE" == "gpu-dev" ]]
}

DOCKER_COMPOSE_EXTRA_ARGS=()

if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|gpu-dev|java)$ ]]; then
  echo "Internal error: A valid variant type (cpu, gpu, gpu-dev, or java) is required. Set VARIANT_TYPE to an appropriate value."
  exit 1
fi

if [[ -z ${SCRIPT_NAME} ]]; then
  echo "Internal error: SCRIPT_NAME must be set."
  exit 1
fi

# Validate sibling repos
if [[ "$VARIANT_TYPE" == "java" ]]; then
  ../../scripts/validate_directories_exist.sh "../../../presto"
else
  ../../scripts/validate_directories_exist.sh "../../../presto" "../../../velox"
fi

source ./start_presto_helper_parse_args.sh


if [[ "$PROFILE" == "ON" ]] && ! is_gpu_variant; then
  echo "Error: the --profile argument is only supported for Presto GPU variants"
  exit 1
fi

COORDINATOR_SERVICE="presto-coordinator"
COORDINATOR_IMAGE=${COORDINATOR_SERVICE}:latest
JAVA_WORKER_SERVICE="presto-java-worker"
JAVA_WORKER_IMAGE=${JAVA_WORKER_SERVICE}:latest
CPU_WORKER_SERVICE="presto-native-worker-cpu"
CPU_WORKER_IMAGE=${CPU_WORKER_SERVICE}:latest
GPU_WORKER_SERVICE="presto-native-worker-gpu"
GPU_WORKER_IMAGE=${GPU_WORKER_SERVICE}:latest
GPU_DEV_WORKER_SERVICE="presto-native-worker-gpu-dev"
GPU_DEV_WORKER_IMAGE=${GPU_DEV_WORKER_SERVICE}:latest

DEPS_IMAGE="presto/prestissimo-dependency:centos9"

BUILD_TARGET_ARG=()

function is_image_missing() {
  [[ -z "$(docker images -q $1)" ]]
}

function conditionally_add_build_target() {
  if is_image_missing $1; then
    echo "Added $2 to the list of services to build because the $1 image is missing"
    BUILD_TARGET_ARG+=($2)
  elif [[ ${BUILD_TARGET} =~ ^($3|all|a)$ ]]; then
    echo "Added $2 to the list of services to build because the '$BUILD_TARGET' build target was specified"
    BUILD_TARGET_ARG+=($2)
  fi
} 

function conditionally_add_build_target_if_missing() {
  if is_image_missing $1; then
    echo "Added $2 to the list of services to build because the $1 image is missing"
    BUILD_TARGET_ARG+=($2)
  fi
}

function ensure_build_target() {
  local service="$1"
  for existing in "${BUILD_TARGET_ARG[@]}"; do
    if [[ "$existing" == "$service" ]]; then
      return 0
    fi
  done
  BUILD_TARGET_ARG+=("$service")
}

conditionally_add_build_target $COORDINATOR_IMAGE $COORDINATOR_SERVICE "coordinator|c"

if [[ "$VARIANT_TYPE" == "java" ]]; then
  DOCKER_COMPOSE_FILE="java"
  conditionally_add_build_target $JAVA_WORKER_IMAGE $JAVA_WORKER_SERVICE "worker|w"
elif [[ "$VARIANT_TYPE" == "cpu" ]]; then
  DOCKER_COMPOSE_FILE="native-cpu"
  conditionally_add_build_target $CPU_WORKER_IMAGE $CPU_WORKER_SERVICE "worker|w"
elif [[ "$VARIANT_TYPE" == "gpu" ]]; then
  DOCKER_COMPOSE_FILE="native-gpu"
  conditionally_add_build_target $GPU_WORKER_IMAGE $GPU_WORKER_SERVICE "worker|w"
elif [[ "$VARIANT_TYPE" == "gpu-dev" ]]; then
  DOCKER_COMPOSE_FILE="native-gpu-dev"
  prepare_gpu_dev_environment

  # Optional: mount a local cuDF checkout into the dev worker container and
  # forward the in-container path via PRESTO_CUDF_DIR.
  #
  # Usage:
  #   PRESTO_CUDF_DIR=/abs/path/to/cudf ./start_native_gpu_dev_presto.sh
  #
  if [[ -n "${PRESTO_CUDF_DIR:-}" ]]; then
    if [[ ! -d "$PRESTO_CUDF_DIR" || ! -d "$PRESTO_CUDF_DIR/cpp" ]]; then
      echo "ERROR: PRESTO_CUDF_DIR must point to a cuDF checkout containing a 'cpp/' directory. Got: $PRESTO_CUDF_DIR"
      exit 1
    fi

    PRESTO_CUDF_DIR=$(to_abs_path "$PRESTO_CUDF_DIR")

    # Keep separate build dirs for "default cuDF" vs "local cuDF override" so you can
    # switch PRESTO_CUDF_DIR on/off without wiping the build directory.
    #
    # If you want a custom naming scheme, set PRESTO_BUILD_DIR_NAME explicitly.
    if [[ -z "${PRESTO_BUILD_DIR_NAME:-}" ]]; then
      export PRESTO_BUILD_DIR_NAME="relwithdebinfo-localcudf"
    fi

    # Make the override work even if the gpu-dev image wasn't rebuilt yet by passing the
    # cuDF FetchContent override via PRESTO_EXTRA_CMAKE_FLAGS_APPEND (consumed by the container
    # entrypoint /opt/launch_presto_server_dev.sh).
    presto_extra_flags_append="${PRESTO_EXTRA_CMAKE_FLAGS_APPEND:-}"
    presto_extra_flags_append="${presto_extra_flags_append} -Dcudf_SOURCE=BUNDLED -DFETCHCONTENT_SOURCE_DIR_CUDF=/workspace/cudf"

    override_file="$(mktemp "${PRESTO_DEV_STATE_ROOT}/docker-compose.cudf.XXXXXX.yml")"
    {
      printf '%s\n' "services:"
      printf '%s\n' "  presto-native-worker-gpu-dev:"
      printf '%s\n' "    volumes:"
      printf '%s\n' "      - ${PRESTO_CUDF_DIR}:/workspace/cudf:rw"
      printf '%s\n' "    environment:"
      printf '%s\n' "      - PRESTO_CUDF_DIR=/workspace/cudf"
      printf '%s\n' "      - PRESTO_BUILD_DIR_NAME=${PRESTO_BUILD_DIR_NAME}"
      # Forward host-provided base flags only if explicitly set (otherwise keep container defaults).
      if [[ -n "${PRESTO_EXTRA_CMAKE_FLAGS:-}" ]]; then
        printf '%s\n' "      - PRESTO_EXTRA_CMAKE_FLAGS=${PRESTO_EXTRA_CMAKE_FLAGS}"
      fi
      printf '%s\n' "      - PRESTO_EXTRA_CMAKE_FLAGS_APPEND=${presto_extra_flags_append}"
    } >"$override_file"
    DOCKER_COMPOSE_EXTRA_ARGS=(-f "$override_file")

    # The override file is only needed to start/build the containers.
    trap 'rm -f "$override_file"' EXIT
  fi

  # For the dev worker, image rebuilds are usually not needed for code changes.
  # The presto_server binary is built inside the running container against the
  # persistent build dir. Only rebuild the dev image lazily if missing.
  conditionally_add_build_target_if_missing $GPU_DEV_WORKER_IMAGE $GPU_DEV_WORKER_SERVICE

  # Honor "-b worker" (or "-b all") for the dev variant by forcing an incremental
  # in-container rebuild of presto_server at startup (without wiping the build dir).
  if [[ ${BUILD_TARGET} =~ ^(worker|w|all|a)$ ]]; then
    if [[ -n ${SKIP_CACHE_ARG:-} ]]; then
      # Special case: "-b w --no-cache" should rebuild the dev image and force a
      # clean native rebuild by deleting the build dir (for the selected build type).
      ensure_build_target "$GPU_DEV_WORKER_SERVICE"
      export PRESTO_FORCE_REBUILD=1
      export PRESTO_REBUILD=0
    else
      export PRESTO_REBUILD=1
    fi
  fi
else
  echo "Internal error: unexpected VARIANT_TYPE value: $VARIANT_TYPE"
fi

./stop_presto.sh

./generate_presto_config.sh

# must determine CUDA_ARCHITECTURES here as nvidia-smi is not available in the docker build context
CUDA_ARCHITECTURES=""
if is_gpu_variant && [[ "$ALL_CUDA_ARCHS" == "true" ]]; then
  # build for all supported CUDA architectures
  CUDA_ARCHITECTURES="75;80;86;90;100;120"
  echo "Building GPU with all supported CUDA architectures"
elif is_gpu_variant; then
  # check that nvidia-smi is available
  if ! command -v nvidia-smi &> /dev/null; then
    echo "ERROR: nvidia-smi could not be found. Please ensure that the NVIDIA drivers and Docker runtime are properly installed."
    exit 1
  fi
  # build for the native compute capability of the first GPU (assuming all GPUs are the same)
  CUDA_ARCHITECTURES="$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -n 1 | sed 's/\.//g')"
  echo "Building GPU with CUDA_ARCHITECTURES=$CUDA_ARCHITECTURES"
elif [[ "$ALL_CUDA_ARCHS" == "true" ]]; then
  # invalid options combination
  echo "ERROR: --all-cuda-archs specified but VARIANT_TYPE is not 'gpu'."
  exit 1
fi

DOCKER_COMPOSE_FILE_PATH=../docker/docker-compose.$DOCKER_COMPOSE_FILE.yml
if (( ${#BUILD_TARGET_ARG[@]} )); then
  if [[ ${BUILD_TARGET_ARG[@]} =~ ($CPU_WORKER_SERVICE|$GPU_WORKER_SERVICE|$GPU_DEV_WORKER_SERVICE) ]] && is_image_missing ${DEPS_IMAGE}; then
    echo "ERROR: Presto dependencies/run-time image '${DEPS_IMAGE}' not found!"
    echo "Either build a local image using build_centos9_deps_image.sh or fetch a pre-built"
    echo "image using fetch_centos9_deps_image.sh (credentials may be required)."
    exit 1
  fi

  PRESTO_VERSION=testing
  if [[ ${BUILD_TARGET_ARG[@]} =~ ($COORDINATOR_SERVICE|$JAVA_WORKER_SERVICE) ]]; then
    PRESTO_VERSION=$PRESTO_VERSION ./build_presto_java_package.sh
  fi

  echo "Building services: ${BUILD_TARGET_ARG[@]}"
  docker compose --progress plain -f $DOCKER_COMPOSE_FILE_PATH "${DOCKER_COMPOSE_EXTRA_ARGS[@]}" build \
  $SKIP_CACHE_ARG --build-arg PRESTO_VERSION=$PRESTO_VERSION \
  --build-arg NUM_THREADS=$NUM_THREADS --build-arg BUILD_TYPE=$BUILD_TYPE \
  --build-arg CUDA_ARCHITECTURES=$CUDA_ARCHITECTURES \
  ${BUILD_TARGET_ARG[@]}
fi

docker compose -f $DOCKER_COMPOSE_FILE_PATH "${DOCKER_COMPOSE_EXTRA_ARGS[@]}" up -d
