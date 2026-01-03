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

if [[ "$PROFILE" == "ON" && $(( $NUM_WORKERS > 1 )) && "$SINGLE_CONTAINER" == "false" ]]; then
  echo "Error: multi-worker --profile argument is only currently supported with the --single-container option"
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

# Default GPU_IDS if NUM_WORKERS is set but GPU_IDS is not
if [[ -n $NUM_WORKERS && -z $GPU_IDS ]]; then
  # Generate default GPU IDs: 0,1,2,...,N-1
  export GPU_IDS=$(seq -s, 0 $((NUM_WORKERS - 1)))
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
# For GPU, the docker-compose file is a Jinja template. Render it before any docker compose operations.
if [[ "$VARIANT_TYPE" == "gpu" ]]; then
  TEMPLATE_PATH="../docker/docker-compose/template/docker-compose.$DOCKER_COMPOSE_FILE.yml.jinja"
  RENDERED_DIR="../docker/docker-compose/generated"
  mkdir -p "$RENDERED_DIR"
  RENDERED_PATH="$RENDERED_DIR/docker-compose.$DOCKER_COMPOSE_FILE.rendered.yml"
  # Default to 0 if not provided, which results in no per-GPU workers being rendered.
  LOCAL_NUM_WORKERS="${NUM_WORKERS:-0}"

  RENDER_SCRIPT_PATH=$(readlink -f ../../template_rendering/render_docker_compose_template.py)
  if [[ -n $GPU_IDS ]]; then
    ../../scripts/run_py_script.sh -p "$RENDER_SCRIPT_PATH" "--template-path $TEMPLATE_PATH" "--output-path $RENDERED_PATH" "--num-workers $NUM_WORKERS" "--single-container $SINGLE_CONTAINER" "--gpu-ids $GPU_IDS" "--kvikio-threads $KVIKIO_THREADS"
  else
    ../../scripts/run_py_script.sh -p "$RENDER_SCRIPT_PATH" "--template-path $TEMPLATE_PATH" "--output-path $RENDERED_PATH" "--num-workers $NUM_WORKERS" "--single-container $SINGLE_CONTAINER" "--kvikio-threads $KVIKIO_THREADS"
  fi
  DOCKER_COMPOSE_FILE_PATH="$RENDERED_PATH"
fi
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
  docker compose --progress plain -f $DOCKER_COMPOSE_FILE_PATH build \
  $SKIP_CACHE_ARG --build-arg PRESTO_VERSION=$PRESTO_VERSION \
  --build-arg NUM_THREADS=$NUM_THREADS --build-arg BUILD_TYPE=$BUILD_TYPE \
  --build-arg CUDA_ARCHITECTURES=$CUDA_ARCHITECTURES \
  ${BUILD_TARGET_ARG[@]}
fi

# Start all services defined in the rendered docker-compose file.
docker compose -f $DOCKER_COMPOSE_FILE_PATH up -d
