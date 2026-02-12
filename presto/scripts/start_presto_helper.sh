#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|java)$ ]]; then
  echo "Internal error: A valid variant type (cpu, gpu, or java) is required. Set VARIANT_TYPE to an appropriate value."
  exit 1
fi

if [[ -z ${SCRIPT_NAME} ]]; then
  echo "Internal error: SCRIPT_NAME must be set."
  exit 1
fi

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the root of the git repository
if command -v git &> /dev/null; then
  REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
else
  REPO_ROOT="$SCRIPT_DIR/../.."
fi
       

# Validate sibling repos
if [[ "$VARIANT_TYPE" == "java" ]]; then
  "${REPO_ROOT}/scripts/validate_directories_exist.sh" "${REPO_ROOT}/../presto"
else
  "${REPO_ROOT}/scripts/validate_directories_exist.sh" "${REPO_ROOT}/../presto" "${REPO_ROOT}/../velox"
fi

source "${SCRIPT_DIR}/start_presto_helper_parse_args.sh"


if [[ "$PROFILE" == "ON" && "$VARIANT_TYPE" != "gpu" ]]; then
  echo "Error: the --profile argument is only supported for Presto GPU"
  exit 1
fi

if [[ "$PROFILE" == "ON" && $NUM_WORKERS -gt 1 && "$SINGLE_CONTAINER" == "false" ]]; then
  echo "Error: multi-worker --profile argument is only currently supported with the --single-container option"
  exit 1
fi

# Set PRESTO_IMAGE_TAG to the username in order to avoid conflicts when multiple users build images.
# Falls back to "latest" if USER is not set.
export PRESTO_IMAGE_TAG="${USER:-latest}"
echo "Using PRESTO_IMAGE_TAG: $PRESTO_IMAGE_TAG"

COORDINATOR_SERVICE="presto-coordinator"
COORDINATOR_IMAGE=${COORDINATOR_SERVICE}:${PRESTO_IMAGE_TAG}
JAVA_WORKER_SERVICE="presto-java-worker"
JAVA_WORKER_IMAGE=${JAVA_WORKER_SERVICE}:${PRESTO_IMAGE_TAG}
CPU_WORKER_SERVICE="presto-native-worker-cpu"
CPU_WORKER_IMAGE=${CPU_WORKER_SERVICE}:${PRESTO_IMAGE_TAG}
GPU_WORKER_SERVICE="presto-native-worker-gpu"
GPU_WORKER_IMAGE=${GPU_WORKER_SERVICE}:${PRESTO_IMAGE_TAG}

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
else
  echo "Internal error: unexpected VARIANT_TYPE value: $VARIANT_TYPE"
fi

"${SCRIPT_DIR}/stop_presto.sh"

"${SCRIPT_DIR}/generate_presto_config.sh"

# must determine CUDA_ARCHITECTURES here as nvidia-smi is not available in the docker build context
CUDA_ARCHITECTURES=""
if [[ "$VARIANT_TYPE" == "gpu" && "$ALL_CUDA_ARCHS" == "true" ]]; then
  # build for all supported CUDA architectures
  CUDA_ARCHITECTURES="75;80;86;90;100;120"
  echo "Building GPU with all supported CUDA architectures"
elif [[ "$VARIANT_TYPE" == "gpu" ]]; then
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

DOCKER_COMPOSE_FILE_PATH="${SCRIPT_DIR}/../docker/docker-compose.$DOCKER_COMPOSE_FILE.yml"
# For GPU, the docker-compose file is a Jinja template. Render it before any docker compose operations.
if [[ "$VARIANT_TYPE" == "gpu" ]]; then
  TEMPLATE_PATH="${SCRIPT_DIR}/../docker/docker-compose/template/docker-compose.$DOCKER_COMPOSE_FILE.yml.jinja"
  RENDERED_DIR="${SCRIPT_DIR}/../docker/docker-compose/generated"
  mkdir -p "$RENDERED_DIR"
  RENDERED_PATH="$RENDERED_DIR/docker-compose.$DOCKER_COMPOSE_FILE.rendered.yml"
  # Default to 0 if not provided, which results in no per-GPU workers being rendered.
  LOCAL_NUM_WORKERS="${NUM_WORKERS:-0}"

  RENDER_SCRIPT_PATH=$(readlink -f "${SCRIPT_DIR}/../../template_rendering/render_docker_compose_template.py")
  if [[ -n $GPU_IDS ]]; then
    "${SCRIPT_DIR}/../../scripts/run_py_script.sh" -p "$RENDER_SCRIPT_PATH" "--template-path $TEMPLATE_PATH" "--output-path $RENDERED_PATH" "--num-workers $NUM_WORKERS" "--single-container $SINGLE_CONTAINER" "--gpu-ids $GPU_IDS" "--kvikio-threads $KVIKIO_THREADS"
  else
    "${SCRIPT_DIR}/../../scripts/run_py_script.sh" -p "$RENDER_SCRIPT_PATH" "--template-path $TEMPLATE_PATH" "--output-path $RENDERED_PATH" "--num-workers $NUM_WORKERS" "--single-container $SINGLE_CONTAINER" "--kvikio-threads $KVIKIO_THREADS"
  fi
  DOCKER_COMPOSE_FILE_PATH="$RENDERED_PATH"
fi
if (( ${#BUILD_TARGET_ARG[@]} )); then
  if [[ ${BUILD_TARGET_ARG[@]} =~ ($CPU_WORKER_SERVICE|$GPU_WORKER_SERVICE) ]] && is_image_missing ${DEPS_IMAGE}; then
    echo "ERROR: Presto dependencies/run-time image '${DEPS_IMAGE}' not found!"
    echo "Either build a local image using build_centos9_deps_image.sh or fetch a pre-built"
    echo "image using fetch_centos9_deps_image.sh (credentials may be required)."
    exit 1
  fi

  PRESTO_VERSION=testing
  if [[ ${BUILD_TARGET_ARG[@]} =~ ($COORDINATOR_SERVICE|$JAVA_WORKER_SERVICE) ]]; then
    PRESTO_VERSION=$PRESTO_VERSION "${SCRIPT_DIR}/build_presto_java_package.sh"
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
