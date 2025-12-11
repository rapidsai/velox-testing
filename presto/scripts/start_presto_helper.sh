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

if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|java)$ ]]; then
  echo "Internal error: A valid variant type (cpu, gpu, or java) is required. Set VARIANT_TYPE to an appropriate value."
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


if [[ "$PROFILE" == "ON" && "$VARIANT_TYPE" != "gpu" ]]; then
  echo "Error: the --profile argument is only supported for Presto GPU"
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

./stop_presto.sh

./generate_presto_config.sh

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

DOCKER_COMPOSE_FILE_PATH=../docker/docker-compose.$DOCKER_COMPOSE_FILE.yml
if (( ${#BUILD_TARGET_ARG[@]} )); then
  if [[ ${BUILD_TARGET_ARG[@]} =~ ($CPU_WORKER_SERVICE|$GPU_WORKER_SERVICE) ]] && is_image_missing ${DEPS_IMAGE}; then
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

function generate_worker_config() {
    local worker_id=$1
    local worker_config="../docker/config/generated/gpu/etc_worker_${worker_id}"
    rm -rf ${worker_config}
    # We are duplicating the config generated by pbench,
    # but need to modify it for multi-worker execution.
    cp -r ../docker/config/generated/gpu/etc_worker ${worker_config}

    # Disable single-execution mode.
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" \
        ${worker_config}/config_native.properties

    # Give each worker a unique id.
    sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_config}/node.properties
}

function generate_worker_service() {
    # These variables were created so that we have more fine-grain control over the naming
    # of services and files (so we don't have to change legacy names for 1-worker setup)
    local worker_suffix="$1"
    local file_suffix="$2"
    local worker_gpu="$3"

    cat >> "$DOCKER_COMPOSE_FILE_PATH" <<YAML

  presto-native-worker-gpu${worker_suffix}:
    extends:
      file: docker-compose.common.yml
      service: presto-base-native-worker
    container_name: presto-native-worker-gpu${worker_suffix}
    image: presto-native-worker-gpu:latest
    build:
      args:
        - GPU=ON
    runtime: nvidia
    environment:
      - NVIDIA_VISIBLE_DEVICES=${worker_gpu}
      - PROFILE=\${PROFILE}
      - PROFILE_ARGS=\${PROFILE_ARGS}
    depends_on:
      - presto-coordinator
    volumes:
      - ./config/generated/gpu/etc_common:/opt/presto-server/etc
      - ./config/generated/gpu/etc_worker${file_suffix}/node.properties:/opt/presto-server/etc/node.properties
      - ./config/generated/gpu/etc_worker${file_suffix}/config_native.properties:/opt/presto-server/etc/config.properties
YAML
}

function generate_coordinator_service() {
    # Start the override file
    cat > "$DOCKER_COMPOSE_FILE_PATH" <<YAML
services:
  presto-coordinator:
    extends:
      file: docker-compose.common.yml
      service: presto-base-coordinator
    volumes:
      - ./config/generated/gpu/etc_common:/opt/presto-server/etc
      - ./config/generated/gpu/etc_coordinator/config_native.properties:/opt/presto-server/etc/config.properties
      - ./config/generated/gpu/etc_coordinator/node.properties:/opt/presto-server/etc/node.properties
YAML
}

function generate_worker_compose() {
    generate_coordinator_service

    if [[ "$NUM_WORKERS" -gt "1" ]]; then
        # Generate NUM_WORKERS services, each with one gpu.
        for i in $(seq 0 $((NUM_WORKERS-1))); do
            generate_worker_service "-$i" "_$i" "$i"
            generate_worker_config $i
        done
    else
        # Generate one worker service with access to all gpus
        generate_worker_service "" "" "all"
    fi
}

if [[ "$VARIANT_TYPE" == "gpu" ]]; then
    generate_worker_compose
fi

docker compose -f $DOCKER_COMPOSE_FILE_PATH up -d
