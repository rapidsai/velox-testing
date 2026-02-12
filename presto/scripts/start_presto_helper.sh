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
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"

#ptodo conditionally check if build is needed and show error message
#ptodo change existing start scripts in CI
#ptodo change documentation (see for all referenct of start scripts)

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


# Default GPU_IDS if NUM_WORKERS is set but GPU_IDS is not
if [[ -n $NUM_WORKERS && -z $GPU_IDS ]]; then
  # Generate default GPU IDs: 0,1,2,...,N-1
  export GPU_IDS=$(seq -s, 0 $((NUM_WORKERS - 1)))
fi

"${SCRIPT_DIR}/stop_presto.sh"

"${SCRIPT_DIR}/generate_presto_config.sh"


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

# Start all services defined in the rendered docker-compose file.
docker compose -f $DOCKER_COMPOSE_FILE_PATH up -d
