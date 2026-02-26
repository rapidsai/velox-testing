#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Shared helpers for running Gluten tests / benchmarks inside Docker.
#
# Source this file to get two functions:
#
#   resolve_docker_image   – determines which Docker image and arguments to use
#   run_in_docker          – sets up a venv and runs pytest inside the chosen image
#
# Optional variables consumed by resolve_docker_image:
#   GLUTEN_JAR_PATH – host path to a statically-linked Gluten JAR (static mode)
#   IMAGE_TAG       – Docker image tag for dynamic mode (default: dynamic_gpu_${USER:-latest})

STATIC_TEST_IMAGE="gluten-static-test:latest"

# Compute SCRIPT_DIR and WORKSPACE_ROOT relative to this file.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if command -v git &> /dev/null; then
  REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
else
  REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
fi
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

# ---------------------------------------------------------------------------
# resolve_docker_image
#
# Sets the following variables:
#   DOCKER_IMAGE          – image to use for docker compose run
#   COMPOSE_SERVICE       – compose service name (gluten-test or gluten-test-gpu)
#   SETUP_CMD             – shell snippet to run before the test
#   CONTAINER_GLUTEN_JARS – Gluten JAR path(s) resolved inside the container
#   EXTRA_DOCKER_ARGS     – array of extra docker compose run arguments (-v, -e)
# ---------------------------------------------------------------------------
resolve_docker_image() {
  if [[ -n ${GLUTEN_JAR_PATH} ]]; then
    # --- Static JAR mode ---------------------------------------------------
    DOCKER_IMAGE="${STATIC_TEST_IMAGE}"
    COMPOSE_SERVICE="gluten-test"

    # Build the static test image if it does not exist locally.
    if [[ -z "$(docker images -q "${DOCKER_IMAGE}" 2>/dev/null)" ]]; then
      echo "Building static test image ${DOCKER_IMAGE} ..."
      docker build -t "${DOCKER_IMAGE}" -f "${REPO_ROOT}/spark_gluten/docker/static_jar_test.dockerfile" "${SCRIPT_DIR}"
    fi

    # Resolve the host JAR path and translate it for the container.
    RESOLVED_JAR_PATH="$(readlink -f "${GLUTEN_JAR_PATH}")"
    if [[ ! -f "${RESOLVED_JAR_PATH}" ]]; then
      echo "Error: JAR file not found: ${GLUTEN_JAR_PATH}"
      exit 1
    fi

    EXTRA_DOCKER_ARGS=()
    if [[ "${RESOLVED_JAR_PATH}" == "${WORKSPACE_ROOT}"/* ]]; then
      # JAR is inside the workspace – already available via the workspace mount.
      CONTAINER_GLUTEN_JARS="/workspace/${RESOLVED_JAR_PATH#"${WORKSPACE_ROOT}/"}"
    else
      # JAR is outside the workspace – mount its directory.
      local jar_dir jar_file
      jar_dir="$(dirname "${RESOLVED_JAR_PATH}")"
      jar_file="$(basename "${RESOLVED_JAR_PATH}")"
      EXTRA_DOCKER_ARGS+=(-v "${jar_dir}:/host_jars:ro")
      CONTAINER_GLUTEN_JARS="/host_jars/${jar_file}"
    fi

    SETUP_CMD=""
  else
    # --- Docker image mode --------------------------------------------------
    IMAGE_TAG="${IMAGE_TAG:-dynamic_gpu_${USER:-latest}}"
    DOCKER_IMAGE="apache/gluten:${IMAGE_TAG}"

    EXTRA_DOCKER_ARGS=(-e MINIFORGE_HOME=/opt/miniforge3)

    if [[ "${IMAGE_TAG}" == *gpu* ]]; then
      COMPOSE_SERVICE="gluten-test-gpu"
    else
      COMPOSE_SERVICE="gluten-test"
    fi

    SETUP_CMD='source /opt/rh/gcc-toolset-14/enable 2>/dev/null || source /opt/rh/gcc-toolset-12/enable 2>/dev/null || true;'
    CONTAINER_GLUTEN_JARS=""
  fi
}

# ---------------------------------------------------------------------------
# run_in_docker <venv_dir> <output_dir> [pytest_args...]
#
# Sets up a Python virtual environment and runs pytest inside the Docker
# image chosen by resolve_docker_image.
#
# Arguments:
#   venv_dir        – Python virtual environment directory name
#   output_dir      – output directory to chown back to the host user
#   pytest_args...  – remaining arguments forwarded to pytest (including
#                     flags and the test file path)
#
# Callers may append to EXTRA_DOCKER_ARGS after resolve_docker_image
# and before calling this function (e.g. to mount data directories).
# ---------------------------------------------------------------------------
run_in_docker() {
  local venv_dir="$1"; shift
  local output_dir="$1"; shift
  # remaining "$@" are pytest args

  echo "Running in ${DOCKER_IMAGE} (service: ${COMPOSE_SERVICE}) ..."

  export DOCKER_IMAGE
  export WORKSPACE_ROOT

  docker compose -f "${SCRIPT_DIR}/docker-compose.test.yml" run --rm \
    -e "SETUP_CMD=${SETUP_CMD}" \
    -e "CONTAINER_GLUTEN_JARS=${CONTAINER_GLUTEN_JARS}" \
    -e "REUSE_VENV=${REUSE_VENV}" \
    -e "VENV_DIR=${venv_dir}" \
    -e "OUTPUT_DIR=${output_dir}" \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    "${EXTRA_DOCKER_ARGS[@]}" \
    "${COMPOSE_SERVICE}" \
    bash /workspace/velox-testing/spark_gluten/scripts/run_in_docker_helper.sh "$@"
}
