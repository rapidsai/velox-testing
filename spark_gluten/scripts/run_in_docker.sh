#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Shared helper for running Gluten tests / benchmarks inside Docker.
#
# Source this file to get the run_in_docker function.
#
# Variables consumed by run_in_docker:
#   GLUTEN_JAR_PATH   – host path to a statically-linked Gluten JAR (static mode)
#   IMAGE_TAG         – Docker image tag for dynamic mode (default: dynamic_gpu_${USER:-latest})
#   REUSE_VENV        – "true" to reuse an existing Python venv
#   EXTRA_DOCKER_ARGS – (optional) array of extra docker compose run arguments
#                       (-v, -e) set by callers before invoking run_in_docker

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
# run_in_docker <venv_dir> <output_dir> [pytest_args...]
#
# Resolves the Docker image, sets up a Python virtual environment, and runs
# pytest inside a Docker container.
#
# Arguments:
#   venv_dir        – Python virtual environment directory name
#   output_dir      – output directory to chown back to the host user
#   pytest_args...  – remaining arguments forwarded to pytest (including
#                     flags and the test file path)
#
# Callers may populate EXTRA_DOCKER_ARGS before calling this function
# (e.g. to mount data directories).  Mode-specific arguments are appended
# to the array automatically.
# ---------------------------------------------------------------------------
run_in_docker() {
  local venv_dir="$1"; shift
  local output_dir="$1"; shift
  # remaining "$@" are pytest args

  local docker_image compose_service setup_cmd container_gluten_jars

  if [[ -n ${GLUTEN_JAR_PATH} ]]; then
    # --- Static JAR mode ---------------------------------------------------
    docker_image="${STATIC_TEST_IMAGE}"
    compose_service="gluten-test"

    # Build the static test image if it does not exist locally.
    if [[ -z "$(docker images -q "${docker_image}" 2>/dev/null)" ]]; then
      echo "Building static test image ${docker_image} ..."
      docker build -t "${docker_image}" -f "${REPO_ROOT}/spark_gluten/docker/static_jar_test.dockerfile" "${SCRIPT_DIR}"
    fi

    # Resolve the host JAR path and translate it for the container.
    local resolved_jar_path
    resolved_jar_path="$(readlink -f "${GLUTEN_JAR_PATH}")"
    if [[ ! -f "${resolved_jar_path}" ]]; then
      echo "Error: JAR file not found: ${GLUTEN_JAR_PATH}"
      exit 1
    fi

    if [[ "${resolved_jar_path}" == "${WORKSPACE_ROOT}"/* ]]; then
      # JAR is inside the workspace – already available via the workspace mount.
      container_gluten_jars="/workspace/${resolved_jar_path#"${WORKSPACE_ROOT}/"}"
    else
      # JAR is outside the workspace – mount its directory.
      local jar_dir jar_file
      jar_dir="$(dirname "${resolved_jar_path}")"
      jar_file="$(basename "${resolved_jar_path}")"
      EXTRA_DOCKER_ARGS+=(-v "${jar_dir}:/host_jars:ro")
      container_gluten_jars="/host_jars/${jar_file}"
    fi

    setup_cmd=""
  else
    # --- Docker image mode --------------------------------------------------
    IMAGE_TAG="${IMAGE_TAG:-dynamic_gpu_${USER:-latest}}"
    docker_image="apache/gluten:${IMAGE_TAG}"

    EXTRA_DOCKER_ARGS+=(-e MINIFORGE_HOME=/opt/miniforge3)

    if [[ "${IMAGE_TAG}" == *gpu* ]]; then
      compose_service="gluten-test-gpu"
    else
      compose_service="gluten-test"
    fi

    setup_cmd='source /opt/rh/gcc-toolset-14/enable 2>/dev/null || source /opt/rh/gcc-toolset-12/enable 2>/dev/null || true;'
    container_gluten_jars=""
  fi

  echo "Running in ${docker_image} (service: ${compose_service}) ..."

  export DOCKER_IMAGE="${docker_image}"
  export WORKSPACE_ROOT

  docker compose -f "${SCRIPT_DIR}/docker-compose.test.yml" run --rm \
    -e "SETUP_CMD=${setup_cmd}" \
    -e "CONTAINER_GLUTEN_JARS=${container_gluten_jars}" \
    -e "REUSE_VENV=${REUSE_VENV}" \
    -e "VENV_DIR=${venv_dir}" \
    -e "OUTPUT_DIR=${output_dir}" \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    "${EXTRA_DOCKER_ARGS[@]}" \
    "${compose_service}" \
    bash /workspace/velox-testing/spark_gluten/scripts/run_in_docker_helper.sh "$@"
}
