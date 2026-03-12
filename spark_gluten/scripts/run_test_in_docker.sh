#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Shared helper for running Gluten tests / benchmarks inside Docker.
#
# Source this file to get the run_test_in_docker function.

STATIC_TEST_IMAGE="gluten-static-test:latest"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

# ---------------------------------------------------------------------------
# resolve_config_args
#
# Resolves --spark-config and --env-file options into EXTRA_DOCKER_ARGS and
# PYTEST_ARGS.  Call this after parsing script-specific options and before
# calling run_test_in_docker.
#
# Variables consumed (set by the calling script):
#   SPARK_CONFIG_FILE – host path to a Spark config file
#   ENV_FILE          – host path to an environment variable file
#
# Variables modified:
#   EXTRA_DOCKER_ARGS – docker args array (bind-mount / -e flags appended)
#   PYTEST_ARGS       – pytest args array (--spark-config appended)
# ---------------------------------------------------------------------------
resolve_config_args() {
  if [[ -n ${SPARK_CONFIG_FILE} ]]; then
    SPARK_CONFIG_FILE="$(readlink -f "${SPARK_CONFIG_FILE}")"
    EXTRA_DOCKER_ARGS+=(-v "${SPARK_CONFIG_FILE}:${SPARK_CONFIG_FILE}:ro")
    PYTEST_ARGS+=("--spark-config" "${SPARK_CONFIG_FILE}")
  fi

  if [[ -n ${ENV_FILE} ]]; then
    ENV_FILE="$(readlink -f "${ENV_FILE}")"
    while IFS= read -r line || [[ -n "$line" ]]; do
      # Skip blank lines and comments.
      [[ -z "$line" || "$line" == \#* ]] && continue
      EXTRA_DOCKER_ARGS+=(-e "$line")
    done < "${ENV_FILE}"
  fi
}

# ---------------------------------------------------------------------------
# run_test_in_docker <venv_dir> <output_dir> [pytest_args...]
#
# Resolves the Docker image, sets up a Python virtual environment, and runs
# pytest inside a Docker container.
#
# Arguments:
#   venv_dir        – Python virtual environment directory name
#   output_dir      – output directory
#   pytest_args...  – remaining arguments forwarded to pytest (including
#                     flags and the test file path)
#
# Variables consumed by run_test_in_docker:
#   GLUTEN_JAR_PATH   – host path to a Gluten JAR (its directory is mounted to /opt/gluten/jars)
#   IMAGE_TAG         – Tag of image where the test or benchmark will be run
#   REUSE_VENV        – "true" to reuse an existing Python venv
#   EXTRA_DOCKER_ARGS – (optional) array of extra docker compose run arguments
#                       (e.g. -v, -e) set by callers before invoking run_test_in_docker
#
# Callers may populate EXTRA_DOCKER_ARGS before calling this function
# (e.g. to mount data directories).
# ---------------------------------------------------------------------------
run_test_in_docker() {
  local venv_dir="$1"; shift
  local output_dir="$1"; shift
  # remaining "$@" are pytest args

  local docker_image
  local compose_service

  if [[ -n ${GLUTEN_JAR_PATH} ]]; then
    docker_image="${STATIC_TEST_IMAGE}"
    compose_service="gluten-test"

    # Build the static test image if it does not exist locally.
    if [[ -z "$(docker images -q "${docker_image}" 2>/dev/null)" ]]; then
      echo "Building static test image ${docker_image} ..."
      docker build -t "${docker_image}" -f "${REPO_ROOT}/spark_gluten/docker/static_jar_test.dockerfile" "${SCRIPT_DIR}"
    fi

    # Resolve the host JAR path and mount its directory to /opt/gluten/jars.
    local resolved_jar_path
    resolved_jar_path="$(readlink -f "${GLUTEN_JAR_PATH}")"
    if [[ ! -f "${resolved_jar_path}" ]]; then
      echo "Error: JAR file not found: ${GLUTEN_JAR_PATH}"
      exit 1
    fi
    EXTRA_DOCKER_ARGS+=(-v "$(dirname "${resolved_jar_path}"):/opt/gluten/jars:ro")
  else
    IMAGE_TAG="${IMAGE_TAG:-dynamic_gpu_${USER:-latest}}"
    docker_image="apache/gluten:${IMAGE_TAG}"

    # Read GLUTEN_DEVICE_TYPE from the image to determine the compose service.
    local device_type
    device_type="$(docker run --rm "${docker_image}" bash -c 'echo $GLUTEN_DEVICE_TYPE')"
    if [[ "${device_type}" == "gpu" ]]; then
      compose_service="gluten-test-gpu"
    else
      compose_service="gluten-test"
    fi
  fi

  echo "Running in ${docker_image} (service: ${compose_service}) ..."

  export DOCKER_IMAGE="${docker_image}"
  export WORKSPACE_ROOT

  docker compose -f "${SCRIPT_DIR}/docker-compose.test.yml" run --rm \
    -e "REUSE_VENV=${REUSE_VENV}" \
    -e "VENV_DIR=${venv_dir}" \
    -e "OUTPUT_DIR=${output_dir}" \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    "${EXTRA_DOCKER_ARGS[@]}" \
    "${compose_service}" \
    bash /workspace/velox-testing/spark_gluten/scripts/run_test_in_docker_helper.sh "$@"
}
