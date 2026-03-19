#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Starts a Spark Connect server inside a Docker container via docker compose.
# The server runs in the foreground; Ctrl+C to stop.

set -e

STATIC_TEST_IMAGE="gluten-static-test:latest"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script starts a Spark Connect server in a Docker container.

OPTIONS:
    -h, --help                      Show this help message.
    --image-tag                     Docker image tag. The full image reference is
                                    "apache/gluten:{image-tag}". Cannot be used with
                                    --static-gluten-jar-path.
    --static-gluten-jar-path        Path to a statically-linked Gluten JAR on the host.
                                    Cannot be used with --image-tag.
    --spark-config                  Path to a Spark config file. Values are merged on top of
                                    the default config (and the GPU config for GPU images) and
                                    passed to the server as --conf args.
    --env-file                      Path to an environment file. Each non-blank, non-comment
                                    line is passed as a Docker -e flag. For GPU images, the
                                    default GPU env file is applied automatically.
    --port                          Spark Connect gRPC port (default: 15002).
    --ui-port                       Spark UI port (default: 4040).
    -p, --profile                   Launch the Spark Connect server with nsys profiling enabled.
    --profile-args                  Arguments to pass to nsys when it launches the Spark Connect
                                    server. This will override the default arguments. Requires
                                    --profile.

    If neither --image-tag nor --static-gluten-jar-path is provided, the default
    image tag "dynamic_gpu_\${USER:-latest}" is used.

EXAMPLES:
    $0
    $0 --image-tag dynamic_gpu_myuser
    $0 --static-gluten-jar-path /path/to/gluten-bundle.jar
    $0 --spark-config custom_overrides.conf
    $0 --port 15002 --ui-port 4040
    $0 -h

EOF
}

PORT=15002
UI_PORT=4040

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      --image-tag)
        if [[ -n $2 ]]; then
          IMAGE_TAG=$2
          shift 2
        else
          echo "Error: --image-tag requires a value"
          exit 1
        fi
        ;;
      --static-gluten-jar-path)
        if [[ -n $2 ]]; then
          GLUTEN_JAR_PATH=$2
          shift 2
        else
          echo "Error: --static-gluten-jar-path requires a value"
          exit 1
        fi
        ;;
      --spark-config)
        if [[ -n $2 ]]; then
          SPARK_CONFIG_FILE=$2
          shift 2
        else
          echo "Error: --spark-config requires a value"
          exit 1
        fi
        ;;
      --env-file)
        if [[ -n $2 ]]; then
          ENV_FILE=$2
          shift 2
        else
          echo "Error: --env-file requires a value"
          exit 1
        fi
        ;;
      --port)
        if [[ -n $2 ]]; then
          PORT=$2
          shift 2
        else
          echo "Error: --port requires a value"
          exit 1
        fi
        ;;
      --ui-port)
        if [[ -n $2 ]]; then
          UI_PORT=$2
          shift 2
        else
          echo "Error: --ui-port requires a value"
          exit 1
        fi
        ;;
      -p|--profile)
        PROFILE=true
        shift
        ;;
      --profile-args)
        if [[ -n $2 ]]; then
          PROFILE_ARGS="$2"
          shift 2
        else
          echo "Error: --profile-args requires a value"
          exit 1
        fi
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

if [[ -n ${PROFILE_ARGS} && "${PROFILE}" != "true" ]]; then
  echo "Error: --profile-args should only be set when --profile is enabled"
  exit 1
fi

if [[ -n ${IMAGE_TAG} && -n ${GLUTEN_JAR_PATH} ]]; then
  echo "Error: --image-tag and --static-gluten-jar-path are mutually exclusive."
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

# Stop any previously running Spark Connect server.
"${SCRIPT_DIR}/stop_spark_connect.sh"

EXTRA_DOCKER_ARGS=()
docker_image=""
compose_service=""

if [[ -n ${GLUTEN_JAR_PATH} ]]; then
  docker_image="${STATIC_TEST_IMAGE}"
  compose_service="spark-connect"

  # Build the static test image if it does not exist locally.
  if [[ -z "$(docker images -q "${docker_image}" 2>/dev/null)" ]]; then
    echo "Building static test image ${docker_image} ..."
    docker build -t "${docker_image}" -f "${REPO_ROOT}/spark_gluten/docker/static_jar_test.dockerfile" "${SCRIPT_DIR}"
  fi

  local_jar_path="$(readlink -f "${GLUTEN_JAR_PATH}")"
  if [[ ! -f "${local_jar_path}" ]]; then
    echo "Error: JAR file not found: ${GLUTEN_JAR_PATH}"
    exit 1
  fi
  EXTRA_DOCKER_ARGS+=(-v "$(dirname "${local_jar_path}"):/opt/gluten/jars:ro")
else
  IMAGE_TAG="${IMAGE_TAG:-dynamic_gpu_${USER:-latest}}"
  docker_image="apache/gluten:${IMAGE_TAG}"

  device_type="$(docker run --rm "${docker_image}" bash -c 'echo $GLUTEN_DEVICE_TYPE')"
  if [[ "${device_type}" == "gpu" ]]; then
    compose_service="spark-connect-gpu"
  else
    compose_service="spark-connect"
  fi
fi

# Build a merged config file: default.conf [+ gpu_default.conf] + optional user overlay.
CONFIG_DIR="${REPO_ROOT}/spark_gluten/testing/config"
MERGED_CONFIG="$(mktemp)"
trap 'rm -f "${MERGED_CONFIG}"' EXIT

if [[ -f "${CONFIG_DIR}/default.conf" ]]; then
  cp "${CONFIG_DIR}/default.conf" "${MERGED_CONFIG}"
fi

if [[ "${device_type}" == "gpu" && -f "${CONFIG_DIR}/gpu_default.conf" ]]; then
  cat "${CONFIG_DIR}/gpu_default.conf" >> "${MERGED_CONFIG}"
fi

if [[ -n ${SPARK_CONFIG_FILE} ]]; then
  SPARK_CONFIG_FILE="$(readlink -f "${SPARK_CONFIG_FILE}")"
  if [[ ! -f "${SPARK_CONFIG_FILE}" ]]; then
    echo "Error: Spark config file not found: ${SPARK_CONFIG_FILE}"
    exit 1
  fi
  cat "${SPARK_CONFIG_FILE}" >> "${MERGED_CONFIG}"
fi

CONTAINER_CONFIG_PATH="/tmp/spark-connect.conf"
EXTRA_DOCKER_ARGS+=(-v "${MERGED_CONFIG}:${CONTAINER_CONFIG_PATH}:ro")

# Apply environment file. For GPU images, the default GPU env file is used
# automatically unless --env-file is provided.
if [[ -z ${ENV_FILE} && "${device_type}" == "gpu" && -f "${CONFIG_DIR}/gpu_default.env" ]]; then
  ENV_FILE="${CONFIG_DIR}/gpu_default.env"
fi

if [[ -n ${ENV_FILE} ]]; then
  ENV_FILE="$(readlink -f "${ENV_FILE}")"
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Error: Environment file not found: ${ENV_FILE}"
    exit 1
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    EXTRA_DOCKER_ARGS+=(-e "$line")
  done < "${ENV_FILE}"
fi

# Mount SPARK_DATA_DIR if set.
if [[ -n ${SPARK_DATA_DIR} ]]; then
  EXTRA_DOCKER_ARGS+=(-v "${SPARK_DATA_DIR}:${SPARK_DATA_DIR}")
fi

if [[ "${PROFILE}" == "true" ]]; then
  EXTRA_DOCKER_ARGS+=(-e "PROFILE=ON")
  if [[ -n "${PROFILE_ARGS}" ]]; then
    EXTRA_DOCKER_ARGS+=(-e "PROFILE_ARGS=${PROFILE_ARGS}")
  fi
fi

echo "Starting Spark Connect server (image: ${docker_image}, service: ${compose_service}, port: ${PORT}, ui-port: ${UI_PORT}) ..."

export DOCKER_IMAGE="${docker_image}"
export WORKSPACE_ROOT
export CONNECT_PORT="${PORT}"
export UI_PORT
export SPARK_CONFIG_PATH="${CONTAINER_CONFIG_PATH}"

docker compose -f "${REPO_ROOT}/spark_gluten/docker/docker-compose.spark-connect.yml" run --rm \
    --service-ports \
    "${EXTRA_DOCKER_ARGS[@]}" \
    "${compose_service}"
