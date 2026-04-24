#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

STATIC_TEST_IMAGE="gluten-static-test:latest"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script starts a Spark Connect server in a Docker container.

OPTIONS:
    -h, --help                      Show this help message.
    --image-tag                     Tag of the docker image to use for the server launch. The full
                                    image reference is "apache/gluten:{image-tag}". Default value
                                    is "dynamic_gpu_\${USER:-latest}". Cannot be used with
                                    --static-gluten-jar-path.
    --static-gluten-jar-path        Path to a statically-linked Gluten JAR file on the host.
                                    Cannot be used with --image-tag.
    --spark-config                  Path to a Spark configuration file. Values in this file are merged
                                    on top of the default config (and the GPU config for GPU images)
                                    and passed to the server as --conf args.
    --env-file                      Path to an environment variable file. Each line should contain a
                                    variable assignment in KEY=VALUE format. For GPU images, the
                                    default GPU environment variable file is applied automatically.
    --port                          Spark Connect gRPC port (default: 15002).
    --ui-port                       Spark UI port (default: 4040).
    -p, --profile                   Launch the Spark Connect server with profiling enabled.
    --profile-args                  Arguments to pass to the profiler when it launches the Spark Connect
                                    server. This will override the default arguments. Requires
                                    --profile.
    --logs-dir                      Directory for Spark Connect server logs. Each invocation creates a
                                    timestamped log file and a "spark_connect.log" symlink pointing to it.
                                    Default: "<script_dir>/spark_logs".
    -e, --num-executors             Number of Spark executors (default: 1). When greater than 1, starts a Spark
                                    Standalone cluster with a master, N executor containers, and a
                                    Spark Connect server that submits to the master.
    -g, --gpu-ids                   Comma-separated list of GPU device IDs to assign to executors
                                    (e.g. "0,1,2"). Must match --num-executors count. If omitted,
                                    defaults to 0..N-1.

    If neither --image-tag nor --static-gluten-jar-path is provided, the default
    image tag "dynamic_gpu_\${USER:-latest}" is used.

EXAMPLES:
    $0
    $0 --image-tag dynamic_gpu_myuser
    $0 --static-gluten-jar-path /path/to/gluten-bundle.jar
    $0 --spark-config custom_overrides.conf
    $0 --port 15002 --ui-port 4040
    $0 --profile
    $0 --profile --profile-args "-t cuda"
    $0 -e 4 -g "0,1,2,3"
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
      --logs-dir)
        if [[ -n $2 ]]; then
          LOGS_DIR="$2"
          shift 2
        else
          echo "Error: --logs-dir requires a value"
          exit 1
        fi
        ;;
      -e|--num-executors)
        if [[ -n $2 ]]; then
          NUM_EXECUTORS=$2
          shift 2
        else
          echo "Error: --num-executors requires a value"
          exit 1
        fi
        ;;
      -g|--gpu-ids)
        if [[ -n $2 ]]; then
          GPU_IDS=$2
          shift 2
        else
          echo "Error: --gpu-ids requires a value"
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

NUM_EXECUTORS="${NUM_EXECUTORS:-1}"
MASTER_WEB_PORT="${MASTER_WEB_PORT:-8081}"

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

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/start_spark_connect_helper.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/spark_connect_functions.sh"

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
  GLUTEN_JAR_DIR="$(dirname "${local_jar_path}")"
  export GLUTEN_JAR_DIR
  EXTRA_DOCKER_ARGS+=(-v "${GLUTEN_JAR_DIR}:/opt/gluten/jars:ro")
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

MERGED_CONFIG=$(merge_config_files "${device_type}" "${SPARK_CONFIG_FILE}")

CONTAINER_CONFIG_PATH="/tmp/spark-connect.conf"
EXTRA_DOCKER_ARGS+=(-v "${MERGED_CONFIG}:${CONTAINER_CONFIG_PATH}:ro")

parse_env_file "${device_type}" "${ENV_FILE}"

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

LOGS_DIR="${LOGS_DIR:-${SCRIPT_DIR}/spark_logs}"
mkdir -p "${LOGS_DIR}"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOGS_DIR}/spark_connect_${TIMESTAMP}.log"
LOG_SYMLINK="${LOGS_DIR}/spark_connect.log"

export DOCKER_IMAGE="${docker_image}"
export WORKSPACE_ROOT
export CONNECT_PORT="${PORT}"
export UI_PORT
export SPARK_CONNECT_USER="${USER}"
export MASTER_WEB_PORT
export SPARK_DATA_DIR="${SPARK_DATA_DIR:-${WORKSPACE_ROOT}}"

CONFIG_DIR="${REPO_ROOT}/spark_gluten/testing/config"
if [[ "${device_type}" == "gpu" && -f "${CONFIG_DIR}/gpu_default.env" ]]; then
  export SPARK_GPU_ENV_FILE="${CONFIG_DIR}/gpu_default.env"
else
  EMPTY_ENV="${SCRIPT_DIR}/.empty.env"
  : > "${EMPTY_ENV}"
  export SPARK_GPU_ENV_FILE="${EMPTY_ENV}"
fi

if [[ ${NUM_EXECUTORS} -gt 1 ]]; then
  # In cluster mode the config file is under WORKSPACE_ROOT, accessible via
  # the volume mount -- use the host path directly instead of /tmp.
  # Override executor cores to match worker topology so each worker gets
  # exactly 1 executor.  spark.executor.instances is ignored in Standalone
  # mode; allocation is driven by available resources.
  WORKER_CORES="${SPARK_WORKER_CORES:-4}"
  sed -i "/^spark\.executor\.instances /d" "${MERGED_CONFIG}"
  sed -i "/^spark\.executor\.cores /d" "${MERGED_CONFIG}"
  echo "spark.executor.cores ${WORKER_CORES}" >> "${MERGED_CONFIG}"
  export SPARK_CONFIG_PATH="${MERGED_CONFIG}"
  echo "Starting Spark cluster (${NUM_EXECUTORS} executors, image: ${docker_image}, port: ${PORT}, ui-port: ${UI_PORT}) ..."

  TEMPLATE_PATH="${REPO_ROOT}/spark_gluten/docker/docker-compose/template/docker-compose.spark-cluster.yml.jinja"
  RENDERED_DIR="${REPO_ROOT}/spark_gluten/docker/docker-compose/generated"
  mkdir -p "${RENDERED_DIR}"
  RENDERED_PATH="${RENDERED_DIR}/docker-compose.spark-cluster.rendered.yml"

  RENDER_SCRIPT_PATH=$(readlink -f "${REPO_ROOT}/template_rendering/render_docker_compose_template.py")
  STATIC_JAR_FLAG="false"
  if [[ -n ${GLUTEN_JAR_PATH} ]]; then
    STATIC_JAR_FLAG="true"
  fi
  GPU_FLAG="false"
  if [[ "${device_type}" == "gpu" ]]; then
    GPU_FLAG="true"
  fi
  RENDER_ARGS="--template-path ${TEMPLATE_PATH} --output-path ${RENDERED_PATH} --num-workers ${NUM_EXECUTORS} --single-container false --static-jar ${STATIC_JAR_FLAG} --gpu ${GPU_FLAG}"
  if [[ -n ${GPU_IDS} ]]; then
    RENDER_ARGS="${RENDER_ARGS} --gpu-ids ${GPU_IDS}"
  fi
  "${REPO_ROOT}/scripts/run_py_script.sh" -q -p "${RENDER_SCRIPT_PATH}" ${RENDER_ARGS}

  docker compose -f "${RENDERED_PATH}" up -d

  ln -sf "$(basename "${LOG_FILE}")" "${LOG_SYMLINK}"
  docker compose -f "${RENDERED_PATH}" logs -f > "${LOG_FILE}" 2>&1 &

  echo "Logs: ${LOG_FILE}"

  wait_for_spark_executors "localhost" "${MASTER_WEB_PORT}" "${NUM_EXECUTORS}"
  wait_for_spark_connect_server "localhost" "${PORT}"
else
  export SPARK_CONFIG_PATH="${CONTAINER_CONFIG_PATH}"
  echo "Starting Spark Connect server (image: ${docker_image}, service: ${compose_service}, port: ${PORT}, ui-port: ${UI_PORT}) ..."

  CONTAINER_ID=$(docker compose -f "${REPO_ROOT}/spark_gluten/docker/docker-compose.spark-connect.yml" run --rm -d \
      --service-ports \
      "${EXTRA_DOCKER_ARGS[@]}" \
      "${compose_service}")

  ln -sf "$(basename "${LOG_FILE}")" "${LOG_SYMLINK}"

  docker logs -f "${CONTAINER_ID}" > "${LOG_FILE}" 2>&1 &

  echo "Container: ${CONTAINER_ID}"
  echo "Logs: ${LOG_FILE}"

  wait_for_spark_connect_server "localhost" "${PORT}"
fi
