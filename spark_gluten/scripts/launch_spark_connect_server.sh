#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Container entrypoint for the Spark Connect server.
# Usage: launch_spark_connect_server.sh [connect_port] [ui_port] [config_file_path]

set -e

CONNECT_PORT="${1:-15002}"
UI_PORT="${2:-4040}"
CONFIG_FILE="$3"

GLUTEN_JARS=$(find /opt/gluten/jars/ -maxdepth 1 -name 'gluten-*.jar' -print)
if [[ -z "${GLUTEN_JARS}" ]]; then
  echo "Error: No Gluten JARs found in /opt/gluten/jars/"
  exit 1
fi
jar_count=$(echo "${GLUTEN_JARS}" | wc -l)
if [[ ${jar_count} -gt 1 ]]; then
  echo "Error: Expected exactly one Gluten JAR in /opt/gluten/jars/ but found ${jar_count}:"
  echo "${GLUTEN_JARS}"
  exit 1
fi
GLUTEN_JAR="${GLUTEN_JARS}"

PROFILE_CMD=""
if [[ "${PROFILE}" == "ON" ]]; then
  mkdir -p /spark_profiles

  if [[ -z "${PROFILE_ARGS}" ]]; then
    PROFILE_ARGS="-t nvtx,cuda"
  fi
  PROFILE_CMD="nsys launch ${PROFILE_ARGS}"
fi

CONF_ARGS=()
if [[ -n "${CONFIG_FILE}" && -f "${CONFIG_FILE}" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    # Remove comments and trim whitespaces.
    line=$(echo "${line}" | cut -d '#' -f 1 | tr -s ' ' | sed 's/^ //;s/ $//')
    if [[ -z "$line" ]]; then
      continue
    fi
    key=$(echo "${line}" | cut -d ' ' -f 1)
    value=$(echo "${line}" | cut -d ' ' -f 2-)
    CONF_ARGS+=(--conf "${key}=${value}")
  done < "${CONFIG_FILE}"
fi

MASTER="${SPARK_MASTER_URL:-local[*]}"

$PROFILE_CMD $SPARK_HOME/bin/spark-submit \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --master "${MASTER}" \
    --jars "${GLUTEN_JAR}" \
    --conf spark.plugins=org.apache.gluten.GlutenPlugin \
    --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    --conf spark.connect.grpc.binding.port="${CONNECT_PORT}" \
    --conf spark.ui.port="${UI_PORT}" \
    "${CONF_ARGS[@]}"
