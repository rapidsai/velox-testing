#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# In-container entrypoint for the Spark Connect server.
# Baked into Docker images at /opt/spark/launch_spark_connect_server.sh.
#
# Usage: launch_spark_connect_server.sh [connect_port] [ui_port] [config_file]

set -e

CONNECT_PORT="${1:-15002}"
UI_PORT="${2:-4040}"
CONFIG_FILE="$3"

GLUTEN_JARS=$(find /opt/gluten/jars/ -maxdepth 1 -name 'gluten-*.jar' -print | paste -sd, -)
if [[ -z "${GLUTEN_JARS}" ]]; then
  echo "Error: No Gluten JARs found in /opt/gluten/jars/"
  exit 1
fi

CONF_ARGS=()
if [[ -n "${CONFIG_FILE}" && -f "${CONFIG_FILE}" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(echo "$line" | xargs)"
    [[ -z "$line" ]] && continue
    key="${line%% *}"
    value="${line#* }"
    CONF_ARGS+=(--conf "${key}=${value}")
  done < "${CONFIG_FILE}"
fi

exec $SPARK_HOME/bin/spark-submit \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --master "local[*]" \
    --jars "${GLUTEN_JARS}" \
    --conf spark.plugins=org.apache.gluten.GlutenPlugin \
    --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    --conf spark.connect.grpc.binding.port="${CONNECT_PORT}" \
    --conf spark.ui.port="${UI_PORT}" \
    "${CONF_ARGS[@]}"
