#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Check if the Spark Connect server is reachable at the given host and port.
# Returns 0 if reachable, 1 otherwise.
check_spark_connect_server() {
  local -r host="$1"
  local -r port="$2"
  bash -c "echo > /dev/tcp/${host}/${port}" 2>/dev/null
}

# Wait for the Spark Connect server to accept connections.
# Polls the given host:port until it responds or the timeout is reached.
wait_for_spark_connect_server() {
  local -r host="$1"
  local -r port="$2"
  local -r timeout_seconds="${3:-120}"
  local -r poll_interval=3
  local elapsed=0

  echo "Waiting for Spark Connect server at ${host}:${port} ..."
  while ! check_spark_connect_server "${host}" "${port}"; do
    if (( elapsed >= timeout_seconds )); then
      echo "Error: Spark Connect server at ${host}:${port} not ready after ${timeout_seconds}s."
      return 1
    fi
    sleep "${poll_interval}"
    elapsed=$(( elapsed + poll_interval ))
  done
  echo "Spark Connect server is ready (${host}:${port})."
}
