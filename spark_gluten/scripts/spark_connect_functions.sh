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

# Wait until the expected number of executors have registered with the
# Spark Standalone Master.  Polls the Master's JSON REST API.
wait_for_spark_executors() {
  local -r host="$1"
  local -r port="$2"
  local -r expected="$3"
  local -r max_retries="${4:-24}"
  local -r poll_interval=5
  local retry_count=0

  echo "Waiting for ${expected} executor(s) to register with Spark Master at ${host}:${port} ..."

  while true; do
    local alive
    alive=$(curl -sf "http://${host}:${port}/json/" | jq -r '.aliveworkers // 0') 2>/dev/null || alive=0
    alive="${alive:-0}"

    if (( alive == expected )); then
      echo "All ${expected} executor(s) registered with Spark Master."
      return 0
    fi

    retry_count=$(( retry_count + 1 ))
    if (( retry_count >= max_retries )); then
      echo "Error: Only ${alive}/${expected} executor(s) registered after $(( max_retries * poll_interval ))s."
      return 1
    fi

    echo "  ${alive}/${expected} executor(s) registered, retrying in ${poll_interval}s ..."
    sleep "${poll_interval}"
  done
}
