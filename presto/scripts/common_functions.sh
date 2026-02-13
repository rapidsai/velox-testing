#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

function wait_for_worker_node_registration() {
  trap "rm -rf node_response.json" RETURN

  echo "Waiting for a worker node to be registered..."
  COORDINATOR_URL=http://${HOSTNAME}:${PORT}
  echo "Coordinator URL: $COORDINATOR_URL"
  local -r MAX_RETRIES=12
  local retry_count=0
  until curl -s -f -o node_response.json ${COORDINATOR_URL}/v1/node && \
        (( $(jq length node_response.json) > 0 )); do
    if (( $retry_count >= $MAX_RETRIES )); then
      echo "Error: Worker node not registered after 60s. Exiting."
      exit 1
    fi
    sleep 5
    retry_count=$(( retry_count + 1 ))
  done
  echo "Worker node registered"
}
