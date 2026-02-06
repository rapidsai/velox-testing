#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function wait_for_worker_node_registration() {
  trap "rm -rf node_response.json" RETURN

  echo "Waiting for a worker node to be registered..."
  COORDINATOR_URL=http://${HOSTNAME}:${PORT}
  echo "Coordinator URL: $COORDINATOR_URL"
  local -r MAX_RETRIES=24
  local retry_count=0
  until {
        # Try Trino active nodes endpoint first
        curl -s -f -H 'Accept: application/json' -o node_response.json ${COORDINATOR_URL}/v1/node/active && \
        (( $(jq length node_response.json 2>/dev/null || echo 0) > 0 )) \
      ; } || {
        # Try legacy nodes endpoint
        curl -s -f -H 'Accept: application/json' -o node_response.json ${COORDINATOR_URL}/v1/node && \
        (( $(jq length node_response.json 2>/dev/null || echo 0) > 0 )) \
      ; } || {
        # Fallback: use Trino statements API to count workers
        RESP=$(curl -s -f -X POST ${COORDINATOR_URL}/v1/statement \
          -H 'X-Trino-User: health' \
          -H 'X-Trino-Source: wait_for_workers' \
          -H 'Accept: application/json' \
          --data-binary 'select count(*) from system.runtime.nodes where coordinator=false' 2>/dev/null || true)
        NEXT=$(echo "$RESP" | jq -r '.nextUri // empty')
        DATA=$(echo "$RESP" | jq -r '.data[0][0] // empty')
        STEPS=0
        while [[ -z "$DATA" && -n "$NEXT" && $STEPS -lt 10 ]]; do
          RESP=$(curl -s -f "$NEXT" 2>/dev/null || true)
          NEXT=$(echo "$RESP" | jq -r '.nextUri // empty')
          DATA=$(echo "$RESP" | jq -r '.data[0][0] // empty')
          STEPS=$((STEPS+1))
        done
        if [[ "$DATA" =~ ^[0-9]+$ ]] && (( DATA > 0 )); then
          echo '{}' > node_response.json
          true
        else
          false
        fi
      }; do
    if (( $retry_count >= $MAX_RETRIES )); then
      echo "Error: Worker node not registered after 120s. Exiting."
      exit 1
    fi
    sleep 5
    retry_count=$(( retry_count + 1 ))
  done
  echo "Worker node registered"
}
