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

  echo "Waiting for an ACTIVE worker node to be registered..."
  HOSTNAME=${1:-localhost}
  PORT=${2:-8080}
  COORDINATOR_URL=http://${HOSTNAME}:${PORT}
  echo "Coordinator URL: $COORDINATOR_URL"
  local -r MAX_RETRIES=24
  local retry_count=0
  until curl -s -f -o node_response.json ${COORDINATOR_URL}/v1/node && \
        len=$(jq 'length' node_response.json) && \
        { \
          # If API exposes state/active, require at least one ACTIVE; otherwise, accept any node presence
          if jq -e 'length>0 and (.[0] | has("state") or has("active"))' node_response.json >/dev/null 2>&1; then \
            active_count=$(jq '[ .[] | select((.state=="ACTIVE") or (.active==true)) ] | length' node_response.json); \
            (( active_count > 0 )); \
          else \
            (( len > 0 )); \
          fi; \
        }; do
    if (( $retry_count >= $MAX_RETRIES )); then
      echo "Error: No ACTIVE worker after $((5*MAX_RETRIES))s. Exiting."
      exit 1
    fi
    sleep 5
    retry_count=$(( retry_count + 1 ))
  done
  echo "Active worker node registered"
}
