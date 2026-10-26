#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Restarts a crashed native worker container in place (same config and mounts) and
# waits until its HTTP port accepts connections and it has re-announced itself.
#
# Usage: restart_worker.sh <container> [port]

set -euo pipefail

CONTAINER=$1
PORT=${2:-10000}

docker start "${CONTAINER}" > /dev/null
for _ in $(seq 1 120); do
  if docker exec "${CONTAINER}" bash -c "exec 3<>/dev/tcp/127.0.0.1/${PORT}" 2> /dev/null; then
    # Workers announce to the coordinator every few seconds after the port opens.
    sleep 5
    exit 0
  fi
  sleep 1
done
echo "Error: ${CONTAINER} did not open port ${PORT}" >&2
exit 1
