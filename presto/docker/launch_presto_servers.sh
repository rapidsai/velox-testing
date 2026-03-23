#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
# Run ldconfig once
ldconfig

LOGS_DIR="/opt/presto-server/logs"
mkdir -p "${LOGS_DIR}"
: "${SERVER_START_TIMESTAMP:?SERVER_START_TIMESTAMP must be set before starting the container}"

if [ $# -eq 0 ]; then
  # Single worker mode.  Use WORKER_ID env var for log filename (defaults to 0).
  local_id="${WORKER_ID:-0}"
  log_file="${LOGS_DIR}/worker_${local_id}_${SERVER_START_TIMESTAMP}.log"
  gpu_name="$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -n 1)"
  echo "GPU Name: ${gpu_name:-unknown}" > "${log_file}"
  presto_server --etc-dir="/opt/presto-server/etc/" >> "${log_file}" 2>&1 &
else
  # Multi-worker single-container mode.  Each GPU ID is an argument.
  for gpu_id in "$@"; do
    (
      export CUDA_VISIBLE_DEVICES=$gpu_id
      log_file="${LOGS_DIR}/worker_${gpu_id}_${SERVER_START_TIMESTAMP}.log"
      gpu_name="$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -n 1)"
      echo "GPU Name: ${gpu_name:-unknown}" > "${log_file}"
      exec presto_server --etc-dir="/opt/presto-server/etc${gpu_id}" >> "${log_file}" 2>&1
    ) &
  done
fi

# Wait for all background processes
wait
