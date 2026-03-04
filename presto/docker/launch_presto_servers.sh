#!/bin/bash

set -e
# Run ldconfig once
echo ldconfig

LOGS_DIR="/opt/presto-server/logs"
mkdir -p "${LOGS_DIR}"

if [ $# -eq 0 ]; then
  # Single worker mode.  Use WORKER_ID env var for log filename (defaults to 0).
  local_id="${WORKER_ID:-0}"
  log_file="${LOGS_DIR}/worker_${local_id}.log"
  nvidia-smi -L > "${log_file}" 2>&1 || true
  presto_server --etc-dir="/opt/presto-server/etc/" >> "${log_file}" 2>&1 &
else
  # Multi-worker single-container mode.  Each GPU ID is an argument.
  worker_id=0
  for gpu_id in "$@"; do
    (
      export CUDA_VISIBLE_DEVICES=$gpu_id
      log_file="${LOGS_DIR}/worker_${worker_id}.log"
      nvidia-smi -L > "${log_file}" 2>&1 || true
      exec presto_server --etc-dir="/opt/presto-server/etc${gpu_id}" >> "${log_file}" 2>&1
    ) &
    worker_id=$((worker_id + 1))
  done
fi

# Wait for all background processes
wait
