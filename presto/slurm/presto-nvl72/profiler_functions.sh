#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

function start_profiler() {
  local -r qid=$(basename "$1")
  local -r token_dir="/var/log/nsys"
  # Accept either the new CSV form (NSYS_WORKER_IDS=0,3,5) or the legacy single
  # int (NSYS_WORKER_ID=0). At least one must be set.
  local -r wid_csv="${NSYS_WORKER_IDS:-${NSYS_WORKER_ID:?NSYS_WORKER_IDS not set}}"
  local -r timeout="${NSYS_START_TIMEOUT:-600}"
  local -a wid_list
  IFS=',' read -ra wid_list <<< "${wid_csv}"

  # Drop any stale tokens from a prior failed iteration of this qid, then
  # signal every profiled worker to start.
  local wid
  for wid in "${wid_list[@]}"; do
    rm -f "${token_dir}/.nsys_started_token_w${wid}_${qid}" \
          "${token_dir}/.nsys_stop_token_w${wid}_${qid}"
    touch "${token_dir}/.nsys_start_token_w${wid}_${qid}"
  done

  # Wait for each worker to acknowledge. Workers all see their start token
  # at roughly the same instant, so polling sequentially per wid is fine —
  # the per-worker timeout is preserved.
  local waited
  for wid in "${wid_list[@]}"; do
    waited=0
    while [[ ! -f "${token_dir}/.nsys_started_token_w${wid}_${qid}" ]]; do
      sleep 2
      waited=$((waited + 2))
      if (( waited > timeout )); then
        echo "Error: worker ${wid} did not start nsys for ${qid} within ${timeout}s" >&2
        return 1
      fi
    done
    rm "${token_dir}/.nsys_started_token_w${wid}_${qid}"
  done
}

function stop_profiler() {
  local -r qid=$(basename "$1")
  local -r token_dir="/var/log/nsys"
  local -r wid_csv="${NSYS_WORKER_IDS:-${NSYS_WORKER_ID:?NSYS_WORKER_IDS not set}}"
  local -a wid_list
  IFS=',' read -ra wid_list <<< "${wid_csv}"
  local wid
  for wid in "${wid_list[@]}"; do
    touch "${token_dir}/.nsys_stop_token_w${wid}_${qid}"
  done
}
