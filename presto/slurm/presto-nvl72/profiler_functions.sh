#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

function start_profiler() {
  local -r qid=$(basename "$1")
  local -r token_dir="/var/log/nsys"
  local -r wid="${NSYS_WORKER_ID:?NSYS_WORKER_ID not set}"

  # Clear any stale tokens from a prior failed iteration of this qid.
  rm -f "${token_dir}/.nsys_started_token_w${wid}_${qid}" \
    "${token_dir}/.nsys_stop_token_w${wid}_${qid}"

  touch "${token_dir}/.nsys_start_token_w${wid}_${qid}"

  local waited=0
  while [[ ! -f "${token_dir}/.nsys_started_token_w${wid}_${qid}" ]]; do
    sleep 2
    waited=$((waited + 2))
    if (( waited > 60 )); then
      echo "Error: worker ${wid} did not start nsys for ${qid} within 60s" >&2
      return 1
    fi
  done
  rm "${token_dir}/.nsys_started_token_w${wid}_${qid}"
}

function stop_profiler() {
  local -r qid=$(basename "$1")
  local -r token_dir="/var/log/nsys"
  local -r wid="${NSYS_WORKER_ID:?NSYS_WORKER_ID not set}"
  touch "${token_dir}/.nsys_stop_token_w${wid}_${qid}"
}
