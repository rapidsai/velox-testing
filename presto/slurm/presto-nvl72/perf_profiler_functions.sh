#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Query-side half of the host-perf handshake. This file is sourced by the
# benchmark CLI container. The same host LOGS directory is bind-mounted at
# /var/log/nsys, allowing the CLI and the host-side sampler to exchange tokens
# without network calls or PID-namespace assumptions in the CLI container.

set -e

_perf_wait_for_token() {
  local -r wanted="$1"
  local -r error_file="$2"
  local -r sidecar_error_file="$3"
  local -r timeout="$4"
  local -r deadline=$((SECONDS + timeout))

  while [[ ! -f "${wanted}" ]]; do
    if [[ -s "${error_file}" ]]; then
      cat "${error_file}" >&2
      return 1
    fi
    if [[ -s "${sidecar_error_file}" ]]; then
      cat "${sidecar_error_file}" >&2
      return 1
    fi
    # These tokens bracket the measured query. A one-second poll interval adds
    # a visible idle tail to short CPU profiles, so poll the shared filesystem
    # promptly while retaining a wall-clock timeout.
    sleep 0.1
    if (( SECONDS >= deadline )); then
      echo "Error: timed out after ${timeout}s waiting for $(basename "${wanted}")" >&2
      return 1
    fi
  done
}

start_profiler() {
  local -r qid="$(basename -- "$1")"
  local -r worker_id="${PERF_WORKER_ID:-0}"
  local -r token_dir="${PERF_TOKEN_DIR:-/var/log/nsys}"
  local -r timeout="${PERF_START_TIMEOUT:-120}"
  local -r prefix="${token_dir}/.perf"
  local -r error_file="${prefix}_error_w${worker_id}_${qid}"
  local -r sidecar_error_file="${prefix}_sidecar_error_w${worker_id}"

  [[ "${qid}" =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "Error: unsafe perf profile identifier: ${qid}" >&2
    return 1
  }

  rm -f \
    "${prefix}_started_w${worker_id}_${qid}" \
    "${prefix}_stop_w${worker_id}_${qid}" \
    "${prefix}_finished_w${worker_id}_${qid}" \
    "${error_file}"
  touch "${prefix}_start_w${worker_id}_${qid}"

  _perf_wait_for_token \
    "${prefix}_started_w${worker_id}_${qid}" \
    "${error_file}" \
    "${sidecar_error_file}" \
    "${timeout}"
  rm -f "${prefix}_started_w${worker_id}_${qid}"
}

stop_profiler() {
  local -r qid="$(basename -- "$1")"
  local -r worker_id="${PERF_WORKER_ID:-0}"
  local -r token_dir="${PERF_TOKEN_DIR:-/var/log/nsys}"
  # The finished token acknowledges that perf record has stopped and its closed
  # raw file is queued safely on node-local storage. Shared-filesystem
  # publication and report generation are not part of this query-side timeout.
  local -r timeout="${PERF_STOP_TIMEOUT:-600}"
  local -r prefix="${token_dir}/.perf"
  local -r error_file="${prefix}_error_w${worker_id}_${qid}"
  local -r sidecar_error_file="${prefix}_sidecar_error_w${worker_id}"

  touch "${prefix}_stop_w${worker_id}_${qid}"
  _perf_wait_for_token \
    "${prefix}_finished_w${worker_id}_${qid}" \
    "${error_file}" \
    "${sidecar_error_file}" \
    "${timeout}"
  rm -f "${prefix}_finished_w${worker_id}_${qid}"

  if [[ -s "${error_file}" ]]; then
    cat "${error_file}" >&2
    return 1
  fi
}
