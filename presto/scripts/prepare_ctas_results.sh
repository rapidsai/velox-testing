#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

HOST_RESULTS_DIR=$1
CONTAINER_RESULTS_DIR="/var/lib/presto/data/hive/ctas_scratch_output/$(basename "${HOST_RESULTS_DIR}")"

if [[ ! -d "${HOST_RESULTS_DIR}" ]]; then
  echo "Error: CTAS result directory not found: ${HOST_RESULTS_DIR}" >&2
  exit 1
fi

# A committed Hive table contains .prestoSchema even when the query produced no
# rows and therefore no Parquet parts. Use that marker instead of requiring an
# output file.
has_committed_table() {
  find "${HOST_RESULTS_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'q[0-9]*' \
    -exec test -f '{}/.prestoSchema' \; -print -quit 2>/dev/null
}

# Host-side normalization must read every Parquet part and remove the temporary
# table afterward. Return success when a file is unreadable or a directory is
# not writable, triggering Docker-side permission repair.
has_inaccessible_results() {
  [[ -n "$(find "${HOST_RESULTS_DIR}" -type d ! -writable -print -quit 2>/dev/null || true)" \
    || -n "$(find "${HOST_RESULTS_DIR}" -type f -name '*.parquet' ! -readable -print -quit 2>/dev/null || true)" ]]
}

# Slurm and non-remapped local files are normally owned by the invoking user.
# Normalization reads the Parquet files and removes each temporary table, so
# it needs write access to directories as well as read access to the files.
find "${HOST_RESULTS_DIR}" -type d -exec chmod a+rwx {} + 2>/dev/null || true
find "${HOST_RESULTS_DIR}" -type f -name '*.parquet' -exec chmod a+r {} + 2>/dev/null || true

if [[ -n "$(has_committed_table)" ]] && ! has_inaccessible_results; then
  exit 0
fi

# Follow the profiling teardown pattern: execute permission repair inside the
# running worker containers before the host process reads their output.
if command -v docker >/dev/null 2>&1; then
  while IFS= read -r container; do
    [[ -n ${container} ]] || continue
    docker exec "${container}" find "${CONTAINER_RESULTS_DIR}" -type d \
      -exec chmod a+rwx {} + 2>/dev/null || true
    docker exec "${container}" find "${CONTAINER_RESULTS_DIR}" -type f -name '*.parquet' \
      -exec chmod a+r {} + 2>/dev/null || true
  done < <(docker ps --format '{{.Names}}' | grep -E '^presto-.*worker')
fi

if [[ -z "$(has_committed_table)" ]]; then
  echo "Error: No committed CTAS result tables found in ${HOST_RESULTS_DIR}" >&2
  exit 1
fi
if has_inaccessible_results; then
  echo "Error: CTAS result files or directories are inaccessible from the benchmark host: ${HOST_RESULTS_DIR}" >&2
  exit 1
fi
