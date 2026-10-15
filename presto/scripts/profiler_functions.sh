#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

function get_worker_container_names() {
  local -r image_tag="${PRESTO_IMAGE_TAG}"
  local -r image_name="presto-native-worker-gpu:${image_tag}"
  local -r container_names=$(docker ps --format "{{.Names}}" --filter "ancestor=${image_name}")
  if [[ -z $container_names ]]; then
    echo "Error: no docker container found for image: ${image_name}" >&2
    return 1
  fi
  echo "$container_names"
}

function get_worker_index_from_container_name() {
  local -r container_name="$1"
  local result="${container_name#presto-native-worker-gpu}"
  echo "$result"
}

function check_profile_output_directory() {
  local -r error_message="Error: Missing /presto_profiles directory in the Presto server container. \
                          The server was likely not started with the --profile option."
  local container_names
  container_names=$(get_worker_container_names)
  for container_name in $container_names; do
    docker exec $container_name bash -c "[[ -d /presto_profiles ]] || { echo $error_message; exit 1; }"
  done
}

function start_profiler() {
  local -r profile_output_base_path=$1
  local container_names
  container_names=$(get_worker_container_names)
  check_profile_output_directory

  for container_name in $container_names; do
    local worker_index=$(get_worker_index_from_container_name $container_name)
    local profile_output_file_path="${profile_output_base_path}${worker_index}.nsys-rep"
    # cuda-visible: collect GPU metrics only for the GPU(s) visible to this worker container.
    # --force-overwrite: do not fail on a stale report left behind by an earlier aborted run.
    docker exec $container_name nsys start --force-overwrite true --gpu-metrics-devices=cuda-visible -o /presto_profiles/$(basename $profile_output_file_path)
  done
}

function stop_profiler() {
  local -r profile_output_base_path=$1
  local container_names
  container_names=$(get_worker_container_names)
  check_profile_output_directory

  # `nsys stop` blocks until the report is completely written, so no extra wait is needed
  # (the time grows with trace size). Stop all workers in parallel so the total time is that
  # of the slowest worker rather than the sum over workers.
  local pids=()
  for container_name in $container_names; do
    {
      docker exec $container_name nsys stop
      echo "Stopped profiler for container: $container_name"
    } &
    pids+=($!)
  done
  local failed=0
  for pid in "${pids[@]}"; do
    wait $pid || failed=1
  done
  if [[ $failed -ne 0 ]]; then
    echo "Error: failed to stop the profiler in one or more worker containers" >&2
    return 1
  fi

  for container_name in $container_names; do
    docker exec $container_name chown -R $(id -u):$(id -g) /presto_profiles/
    local worker_index=$(get_worker_index_from_container_name $container_name)
    local profile_output_file_path="${profile_output_base_path}${worker_index}.nsys-rep"
    local container_file_path="/presto_profiles/$(basename $profile_output_file_path)"
    docker cp ${container_name}:${container_file_path} $profile_output_file_path
    docker exec $container_name rm ${container_file_path}
  done
}
