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
    docker exec $container_name nsys start --force-overwrite true --gpu-metrics-devices=cuda-visible -o /presto_profiles/$(basename $profile_output_file_path)
  done
}

function stop_profiler() {
  local -r profile_output_base_path=$1
  local container_names
  container_names=$(get_worker_container_names)
  check_profile_output_directory
  for container_name in $container_names; do
    docker exec $container_name nsys stop
    echo "Stopped profiler for container: $container_name"
  done
  # wait for 10 seconds
  sleep 10
  for container_name in $container_names; do
    # docker exec $container_name nsys stop
    docker exec $container_name chown -R $(id -u):$(id -g) /presto_profiles/
    local worker_index=$(get_worker_index_from_container_name $container_name)
    local profile_output_file_path="${profile_output_base_path}${worker_index}.nsys-rep"
    local container_file_path="/presto_profiles/$(basename $profile_output_file_path)"
    docker cp ${container_name}:${container_file_path} $profile_output_file_path
    docker exec $container_name rm ${container_file_path}
  done
}
