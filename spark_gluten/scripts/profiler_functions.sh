#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

function get_container_id() {
  local container_id
  container_id=$(docker ps -q \
    --filter "label=com.nvidia.spark-connect.user=${USER}")
  if [[ -z "$container_id" ]]; then
    echo "Error: no running Spark Connect container found" >&2
    return 1
  fi
  echo "$container_id"
}

function get_docker_exec_command() {
  local container_id
  container_id=$(get_container_id) || return 1
  echo "docker exec $container_id"
}

function check_profile_output_directory() {
  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)
  $docker_exec_command bash -c "[[ -d /spark_profiles ]] || \
    { echo 'Error: Missing /spark_profiles directory. The server was likely not started with --profile.'; exit 1; }"
}

function start_profiler() {
  local -r profile_output_file_path=$1

  check_profile_output_directory

  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)
  $docker_exec_command nsys start --gpu-metrics-devices=all -o /spark_profiles/$(basename $profile_output_file_path).nsys-rep
}

function stop_profiler() {
  local -r profile_output_file_path=$1.nsys-rep
  local -r container_file_path="/spark_profiles/$(basename $profile_output_file_path)"
  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)

  check_profile_output_directory
  $docker_exec_command nsys stop
  $docker_exec_command chown -R $(id -u):$(id -g) /spark_profiles

  local container_id
  container_id=$(get_container_id)
  docker cp ${container_id}:${container_file_path} $profile_output_file_path
  $docker_exec_command rm ${container_file_path}
}
