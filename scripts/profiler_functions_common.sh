#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Shared profiler helper functions for nsys-based GPU profiling.
#
# Sourcing scripts must define:
#   get_container_id()  – returns the Docker container ID of the target server

set -e

function get_docker_exec_command() {
  local container_id
  container_id=$(get_container_id) || return 1
  echo "docker exec $container_id"
}

function check_profile_output_directory() {
  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)
  $docker_exec_command bash -c "[[ -d /presto_profiles ]] || \
    { echo 'Error: Missing /presto_profiles directory. The server was likely not started with --profile.'; exit 1; }"
}

function start_profiler() {
  local -r profile_output_file_path=$1

  check_profile_output_directory

  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)
  $docker_exec_command nsys start --gpu-metrics-devices=all -o /presto_profiles/$(basename $profile_output_file_path).nsys-rep
}

function stop_profiler() {
  local -r profile_output_file_path=$1.nsys-rep
  local -r container_file_path="/presto_profiles/$(basename $profile_output_file_path)"
  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)

  check_profile_output_directory
  $docker_exec_command nsys stop
  $docker_exec_command chown -R $(id -u):$(id -g) /presto_profiles

  local container_id
  container_id=$(get_container_id)
  docker cp ${container_id}:${container_file_path} $profile_output_file_path
  $docker_exec_command rm ${container_file_path}
}
