#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

function get_spark_connect_container_id() {
  local container_id
  container_id=$(docker ps -q \
    --filter "label=com.docker.compose.service=spark-connect-gpu")
  if [[ -z "$container_id" ]]; then
    container_id=$(docker ps -q \
      --filter "label=com.docker.compose.service=spark-connect")
  fi
  if [[ -z "$container_id" ]]; then
    echo "Error: no running Spark Connect container found" >&2
    return 1
  fi
  echo "$container_id"
}

function get_docker_exec_command() {
  local container_id
  container_id=$(get_spark_connect_container_id) || return 1
  echo "docker exec $container_id"
}

function check_profile_output_directory() {
  local -r error_message="Error: Missing /presto_profiles directory in the Spark Connect container. \
                          The server was likely not started with the --profile option."
  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)
  $docker_exec_command bash -c "[[ -d /presto_profiles ]] || { echo $error_message; exit 1; }"
}

function get_nsys_bin() {
  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)
  local nsys_cli_bin
  nsys_cli_bin=$($docker_exec_command bash -c 'compgen -G "/opt/nvidia/nsight-systems-cli/*/target-linux-x64/nsys" | sort -V | tail -1')
  if [[ -n "$nsys_cli_bin" ]]; then
    echo "$nsys_cli_bin"
  else
    echo "nsys"
  fi
}

function start_profiler() {
  local -r profile_output_file_path=$1

  check_profile_output_directory

  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)
  local nsys_bin
  nsys_bin=$(get_nsys_bin)
  $docker_exec_command $nsys_bin start --gpu-metrics-devices=all -o /presto_profiles/$(basename $profile_output_file_path).nsys-rep
}

function stop_profiler() {
  local -r profile_output_file_path=$1.nsys-rep
  local -r container_file_path="/presto_profiles/$(basename $profile_output_file_path)"
  local docker_exec_command
  docker_exec_command=$(get_docker_exec_command)

  check_profile_output_directory
  local nsys_bin
  nsys_bin=$(get_nsys_bin)
  $docker_exec_command $nsys_bin stop
  $docker_exec_command chown -R $(id -u):$(id -g) /presto_profiles

  local container_id
  container_id=$(get_spark_connect_container_id)
  docker cp ${container_id}:${container_file_path} $profile_output_file_path
  $docker_exec_command rm ${container_file_path}
}
