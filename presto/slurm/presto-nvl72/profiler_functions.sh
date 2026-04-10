#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

function start_profiler() {
  local -r profile_output_file_path=$1
  ${NSYS_BIN} start --gpu-metrics-devices=all -o ${profile_output_file_path}.nsys-rep
}

function stop_profiler() {
  local -r profile_output_file_path=$1.nsys-rep
#   local -r container_file_path="/presto_profiles/$(basename $profile_output_file_path)"
  ${NSYS_BIN} stop
#   chown -R $(id -u):$(id -g) /presto_profiles

#   local container_id
#   container_id=$(get_worker_container_id)
#   docker cp ${container_id}:${container_file_path} $profile_output_file_path
#   $docker_exec_command rm ${container_file_path}
}
