#!/bin/bash

set -e

function get_worker_container_id() {
  local -r image_name="presto-native-worker-gpu:latest"
  local -r container_id=$(docker ps -q --filter "ancestor=${image_name}")
  if [[ -z $container_id ]]; then
    echo "Error: no docker container found for image: ${image_name}"
    exit 1
  fi
  echo $container_id
}

function get_docker_exec_command() {
  echo "docker exec $(get_worker_container_id)"
}

function start_profiler() {
  local -r profile_output_file_path=$1

  $(get_docker_exec_command) nsys start -o /presto_profiles/$(basename $profile_output_file_path)
}

function stop_profiler() {
  local -r profile_output_file_path=$1
  local -r container_file_path="/presto_profiles/$(basename $profile_output_file_path)"
  local -r docker_exec_command=$(get_docker_exec_command)
  
  $docker_exec_command nsys stop
  $docker_exec_command chown -R $(id -u):$(id -g) /presto_profiles
  docker cp $(get_worker_container_id):${container_file_path} $profile_output_file_path
  $docker_exec_command rm ${container_file_path}
}
