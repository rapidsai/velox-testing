#!/bin/bash

set -e

function get_worker_container_id() {
  local -r image_tag="${IMAGE_TAG:-latest}"
  local -r image_name="presto-native-worker-gpu:${image_tag}"
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

function check_profile_output_directory() {
  local -r error_message="Error: Missing /presto_profile directory in the Presto server container. \
                          The server was likely not started with the --profile option."
  $(get_docker_exec_command) bash -c "[[ -d /presto_profiles ]] || { echo $error_message; exit 1; }"
}

function start_profiler() {
  local -r profile_output_file_path=$1

  check_profile_output_directory
  $(get_docker_exec_command) nsys start -o /presto_profiles/$(basename $profile_output_file_path)
}

function stop_profiler() {
  local -r profile_output_file_path=$1
  local -r container_file_path="/presto_profiles/$(basename $profile_output_file_path)"
  local -r docker_exec_command=$(get_docker_exec_command)
  
  check_profile_output_directory
  $docker_exec_command nsys stop
  $docker_exec_command chown -R $(id -u):$(id -g) /presto_profiles
  docker cp $(get_worker_container_id):${container_file_path} $profile_output_file_path
  $docker_exec_command rm ${container_file_path}
}
