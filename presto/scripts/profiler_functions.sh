#!/bin/bash

set -e

readonly IMAGE_NAME="presto-native-worker-gpu:latest"

# Get all worker container names matching the image
function get_worker_container_names() {
  local -r container_names=$(docker ps --filter "ancestor=${IMAGE_NAME}" --format '{{.Names}}')
  if [[ -z $container_names ]]; then
    echo "Error: no docker containers found for image: ${IMAGE_NAME}" >&2
    exit 1
  fi
  echo "$container_names"
}

function check_profile_output_directory() {
  local -r container_name=$1
  local -r error_message="Error: Missing /presto_profiles directory in container ${container_name}. \
                          The server was likely not started with the --profile option."
  docker exec "$container_name" bash -c "[[ -d /presto_profiles ]] || { echo '$error_message'; exit 1; }"
}

function start_profiler() {
  local -r profile_output_file_path=$1
  local -r container_names=$(get_worker_container_names)

  # Create the output directory if it doesn't exist
  mkdir -p "$(dirname "$profile_output_file_path")"

  for container_name in $container_names; do
    check_profile_output_directory "$container_name"
    # Profile file named as {profile_output_file_path}_{container_name}.nsys-rep
    docker exec "$container_name" nsys start --force-overwrite=true  --gpu-metrics-devices=cuda-visible -o "/presto_profiles/$(basename "${profile_output_file_path}")_${container_name}.nsys-rep"
  done
}

function stop_profiler() {
  local -r profile_output_file_path=$1
  local -r container_names=$(get_worker_container_names)
  local -r output_dir=$(dirname "$profile_output_file_path")
  local -r base_name=$(basename "$profile_output_file_path")

  local pids=()
  for container_name in $container_names; do
    echo docker exec "$container_name" bash -c "nsys stop; ls -l /presto_profiles"
    docker exec "$container_name" bash -c "nsys stop; ls -l /presto_profiles" &
    pids+=($!)
  done
  wait ${pids[@]}
  for container_name in $container_names; do
    local container_file_path="/presto_profiles/${base_name}_${container_name}.nsys-rep"
    local host_file_path="${output_dir}/${base_name}_${container_name}.nsys-rep"
    docker exec "$container_name" chown -R "$(id -u):$(id -g)" /presto_profiles
    docker cp "${container_name}:${container_file_path}" "$host_file_path"
    docker exec "$container_name" rm "${container_file_path}"
  done
}
