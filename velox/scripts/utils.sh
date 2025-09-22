#!/bin/bash

# Shared utility functions for Velox build and test scripts

# Function to detect which build directory exists in the container depending on
# the build type
# Container must be accessible
detect_build_dir() {

  local compose_file=$1
  local container_name=$2

  for build_dir in "release" "relwithdebinfo" "debug"; do
  if docker compose -f "$compose_file" run --rm "${container_name}" test -d "/opt/velox-build/${build_dir}" 2>/dev/null; then
      echo "$build_dir"
  fi
  done
  
} 


# Helper function to detect which build directory exists in the container depending on
# the build type, uses a run_in_container function to run the command
# Container must be accessible
detect_build_dir_with_run_in_container() {
    local run_in_container_func=$1

    for build_dir in "release" "relwithdebinfo" "debug"; do
    if $run_in_container_func "test -d /opt/velox-build/${build_dir}" 2>/dev/null; then
        echo "$build_dir"
    fi
    done
}