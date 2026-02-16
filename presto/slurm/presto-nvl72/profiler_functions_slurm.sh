#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

# This script provides profiling functions for SLURM/Singularity-based Presto workers.
# It uses srun to execute commands in worker containers instead of docker exec.

# Set default values if not provided (when called from within container)
# IMPORTANT: SCRIPT_DIR must point to the slurm directory where worker info files are stored
# If it's set to the scripts directory, fix it
if [[ -z "${SCRIPT_DIR:-}" ]] || [[ "${SCRIPT_DIR}" == *"/presto/scripts"* ]]; then
    SCRIPT_DIR="/workspace/presto/slurm/presto-nvl72"
fi
VT_ROOT="${VT_ROOT:-/workspace}"
IMAGE_DIR="${IMAGE_DIR:-/mnt/data/images/presto}"

# Get the worker node and image for a specific worker ID
function get_worker_info() {
    local worker_id=$1
    local worker_info_file="${SCRIPT_DIR}/worker_info/worker_${worker_id}.info"
    
    if [ ! -f "$worker_info_file" ]; then
        echo "Error: Worker info file not found for worker ${worker_id} at ${worker_info_file}" >&2
        echo "SCRIPT_DIR is: ${SCRIPT_DIR}" >&2
        echo "Looking for: ${worker_info_file}" >&2
        ls -la "${SCRIPT_DIR}/worker_info/"* 2>&1 || echo "No worker info files found" >&2
        return 1
    fi
    
    source "$worker_info_file"
    if [ -z "${WORKER_NODE:-}" ] || [ -z "${WORKER_IMAGE:-}" ]; then
        echo "Error: Worker info file incomplete for worker ${worker_id}" >&2
        return 1
    fi
    echo "${WORKER_NODE}:${WORKER_IMAGE}"
}

# Execute a command in a worker container using srun
function exec_in_worker() {
    local worker_id=$1
    local command=$2
    
    local worker_info
    worker_info=$(get_worker_info "$worker_id") || return 1
    
    local worker_node="${worker_info%%:*}"
    local worker_image="${worker_info##*:}"
    local worker_image_path="${IMAGE_DIR}/${worker_image}.sqsh"
    
    # Execute command in the worker container
    # Note: This assumes we're running from within the SLURM job context where srun is available
    srun -N1 -w "$worker_node" --ntasks=1 --overlap \
        --container-image="${worker_image_path}" \
        --export=ALL \
        --container-mounts="${VT_ROOT}:/workspace,${SCRIPT_DIR}/profiles:/presto_profiles,${SCRIPT_DIR}/worker_info:/worker_info" \
        -- bash -c "$command"
}

# Check if profiling directory exists in worker container, create it if it doesn't
function check_profile_output_directory() {
    local worker_id=$1
    
    # Try to create the directory if it doesn't exist
    exec_in_worker "$worker_id" "mkdir -p /presto_profiles" >/dev/null 2>&1 || true
    
    # Verify it exists now
    if ! exec_in_worker "$worker_id" "[[ -d /presto_profiles ]]" 2>/dev/null; then
        echo "Warning: Could not create /presto_profiles directory in worker ${worker_id} container" >&2
        return 1
    fi
}

# Get the PID of presto_server process in a worker container
function get_presto_pid() {
    local worker_id=$1
    local pid_file="/worker_info/worker_${worker_id}_pid.txt"
    
    # Try to read PID from file first
    local pid=$(exec_in_worker "$worker_id" "cat ${pid_file} 2>/dev/null" 2>/dev/null | tr -d '\n\r ' || echo "")
    
    # If not found in file, try to find it by process name
    if [ -z "$pid" ] || [ "$pid" = "0" ] || ! kill -0 "$pid" 2>/dev/null; then
        pid=$(exec_in_worker "$worker_id" "pgrep -f 'presto_server.*--etc-dir' | head -1" 2>/dev/null | tr -d '\n\r ' || echo "")
    fi
    
    if [ -z "$pid" ] || [ "$pid" = "0" ]; then
        echo "Error: Could not find presto_server PID for worker ${worker_id}" >&2
        return 1
    fi
    
    echo "$pid"
}

# Start profiling on a specific worker using nsys attach
function start_profiler_worker() {
    local worker_id=$1
    local profile_output_file_path=$2
    
    check_profile_output_directory "$worker_id"
    
    # Get the PID of the presto_server process
    local pid
    pid=$(get_presto_pid "$worker_id") || return 1
    
    local profile_basename=$(basename "$profile_output_file_path")
    local output_file="/presto_profiles/${profile_basename}.nsys-rep"
    
    # Use nsys attach to attach to the running process
    # Note: This must be executed from the HOST, not from inside a container
    # We'll write a command file that gets executed from the host
    echo "Attaching nsys to presto_server (PID: $pid) in worker ${worker_id}" >&2
    
    # For now, try to execute from container - this will fail but show the approach
    # The real solution requires executing from host, which we'll implement via a command file
    exec_in_worker "$worker_id" \
        "nsys attach --pid=$pid --gpu-metrics-devices=all -t nvtx,cuda,osrt,ucx --cuda-memory-usage=true --cuda-um-cpu-page-faults=true --cuda-um-gpu-page-faults=true --cudabacktrace=true -o ${output_file}" || {
        echo "Warning: nsys attach failed. Trying alternative: writing command to file for host execution" >&2
        # Write command to a file that can be executed from the host
        echo "nsys attach --pid=$pid -o ${output_file}" > "${SCRIPT_DIR}/profiles/.profiler_cmd_${worker_id}.sh"
        return 1
    }
}

# Stop profiling on a specific worker and ensure file is accessible
function stop_profiler_worker() {
    local worker_id=$1
    local profile_output_file_path=$2
    
    check_profile_output_directory "$worker_id"
    
    local profile_basename=$(basename "$profile_output_file_path")
    local container_file_path="/presto_profiles/${profile_basename}.nsys-rep"
    
    # Stop profiling
    exec_in_worker "$worker_id" "nsys stop"
    
    # Change ownership so file is accessible
    exec_in_worker "$worker_id" "chown -R \$(id -u):\$(id -g) /presto_profiles"
    
    # The file should already be accessible via the mounted directory at ${SCRIPT_DIR}/profiles/
    # But we verify it exists
    local host_file_path="${SCRIPT_DIR}/profiles/${profile_basename}.nsys-rep"
    if [ ! -f "$host_file_path" ]; then
        echo "Warning: Profile file not found at expected location: $host_file_path" >&2
        return 1
    fi
    
    echo "Profile saved to: $host_file_path"
}

# Start profiling on all workers
function start_profiler() {
    local profile_output_file_path=$1
    
    if [ -z "${NUM_WORKERS:-}" ]; then
        echo "Error: NUM_WORKERS not set" >&2
        return 1
    fi
    
    echo "Starting profiling on ${NUM_WORKERS} workers for profile: ${profile_output_file_path}" >&2
    for ((worker_id=0; worker_id<NUM_WORKERS; worker_id++)); do
        # Each worker gets a unique profile file name
        local worker_profile_path="${profile_output_file_path}_worker${worker_id}"
        echo "  Starting profiler on worker ${worker_id} -> ${worker_profile_path}" >&2
        if ! start_profiler_worker "$worker_id" "$worker_profile_path"; then
            echo "Warning: Failed to start profiler on worker ${worker_id}" >&2
        fi
    done
}

# Stop profiling on all workers
function stop_profiler() {
    local profile_output_file_path=$1
    
    if [ -z "${NUM_WORKERS:-}" ]; then
        echo "Error: NUM_WORKERS not set" >&2
        return 1
    fi
    
    echo "Stopping profiling on ${NUM_WORKERS} workers for profile: ${profile_output_file_path}" >&2
    for ((worker_id=0; worker_id<NUM_WORKERS; worker_id++)); do
        # Each worker gets a unique profile file name
        local worker_profile_path="${profile_output_file_path}_worker${worker_id}"
        echo "  Stopping profiler on worker ${worker_id} -> ${worker_profile_path}" >&2
        if ! stop_profiler_worker "$worker_id" "$worker_profile_path"; then
            echo "Warning: Failed to stop profiler on worker ${worker_id}" >&2
        fi
    done
}

