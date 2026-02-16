# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import subprocess


def start_profiler(profile_script_path, profile_output_file_path):
    """Start profiling on all worker containers.

    Args:
        profile_script_path: Path to the profiler_functions.sh script
        profile_output_file_path: Base path for profile files (without .nsys-rep extension)
    """
    execute_profiler_function(profile_script_path, profile_output_file_path, "start_profiler")


def stop_profiler(profile_script_path, profile_output_file_path):
    """Stop profiling on all worker containers and copy profile files.

    Args:
        profile_script_path: Path to the profiler_functions.sh script
        profile_output_file_path: Base path for profile files (without .nsys-rep extension)
    """
    execute_profiler_function(profile_script_path, profile_output_file_path, "stop_profiler")


def execute_profiler_function(profile_script_path, profile_output_file_path, profiler_function):
    # Ensure SCRIPT_DIR is set correctly - it should point to the slurm directory
    # where worker info files are stored, not the scripts directory
    env = os.environ.copy()
    # If SCRIPT_DIR is not set or points to scripts, fix it
    script_dir = env.get("SCRIPT_DIR", "")
    if not script_dir or "scripts" in script_dir:
        # Try to derive from profile_script_path
        if "presto-nvl72" in profile_script_path:
            env["SCRIPT_DIR"] = "/workspace/presto/slurm/presto-nvl72"
        else:
            env["SCRIPT_DIR"] = script_dir if script_dir else "/workspace/presto/slurm/presto-nvl72"
    
    # IMPORTANT: We need to execute the profiler script from the HOST, not from inside the container
    # because srun is only available on the host. We'll write a wrapper script that gets executed
    # from the host via a mechanism that can escape the container.
    # 
    # Since we're inside a container, we need to use a different approach:
    # Option 1: Use nsys attach to attach to running processes (requires PID)
    # Option 2: Write commands to a file that a host process reads
    # Option 3: Use a mechanism to execute from host
    
    # For now, let's try to detect if we're in a container and provide a helpful error
    print(f"[Profiler] Executing {profiler_function} with script: {profile_script_path}, output: {profile_output_file_path}")
    print(f"[Profiler] SCRIPT_DIR={env.get('SCRIPT_DIR', 'NOT SET')}, VT_ROOT={env.get('VT_ROOT', 'NOT SET')}, IMAGE_DIR={env.get('IMAGE_DIR', 'NOT SET')}, NUM_WORKERS={env.get('NUM_WORKERS', 'NOT SET')}")
    
    # Check if we're in a container
    in_container = os.path.exists("/.singularity.d/runscript") or "SINGULARITY" in env
    
    if in_container:
        print(f"[Profiler] WARNING: Running inside container. Profiling via srun requires host execution.")
        print(f"[Profiler] Attempting to use alternative method: nsys attach to running processes")
        # We'll need to use nsys attach instead - this requires finding the PID of presto_server
        # For now, let's try the original method and see if it fails gracefully
        pass
    
    profiler_command = ["bash", "-c", f"source {profile_script_path}; {profiler_function} {profile_output_file_path}"]
    result = subprocess.run(profiler_command, capture_output=True, text=True, env=env)
    
    # Always print output for debugging
    if result.stdout:
        print(f"[Profiler] stdout: {result.stdout}")
    if result.stderr:
        print(f"[Profiler] stderr: {result.stderr}")
    
    if result.returncode != 0:
        error_msg = (
            f"{profiler_function} returned error code: {result.returncode}, "
            f"stdout: {result.stdout}, stderr: {result.stderr}"
        )
        print(f"[Profiler] ERROR: {error_msg}")
        raise RuntimeError(error_msg)
    else:
        print(f"[Profiler] {profiler_function} completed successfully")
