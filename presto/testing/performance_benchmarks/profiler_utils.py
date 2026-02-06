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
    profiler_command = ["bash", "-c",
                        f"source {profile_script_path}; {profiler_function} {profile_output_file_path}"]
    
    result = subprocess.run(profiler_command, capture_output=True, text=True, env=os.environ)
    if result.returncode != 0:
        raise RuntimeError(
            f"{profiler_function} returned error code: {result.returncode}, "
            f"stdout: {result.stdout}, stderr: {result.stderr}")
