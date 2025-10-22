import subprocess


def start_profiler(profile_script_path, profile_output_file_path):
    execute_profiler_function(profile_script_path, profile_output_file_path, "start_profiler")


def stop_profiler(profile_script_path, profile_output_file_path):
    execute_profiler_function(profile_script_path, profile_output_file_path, "stop_profiler")


def execute_profiler_function(profile_script_path, profile_output_file_path, profiler_function):
    profiler_command = ["bash", "-c",
                        f"source {profile_script_path}; {profiler_function} {profile_output_file_path}"]
    result = subprocess.run(profiler_command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"{profiler_function} returned error code: {result.returncode}, "
            f"stdout: {result.stdout}, stderr: {result.stderr}")
