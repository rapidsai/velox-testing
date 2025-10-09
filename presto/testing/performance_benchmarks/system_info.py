# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import platform
import subprocess
import re
import os


def get_system_specs():
    """
    Get GPU and CPU specifications of the current machine.
    
    Returns:
        dict: A dictionary containing system specifications with keys:
            - arch: CPU architecture (e.g., "x86_64", "aarch64")
            - gpu_count: Number of GPUs available
            - cpu_count: Number of CPU cores
            - cpu_name: CPU model name
            - cpu_memory: Total system memory in GB
            - gpu_name: GPU model name
            - gpu_memory: GPU memory in GB (for a single GPU)
    """
    specs = {}
    
    # CPU Architecture
    specs["arch"] = platform.machine()
    
    # CPU Count (physical cores)
    specs["cpu_count"] = os.cpu_count()
    
    # CPU Name
    specs["cpu_name"] = _get_cpu_name()
    
    # CPU Memory (total system RAM in GB)
    specs["cpu_memory_gb"] = _get_cpu_memory_gb()
    
    # GPU Information
    gpu_info = _get_gpu_info()
    specs["gpu_count"] = gpu_info["count"]
    specs["gpu_name"] = gpu_info["name"]
    specs["gpu_memory_gb"] = gpu_info["memory_gb"]
    
    return specs


def _get_cpu_name():
    """Extract CPU model name from /proc/cpuinfo or platform info."""
    try:
        # Try to get from /proc/cpuinfo (Linux)
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if "model name" in line:
                    # Extract the CPU name after the colon
                    name = line.split(":")[1].strip()
                    # Simplify to just the brand (Intel/AMD)
                    if "Intel" in name:
                        return "intel"
                    elif "AMD" in name:
                        return "amd"
                    else:
                        return name.lower()
    except Exception:
        pass
    
    # Fallback to platform processor
    processor = platform.processor()
    if processor:
        if "Intel" in processor or "intel" in processor:
            return "intel"
        elif "AMD" in processor or "amd" in processor:
            return "amd"
        return processor.lower()
    
    return "unknown"


def _get_cpu_memory_gb():
    """Get total system memory in GB."""
    try:
        # Read from /proc/meminfo (Linux)
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    # Extract memory in KB and convert to GB
                    mem_kb = int(line.split()[1])
                    mem_gb = round(mem_kb / (1024 * 1024))
                    return mem_gb
    except Exception:
        pass
    
    return 0


def _get_gpu_info():
    """
    Get GPU information using nvidia-smi.
    
    Returns:
        dict: Dictionary with keys 'count', 'name', and 'memory_gb'
    """
    gpu_info = {
        "count": 0,
        "name": "none",
        "memory_gb": 0
    }
    
    try:
        # Run nvidia-smi to get GPU information
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            check=True
        )
        
        lines = result.stdout.strip().split("\n")
        gpu_info["count"] = len(lines)
        
        if lines and lines[0]:
            # Parse first GPU info (assuming all GPUs are the same)
            parts = lines[0].split(",")
            if len(parts) >= 2:
                gpu_name = parts[0].strip()
                # Simplify GPU name (e.g., "NVIDIA A100-SXM4-80GB" -> "a100")
                gpu_info["name"] = _simplify_gpu_name(gpu_name)
                
                # Memory in MB, convert to GB
                memory_mb = float(parts[1].strip())
                gpu_info["memory_gb"] = round(memory_mb / 1024)
    
    except (subprocess.CalledProcessError, FileNotFoundError):
        # nvidia-smi not available or failed
        pass
    
    return gpu_info


def _simplify_gpu_name(full_name):
    """
    Simplify GPU name to a short identifier.
    
    Examples:
        "NVIDIA A100-SXM4-80GB" -> "a100"
        "Tesla V100-PCIE-32GB" -> "v100"
        "NVIDIA GeForce RTX 3090" -> "rtx3090"
    """
    name_lower = full_name.lower()
    
    # Common GPU patterns
    patterns = [
        (r"a100", "a100"),
        (r"a40", "a40"),
        (r"a30", "a30"),
        (r"v100", "v100"),
        (r"t4", "t4"),
        (r"h100", "h100"),
        (r"rtx\s*(\d+)", r"rtx\1"),
        (r"gtx\s*(\d+)", r"gtx\1"),
    ]
    
    for pattern, replacement in patterns:
        match = re.search(pattern, name_lower)
        if match:
            if "\\" in replacement:  # Has capture group
                return re.sub(pattern, replacement, name_lower)
            else:
                return replacement
    
    # If no pattern matches, return cleaned name
    # Remove common prefixes and extra info
    clean_name = re.sub(r"(nvidia|tesla|geforce)\s*", "", name_lower)
    clean_name = re.sub(r"[-_].*", "", clean_name)  # Remove everything after dash/underscore
    clean_name = clean_name.strip()
    
    return clean_name if clean_name else full_name.lower()


def get_version_info():
    """
    Get version information for Presto, Velox, CUDA, and CUDA driver.
    
    Returns:
        dict: A dictionary containing version information with keys:
            - version_presto: Git hash of the presto repository
            - version_velox: Git hash of the velox repository
            - version_cuda: CUDA runtime version
            - version_cuda_driver: CUDA driver version
    """
    versions = {}
    
    # Get Presto git hash
    versions["version_presto"] = _get_git_hash("/raid/johallaron/projects/presto")
    
    # Get Velox git hash
    versions["version_velox"] = _get_git_hash("/raid/johallaron/projects/velox")
    
    # Get CUDA versions
    cuda_info = _get_cuda_versions()
    versions["version_cuda"] = cuda_info["runtime"]
    versions["version_cuda_driver"] = cuda_info["driver"]
    
    return versions


def _get_git_hash(repo_path):
    """
    Get the git commit hash for a repository.
    
    Args:
        repo_path: Path to the git repository
        
    Returns:
        str: Git commit hash, or "unknown" if not available
    """
    try:
        result = subprocess.run(
            ["git", "-C", repo_path, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _get_cuda_versions():
    """
    Get CUDA runtime and driver versions.
    
    Returns:
        dict: Dictionary with keys 'runtime' and 'driver'
    """
    cuda_info = {
        "runtime": "unknown",
        "driver": "unknown"
    }
    
    try:
        # Get CUDA driver version from nvidia-smi
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            check=True
        )
        driver_version = result.stdout.strip().split("\n")[0].strip()
        cuda_info["driver"] = driver_version
        
        # Get CUDA runtime version from nvidia-smi
        result = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
            check=True
        )
        # Parse CUDA version from output (typically in header)
        # Example: "CUDA Version: 12.2"
        match = re.search(r"CUDA Version:\s*(\d+\.\d+)", result.stdout)
        if match:
            cuda_info["runtime"] = match.group(1)
    
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    return cuda_info


if __name__ == "__main__":
    # Example usage
    import json
    print("System Specs:")
    specs = get_system_specs()
    print(json.dumps(specs, indent=2))
    
    print("\nVersion Info:")
    versions = get_version_info()
    print(json.dumps(versions, indent=2))

