# Copyright (c) 2026, NVIDIA CORPORATION.
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

import subprocess


def drop_cache():
    """Drop system caches using a privileged Docker container.

    This runs a Docker container with elevated privileges to clear
    the system page cache, dentries, and inodes by writing to
    /proc/sys/vm/drop_caches.
    """
    command = [
        "docker", "run", "--rm", "--privileged", "--gpus", "all",
        "alpine:latest", "sh", "-c",
        "free; echo drop_caches; echo 3 > /proc/sys/vm/drop_caches; free"
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"drop_cache returned error code: {result.returncode}, "
            f"stdout: {result.stdout}, stderr: {result.stderr}")

