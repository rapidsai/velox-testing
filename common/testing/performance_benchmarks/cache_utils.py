# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import subprocess


def drop_cache():
    """Drop system caches using a privileged Docker container.

    This runs a Docker container with elevated privileges to clear
    the system page cache, dentries, and inodes by writing to
    /proc/sys/vm/drop_caches.
    """
    command = [
        "docker",
        "run",
        "--rm",
        "--privileged",
        "alpine:latest",
        "sh",
        "-c",
        "free; echo drop_caches; echo 3 > /proc/sys/vm/drop_caches; free",
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"drop_cache returned error code: {result.returncode}, stdout: {result.stdout}, stderr: {result.stderr}"
        )
