# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import ctypes
import ctypes.util
import functools
import io
import os
import subprocess
from pathlib import Path
from typing import Union


@functools.cache
def _libc():
    return ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)


def _drop_file_cache(
    file: Union[os.PathLike, str, int, io.IOBase],
    offset: int = 0,
    length: int = 0,
    sync_first: bool = True,
) -> None:
    """Drop page cache for a specific file, based on KvikIO's C++ and Python
       implementation.

    Advises the kernel to evict cached pages for the specified file descriptor using
    `posix_fadvise` with `POSIX_FADV_DONTNEED`.

    Parameters
    ----------
    file: a path-like object, or string, or file descriptor, or file object
        File to operate on.
    offset: int, optional
        Starting byte offset (default: 0 for beginning of file)
    length: int, optional
        Number of bytes to drop (default: 0, meaning entire file from offset)
    sync_first: bool, optional
        Whether to flush dirty pages to disk before dropping. If `True`, `fdatasync`
        will be called prior to dropping. This ensures dirty pages become clean and
        thus droppable. Can be set to `False` if we are certain no dirty pages exist
        for this file.

    Notes
    -----
    - This is the preferred method for benchmark cache invalidation as it:

      - Requires no elevated privileges
      - Affects only the specified file
      - Has minimal overhead (no child process spawned)

    - The page cache dropping takes place in granularity of full pages. If the
      specified range does not align to page boundaries, partial pages at the start
      and end of the range are retained. Only pages fully contained within the range
      are dropped.

    - For dropping page cache system-wide (requires elevated privileges), see
      `_drop_system_cache()`.
    """
    fd = None
    should_close = False
    if isinstance(file, (os.PathLike, str)):
        # file is a path or a string object
        fd = os.open(file, os.O_RDONLY)
        should_close = True
    elif isinstance(file, int):
        # file is a file descriptor
        fd = file
    elif isinstance(file, io.IOBase):
        # file is a file object
        # pass its file descriptor to the underlying C++ function
        fd = file.fileno()
    else:
        raise ValueError("The type of `file` must be `os.PathLike`, `str`, `int`, or `io.IOBase`")

    try:
        if sync_first:
            os.fdatasync(fd)
        # POSIX_FADV_DONTNEED informs the kernel that the specified data will not be
        # accessed in the near future, which subsequently attempts to free the
        # associated cached pages. A `length` of 0 means until the end of the file
        # from the offset.
        ret = _libc().posix_fadvise(
            ctypes.c_int(fd),
            ctypes.c_longlong(offset),
            ctypes.c_longlong(length),
            ctypes.c_int(os.POSIX_FADV_DONTNEED),
        )
        if ret != 0:
            raise OSError(ret, f"posix_fadvise failed: {os.strerror(ret)}")
    finally:
        if should_close:
            os.close(fd)


def _drop_system_cache():
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
        "free; echo drop_caches; sync; echo 3 > /proc/sys/vm/drop_caches; free",
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"drop_cache returned error code: {result.returncode}, stdout: {result.stdout}, stderr: {result.stderr}"
        )


def drop_cache(path: Union[os.PathLike, str, int, io.IOBase, None] = None) -> None:
    """Drop page cache for a file, directory, or the entire system.

    This function dispatches to `_drop_file_cache` for per-file dropping
    (unprivileged, via `posix_fadvise`) or `_drop_system_cache` for system-wide
    dropping (privileged, via Docker).

    Parameters
    ----------
    path : os.PathLike, str, int, io.IOBase, or None
        Target to drop cache for:

        - os.PathLike or str: If the path is a file, drops cache for that file. If
          the path is a directory, recursively drops cache for all files within it.
        - int: Treated as a file descriptor. Drops cache for the associated file.
          The caller retains ownership of the file descriptor.
        - io.IOBase: Treated as a file object. Drops cache via its underlying file
          descriptor. The caller retains ownership of the file object.
        - None: Falls back to system-wide cache dropping, which clears all page cache,
          dentries, and inodes. This requires Docker and elevated privileges.
    """
    if path is None:
        _drop_system_cache()
        return

    if isinstance(path, (int, io.IOBase)):
        _drop_file_cache(path)
        return

    # path is os.PathLike or str
    expanded_path = Path(path).expanduser()
    if not expanded_path.exists():
        raise FileNotFoundError(f"Path does not exist: {expanded_path}")

    # If the path is a file, drop the cache for that file
    if expanded_path.is_file():
        _drop_file_cache(expanded_path)
        return

    # If the path is a directory, drop the cache for all files in the directory
    # recursively
    if expanded_path.is_dir():
        for file in expanded_path.rglob("*"):
            if file.is_file():
                _drop_file_cache(file)
        return

    raise ValueError(f"Path is neither a regular file nor a directory: {expanded_path}")
