# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Thin wrappers around `git` and `gh` CLIs with retry support."""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Sequence


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def require_cmd(cmd: str) -> None:
    if shutil.which(cmd) is None:
        raise RuntimeError(f"missing required command: {cmd}")


def run(
    cmd: Sequence[str],
    *,
    cwd: Optional[Path] = None,
    check: bool = True,
    capture: bool = True,
    text: bool = True,
    env: Optional[dict] = None,
    input: Optional[str] = None,
    timeout: Optional[int] = None,
) -> subprocess.CompletedProcess:
    """Run a command and return CompletedProcess. Stderr is always captured for diagnostics."""
    result = subprocess.run(
        list(cmd),
        cwd=str(cwd) if cwd else None,
        check=False,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE,
        text=text,
        env=env,
        input=input,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise subprocess.CalledProcessError(result.returncode, list(cmd), output=result.stdout, stderr=stderr)
    return result


def retry(
    cmd: Sequence[str],
    *,
    cwd: Optional[Path] = None,
    attempts: int = 5,
    initial_sleep: float = 2.0,
    **kwargs,
) -> subprocess.CompletedProcess:
    """Retry a command with exponential backoff."""
    sleep_s = initial_sleep
    last_exc: Optional[Exception] = None
    for n in range(1, attempts + 1):
        try:
            return run(cmd, cwd=cwd, check=True, **kwargs)
        except subprocess.CalledProcessError as exc:
            last_exc = exc
            if n >= attempts:
                break
            _log(f"WARN: command failed (attempt {n}/{attempts}), retrying in {sleep_s:.0f}s: {' '.join(cmd)}")
            time.sleep(sleep_s)
            sleep_s *= 2
    assert last_exc is not None
    raise last_exc


def git(args: Sequence[str], *, cwd: Path, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    return run(["git", "-C", str(cwd), *args], check=check, **kwargs)


def git_retry(args: Sequence[str], *, cwd: Path, **kwargs) -> subprocess.CompletedProcess:
    return retry(["git", "-C", str(cwd), *args], **kwargs)


def gh(args: Sequence[str], *, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    return run(["gh", *args], check=check, **kwargs)


_REPO_RE = re.compile(r"[:/]([^:/]+)/([^:/]+?)(?:\.git)?$")


def normalize_repo_url(url: str) -> str:
    """Return owner/repo from a remote URL. Empty string if it can't be parsed."""
    if not url:
        return ""
    m = _REPO_RE.search(url.strip())
    return f"{m.group(1)}/{m.group(2)}" if m else ""


def list_unmerged_files(repo: Path) -> List[str]:
    """Return paths of files with unresolved merge conflicts (UU/AA/UD/DU/AU/UA states)."""
    res = git(["status", "--porcelain"], cwd=repo, check=False)
    files: List[str] = []
    for line in (res.stdout or "").splitlines():
        if len(line) < 4:
            continue
        xy = line[:2]
        if xy in ("UU", "AA", "DD", "AU", "UA", "DU", "UD"):
            files.append(line[3:])
    return files
