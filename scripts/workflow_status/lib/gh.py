#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""GitHub CLI (``gh``) wrapper with retry and helpers for runs/jobs/logs."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from typing import Any

from .config import Config

_TRANSIENT_PATTERNS = re.compile(
    r"TLS handshake timeout|i/o timeout|timeout|temporarily unavailable"
    r"|connection reset|EOF",
    re.IGNORECASE,
)


def run_gh(*args: str, config: Config, retries: int | None = None, timeout: int | None = None) -> str:
    """Run a ``gh`` CLI command with retry on transient errors.

    Returns stdout on success, raises ``RuntimeError`` on persistent failure.
    """
    retries = retries if retries is not None else config.gh_retries
    sleep_s = config.gh_retry_sleep
    env = {**os.environ, "GH_HTTP_TIMEOUT": str(config.gh_http_timeout)}
    cmd = ["gh", *args]

    attempt = 1
    while True:
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout or (config.gh_http_timeout + 30),
                env=env,
            )
        except subprocess.TimeoutExpired:
            combined = "subprocess timed out"
        else:
            if proc.returncode == 0:
                return proc.stdout
            combined = (proc.stdout + proc.stderr).strip()

        if _TRANSIENT_PATTERNS.search(combined) and attempt < retries:
            print(
                f"WARN: transient GitHub API error (attempt {attempt}/{retries}), "
                f"retrying in {sleep_s}s...\n      {combined}",
                file=sys.stderr,
            )
            time.sleep(sleep_s)
            attempt += 1
            sleep_s *= 2
            continue

        raise RuntimeError(f"gh command failed: {' '.join(cmd)}\n{combined}")


def run_gh_safe(*args: str, config: Config, **kwargs: Any) -> str | None:
    """Like ``run_gh`` but returns ``None`` on any failure."""
    try:
        return run_gh(*args, config=config, **kwargs)
    except Exception:
        return None


# ---- High-level helpers ---------------------------------------------------


def fetch_run(run_id: int, config: Config) -> dict | None:
    """Fetch metadata for a single workflow run."""
    fields = "databaseId,attempt,createdAt,startedAt,updatedAt,conclusion,status,url,workflowName,displayTitle,number"
    out = run_gh_safe(
        "run",
        "view",
        "-R",
        config.repo,
        str(run_id),
        "--json",
        fields,
        config=config,
    )
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def fetch_jobs(run_id: int, config: Config) -> list[dict]:
    """Return the list of job dicts for a workflow run."""
    out = run_gh_safe(
        "run",
        "view",
        "-R",
        config.repo,
        str(run_id),
        "--json",
        "jobs",
        config=config,
    )
    if not out:
        return []
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return []
    return data.get("jobs", [])


def fetch_log(run_id: int, config: Config, failed_only: bool = True) -> str | None:
    """Fetch the log for a whole run (``--log-failed`` or ``--log``).

    NOTE: ``gh run view --log[-failed]`` is rejected by the CLI when the run has
    too many jobs (``too many API requests needed to fetch logs; try narrowing
    down to a specific job with the --job option``). For analysis use
    :func:`fetch_job_log` instead, which fetches per-job and avoids the throttle.
    """
    flag = "--log-failed" if failed_only else "--log"
    out = run_gh_safe(
        "run",
        "view",
        "-R",
        config.repo,
        str(run_id),
        flag,
        config=config,
    )
    if not out and failed_only:
        out = run_gh_safe(
            "run",
            "view",
            "-R",
            config.repo,
            str(run_id),
            "--log",
            config=config,
        )
    return out


def fetch_job_log(job_id: int, config: Config) -> str | None:
    """Fetch the log for a single job by ID.

    Tries ``gh run view --job <id> --log`` first, which returns the log with
    the standard ``<job_name>\\t<step>\\t<timestamp>\\t<line>`` prefixes used
    elsewhere in the analyser. Falls back to the raw REST endpoint
    ``/repos/{owner}/{repo}/actions/jobs/{job_id}/logs`` if the gh wrapper
    returns nothing.

    Per-job fetching is the only reliable path for runs with many jobs (large
    matrix builds): the run-level log endpoints are rejected by the gh CLI
    with ``too many API requests needed to fetch logs; try narrowing down to a
    specific job with the --job option``.
    """
    out = run_gh_safe(
        "run",
        "view",
        "-R",
        config.repo,
        "--job",
        str(job_id),
        "--log",
        config=config,
    )
    if out:
        return out

    raw = run_gh_safe(
        "api",
        "-H",
        "Accept: application/vnd.github.v3.raw",
        f"/repos/{config.repo}/actions/jobs/{job_id}/logs",
        config=config,
    )
    return raw


def detect_repo(config: Config) -> str:
    """Auto-detect the repo via ``gh repo view``."""
    out = run_gh_safe("repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner", config=config)
    return out.strip() if out else ""
