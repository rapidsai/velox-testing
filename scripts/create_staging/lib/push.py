# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Push the target/dated staging branches back to origin."""

from __future__ import annotations

import os

from .config import Config
from .formatting import log, step
from .git_ops import git, git_retry


def push_branches(cfg: Config) -> None:
    repo = cfg.work_dir
    if not cfg.is_ci:
        log("Local mode: skipping push to remote.")
        return

    if git(["remote", "get-url", "origin"], cwd=repo, check=False).returncode != 0:
        raise RuntimeError(f"origin remote not set for {repo}")

    dated = os.environ.get("DATED_BRANCH", "").strip()
    refs = [f"HEAD:{cfg.target_branch}"]
    if dated:
        refs.append(f"HEAD:{dated}")

    if cfg.force_push:
        step(f"Force push {cfg.target_branch}" + (f" and {dated}" if dated else ""))
        log(f"Force pushing {' '.join(refs)}...")
        git_retry(["push", "origin", *refs, "--force"], cwd=repo)
    else:
        step(f"Push {cfg.target_branch}" + (f" and {dated}" if dated else ""))
        log(f"Pushing {' '.join(refs)}...")
        git_retry(["push", "origin", *refs], cwd=repo)
