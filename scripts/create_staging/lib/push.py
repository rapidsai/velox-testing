# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Push the target/dated staging branches back to origin."""

from __future__ import annotations

import datetime as _dt
import os
import subprocess
from urllib.parse import quote

from .config import Config
from .formatting import log, step
from .git_ops import git, git_retry


def push_refs(cfg: Config) -> list[str]:
    """Refspecs used for push / preflight (``HEAD:<branch>`` …).

    Uses ``DATED_BRANCH`` from the environment when set (multi-step CI). If
    unset but ``cfg.is_ci``, derives the same dated name as ``set_dated_branch``
    so single-process runs and preflight still match the eventual push.
    """
    dated = os.environ.get("DATED_BRANCH", "").strip()
    if not dated and cfg.is_ci:
        today = _dt.datetime.utcnow().strftime("%m-%d-%Y")
        dated = f"{cfg.target_branch}_{today}"
    refs = [f"HEAD:{cfg.target_branch}"]
    if dated:
        refs.append(f"HEAD:{dated}")
    return refs


def _refresh_origin_push_url_ci(cfg: Config) -> None:
    """Point origin at an authenticated HTTPS URL so push works in Actions.

    Clones often use ``x-access-token`` in the URL, but credential helpers or
    Git can normalize ``origin`` to a token-less URL; ``git push`` then fails
    with 403. ``GH_TOKEN`` is the PAT used for clone/API in CI.
    """
    token = os.environ.get("GH_TOKEN", "").strip()
    slug = (cfg.target_repository or "").strip()
    if not token or not slug:
        return
    safe_tok = quote(token, safe="")
    url = f"https://x-access-token:{safe_tok}@github.com/{slug}.git"
    git(["remote", "set-url", "origin", url], cwd=cfg.work_dir)
    log(f"Refreshed origin remote for push (https://github.com/{slug}.git, GH_TOKEN)")


def preflight_push(cfg: Config) -> None:
    """CI-only: ``git push --dry-run`` with the same refspecs as :func:`push_branches`.

    Intended to run right after clone (before reset/merges): validates PAT and
    many branch-protection rules. The tip of ``HEAD`` may still change before
    the real ``push`` step; auth failures should surface here regardless.
    """
    if not cfg.is_ci:
        log("Local mode: skipping preflight push test.")
        return

    repo = cfg.work_dir
    if git(["remote", "get-url", "origin"], cwd=repo, check=False).returncode != 0:
        raise RuntimeError(f"origin remote not set for {repo}")

    _refresh_origin_push_url_ci(cfg)
    refs = push_refs(cfg)
    step("Preflight: git push --dry-run (target repo)")
    log(f"Dry-run: {' '.join(refs)}")
    args = ["push", "--dry-run", "origin", *refs]
    if cfg.force_push:
        args.append("--force")
    try:
        git_retry(args, cwd=repo)
    except subprocess.CalledProcessError as exc:
        err = (getattr(exc, "stderr", None) or "").strip()
        if err:
            log(f"git push --dry-run stderr:\n{err}")
        log(
            "Preflight failed. Typical causes: GH_TOKEN cannot push to the target repo "
            "(use a PAT with repo scope on that repo), branch protection blocking pushes, "
            "or a non-fast-forward update (try --force-push or reconcile remote branches)."
        )
        raise
    log("Preflight push dry-run succeeded.")


def push_branches(cfg: Config) -> None:
    repo = cfg.work_dir
    if not cfg.is_ci:
        log("Local mode: skipping push to remote.")
        return

    if git(["remote", "get-url", "origin"], cwd=repo, check=False).returncode != 0:
        raise RuntimeError(f"origin remote not set for {repo}")

    _refresh_origin_push_url_ci(cfg)

    refs = push_refs(cfg)
    dated_suffix = f" and {refs[1].split(':', 1)[1]}" if len(refs) > 1 else ""

    try:
        if cfg.force_push:
            step(f"Force push {cfg.target_branch}{dated_suffix}")
            log(f"Force pushing {' '.join(refs)}...")
            git_retry(["push", "origin", *refs, "--force"], cwd=repo)
        else:
            step(f"Push {cfg.target_branch}{dated_suffix}")
            log(f"Pushing {' '.join(refs)}...")
            git_retry(["push", "origin", *refs], cwd=repo)
    except subprocess.CalledProcessError as exc:
        err = (getattr(exc, "stderr", None) or "").strip()
        if err:
            log(f"git push stderr:\n{err}")
        log(
            "If this is a permission error, ensure GH_TOKEN can push to the target repo "
            "(use a PAT with repo scope on the fork; the default GITHUB_TOKEN cannot push to another repo). "
            "If the remote branch diverged, enable --force-push or reconcile the remote branch first."
        )
        raise
