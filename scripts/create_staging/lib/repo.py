# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Repository initialisation: layout checks, remotes, branches, rerere setup."""

from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

from .config import Config, emit_output
from .formatting import log
from .git_ops import git, git_retry, normalize_repo_url


def ensure_sibling_layout(target_dir: Path) -> None:
    """Verify the target repo lives next to a `velox-testing` sibling directory.

    The expected layout (both locally and in CI):

        <parent>/
          velox-testing/   # this repo
          velox/           # or presto/, depending on pipeline -- the target
    """
    if not target_dir.is_dir():
        raise RuntimeError(f"Target directory not found: {target_dir}")
    velox_testing = target_dir.parent / "velox-testing"
    if not velox_testing.is_dir():
        raise RuntimeError(
            f"Expected velox-testing sibling directory not found: {velox_testing}. "
            f"Clone the target repo next to velox-testing (e.g. `git clone "
            f"<target-repo> {target_dir.parent}/{target_dir.name}`)."
        )


def setup_git_config(repo_dir: Path) -> None:
    """Make sure user.name/email are set so merge commits succeed."""
    if git(["config", "user.name"], cwd=repo_dir, check=False).returncode != 0:
        git(["config", "user.name", "velox-staging-bot"], cwd=repo_dir)
    if git(["config", "user.email"], cwd=repo_dir, check=False).returncode != 0:
        git(["config", "user.email", "velox-staging-bot@users.noreply.github.com"], cwd=repo_dir)


def setup_rerere(repo_dir: Path) -> None:
    """Enable git rerere so the resolutions Claude (or a human) records are reused."""
    git(["config", "rerere.enabled", "true"], cwd=repo_dir)
    git(["config", "rerere.autoUpdate", "true"], cwd=repo_dir)


def setup_remotes(cfg: Config, repo_dir: Path) -> None:
    """Ensure an `upstream` remote pointing at the base repository exists.

    For local target paths we only verify (and add) the remote, no fetch yet.
    For freshly cloned repos we both add the remote and fetch the base branch.
    """
    if cfg.use_local_path:
        upstream_url = git(["remote", "get-url", "upstream"], cwd=repo_dir, check=False).stdout.strip()
        if not upstream_url:
            log(f"Upstream remote not set. Adding upstream -> https://github.com/{cfg.base_repository}.git")
            git(["remote", "add", "upstream", f"https://github.com/{cfg.base_repository}.git"], cwd=repo_dir)
            return
        normalized = normalize_repo_url(upstream_url)
        if normalized != cfg.base_repository:
            raise RuntimeError(
                f"Upstream remote points to {normalized}. Add a different remote name for "
                f"{cfg.base_repository} and update --base-repository accordingly."
            )
        return

    if git(["remote", "get-url", "upstream"], cwd=repo_dir, check=False).returncode != 0:
        git(["remote", "add", "upstream", f"https://github.com/{cfg.base_repository}.git"], cwd=repo_dir)
    log(f"Fetching upstream {cfg.base_branch}...")
    git_retry(["fetch", "upstream", cfg.base_branch], cwd=repo_dir)


def init_repo(cfg: Config) -> None:
    """Validate the target repo, set up git config, remotes, rerere, and the dated branch."""
    target = cfg.target_path
    if not target.is_dir():
        raise RuntimeError(f"target path not found: {target}")
    if not (target / ".git").exists() and git(["rev-parse", "--git-dir"], cwd=target, check=False).returncode != 0:
        raise RuntimeError(f"target path is not a git repo: {target}")
    cfg.work_dir = target

    if not cfg.target_repository:
        origin_url = git(["remote", "get-url", "origin"], cwd=target, check=False).stdout.strip()
        cfg.target_repository = normalize_repo_url(origin_url)
        if not cfg.target_repository:
            raise RuntimeError("Could not determine target repository from origin remote.")

    ensure_sibling_layout(cfg.work_dir)
    setup_git_config(cfg.work_dir)
    setup_rerere(cfg.work_dir)
    setup_remotes(cfg, cfg.work_dir)
    set_dated_branch(cfg)


def set_dated_branch(cfg: Config) -> None:
    if not cfg.is_ci:
        emit_output("DATED_BRANCH", "")
        return
    today = _dt.datetime.utcnow().strftime("%m-%d-%Y")
    dated = f"{cfg.target_branch}_{today}"
    emit_output("DATED_BRANCH", dated)
    log(f"Dated branch: {dated}")


def maybe_confirm_reset(cfg: Config) -> None:
    if cfg.is_ci:
        log(f"CI mode: auto-confirming reset of {cfg.target_branch}.")
        return
    if not sys.stdin.isatty():
        raise RuntimeError(f"Confirmation required to reset {cfg.target_branch} but no TTY available.")
    dirty = git(["status", "--porcelain"], cwd=cfg.work_dir, check=False).stdout.strip()
    if dirty:
        log("")
        log(f"WARNING: The repository at {cfg.work_dir} has uncommitted changes:")
        log(dirty)
        log("")
        log("These changes WILL BE LOST. The script performs hard resets and cleans untracked files.")
        log("Consider running 'git stash' first to preserve your work.")
        log("")
    log(f"About to reset {cfg.target_branch} to {cfg.base_repository}/{cfg.base_branch} in {cfg.work_dir}.")
    log("This will DESTRUCTIVELY modify the repository (checkout -B, reset --hard, clean -fd).")
    confirm = input("Continue? [y/N] ").strip().lower()
    if confirm not in ("y", "yes"):
        raise RuntimeError("Aborted by user.")


def reset_target_branch(cfg: Config) -> str:
    """Reset target branch to the base repo's branch and return the new HEAD SHA."""
    repo = cfg.work_dir
    log(f"Resetting {cfg.target_branch} to {cfg.base_repository}/{cfg.base_branch} (via direct fetch)...")
    git_retry(["fetch", f"https://github.com/{cfg.base_repository}.git", cfg.base_branch], cwd=repo)
    git(["checkout", "-B", cfg.target_branch, "FETCH_HEAD"], cwd=repo)
    base_commit = git(["rev-parse", "HEAD"], cwd=repo).stdout.strip()
    emit_output("BASE_COMMIT", base_commit)
    log(f"Base commit: {base_commit}")
    return base_commit
