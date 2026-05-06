#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Staging Branch Creator (Python port of scripts/create_staging_branch.sh).

Creates a staging branch by merging PRs from a base repository into a target
repository. Conflicting merges are auto-resolved using ``git rerere`` (for
previously seen conflicts) plus a Claude Code CLI fallback for new ones.

Pipeline (run end-to-end with ``--step all`` or one step per CI job):

    reset           Reset target branch to base repo / branch
    merge-additional  Merge an optional auxiliary repo / branch (e.g. cuDF exchange)
    fetch-prs       Resolve the list of PRs (auto-fetch by labels and/or manual)
    test-merge      Verify each PR merges cleanly against the base
    test-pairwise   Verify all PRs are mutually compatible (NxN matrix)
    merge           Sequentially merge PRs (with rerere + Claude conflict assist)
    manifest        Write and commit .staging-manifest.yaml
    push            Push the target / dated branch (CI only)

Single-step CI runs share state via GITHUB_ENV/GITHUB_OUTPUT (PR_LIST,
BASE_COMMIT, MERGED_PRS, ADDITIONAL_MERGE_COMMIT, DATED_BRANCH).

Examples:

    # Local end-to-end run
    python3 scripts/create_staging/create_staging.py \
        --target-path ../velox \
        --base-repository facebookincubator/velox \
        --base-branch main \
        --pr-labels cudf

    # CI single step
    python3 scripts/create_staging/create_staging.py \\
        --mode ci --step merge \\
        --target-path ./velox \\
        --base-repository facebookincubator/velox \\
        --base-branch main

Environment:
    GH_TOKEN        GitHub token used by `gh` and for fetch/push
    CLAUDE_BIN      Override Claude CLI binary (default: claude)
    CLAUDE_MODEL    Claude model (default: claude-sonnet-4-5)
    CLAUDE_TIMEOUT  Per-file Claude timeout in seconds (default: 300)
    PR_TIMEOUT      Per-PR auto-resolve wall-clock timeout in seconds
                    (default: 900; set 0 to disable)
"""

from __future__ import annotations

import argparse
import os
import sys

from lib import config as cfg_mod
from lib.config import Config, require_env
from lib.conflict import claude_available
from lib.formatting import log
from lib.git_ops import require_cmd
from lib.manifest import create_manifest
from lib.merge import merge_additional_repository, merge_prs
from lib.prs import fetch_pr_list
from lib.push import push_branches
from lib.repo import init_repo, maybe_confirm_reset, reset_target_branch
from lib.test_merge import test_merge_compatibility, test_pairwise_compatibility

_STEPS = (
    "all",
    "reset",
    "merge-additional",
    "fetch-prs",
    "test-merge",
    "test-pairwise",
    "merge",
    "manifest",
    "push",
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="create_staging.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cfg_mod.add_arguments(parser)
    return parser


def _validate_step(step: str) -> None:
    if step and step not in _STEPS:
        raise SystemExit(f"Unknown --step value: {step}. Valid: {', '.join(_STEPS)}")


def _env_pr_list() -> list:
    raw = require_env("PR_LIST")
    return [p for p in raw.split() if p.strip()]


def _run_single_step(cfg: Config, step: str) -> None:
    if step == "reset":
        maybe_confirm_reset(cfg)
        reset_target_branch(cfg)
    elif step == "merge-additional":
        merge_additional_repository(cfg)
    elif step == "fetch-prs":
        fetch_pr_list(cfg)
    elif step == "test-merge":
        test_merge_compatibility(cfg, require_env("BASE_COMMIT"), _env_pr_list())
    elif step == "test-pairwise":
        test_pairwise_compatibility(cfg, require_env("BASE_COMMIT"), _env_pr_list())
    elif step == "merge":
        merge_prs(cfg, require_env("BASE_COMMIT"), _env_pr_list())
    elif step == "manifest":
        merged = [p for p in require_env("MERGED_PRS").split() if p.strip()]
        skipped = [p for p in os.environ.get("FAILED_PRS", "").split() if p.strip()]
        additional = os.environ.get("ADDITIONAL_MERGE_COMMIT", "").strip()
        create_manifest(
            cfg,
            require_env("BASE_COMMIT"),
            merged,
            additional,
            skipped_prs=skipped,
        )
    elif step == "push":
        push_branches(cfg)
    else:
        raise SystemExit(f"Unknown --step value: {step}")


def _run_all(cfg: Config) -> None:
    maybe_confirm_reset(cfg)
    base_commit = reset_target_branch(cfg)
    additional_commit = merge_additional_repository(cfg)
    if additional_commit:
        # merge_additional_repository updates BASE_COMMIT in env; rebind here too.
        from lib.git_ops import git as _git

        base_commit = _git(["rev-parse", "HEAD"], cwd=cfg.work_dir).stdout.strip()

    pr_list = fetch_pr_list(cfg)
    test_merge_compatibility(cfg, base_commit, pr_list)
    test_pairwise_compatibility(cfg, base_commit, pr_list)
    result = merge_prs(cfg, base_commit, pr_list)
    create_manifest(
        cfg,
        base_commit,
        result["merged"],
        additional_commit,
        skipped_prs=result.get("failed", []),
    )
    push_branches(cfg)


def main(argv: list | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _validate_step(args.step)

    require_cmd("git")
    require_cmd("gh")

    cfg = cfg_mod.load_from_args(args)

    if cfg.enable_auto_resolve:
        if not claude_available(cfg):
            log(
                f"WARN: Claude CLI '{cfg.claude_bin}' not found on PATH. "
                f"Conflicts will not be auto-resolved (rerere still active)."
            )

    init_repo(cfg)

    step = cfg.step or "all"
    if step == "all":
        _run_all(cfg)
    else:
        _run_single_step(cfg, step)
    log("Done.")
    return 0


if __name__ == "__main__":
    # Allow direct script execution (`python3 path/to/create_staging.py`) without
    # requiring the parent directory to be on PYTHONPATH.
    if __package__ in (None, ""):
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main())
