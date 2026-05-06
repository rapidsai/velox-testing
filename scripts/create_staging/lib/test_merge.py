# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Pre-flight merge compatibility checks (single-PR and pairwise)."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

from .config import Config
from .formatting import (
    divider,
    log,
    render_compatibility_matrix,
    render_conflict_pr_table,
    step,
)
from .git_ops import git
from .prs import fetch_pr_head, fetch_pr_metadata


def _reset_to_base(repo: Path, base_commit: str) -> None:
    git(["merge", "--abort"], cwd=repo, check=False)
    git(["reset", "--hard", base_commit], cwd=repo, check=False)
    git(["clean", "-fd"], cwd=repo, check=False)


def test_merge_compatibility(
    cfg: Config,
    base_commit: str,
    pr_list: List[str],
) -> None:
    """Check each PR's mergeability against the current base.

    This is a non-fatal diagnostic: conflicts are reported but never abort the
    pipeline. The real merge step (`merge_prs`) will attempt rerere + Claude
    resolution for any PR flagged here.
    """
    repo = cfg.work_dir
    cache: Dict[str, str] = {}
    conflicts: List[str] = []

    step(f"Testing merge compatibility against {cfg.base_repository}/{cfg.base_branch}")
    for pr_num in pr_list:
        divider(f"PR #{pr_num}")
        sha = fetch_pr_head(cfg, repo, pr_num, cache)
        res = git(["merge", "--no-commit", "--no-ff", sha], cwd=repo, check=False)
        if res.returncode != 0:
            conflicts.append(pr_num)
        _reset_to_base(repo, base_commit)

    if conflicts:
        log(f"WARNING: Conflicts detected for PRs: {' '.join(conflicts)}")
        for pr_num in conflicts:
            log(f"  PR #{pr_num}: https://github.com/{cfg.base_repository}/pull/{pr_num}")
        log("Continuing; rerere + Claude will attempt to resolve during the merge step.")
        return
    log(f"All PRs can merge cleanly with {cfg.base_branch}.")


def test_pairwise_compatibility(
    cfg: Config,
    base_commit: str,
    pr_list: List[str],
) -> None:
    """Run an N x N pairwise merge check and pretty-print a compatibility matrix.

    Non-fatal: conflicts are reported but the pipeline continues so the merge
    step can attempt rerere + Claude resolution.
    """
    repo = cfg.work_dir
    if len(pr_list) < 2:
        return
    cache: Dict[str, str] = {}
    pair_results: Dict[Tuple[str, str], str] = {}
    conflict_pairs: List[Tuple[str, str]] = []
    conflict_prs: set = set()

    step("Testing pairwise merge compatibility")
    for i, pr1 in enumerate(pr_list):
        for pr2 in pr_list[i + 1 :]:
            sha1 = fetch_pr_head(cfg, repo, pr1, cache)
            sha2 = fetch_pr_head(cfg, repo, pr2, cache)
            _reset_to_base(repo, base_commit)

            res1 = git(["merge", "--no-edit", sha1], cwd=repo, check=False)
            if res1.returncode != 0:
                _reset_to_base(repo, base_commit)
                pair_results[(pr1, pr2)] = "XX"
                conflict_pairs.append((pr1, pr2))
                conflict_prs.update({pr1, pr2})
                continue

            res2 = git(["merge", "--no-commit", "--no-ff", sha2], cwd=repo, check=False)
            if res2.returncode == 0:
                pair_results[(pr1, pr2)] = "OK"
            else:
                pair_results[(pr1, pr2)] = "XX"
                conflict_pairs.append((pr1, pr2))
                conflict_prs.update({pr1, pr2})
            _reset_to_base(repo, base_commit)

    log("")
    log("Pairwise Compatibility Matrix:")
    log("Legend: OK = Compatible, XX = Conflict")
    log("")
    for line in render_compatibility_matrix(pr_list, pair_results):
        log(line)

    if conflict_pairs:
        log("")
        log("PRs Involved in Conflicts:")
        log("")
        rows = []
        for pr_num in sorted(conflict_prs, key=lambda s: int(s) if s.isdigit() else 0):
            meta = fetch_pr_metadata(cfg, pr_num)
            rows.append(
                {
                    "pr": f"#{pr_num}",
                    "author": meta["author"] or "N/A",
                    "title": meta["title"] or "N/A",
                    "url": f"https://github.com/{cfg.base_repository}/pull/{pr_num}",
                }
            )
        for line in render_conflict_pr_table(rows):
            log(line)
        log("")
        log("Conflict Pairs: " + " ".join(f"{a}+{b}" for a, b in conflict_pairs))
        log("")
        log("WARNING: PR pairs have conflicts; see matrix above.")
        log("Continuing; rerere + Claude will attempt to resolve during the merge step.")
        return
    log("")
    log("All PR pairs can merge cleanly together.")
