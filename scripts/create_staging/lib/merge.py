# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""High-level merge operations: PR merges and the optional additional-repo merge.

Merge attempts use git-rerere first (so previously seen resolutions are reused)
and fall back to Claude Code-assisted resolution for anything still conflicted.
Claude is given the full stack context (current PR, previously merged PRs, and
still-pending PRs) so it can `WebFetch` upstream PR pages when conflict intent
is unclear.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Dict, List

from .config import Config, emit_output
from .conflict import MergeContext, auto_resolve_conflicts
from .formatting import log, step
from .git_ops import git, git_retry, list_unmerged_files
from .prs import fetch_pr_head, fetch_pr_metadata_batch


def _abort_and_reset(repo: Path, base_commit: str) -> None:
    git(["merge", "--abort"], cwd=repo, check=False)
    git(["reset", "--hard", base_commit], cwd=repo, check=False)
    git(["clean", "-fd"], cwd=repo, check=False)


def _abort_keeping_head(repo: Path) -> None:
    """Abort an in-progress merge but keep prior successful merges on HEAD.

    Used when we want to skip a single stuck PR and keep going, instead of
    rolling the whole staging branch back to the original base commit.
    """
    git(["merge", "--abort"], cwd=repo, check=False)
    git(["reset", "--hard", "HEAD"], cwd=repo, check=False)
    git(["clean", "-fd"], cwd=repo, check=False)


def _attempt_merge(
    cfg: Config,
    repo: Path,
    sha: str,
    message: str,
    *,
    context: MergeContext,
) -> bool:
    """Run `git merge`. If it conflicts, try rerere + Claude. Returns True on success."""
    res = git(["merge", sha, "--log", "-m", message], cwd=repo, check=False)
    if res.returncode == 0:
        return True

    if not list_unmerged_files(repo):
        try:
            git(["commit", "--no-edit"], cwd=repo)
            log(f"  rerere fully resolved conflicts for {context.merge_label}")
            return True
        except subprocess.CalledProcessError as exc:
            log(f"  rerere claimed full resolution but commit failed: {exc.stderr}")
            return False

    failed = auto_resolve_conflicts(cfg, repo, context=context)
    if failed:
        return False
    try:
        git(["commit", "--no-edit"], cwd=repo)
    except subprocess.CalledProcessError as exc:
        log(f"  failed to finalise merge for {context.merge_label}: {exc.stderr}")
        return False
    log(f"  Claude resolved conflicts for {context.merge_label}; rerere will reuse next time")
    return True


def merge_prs(cfg: Config, base_commit: str, pr_list: List[str]) -> Dict[str, List[str]]:
    """Merge each PR sequentially. Returns {'merged': [...], 'failed': [...]}.

    PRs that rerere + Claude cannot resolve are skipped (logged with their URL);
    the pipeline continues with the remaining PRs and prints a final summary.
    """
    repo = cfg.work_dir
    head_cache: Dict[str, str] = {}
    merged: List[str] = []
    failed: List[Dict[str, str]] = []

    step(f"Merging PRs: {' '.join(pr_list)}")
    meta_by_pr = fetch_pr_metadata_batch(cfg, pr_list)

    for idx, pr_num in enumerate(pr_list):
        sha = fetch_pr_head(cfg, repo, pr_num, head_cache)
        meta = meta_by_pr.get(pr_num, {"number": pr_num, "title": "", "url": ""})
        title = meta.get("title") or f"PR #{pr_num}"
        url = meta.get("url") or f"https://github.com/{cfg.base_repository}/pull/{pr_num}"
        msg = f"Merge PR #{pr_num}: {title}"

        context = MergeContext(
            merge_label=f"PR #{pr_num}",
            pr_meta=meta,
            merged_prs=[meta_by_pr[p] for p in merged if p in meta_by_pr],
            pending_prs=[meta_by_pr[p] for p in pr_list[idx + 1 :] if p in meta_by_pr],
        )

        ok = _attempt_merge(cfg, repo, sha, msg, context=context)
        if not ok:
            log(f"WARNING: PR #{pr_num} could not be auto-merged; skipping and continuing.")
            log(f"  {url}")
            _abort_keeping_head(repo)
            failed.append({"pr": pr_num, "title": title, "url": url})
            continue
        merged.append(pr_num)

    merged_str = " ".join(merged)
    failed_str = " ".join(f["pr"] for f in failed)
    emit_output("MERGED_PRS", merged_str)
    emit_output("MERGED_COUNT", str(len(merged)))
    emit_output("FAILED_PRS", failed_str)
    emit_output("FAILED_COUNT", str(len(failed)))

    log("")
    log(f"Merged {len(merged)} PR(s); skipped {len(failed)} PR(s) that need manual resolution.")
    if failed:
        log("")
        log("PRs requiring manual conflict resolution:")
        for entry in failed:
            log(f"  - PR #{entry['pr']}: {entry['title']}")
            log(f"      {entry['url']}")
        log("")
        log("These PRs were skipped; the staging branch contains only the merged ones above.")

    return {"merged": merged, "failed": [f["pr"] for f in failed]}


def merge_additional_repository(cfg: Config) -> str:
    """Merge an optional additional repo/branch (e.g. cuDF exchange) into the staging branch.

    Returns the commit SHA of the merged branch (or empty string if skipped).
    Updates BASE_COMMIT to the new HEAD so subsequent steps preserve this merge.
    """
    if not cfg.additional_repository or not cfg.additional_branch:
        log("No additional repository configured, skipping.")
        return ""

    repo = cfg.work_dir
    remote = "additional-merge-source"
    url = f"https://github.com/{cfg.additional_repository}.git"

    step(f"Merging additional repository: {cfg.additional_repository}/{cfg.additional_branch}")
    if git(["remote", "get-url", remote], cwd=repo, check=False).returncode != 0:
        git(["remote", "add", remote, url], cwd=repo)

    log(f"Fetching {cfg.additional_branch} from {cfg.additional_repository}...")
    try:
        git_retry(["fetch", remote, cfg.additional_branch], cwd=repo)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Failed to fetch {cfg.additional_branch} from {cfg.additional_repository}: {exc.stderr}"
        ) from exc

    additional_sha = git(["rev-parse", f"{remote}/{cfg.additional_branch}"], cwd=repo).stdout.strip()
    base_commit = git(["rev-parse", "HEAD"], cwd=repo).stdout.strip()

    label = f"{cfg.additional_repository}/{cfg.additional_branch}"
    context = MergeContext(
        merge_label=label,
        pr_meta={
            "number": "",
            "title": f"{cfg.additional_repository}@{cfg.additional_branch}",
            "author": "",
            "body": "",
            "url": f"https://github.com/{cfg.additional_repository}/tree/{cfg.additional_branch}",
            "head_sha": additional_sha,
        },
    )

    msg = f"Merge {label}"
    ok = _attempt_merge(cfg, repo, f"{remote}/{cfg.additional_branch}", msg, context=context)
    if not ok:
        log("Merge conflict with additional repository. Aborting.")
        _abort_and_reset(repo, base_commit)
        raise RuntimeError(f"Failed to merge {label}")

    emit_output("ADDITIONAL_MERGE_COMMIT", additional_sha)
    log(f"Successfully merged {label} ({additional_sha})")

    new_head = git(["rev-parse", "HEAD"], cwd=repo).stdout.strip()
    emit_output("BASE_COMMIT", new_head)
    log(f"Updated BASE_COMMIT to include additional merge: {new_head}")
    return additional_sha
