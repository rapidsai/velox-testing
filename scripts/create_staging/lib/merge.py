# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""High-level merge operations: PR merges and the optional additional-repo merge.

Merge attempts use git rerere first (so previously seen resolutions are reused)
and fall back to Claude Code-assisted resolution for anything still conflicted.
Claude is given the full stack context (current PR, previously merged PRs, and
still-pending PRs) so it can `WebFetch` upstream PR pages when conflict intent
is unclear.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from .config import Config, emit_output
from .conflict import AutoResolveOutcome, MergeContext, auto_resolve_conflicts
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


def _truncate_stderr(text: str, limit: int = 240) -> str:
    t = (text or "").strip().replace("\n", " ")
    if len(t) <= limit:
        return t
    return t[: limit - 3] + "..."


def _auto_resolve_human_reason(cfg: Config, outcome: AutoResolveOutcome) -> str:
    n = len(outcome.unresolved_paths)
    if outcome.failure_kind == "pr_timeout":
        return (
            f"PR_TIMEOUT ({cfg.pr_timeout_s}s): conflict auto-resolve stopped with "
            f"{n} file(s) still unmerged (examples: {_path_preview(outcome.unresolved_paths)})."
        )
    if outcome.failure_kind == "unresolved":
        return f"Conflicts remain after rerere/Claude ({n} file(s): {_path_preview(outcome.unresolved_paths)})."
    if outcome.failure_kind == "auto_resolve_disabled":
        return "Conflict auto-resolve disabled (--no-auto-resolve); unmerged files left as-is."
    if outcome.failure_kind == "claude_unavailable":
        return f"Claude CLI ({cfg.claude_bin!r}) not on PATH; could not auto-resolve {n} conflicted file(s)."
    return f"Auto-resolve failed ({outcome.failure_kind!r}); {n} file(s) still unmerged."


def _path_preview(paths: List[str], max_files: int = 4, max_chars: int = 160) -> str:
    if not paths:
        return "(none)"
    parts = paths[:max_files]
    s = ", ".join(parts)
    if len(paths) > max_files:
        s += f", …(+{len(paths) - max_files} more)"
    if len(s) > max_chars:
        return s[: max_chars - 3] + "..."
    return s


@dataclass
class MergeAttemptResult:
    """Outcome of ``_attempt_merge``."""

    ok: bool
    skip_reason: str = ""


def _attempt_merge(
    cfg: Config,
    repo: Path,
    sha: str,
    message: str,
    *,
    context: MergeContext,
) -> MergeAttemptResult:
    """Run ``git merge``. If it conflicts, try rerere + Claude."""
    res = git(["merge", sha, "--log", "-m", message], cwd=repo, check=False)
    if res.returncode == 0:
        return MergeAttemptResult(True)

    if not list_unmerged_files(repo):
        try:
            git(["commit", "--no-edit"], cwd=repo)
            log(f"  rerere fully resolved conflicts for {context.merge_label}")
            return MergeAttemptResult(True)
        except subprocess.CalledProcessError as exc:
            log(f"  rerere claimed full resolution but commit failed: {exc.stderr}")
            detail = _truncate_stderr(exc.stderr or "")
            return MergeAttemptResult(
                False,
                f"git commit failed after rerere auto-resolution for {context.merge_label}: {detail}",
            )

    outcome = auto_resolve_conflicts(cfg, repo, context=context)
    if not outcome.ok:
        return MergeAttemptResult(False, _auto_resolve_human_reason(cfg, outcome))
    try:
        git(["commit", "--no-edit"], cwd=repo)
    except subprocess.CalledProcessError as exc:
        log(f"  failed to finalise merge for {context.merge_label}: {exc.stderr}")
        detail = _truncate_stderr(exc.stderr or "")
        return MergeAttemptResult(
            False,
            f"git commit --no-edit failed after conflict resolution for {context.merge_label}: {detail}",
        )
    log(f"  Claude resolved conflicts for {context.merge_label}; rerere will reuse next time")
    return MergeAttemptResult(True)


def merge_prs(cfg: Config, base_commit: str, pr_list: List[str]) -> Dict[str, object]:
    """Merge each PR sequentially.

    Returns a dict with:
      ``merged`` — PR numbers that landed successfully,
      ``failed`` — PR numbers that were skipped (same shape as before),
      ``failed_details`` — list of dicts with ``pr``, ``title``, ``url``, ``reason``.
    """
    repo = cfg.work_dir
    head_cache: Dict[str, str] = {}
    merged: List[str] = []
    failed: List[Dict[str, str]] = []

    step(f"Merging PRs: {' '.join(pr_list)}")
    meta_by_pr = fetch_pr_metadata_batch(cfg, pr_list)
    total_prs = len(pr_list)

    for idx, pr_num in enumerate(pr_list):
        meta = meta_by_pr.get(pr_num, {"number": pr_num, "title": "", "url": ""})
        title = meta.get("title") or f"PR #{pr_num}"
        url = meta.get("url") or f"https://github.com/{cfg.base_repository}/pull/{pr_num}"
        cur = idx + 1
        log("")
        log(f"== Merging PR #{pr_num} ({cur}/{total_prs}): {title}")
        log(f"   {url}")

        sha = fetch_pr_head(cfg, repo, pr_num, head_cache)
        msg = f"Merge PR #{pr_num}: {title}"

        context = MergeContext(
            merge_label=f"PR #{pr_num}",
            pr_meta=meta,
            merged_prs=[meta_by_pr[p] for p in merged if p in meta_by_pr],
            pending_prs=[meta_by_pr[p] for p in pr_list[idx + 1 :] if p in meta_by_pr],
        )

        attempt = _attempt_merge(cfg, repo, sha, msg, context=context)
        if not attempt.ok:
            log(f"WARNING: PR #{pr_num} ({cur}/{total_prs}) could not be auto-merged; skipping and continuing.")
            log(f"  Reason: {attempt.skip_reason}")
            log(f"  {url}")
            _abort_keeping_head(repo)
            failed.append(
                {
                    "pr": pr_num,
                    "title": title,
                    "url": url,
                    "reason": attempt.skip_reason,
                }
            )
            continue
        merged.append(pr_num)

    merged_str = " ".join(merged)
    failed_str = " ".join(f["pr"] for f in failed)
    emit_output("MERGED_PRS", merged_str)
    emit_output("MERGED_COUNT", str(len(merged)))
    emit_output("FAILED_PRS", failed_str)
    emit_output("FAILED_COUNT", str(len(failed)))
    emit_output("FAILED_SKIP_JSON", json.dumps([{"pr": e["pr"], "reason": e["reason"]} for e in failed]))

    log("")
    log(f"Merged {len(merged)} PR(s); skipped {len(failed)} PR(s) that need manual resolution.")
    if failed:
        log("")
        log("PRs requiring manual conflict resolution:")
        for entry in failed:
            log(f"  - PR #{entry['pr']}: {entry['title']}")
            log(f"      {entry['url']}")
            log(f"      reason: {entry['reason']}")
        log("")
        log("These PRs were skipped; the staging branch contains only the merged ones above.")

    return {
        "merged": merged,
        "failed": [f["pr"] for f in failed],
        "failed_details": failed,
    }


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
    attempt = _attempt_merge(cfg, repo, f"{remote}/{cfg.additional_branch}", msg, context=context)
    if not attempt.ok:
        log("Merge conflict with additional repository. Aborting.")
        log(f"  Reason: {attempt.skip_reason}")
        _abort_and_reset(repo, base_commit)
        raise RuntimeError(f"Failed to merge {label}: {attempt.skip_reason}")

    emit_output("ADDITIONAL_MERGE_COMMIT", additional_sha)
    log(f"Successfully merged {label} ({additional_sha})")

    new_head = git(["rev-parse", "HEAD"], cwd=repo).stdout.strip()
    emit_output("BASE_COMMIT", new_head)
    log(f"Updated BASE_COMMIT to include additional merge: {new_head}")
    return additional_sha
