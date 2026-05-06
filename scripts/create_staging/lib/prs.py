# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""PR list resolution and PR HEAD fetching."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from .config import Config, emit_output
from .formatting import log, step
from .git_ops import gh, git_retry


def _order_auto_fetched(items: List[dict], pr_order: str) -> List[dict]:
    """Sort PRs returned by `gh pr list` according to cfg.pr_order.

    Sorts by `createdAt` (ISO-8601 strings sort lexicographically). Falls back
    to PR number when `createdAt` is missing.
    """
    if pr_order == "as-given":
        return items
    reverse = pr_order == "newest"

    def _key(item: dict):
        created = item.get("createdAt") or ""
        try:
            number = int(item.get("number") or 0)
        except (TypeError, ValueError):
            number = 0
        return (created, number)

    return sorted(items, key=_key, reverse=reverse)


def fetch_pr_list(cfg: Config) -> List[str]:
    """Resolve the final list of PR numbers (auto-fetch by labels, then exclude)."""
    if cfg.auto_fetch_prs:
        step(f"Auto-fetch PRs with labels: {','.join(cfg.pr_labels)}")
        if not cfg.pr_labels:
            raise RuntimeError("--pr-labels is required when auto-fetch is enabled.")
        args = [
            "pr",
            "list",
            "--repo",
            cfg.base_repository,
            "--state",
            "open",
            "--json",
            "number,isDraft,createdAt",
            "--limit",
            "200",
        ]
        for label in cfg.pr_labels:
            args.extend(["--label", label])
        result = gh(args, check=True)
        try:
            data = json.loads(result.stdout or "[]")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse gh pr list output: {exc}") from exc
        active = [item for item in data if not item.get("isDraft", False)]
        ordered = _order_auto_fetched(active, cfg.pr_order)
        log(f"Merge order: {cfg.pr_order} (by createdAt)")
        pr_list = [str(item["number"]) for item in ordered]
    else:
        pr_list = list(cfg.manual_pr_numbers)
        log("Merge order: as-given (from --manual-pr-numbers)")

    if cfg.exclude_pr_numbers:
        excluded = set(cfg.exclude_pr_numbers)
        kept: List[str] = []
        for pr in pr_list:
            if pr in excluded:
                log(f"Excluding PR #{pr}")
            else:
                kept.append(pr)
        pr_list = kept

    if not pr_list:
        raise RuntimeError("No PRs found to merge.")

    pr_str = " ".join(pr_list)
    emit_output("PR_LIST", pr_str)
    emit_output("PR_COUNT", str(len(pr_list)))
    log(f"PRs to process: {pr_str}")
    return pr_list


def fetch_pr_head(cfg: Config, repo: Path, pr_num: str, cache: Dict[str, str]) -> str:
    """Fetch PR HEAD into FETCH_HEAD and return its SHA. Cached per-PR."""
    if pr_num in cache:
        return cache[pr_num]
    git_retry(["fetch", f"https://github.com/{cfg.base_repository}.git", f"pull/{pr_num}/head"], cwd=repo)
    from .git_ops import git

    sha = git(["rev-parse", "FETCH_HEAD"], cwd=repo).stdout.strip()
    if not sha:
        raise RuntimeError(f"Failed to resolve PR #{pr_num} HEAD SHA")
    cache[pr_num] = sha
    return sha


def _pr_url(base_repository: str, pr_num: str) -> str:
    return f"https://github.com/{base_repository}/pull/{pr_num}"


def fetch_pr_metadata(cfg: Config, pr_num: str) -> Dict[str, str]:
    """Best-effort fetch of PR title, author, body, url, headRefOid; never raises."""
    meta: Dict[str, str] = {
        "number": str(pr_num),
        "title": "",
        "author": "",
        "body": "",
        "url": _pr_url(cfg.base_repository, pr_num),
        "head_sha": "",
    }
    try:
        result = gh(
            ["pr", "view", pr_num, "--repo", cfg.base_repository, "--json", "title,author,body,url,headRefOid"],
            check=True,
        )
        data = json.loads(result.stdout or "{}")
        meta["title"] = data.get("title", "") or ""
        meta["author"] = (data.get("author") or {}).get("login", "") or ""
        meta["body"] = data.get("body", "") or ""
        meta["url"] = data.get("url", "") or meta["url"]
        meta["head_sha"] = data.get("headRefOid", "") or ""
    except Exception as exc:
        log(f"WARN: failed to fetch metadata for PR #{pr_num}: {exc}")
    return meta


def fetch_pr_metadata_batch(cfg: Config, pr_nums: List[str]) -> Dict[str, Dict[str, str]]:
    """Fetch metadata for many PRs (sequential, best-effort, cached by caller if needed)."""
    out: Dict[str, Dict[str, str]] = {}
    for pr in pr_nums:
        out[pr] = fetch_pr_metadata(cfg, pr)
    return out
