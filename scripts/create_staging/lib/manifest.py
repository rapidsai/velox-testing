# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Render the .staging-manifest.yaml file from the repo template."""

from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
from typing import List, Optional

from .config import Config
from .formatting import log, step
from .git_ops import git
from .prs import fetch_pr_metadata

_DEFAULT_TEMPLATE = (
    Path(__file__).resolve().parent.parent.parent.parent / ".github" / "templates" / "staging-manifest.yaml.template"
)


def _yaml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _render_pr_entries(
    cfg: Config,
    pr_nums: List[str],
    *,
    extra_fields: Optional[List[tuple]] = None,
) -> List[str]:
    """Render a list of PR entries as YAML block-sequence lines.

    `extra_fields` is an optional list of (key, value) pairs to append to every
    entry (e.g. a `reason` field for skipped PRs).
    """
    lines: List[str] = []
    for pr_num in pr_nums:
        meta = fetch_pr_metadata(cfg, pr_num)
        commit = meta["head_sha"] or "unknown"
        lines.extend(
            [
                f"  - number: {pr_num}",
                f'    commit: "{commit}"',
                f'    author: "{_yaml_escape(meta["author"] or "N/A")}"',
                f'    title: "{_yaml_escape(meta["title"] or "N/A")}"',
                f'    url: "https://github.com/{cfg.base_repository}/pull/{pr_num}"',
            ]
        )
        for key, value in extra_fields or []:
            lines.append(f'    {key}: "{_yaml_escape(str(value))}"')
    return lines


def create_manifest(
    cfg: Config,
    base_commit: str,
    merged_prs: List[str],
    additional_commit: str = "",
    skipped_prs: Optional[List[str]] = None,
) -> Path:
    """Write the staging manifest, commit it, and return its path.

    `skipped_prs` are PRs that the merge step could not auto-resolve; they are
    recorded under a `skipped_prs:` section so reviewers can see what was left
    out and pick them up manually.
    """
    repo = cfg.work_dir
    template_path = cfg.manifest_template or _DEFAULT_TEMPLATE
    if not template_path.is_file():
        raise RuntimeError(f"template not found: {template_path}")

    skipped_prs = skipped_prs or []

    timestamp = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    dated_branch = os.environ.get("DATED_BRANCH", "") or "N/A"

    step("Create staging manifest")
    template = template_path.read_text(encoding="utf-8")

    if cfg.additional_repository and cfg.additional_branch:
        commit_ref = additional_commit or cfg.additional_branch
        additional_section = (
            f'  repository: "{cfg.additional_repository}"\n'
            f'  branch: "{cfg.additional_branch}"\n'
            f'  commit: "{additional_commit or "unknown"}"\n'
            f'  url: "https://github.com/{cfg.additional_repository}/tree/{commit_ref}"'
        )
    else:
        additional_section = "  null  # No additional repository merged"

    manifest = (
        template.replace("{{TIMESTAMP}}", timestamp)
        .replace("{{TARGET_REPO}}", cfg.target_repository)
        .replace("{{TARGET_BRANCH}}", cfg.target_branch)
        .replace("{{DATED_BRANCH}}", dated_branch)
        .replace("{{BASE_REPO}}", cfg.base_repository)
        .replace("{{BASE_BRANCH}}", cfg.base_branch)
        .replace("{{BASE_COMMIT}}", base_commit)
        .replace("{{ADDITIONAL_MERGE_SECTION}}", additional_section)
    )

    body_lines: List[str] = []
    if merged_prs:
        body_lines.extend(_render_pr_entries(cfg, merged_prs))
    else:
        body_lines.append("  []  # No PRs merged")

    body_lines.append("")
    body_lines.append("skipped_prs:  # PRs that required manual conflict resolution and were not merged")
    if skipped_prs:
        body_lines.extend(
            _render_pr_entries(
                cfg,
                skipped_prs,
                extra_fields=[("reason", "manual conflict resolution required")],
            )
        )
    else:
        body_lines.append("  []  # No PRs skipped")

    if not manifest.endswith("\n"):
        manifest += "\n"
    manifest += "\n".join(body_lines) + "\n"

    manifest_file = repo / ".staging-manifest.yaml"
    manifest_file.write_text(manifest, encoding="utf-8")

    git(["add", str(manifest_file)], cwd=repo)
    git(["commit", "-m", timestamp, "-m", f"Staging branch manifest for {dated_branch or cfg.target_branch}"], cwd=repo)
    log(f"Manifest committed ({len(merged_prs)} merged, {len(skipped_prs)} skipped).")
    return manifest_file
