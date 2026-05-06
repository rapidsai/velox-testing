# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Configuration dataclass and arg/env loading for create_staging."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class Config:
    target_path: Path
    base_repository: str
    base_branch: str
    target_branch: str = "staging"
    target_repository: str = ""
    work_dir: Path = field(default_factory=Path)
    auto_fetch_prs: bool = True
    manual_pr_numbers: List[str] = field(default_factory=list)
    exclude_pr_numbers: List[str] = field(default_factory=list)
    pr_labels: List[str] = field(default_factory=list)
    manifest_template: Optional[Path] = None
    force_push: bool = False
    additional_repository: str = ""
    additional_branch: str = ""
    mode: str = "local"
    step: str = ""

    # PR ordering when auto-fetching by labels.
    # "oldest"   -> ascending by createdAt (recommended for stacking; foundations
    #               and refactors land before features that build on them).
    # "newest"   -> descending by createdAt (`gh pr list` default).
    # "as-given" -> preserve `gh pr list` order verbatim. --manual-pr-numbers is
    #               always treated as as-given regardless of this setting.
    pr_order: str = "oldest"

    # Claude/rerere conflict resolution
    enable_auto_resolve: bool = True
    claude_bin: str = "claude"
    claude_model: str = "claude-sonnet-4-5"
    claude_timeout_s: int = 300
    # Wall-clock timeout per PR for the auto-resolve phase (Claude + tiebreakers).
    # Once exceeded, remaining unresolved files are dropped and the PR is
    # skipped for manual merging. 0 disables the cap.
    pr_timeout_s: int = 900

    @property
    def use_local_path(self) -> bool:
        return True

    @property
    def is_ci(self) -> bool:
        return self.mode == "ci"


def _split_csv(value: str) -> List[str]:
    if not value:
        return []
    return [p.strip() for p in value.replace(" ", ",").split(",") if p.strip()]


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _detect_mode() -> str:
    """Default execution mode based on environment.

    GitHub Actions always sets ``GITHUB_ACTIONS=true``. Anywhere else we
    assume the user is running locally against a checkout next to
    velox-testing.
    """
    if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        return "ci"
    return "local"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--target-path", required=True, type=Path, help="Local path to the target repository (already cloned)"
    )
    parser.add_argument("--base-repository", required=True, help="Base repository, e.g. facebookincubator/velox")
    parser.add_argument("--base-branch", required=True, help="Base branch in the base repository")
    parser.add_argument(
        "--target-branch", default="staging", help="Branch to create/update in the target repo (default: staging)"
    )
    parser.add_argument(
        "--auto-fetch-prs", default="true", help="Auto-fetch open non-draft PRs by --pr-labels (true|false)"
    )
    parser.add_argument(
        "--manual-pr-numbers", default="", help="Comma-separated PR numbers (disables auto-fetch when set)"
    )
    parser.add_argument(
        "--exclude-pr-numbers", default="", help="Comma-separated PR numbers to exclude from auto-fetch"
    )
    parser.add_argument("--pr-labels", default="", help="Comma-separated PR labels to auto-fetch")
    parser.add_argument("--manifest-template", default=None, type=Path, help="Path to staging manifest template")
    parser.add_argument("--force-push", default="false", help="Force-push the target/dated branches (true|false)")
    parser.add_argument(
        "--additional-repository", default="", help="Optional additional repo to merge from (owner/repo)"
    )
    parser.add_argument("--additional-branch", default="", help="Branch from --additional-repository to merge")
    parser.add_argument(
        "--mode",
        default=_detect_mode(),
        choices=("local", "ci"),
        help="Execution mode (default: auto-detected from $GITHUB_ACTIONS; "
        "'ci' inside GitHub Actions, 'local' otherwise)",
    )
    parser.add_argument(
        "--step",
        default="",
        help="Run only one step: reset, merge-additional, fetch-prs, "
        "test-merge, test-pairwise, merge, manifest, push, all",
    )
    parser.add_argument(
        "--pr-order",
        default="oldest",
        choices=("oldest", "newest", "as-given"),
        help="Order to merge auto-fetched PRs (default: oldest first; foundations "
        "land before features). --manual-pr-numbers is always merged in the "
        "exact order given.",
    )
    parser.add_argument(
        "--no-auto-resolve", action="store_true", help="Disable Claude Code-assisted automatic conflict resolution"
    )
    parser.add_argument(
        "--claude-bin",
        default=os.environ.get("CLAUDE_BIN", "claude"),
        help="Path to the claude CLI (default: claude on PATH)",
    )
    parser.add_argument(
        "--claude-model",
        default=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5"),
        help="Claude model to use for conflict resolution",
    )
    parser.add_argument(
        "--claude-timeout",
        type=int,
        default=int(os.environ.get("CLAUDE_TIMEOUT", "300")),
        help="Per-file Claude CLI timeout in seconds (default: 300 = 5 min; env: CLAUDE_TIMEOUT)",
    )
    parser.add_argument(
        "--pr-timeout",
        type=int,
        default=int(os.environ.get("PR_TIMEOUT", "900")),
        help="Wall-clock timeout per PR for auto-resolving conflicts in seconds "
        "(default: 900 = 15 min; env: PR_TIMEOUT). When exceeded, remaining "
        "unresolved files are dropped and the PR is skipped for manual "
        "merging. Use 0 to disable.",
    )


def load_from_args(args: argparse.Namespace) -> Config:
    cfg = Config(
        target_path=args.target_path.expanduser().resolve(),
        base_repository=args.base_repository,
        base_branch=args.base_branch,
        target_branch=args.target_branch,
        auto_fetch_prs=_parse_bool(args.auto_fetch_prs),
        manual_pr_numbers=_split_csv(args.manual_pr_numbers),
        exclude_pr_numbers=_split_csv(args.exclude_pr_numbers),
        pr_labels=_split_csv(args.pr_labels),
        manifest_template=args.manifest_template,
        force_push=_parse_bool(args.force_push),
        additional_repository=args.additional_repository,
        additional_branch=args.additional_branch,
        mode=args.mode,
        step=args.step,
        pr_order=args.pr_order,
        enable_auto_resolve=not args.no_auto_resolve,
        claude_bin=args.claude_bin,
        claude_model=args.claude_model,
        claude_timeout_s=args.claude_timeout,
        pr_timeout_s=max(0, args.pr_timeout),
    )
    if cfg.manual_pr_numbers:
        cfg.auto_fetch_prs = False
    return cfg


def _format_assignment(name: str, value: str) -> str:
    """Return a GitHub Actions env/output assignment string.

    Single-line values use ``NAME=value``. Multi-line values use heredoc
    syntax (``NAME<<DELIM\\n...\\nDELIM``), which is what GitHub Actions
    requires for ``GITHUB_ENV`` / ``GITHUB_OUTPUT``.
    """
    if "\n" in value:
        delim = f"EOF_{name}_{os.getpid()}"
        return f"{name}<<{delim}\n{value}\n{delim}\n"
    return f"{name}={value}\n"


def _append_to_env_file(path: str, payload: str) -> None:
    """Append ``payload`` to ``path`` and fsync so writes survive early exits.

    Equivalent to ``printf '%s' "$payload" >> "$path"`` in shell.
    """
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        try:
            os.fsync(fh.fileno())
        except OSError:
            pass


def emit_output(name: str, value: str) -> None:
    """Append ``NAME=value`` to both ``$GITHUB_OUTPUT`` and ``$GITHUB_ENV``.

    Mirrors the shell pattern::

        echo "${name}=${value}" >> "${GITHUB_OUTPUT}"
        echo "${name}=${value}" >> "${GITHUB_ENV}"

    but also handles multi-line values via heredoc syntax.
    """
    payload = _format_assignment(name, value)

    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        _append_to_env_file(output_path, payload)

    env_path = os.environ.get("GITHUB_ENV")
    if env_path:
        _append_to_env_file(env_path, payload)


def require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"Missing {name}. Run the appropriate prior step to set it.")
    return val
