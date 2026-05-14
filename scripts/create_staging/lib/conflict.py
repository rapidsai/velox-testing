# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Claude Code-assisted merge conflict resolution.

Workflow per merge:
  1. ``git merge`` triggers a conflict.
  2. ``git rerere`` may auto-resolve previously seen conflicts.
  3. For any files still listed as unmerged we invoke the Claude CLI with the
     target repo as its working directory. Claude uses its built-in
     ``Read``/``Edit``/``Write``/``Bash``/``WebFetch``/``WebSearch`` tools to
     understand the PR (title, body, upstream file contents) and to write a
     resolved file back to disk. We then verify no markers remain and
     ``git add`` the file. rerere records the resolution automatically thanks
     to ``rerere.autoupdate=true``.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

from .config import Config
from .formatting import divider, log
from .git_ops import git, list_unmerged_files, run
from .validators import format_validation_errors_for_prompt, validate_unmerged_resolution

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_PROMPT_FILE = _PROMPTS_DIR / "resolve_conflict.txt"
_MARKER_RE = re.compile(r"^(<{7}|={7}|>{7}|\|{7}) ?", re.MULTILINE)
_PR_BODY_LIMIT = 1500
_RETRY_PREFIX = (
    "RETRY: a previous attempt left this file unresolved or unchanged. The "
    "`UNRESOLVED:` escape hatch is NOT available on this pass. You MUST write "
    "a marker-free file by applying the tiebreaker rule from the prompt below "
    "(prefer the currently-merging side when (a) and (b) directly contradict). "
    "All other hard constraints still apply: no hallucination, no invented "
    "APIs, every kept line must come verbatim from (a), (b), or surrounding "
    "context. Briefly note the chosen side in your one-line summary.\n\n"
)


@dataclass
class AutoResolveOutcome:
    """Result of ``auto_resolve_conflicts``.

    ``failure_kind`` is ``\"ok\"`` when ``unresolved_paths`` is empty; otherwise
    one of: ``auto_resolve_disabled``, ``claude_unavailable``, ``pr_timeout``,
    ``unresolved``.
    """

    unresolved_paths: List[str]
    failure_kind: str = "ok"

    @property
    def ok(self) -> bool:
        return not self.unresolved_paths


@dataclass
class MergeContext:
    """Metadata passed to Claude so it can reason about the merge being performed.

    `deadline` is a `time.monotonic()` value (or None for no cap). When set,
    `auto_resolve_conflicts` / `resolve_file` honour it as the per-PR
    wall-clock budget: once the deadline passes, remaining unresolved files
    are dropped and the PR is skipped for manual merging.
    """

    merge_label: str
    pr_meta: Dict[str, str] = field(default_factory=dict)
    merged_prs: List[Dict[str, str]] = field(default_factory=list)
    pending_prs: List[Dict[str, str]] = field(default_factory=list)
    deadline: Optional[float] = None


def _load_prompt_template() -> str:
    return _PROMPT_FILE.read_text(encoding="utf-8")


def _has_conflict_markers(text: str) -> bool:
    return bool(_MARKER_RE.search(text))


def claude_available(cfg: Config) -> bool:
    return shutil.which(cfg.claude_bin) is not None


def _truncate(text: str, limit: int) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "\n... [truncated]"


def _format_pr_list(prs: List[Dict[str, str]]) -> str:
    lines = []
    for meta in prs:
        num = meta.get("number") or ""
        title = (meta.get("title") or "").strip().replace("\n", " ")
        url = meta.get("url") or ""
        if num and title and url:
            lines.append(f'  - #{num} "{title}" ({url})')
        elif num and url:
            lines.append(f"  - #{num} ({url})")
        elif num:
            lines.append(f"  - #{num}")
    return "\n".join(lines)


def _build_pr_context_block(meta: Dict[str, str]) -> str:
    if not meta:
        return ""
    lines = []
    url = meta.get("url") or ""
    title = (meta.get("title") or "").strip()
    author = (meta.get("author") or "").strip()
    body = _truncate(meta.get("body") or "", _PR_BODY_LIMIT)
    if url:
        lines.append(f"  URL:    {url}")
    if title:
        lines.append(f"  Title:  {title}")
    if author:
        lines.append(f"  Author: @{author}")
    if body:
        lines.append("  Body (truncated):")
        for ln in body.splitlines():
            lines.append(f"    {ln}")
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def _build_stack_block(heading: str, prs: List[Dict[str, str]]) -> str:
    rendered = _format_pr_list(prs)
    if not rendered:
        return ""
    return f"\n{heading}\n{rendered}\n"


def _build_prompt(
    cfg: Config,
    file_path: str,
    ctx: MergeContext,
) -> str:
    template = _load_prompt_template()
    return template.format(
        target_repository=cfg.target_repository or "(local checkout)",
        target_branch=cfg.target_branch,
        base_repository=cfg.base_repository,
        base_branch=cfg.base_branch,
        merge_label=ctx.merge_label,
        pr_context=_build_pr_context_block(ctx.pr_meta),
        merged_block=_build_stack_block("PREVIOUSLY MERGED ONTO THE BASE (in order):", ctx.merged_prs),
        pending_block=_build_stack_block("STILL PENDING AFTER THIS ONE (for reference only):", ctx.pending_prs),
        file_path=file_path,
    )


def _ask_claude(
    cfg: Config,
    prompt: str,
    repo: Path,
    *,
    timeout: Optional[int] = None,
) -> str:
    """Run the Claude CLI in headless mode inside ``repo`` and return stdout.

    ``bypassPermissions`` lets Claude use any tool (Read/Edit/Write/Bash/WebFetch/
    WebSearch) without prompting; ``--add-dir`` explicitly grants it access to
    the repo tree in case the CLI considers ``cwd`` outside its default
    trusted roots. `timeout` overrides ``cfg.claude_timeout_s``; callers use
    this to clamp Claude to the remaining per-PR timeout window.
    """
    cmd = [
        cfg.claude_bin,
        "-p",
        prompt,
        "--model",
        cfg.claude_model,
        "--output-format",
        "text",
        "--permission-mode",
        "bypassPermissions",
        "--add-dir",
        str(repo),
    ]
    effective = timeout if timeout is not None else cfg.claude_timeout_s
    result = run(cmd, cwd=repo, check=True, timeout=effective)
    return result.stdout or ""


def _unmerged_stages(repo: Path, file_path: str) -> Set[int]:
    """Return the set of stages (1=base, 2=ours, 3=theirs) currently set for `file_path`.

    Empty set means the file is no longer unmerged.
    """
    res = git(["ls-files", "-u", "--", file_path], cwd=repo, check=False)
    stages: Set[int] = set()
    for line in (res.stdout or "").splitlines():
        parts = line.split()
        if len(parts) >= 4:
            try:
                stages.add(int(parts[2]))
            except ValueError:
                continue
    return stages


def _classify_unmerged(stages: Set[int]) -> str:
    """Map a stage set to a conflict kind.

    Stages: 1=common ancestor, 2=ours (HEAD), 3=theirs (incoming).
      {1,2,3}     -> 'content'         (modify/modify; markers may or may not exist)
      {1,3}       -> 'delete-by-us'    (DU: HEAD removed file, theirs modified)
      {1,2}       -> 'delete-by-them'  (UD: HEAD modified, theirs removed)
      {2,3}       -> 'add-by-both'     (AA: both added the same path)
      otherwise   -> 'unknown'
    """
    if stages == {1, 2, 3}:
        return "content"
    if stages == {1, 3}:
        return "delete-by-us"
    if stages == {1, 2}:
        return "delete-by-them"
    if stages == {2, 3}:
        return "add-by-both"
    return "unknown"


def _resolve_delete_conflict(repo: Path, file_path: str, kind: str) -> bool:
    """Apply the prompt's existing tiebreaker (prefer currently-merging side) to a
    file-level delete/modify conflict. Returns True on success.

    - delete-by-us  (DU): HEAD already deleted the file. The incoming PR modified
      it. The PR is the currently-merging side, so we restore its version.
    - delete-by-them (UD): HEAD still has the file but the PR removed it. The PR
      is the currently-merging side, so we accept the deletion.
    """
    if kind == "delete-by-us":
        log(
            f"  - {file_path}: deleted by HEAD, modified by incoming PR; "
            f"applying tiebreaker -> keeping the PR's version"
        )
        try:
            git(["checkout", "--theirs", "--", file_path], cwd=repo)
            git(["add", "--", file_path], cwd=repo)
            return True
        except subprocess.CalledProcessError as exc:
            log(f"  - {file_path}: failed to take theirs ({exc.stderr.strip()})")
            return False
    if kind == "delete-by-them":
        log(
            f"  - {file_path}: modified by HEAD, deleted by incoming PR; "
            f"applying tiebreaker -> accepting the PR's deletion"
        )
        try:
            git(["rm", "-f", "--", file_path], cwd=repo)
            return True
        except subprocess.CalledProcessError as exc:
            log(f"  - {file_path}: failed to remove ({exc.stderr.strip()})")
            return False
    return False


def _attempt_claude_pass(
    cfg: Config,
    repo: Path,
    file_path: str,
    abs_path: Path,
    prompt: str,
    label: str,
    *,
    timeout: Optional[int] = None,
) -> tuple[str, List[str]]:
    """Run a single Claude pass. Returns ``(outcome, validation_errors)``.

    Outcome is one of: ``ok``, ``markers``, ``noop``, ``timeout``, ``error``,
    ``validation_failed``. ``validation_errors`` is non-empty only for
    ``validation_failed`` and carries the human-readable problem descriptions
    (e.g. duplicate CMake targets) so the caller can inject them into the
    retry prompt. ``timeout`` clamps the Claude CLI invocation; defaults to
    ``cfg.claude_timeout_s``.
    """
    effective = timeout if timeout is not None else cfg.claude_timeout_s
    log(f"  - asking Claude to resolve {file_path} ({label}, model={cfg.claude_model}, timeout={effective}s)")
    before = abs_path.read_text(encoding="utf-8", errors="replace")
    try:
        reply = _ask_claude(cfg, prompt, repo, timeout=effective)
    except subprocess.TimeoutExpired:
        log(f"  - {file_path}: Claude timed out after {effective}s")
        return "timeout", []
    except Exception as exc:
        log(f"  - Claude failed for {file_path}: {exc}")
        return "error", []

    summary = (reply or "").strip().splitlines()[0] if reply.strip() else ""
    if summary:
        log(f"    Claude: {summary[:200]}")

    try:
        after = abs_path.read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeDecodeError) as exc:
        log(f"  - {file_path}: cannot re-read after Claude ({exc})")
        return "error", []

    if _has_conflict_markers(after):
        return "markers", []
    if after == before:
        return "noop", []

    validation_errors = validate_unmerged_resolution(repo, file_path, after)
    if validation_errors:
        log(f"  - {file_path}: post-resolution validation failed:")
        for err in validation_errors:
            log(f"      {err}")
        return "validation_failed", validation_errors
    return "ok", []


def _remaining_pr_timeout(context: MergeContext) -> Optional[int]:
    """Return whole seconds left until `context.deadline`, or None if uncapped.

    Negative or zero remaining is returned as 0 so callers can detect exhaustion.
    """
    if context.deadline is None:
        return None
    remaining = context.deadline - time.monotonic()
    return max(0, int(remaining))


def resolve_file(
    cfg: Config,
    repo: Path,
    file_path: str,
    *,
    context: MergeContext,
) -> bool:
    """Try to auto-resolve a single conflicted file. Returns True on success.

    Strategy:
      1. Classify the conflict via `git ls-files -u`.
      2. If it's a file-level delete/modify conflict (DU/UD), apply the prompt's
         existing tiebreaker rule deterministically (prefer the currently-merging
         side). These cannot be resolved by editing text — they're "keep file"
         vs. "drop file" decisions.
      3. If it's a content conflict, run a Claude pass; on `markers`/`noop`,
         retry once with the stricter prompt. A timeout is treated as terminal
         (re-running with the same model/timeout would just hit the wall again).

    The per-PR deadline on `context.deadline` clamps the Claude timeout per pass
    and short-circuits the function entirely once exhausted.
    """
    abs_path = repo / file_path
    stages = _unmerged_stages(repo, file_path)

    if not stages:
        return True

    kind = _classify_unmerged(stages)

    if kind == "delete-by-us" or kind == "delete-by-them":
        return _resolve_delete_conflict(repo, file_path, kind)

    if kind == "unknown":
        log(f"  - {file_path}: unsupported conflict shape (stages={sorted(stages)}); leaving for manual resolution")
        return False

    try:
        content = abs_path.read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeDecodeError) as exc:
        log(f"  - {file_path}: cannot read file ({exc}); leaving for manual resolution")
        return False

    if not _has_conflict_markers(content):
        log(f"  - {file_path}: unmerged but no markers (kind={kind}); leaving for manual resolution")
        return False

    remaining = _remaining_pr_timeout(context)
    if remaining is not None and remaining <= 0:
        log(f"  - {file_path}: PR timeout exhausted before Claude was invoked; leaving for manual resolution")
        return False

    base_prompt = _build_prompt(cfg, file_path, context)
    pass_timeout = cfg.claude_timeout_s
    if remaining is not None:
        pass_timeout = max(10, min(cfg.claude_timeout_s, remaining))

    outcome, validation_errors = _attempt_claude_pass(
        cfg,
        repo,
        file_path,
        abs_path,
        base_prompt,
        label="pass 1/2",
        timeout=pass_timeout,
    )

    if outcome == "timeout":
        log(f"  - {file_path}: skipping retry; same timeout will recur")
        return False

    if outcome != "ok":
        if outcome == "markers":
            log(f"  - {file_path}: conflict markers remain; retrying with stricter prompt")
        elif outcome == "noop":
            log(f"  - {file_path}: Claude made no changes; retrying with stricter prompt")
        elif outcome == "validation_failed":
            log(f"  - {file_path}: retrying with validator feedback injected into the prompt")
        elif outcome == "error":
            log(f"  - {file_path}: retrying after CLI error")

        remaining = _remaining_pr_timeout(context)
        if remaining is not None and remaining <= 0:
            log(f"  - {file_path}: PR timeout exhausted; skipping retry")
            return False
        retry_timeout = cfg.claude_timeout_s
        if remaining is not None:
            retry_timeout = max(10, min(cfg.claude_timeout_s, remaining))

        retry_prompt = _RETRY_PREFIX + base_prompt
        if outcome == "validation_failed" and validation_errors:
            retry_prompt = format_validation_errors_for_prompt(file_path, validation_errors) + retry_prompt

        outcome, validation_errors = _attempt_claude_pass(
            cfg,
            repo,
            file_path,
            abs_path,
            retry_prompt,
            label="pass 2/2",
            timeout=retry_timeout,
        )

    if outcome != "ok":
        if outcome == "markers":
            log(f"  - {file_path}: conflict markers still present after retry")
        elif outcome == "noop":
            log(f"  - {file_path}: Claude still made no changes after retry")
        elif outcome == "validation_failed":
            log(f"  - {file_path}: validator still rejects the resolved file after retry:")
            for err in validation_errors:
                log(f"      {err}")
        elif outcome == "timeout":
            log(f"  - {file_path}: retry also timed out")
        return False

    git(["add", "--", file_path], cwd=repo)
    return True


def auto_resolve_conflicts(
    cfg: Config,
    repo: Path,
    *,
    context: MergeContext,
) -> AutoResolveOutcome:
    """Auto-resolve every unresolved conflicted file.

    Honours ``cfg.pr_timeout_s`` as a wall-clock cap for the auto-resolve phase
    of this PR. Once the timeout is reached, the remaining files are emitted
    as failed without invoking Claude, so the caller skips the PR quickly.

    Returns an :class:`AutoResolveOutcome` with non-empty ``unresolved_paths``
    when anything could not be resolved.
    """
    unmerged = list_unmerged_files(repo)
    if not unmerged:
        return AutoResolveOutcome([], "ok")
    divider(f"Auto-resolving {len(unmerged)} conflicted file(s) for {context.merge_label}")
    if not cfg.enable_auto_resolve:
        log("Auto-resolve disabled (--no-auto-resolve)")
        return AutoResolveOutcome(list(unmerged), "auto_resolve_disabled")
    if not claude_available(cfg):
        log(f"WARN: Claude CLI '{cfg.claude_bin}' not on PATH; skipping auto-resolve")
        return AutoResolveOutcome(list(unmerged), "claude_unavailable")

    if cfg.pr_timeout_s > 0 and context.deadline is None:
        context.deadline = time.monotonic() + cfg.pr_timeout_s
        log(f"  PR wall-clock timeout: {cfg.pr_timeout_s}s (remaining files will be skipped once exceeded)")

    failed: List[str] = []
    pr_timeout_exhausted = False
    for file_path in unmerged:
        if pr_timeout_exhausted:
            failed.append(file_path)
            continue
        remaining = _remaining_pr_timeout(context)
        if remaining is not None and remaining <= 0:
            log(
                f"  PR timeout exhausted; skipping {len(unmerged) - len(failed)} "
                f"remaining file(s) for {context.merge_label}"
            )
            failed.append(file_path)
            pr_timeout_exhausted = True
            continue
        if not resolve_file(cfg, repo, file_path, context=context):
            failed.append(file_path)

    remaining = list_unmerged_files(repo)
    if remaining:
        log("Files still unresolved after Claude pass:")
        for f in remaining:
            log(f"  - {f}")
    out_paths = list(remaining) if remaining else list(failed)
    if not out_paths:
        return AutoResolveOutcome([], "ok")
    if pr_timeout_exhausted:
        return AutoResolveOutcome(out_paths, "pr_timeout")
    return AutoResolveOutcome(out_paths, "unresolved")
