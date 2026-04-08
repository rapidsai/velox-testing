#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI analysis via Claude, prompt template loading, and token tracking."""

from __future__ import annotations

import re
import shutil
import subprocess
import threading
from pathlib import Path

from .config import Config

_PROMPT_DIR = Path(__file__).resolve().parent.parent / "prompts"


def _load_prompt_template() -> str:
    path = _PROMPT_DIR / "analyze_failure.txt"
    return path.read_text(encoding="utf-8")


class TokenTracker:
    """Thread-safe accumulator for AI token consumption."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.api_calls = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.skipped_by_dedup = 0

    def record(self, input_toks: int, output_toks: int) -> None:
        with self._lock:
            self.api_calls += 1
            self.input_tokens += input_toks
            self.output_tokens += output_toks

    def record_dedup_skip(self, count: int = 1) -> None:
        with self._lock:
            self.skipped_by_dedup += count

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        return max(1, len(text) // 4)

    def record_estimate(self, prompt: str, response: str) -> None:
        self.record(self._estimate_tokens(prompt), self._estimate_tokens(response))

    def summary(self, config: Config) -> str:
        if self.api_calls == 0 and self.skipped_by_dedup == 0:
            return ""
        lines = [
            "AI Token Usage:",
            f"  Model:            {config.claude_model}",
            f"  API calls:        {self.api_calls}",
            f"  Skipped (dedup):  {self.skipped_by_dedup}",
            f"  Input tokens:     ~{self.input_tokens:,}",
            f"  Output tokens:    ~{self.output_tokens:,}",
        ]
        return "\n".join(lines)


# ---- Internal dispatchers --------------------------------------------------


def _truncate_log(content: str, max_chars: int = 30000) -> str:
    if len(content) <= max_chars:
        return content
    return f"[...truncated...]\n{content[-max_chars:]}"


def _analyze_with_claude(
    log_content: str,
    job_name: str,
    workflow_name: str,
    prefetched_items: str,
    config: Config,
    tracker: TokenTracker,
) -> str:
    if not shutil.which(config.claude_bin):
        return (
            "STACKTRACE:Unable to extract - claude CLI not found\nEND_STACKTRACE\n"
            "CAUSE:Unable to analyze - claude CLI not installed or not in PATH\n"
            "FIX:Install Claude Code CLI or set CLAUDE_BIN to the correct path"
        )

    truncated = _truncate_log(log_content)
    template = _load_prompt_template()

    related_section = ""
    if prefetched_items:
        related_section = (
            "\n\nRELATED GITHUB ISSUES AND PRs (found by searching error "
            "identifiers in velox/presto repos):\n"
            f"{prefetched_items}\n\n"
            "When suggesting a FIX, reference any relevant issue or PR from "
            "above that may have introduced or fixed the issue. Include the "
            "number and URL."
        )

    prompt = template.format(
        job_name=job_name,
        workflow_name=workflow_name,
        log_content=truncated,
        related_items_section=related_section,
        fix_extra=(
            ". If any of the RELATED GITHUB ISSUES AND PRs above are "
            "relevant to the failure, mention them with their number and URL"
            if prefetched_items
            else ""
        ),
        fix_format_extra=(", include relevant PR links if applicable" if prefetched_items else ""),
    )

    try:
        proc = subprocess.run(
            [
                config.claude_bin,
                "--print",
                "--model",
                config.claude_model,
                "--no-session-persistence",
                "--allowedTools",
                "",
            ],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=120,
        )
        content = proc.stdout.strip()
        if content:
            tracker.record_estimate(prompt, content)
            return content
    except Exception:
        pass

    return (
        "STACKTRACE:Unable to extract - Claude CLI returned no output\nEND_STACKTRACE\n"
        "CAUSE:Unable to analyze - Claude CLI failed "
        "(check authentication with 'claude --print \"hello\"')\n"
        "FIX:Run 'claude' interactively once to authenticate, "
        "or check ANTHROPIC_API_KEY"
    )


# ---- Public API -----------------------------------------------------------


def analyze_block(
    block: str,
    job_name: str,
    wf_name: str,
    related_items: str,
    config: Config,
    tracker: TokenTracker,
) -> tuple[str, str, str]:
    """Run AI analysis on one failure block.  Returns ``(stacktrace, cause, fix)``."""
    resp = _analyze_with_claude(block, job_name, wf_name, related_items, config, tracker)

    stacktrace = ""
    cause = ""
    fix = ""

    st_match = re.search(
        r"STACKTRACE:\s*(.*?)\s*END_STACKTRACE",
        resp,
        re.DOTALL | re.IGNORECASE,
    )
    if st_match:
        st_lines = [ln for ln in st_match.group(1).strip().splitlines() if ln.strip()]
        stacktrace = "\n".join(st_lines[:5])

    for line in resp.splitlines():
        if line.upper().startswith("CAUSE:"):
            cause = re.sub(r"^CAUSE:\s*", "", line, flags=re.IGNORECASE)
        elif line.upper().startswith("FIX:"):
            fix = re.sub(r"^FIX:\s*", "", line, flags=re.IGNORECASE)

    return stacktrace, cause, fix
