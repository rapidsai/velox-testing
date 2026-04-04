#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""OutputBuffer, emoji constants, job table builder, and failure detail renderer."""

from __future__ import annotations

import io
import json
import sys

from . import slack as slack_mod

# ---- Emoji constants -------------------------------------------------------

EMOJI_DASH = "\u2796"  # heavy minus sign
EMOJI_HOURGLASS = "\u23f3"  # ⏳ hourglass
EMOJI_CHECK = "\u2705"  # ✅ check mark
EMOJI_CROSS = "\u274c"  # ❌ cross mark


def status_emoji(conclusion: str, status: str = "completed") -> str:
    """Map a job conclusion / status to an emoji."""
    if status != "completed":
        return EMOJI_HOURGLASS
    c = conclusion.lower()
    if c == "success":
        return EMOJI_CHECK
    if c == "skipped":
        return EMOJI_DASH
    if c in ("failure", "cancelled", "timed_out"):
        return EMOJI_CROSS
    return EMOJI_DASH


# ---- OutputBuffer ----------------------------------------------------------


class OutputBuffer:
    """Collects mrkdwn output lines."""

    def __init__(self) -> None:
        self._lines: list[str] = []

    def print(self, *args, **kwargs) -> None:
        buf = io.StringIO()
        print(*args, file=buf, **kwargs)
        self._lines.append(buf.getvalue())

    def text(self) -> str:
        return "".join(self._lines)

    def flush_to_stdout(self) -> None:
        sys.stdout.write(self.text())
        sys.stdout.flush()

    def flush_to_file(self, path: str) -> None:
        """Write a Slack Block Kit JSON payload to *path*."""
        payload = slack_mod.build_payload(self.text())
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        block_count = len(payload.get("blocks", []))
        print(f"Wrote {path} ({block_count} Slack blocks)", file=sys.stderr)


# ---- Job sorting / grouping ------------------------------------------------

_VARIANT_ORDER = {"upstream": 0, "staging": 1, "pinned": 2}
_PHASE_ORDER = {
    "resolve-commits": 0,
    "velox-build": 1,
    "presto-build": 1,
    "velox-test": 2,
    "presto-test": 2,
    "velox-benchmark": 3,
    "presto-benchmark": 3,
}
_SUBPHASE_ORDER = {
    "resolve-commits": 0,
    "resolve-inputs": 0,
    "velox-deps": 1,
    "presto-deps": 1,
    "merge-manifests": 2,
    "velox-build": 3,
    "presto-build": 3,
    "presto-coordinator": 3,
    "test-velox-cpu": 4,
    "test-presto-smoke": 4,
    "test-velox-gpu": 5,
    "integration-test": 5,
    "benchmark-velox-gpu": 6,
}


def _parse_job_key(name: str) -> tuple[str, str, str]:
    """Extract (variant, phase, subphase) from a job name.

    Job names follow ``variant / phase / subphase (matrix...)``
    or ``variant / standalone-job``.
    """
    parts = [p.strip() for p in name.split(" / ")]
    variant = parts[0] if parts else ""
    phase = parts[1] if len(parts) > 1 else parts[0] if parts else ""
    subphase = parts[2].split("(")[0].strip() if len(parts) > 2 else phase
    return variant, phase, subphase


def _job_sort_key(job: dict) -> tuple[int, int, int, str]:
    name = job.get("name", "")
    variant, phase, subphase = _parse_job_key(name)
    return (
        _VARIANT_ORDER.get(variant, 99),
        _PHASE_ORDER.get(phase, 99),
        _SUBPHASE_ORDER.get(subphase, 99),
        name,
    )


# ---- Job status table ------------------------------------------------------


def build_job_table(jobs: list[dict]) -> list[str]:
    """Build a monospace table showing every job in a run.

    Jobs are sorted by variant → phase → subphase with divider rows
    between variant groups and between phase groups.  Column widths
    are computed dynamically so nothing is truncated.
    """
    sorted_jobs = sorted(jobs, key=_job_sort_key)

    name_w = max((len(j.get("name", "unknown")) for j in sorted_jobs), default=20)
    name_w = max(name_w, 3)  # minimum
    status_w = 12

    def hdr_sep() -> str:
        return f"|{'':->5}|{'':->{name_w + 2}}|{'':->{status_w + 2}}|"

    lines: list[str] = []
    lines.append(f"| {'NO':>3} | {'Job':<{name_w}} | {'Status':<{status_w}} |")
    lines.append(hdr_sep())

    prev_variant = ""
    prev_phase = ""

    for i, job in enumerate(sorted_jobs, 1):
        name = job.get("name", "unknown")
        conclusion = job.get("conclusion", "")
        status = job.get("status", "")
        emoji = status_emoji(conclusion, status)
        display_status = f"{emoji} {conclusion or status}"

        variant, phase, _ = _parse_job_key(name)

        if prev_variant and variant != prev_variant:
            lines.append(hdr_sep())
        elif prev_phase and phase != prev_phase and variant == prev_variant:
            lines.append(hdr_sep())

        prev_variant = variant
        prev_phase = phase

        lines.append(f"| {i:>3} | {name:<{name_w}} | {display_status:<{status_w}} |")

    return lines


# ---- Failure detail renderer -----------------------------------------------


def format_failure_detail(
    idx: int,
    job: dict,
    failed_steps: list[dict],
    stacktraces: list[tuple[str, str, str]],
    related_items: str,
    duplicates: list[dict] | None = None,
    run_url: str = "",
    analyze_cause: bool = True,
    analyze_fix: bool = True,
) -> str:
    """Render a single failure entry as mrkdwn text.

    Parameters
    ----------
    idx : display index (1-based)
    job : the representative job dict
    failed_steps : list of failed step dicts
    stacktraces : list of ``(stacktrace, cause, fix)`` tuples (one per block group)
    related_items : pre-formatted mrkdwn for related GH issues/PRs
    duplicates : jobs with the same error as *job* (shown as "Same error" links)
    run_url : workflow run URL (for building per-job deep links)
    analyze_cause : whether cause analysis was performed
    analyze_fix : whether fix suggestions were generated
    """
    out = OutputBuffer()
    job_name = job.get("name", "unknown")
    conclusion = job.get("conclusion", "unknown")

    out.print()
    out.print(f"*{idx}. {job_name}*")
    out.print(f"\u2022 *Conclusion:* {conclusion}")

    for s in failed_steps:
        out.print(f"  \u25aa\ufe0e Step: {s.get('name', '?')} ({s.get('conclusion', 'unknown')})")

    n_groups = len(stacktraces)
    for gidx, (st, cause, fix) in enumerate(stacktraces):
        suffix = f" {gidx + 1}/{n_groups}" if n_groups > 1 else ""
        display_st = st or "(no stacktrace available)"

        out.print(f"    *Stacktrace{suffix}:*")
        out.print("```")
        for line in display_st.splitlines()[:5]:
            if line.strip():
                out.print(line)
        out.print("```")

        if analyze_cause:
            out.print(f"    *Cause:* _{cause or 'Unable to determine cause'}_")
            if analyze_fix:
                out.print(f"    *Fix:* _{fix or 'Pending investigation'}_")

    if related_items:
        out.print()
        out.print("    *Related issues/PRs (last 7 days):*")
        out.print(related_items)

    if duplicates:
        out.print()
        out.print("  _Same error also appears in:_")
        for dup in duplicates:
            dup_name = dup.get("name", "unknown")
            dup_job_id = dup.get("databaseId", "")
            if dup_job_id and run_url:
                dup_url = f"{run_url}/job/{dup_job_id}"
                out.print(f"  \u2022 `{dup_name}` \u2192 {dup_url}")
            else:
                out.print(f"  \u2022 `{dup_name}`")

    return out.text()
