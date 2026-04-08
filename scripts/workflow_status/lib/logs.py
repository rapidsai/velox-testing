#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Log extraction, GTest / error parsing, and per-job log filtering."""

from __future__ import annotations

import re
import sys

BLOCK_SEP = "__VELOX_FAILURE_BLOCK__"

# ---- Prefix stripping -----------------------------------------------------

_GH_LOG_PREFIX = re.compile(r"^[^\t]+\t[^\t]+\t")
_TIMESTAMP_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\s?")


def _strip_gh_log_prefix(line: str) -> str:
    parts = line.split("\t")
    if len(parts) >= 4:
        return "\t".join(parts[3:])
    if len(parts) >= 3:
        return _TIMESTAMP_PREFIX.sub("", parts[2])
    return line


def strip_log_prefixes(raw: str) -> str:
    return "\n".join(_strip_gh_log_prefix(line) for line in raw.splitlines())


# ---- GTest patterns -------------------------------------------------------

_GTEST_RUN = re.compile(r"^\[ RUN\s+\]")
_GTEST_FAILED = re.compile(r"^\[  FAILED  \]")
_GTEST_OK = re.compile(r"^\[\s+(OK|DISABLED|SKIPPED)\s+\]")
_GTEST_FAILED_NAME = re.compile(r"^\[  FAILED  \] ([A-Z][A-Za-z0-9_]*\.[A-Za-z][A-Za-z0-9_]*)")
_CTEST_SUMMARY = re.compile(r"^\d+%.*tests passed")
_CTEST_FAILED_LIST = re.compile(r"^The following tests FAILED:")

# ---- Error patterns -------------------------------------------------------

_ERROR_PATTERNS = [
    re.compile(r"[Ee]rror[: \[]"),
    re.compile(r"FAILED"),
    re.compile(r"[Ff]atal"),
    re.compile(r"ninja: build stopped"),
    re.compile(r"make.*\*\*\*"),
    re.compile(r"^failed to solve:"),
    re.compile(r"##\[error\]"),
    re.compile(r"^------$"),
    re.compile(r"^--------------------$"),
    re.compile(r"\.dockerfile:\d+"),
    re.compile(r"^\s*\d+ \|"),
    re.compile(r"^\s*>>>"),
    re.compile(r"undefined reference"),
    re.compile(r"cannot find -l"),
    re.compile(r"[Cc]onfiguring incomplete"),
]


# ---- Public API -----------------------------------------------------------


def extract_relevant_failures(raw_log: str) -> str:
    """Extract the most relevant failure lines from a raw GH Actions log."""
    content = strip_log_prefixes(raw_log)
    lines = content.splitlines()

    has_gtest = any(_GTEST_FAILED.match(ln) or _GTEST_RUN.match(ln) for ln in lines)
    if has_gtest:
        blocks = _extract_gtest_blocks(lines)
        if blocks:
            return blocks

    return _extract_error_lines(lines)


def _extract_gtest_blocks(lines: list[str]) -> str:
    result_parts: list[str] = []
    in_block = False
    block_lines: list[str] = []
    seen_tests: set[str] = set()
    in_ctest = False

    for line in lines:
        if _GTEST_RUN.match(line):
            in_block = True
            block_lines = [line]
            continue
        if in_block:
            block_lines.append(line)
            if _GTEST_FAILED.match(line):
                m = _GTEST_FAILED_NAME.match(line)
                if m:
                    seen_tests.add(m.group(1))
                tname = line.lstrip("[  FAILED  ] ").split(" (")[0].strip()
                if tname:
                    seen_tests.add(tname)
                result_parts.append("\n".join(block_lines))
                result_parts.append(BLOCK_SEP)
                in_block = False
                block_lines = []
                continue
            if _GTEST_OK.match(line):
                in_block = False
                block_lines = []
                continue
            continue

        if _GTEST_FAILED.match(line):
            m = _GTEST_FAILED_NAME.match(line)
            if not m:
                continue
            tname = m.group(1)
            if tname in seen_tests:
                continue
            result_parts.append(line)
            result_parts.append(BLOCK_SEP)
            continue

        if _CTEST_SUMMARY.match(line):
            result_parts.append(line)
            continue
        if _CTEST_FAILED_LIST.match(line):
            in_ctest = True
        if in_ctest:
            result_parts.append(line)
            continue

    return "\n".join(result_parts) if result_parts else ""


def _extract_error_lines(
    lines: list[str],
    context_before: int = 5,
    context_after: int = 3,
    tail: int = 15,
) -> str:
    n = len(lines)
    if n == 0:
        return ""
    marked: set[int] = set()
    for i, line in enumerate(lines):
        if any(p.search(line) for p in _ERROR_PATTERNS):
            for offset in range(max(0, i - context_before), min(n, i + context_after + 1)):
                marked.add(offset)
    for i in range(max(0, n - tail), n):
        marked.add(i)

    if not marked:
        return "\n".join(lines[-30:])

    result: list[str] = []
    prev = -2
    for i in sorted(marked):
        if prev >= 0 and i - prev > 1:
            result.append("...")
        result.append(lines[i])
        prev = i
    return "\n".join(result)


def filter_log_for_job(full_log: str, job_name: str) -> str:
    """Return only lines belonging to *job_name* from GH Actions log output."""
    lines = full_log.splitlines()

    escaped = re.escape(job_name)
    exact_pat = re.compile(rf"^{escaped}\t")
    selected: list[str] = []
    for line in lines:
        if exact_pat.match(line):
            selected.append(line)
            if "Post job cleanup." in line:
                break
    if selected:
        return "\n".join(selected)

    name_lower = job_name.lower()
    selected = []
    for line in lines:
        first_field = line.split("\t", 1)[0].lower()
        if name_lower in first_field:
            selected.append(line)
            if "Post job cleanup." in line:
                break
    if selected:
        return "\n".join(selected)

    print(
        f"WARN: could not match job '{job_name}' in log output, using full log ({len(lines)} lines)",
        file=sys.stderr,
    )
    return full_log


def split_into_blocks(content: str) -> list[str]:
    """Split *content* on ``BLOCK_SEP`` into individual failure blocks."""
    blocks: list[str] = []
    current: list[str] = []
    for line in content.splitlines():
        if line == BLOCK_SEP:
            text = "\n".join(current).strip()
            if text:
                blocks.append(text)
            current = []
        else:
            current.append(line)
    text = "\n".join(current).strip()
    if text:
        blocks.append(text)
    return blocks
