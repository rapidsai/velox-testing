#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Log normalisation, error-token extraction, similarity scoring, and block grouping."""

from __future__ import annotations

import re

from .logs import _GTEST_FAILED_NAME

_DOCKER_STEP_PREFIX = re.compile(r"^#\d+\s+[\d.]+\s*")
_TIMESTAMP_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\s?")

ERROR_SIMILARITY_THRESHOLD = 0.45


def normalize_log_line(line: str) -> str:
    """Aggressively normalise a single log line for comparison.

    Strips every component that commonly varies across architectures, CUDA
    versions, or matrix entries so that the *structure* of the error is
    preserved but platform-specific details are erased.
    """
    s = line.strip()
    s = _DOCKER_STEP_PREFIX.sub("", s)
    s = _TIMESTAMP_PREFIX.sub("", s)
    s = re.sub(r":\d+:\d+:", ":", s)
    s = re.sub(r":\d+:", ":", s)
    s = re.sub(r"/[\w./_-]+/", "", s)
    s = re.sub(r"'[^']{12,}'", "'_'", s)
    s = re.sub(r'"[^"]{12,}"', '"_"', s)
    s = re.sub(r"`[^`]{12,}`", "`_`", s)
    s = re.sub(r"0x[0-9a-fA-F]+", "0xN", s)
    s = re.sub(r"\b\d{4,}\b", "N", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def compute_error_tokens(log_content: str) -> frozenset[str]:
    """Return a set of normalised lines from error log content."""
    if not log_content.strip():
        return frozenset()
    lines: set[str] = set()
    for raw in log_content.splitlines():
        norm = normalize_log_line(raw)
        if norm and len(norm) >= 5:
            lines.add(norm)
    return frozenset(lines)


def error_similarity(a: frozenset[str], b: frozenset[str]) -> float:
    """Overlap coefficient between two normalised-line sets."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def group_similar_blocks(blocks: list[str]) -> list[list[int]]:
    """Group log blocks by error similarity, returning lists of block indices.

    Each group shares a similar root cause.  Only the representative (first
    index in each group) needs AI analysis; the rest are listed as duplicates.
    """
    if not blocks:
        return []
    token_sets = [compute_error_tokens(b) for b in blocks]
    groups: list[list[int]] = []
    for i, tokens in enumerate(token_sets):
        placed = False
        for grp in groups:
            if error_similarity(tokens, token_sets[grp[0]]) >= ERROR_SIMILARITY_THRESHOLD:
                grp.append(i)
                placed = True
                break
        if not placed:
            groups.append([i])
    return groups


def extract_block_test_names(blocks: list[str], indices: list[int]) -> list[str]:
    """Extract GTest test names from the given block indices."""
    names: list[str] = []
    seen: set[str] = set()
    for idx in indices:
        if idx >= len(blocks):
            continue
        m = _GTEST_FAILED_NAME.search(blocks[idx])
        if m:
            name = m.group(1)
            if name not in seen:
                seen.add(name)
                names.append(name)
    return names
