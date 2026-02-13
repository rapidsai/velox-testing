#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Helper for record_resolution.sh: compute per-hunk resolution keys
# and extract resolved hunks from a conflict report.
#
# Reconstructs the original conflict file from .base/.ours/.theirs
# using git merge-file --diff3, then compares against the user-edited
# .conflict file to extract resolved hunks. Each hunk is stored as a
# separate content file keyed by an order-independent hash.
#
# Usage:
#   python3 compute_hunk_resolution.py <ours> <base> <theirs> <conflict_resolved> <filepath> <output_dir>
#
# Writes each resolved hunk to <output_dir>/<hunk_key>.
# Prints one hunk_key per line to stdout.

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile


def parse_conflict_hunks(content: bytes) -> list[dict]:
    """Parse diff3-style conflict markers into structured hunks."""
    hunks: list[dict] = []
    lines = content.splitlines(True)
    byte_offset = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith(b"<<<<<<<"):
            hunk_start = byte_offset
            byte_offset += len(line)
            i += 1
            ours_parts: list[bytes] = []
            while i < len(lines):
                line = lines[i]
                if line.startswith(b"|||||||") or line.startswith(b"======="):
                    break
                ours_parts.append(line)
                byte_offset += len(line)
                i += 1
            base_parts: list[bytes] = []
            if i < len(lines) and lines[i].startswith(b"|||||||"):
                byte_offset += len(lines[i])
                i += 1
                while i < len(lines):
                    line = lines[i]
                    if line.startswith(b"======="):
                        break
                    base_parts.append(line)
                    byte_offset += len(line)
                    i += 1
            if i < len(lines) and lines[i].startswith(b"======="):
                byte_offset += len(lines[i])
                i += 1
            theirs_parts: list[bytes] = []
            while i < len(lines):
                line = lines[i]
                if line.startswith(b">>>>>>>"):
                    byte_offset += len(line)
                    i += 1
                    hunks.append({
                        "start": hunk_start,
                        "end": byte_offset,
                        "ours": b"".join(ours_parts),
                        "base": b"".join(base_parts),
                        "theirs": b"".join(theirs_parts),
                    })
                    break
                theirs_parts.append(line)
                byte_offset += len(line)
                i += 1
        else:
            byte_offset += len(line)
            i += 1
    return hunks


def compute_per_hunk_key(hunk: dict, filepath: str) -> str:
    """Compute order-independent key for a single conflict hunk."""
    ours_hash = hashlib.md5(hunk["ours"]).hexdigest()
    theirs_hash = hashlib.md5(hunk["theirs"]).hexdigest()
    base_hash = hashlib.md5(hunk["base"]).hexdigest()
    filepath_hash = hashlib.md5(filepath.encode()).hexdigest()
    sides = sorted([ours_hash, theirs_hash])
    return sides[0] + sides[1] + base_hash + filepath_hash


def extract_resolved_hunks(
    original: bytes, resolved: bytes, hunks: list[dict]
) -> list[bytes]:
    """Extract resolved text for each hunk by matching non-conflict anchors."""
    resolved_hunks: list[bytes] = []
    resolved_cursor = 0

    for idx, hunk in enumerate(hunks):
        prev_end = hunks[idx - 1]["end"] if idx > 0 else 0
        before_anchor = original[prev_end : hunk["start"]]

        if before_anchor:
            pos = resolved.find(before_anchor, resolved_cursor)
            if pos == -1:
                raise ValueError(f"Cannot find anchor before hunk {idx}")
            resolved_cursor = pos + len(before_anchor)

        next_start = (
            hunks[idx + 1]["start"] if idx + 1 < len(hunks) else len(original)
        )
        after_anchor = original[hunk["end"] : next_start]

        if after_anchor:
            after_pos = resolved.find(after_anchor, resolved_cursor)
            if after_pos == -1:
                raise ValueError(f"Cannot find anchor after hunk {idx}")
            resolved_hunks.append(resolved[resolved_cursor:after_pos])
            resolved_cursor = after_pos
        else:
            resolved_hunks.append(resolved[resolved_cursor:])

    return resolved_hunks


def reconstruct_conflict(ours_path: str, base_path: str, theirs_path: str) -> bytes:
    """Reconstruct the conflict file from base/ours/theirs using git merge-file --diff3."""
    with tempfile.NamedTemporaryFile(suffix=".ours", delete=False) as tmp:
        tmp_path = tmp.name
        with open(ours_path, "rb") as f:
            tmp.write(f.read())

    try:
        subprocess.run(
            ["git", "merge-file", "--diff3",
             "-L", "ours", "-L", "base", "-L", "theirs",
             tmp_path, base_path, theirs_path],
            check=False,
            capture_output=True,
        )
        with open(tmp_path, "rb") as f:
            return f.read()
    finally:
        os.unlink(tmp_path)


def main() -> int:
    if len(sys.argv) != 7:
        print(
            "Usage: compute_hunk_resolution.py <ours> <base> <theirs> <conflict_resolved> <filepath> <output_dir>",
            file=sys.stderr,
        )
        return 1

    ours_path = sys.argv[1]
    base_path = sys.argv[2]
    theirs_path = sys.argv[3]
    conflict_resolved_path = sys.argv[4]
    filepath = sys.argv[5]
    output_dir = sys.argv[6]

    original = reconstruct_conflict(ours_path, base_path, theirs_path)

    with open(conflict_resolved_path, "rb") as f:
        resolved = f.read()

    hunks = parse_conflict_hunks(original)
    if not hunks:
        print("ERROR: no conflict hunks found in reconstructed conflict", file=sys.stderr)
        return 1

    resolved_hunks = extract_resolved_hunks(original, resolved, hunks)

    os.makedirs(output_dir, exist_ok=True)

    for hunk, resolved_text in zip(hunks, resolved_hunks):
        key = compute_per_hunk_key(hunk, filepath)
        content_path = os.path.join(output_dir, key)
        with open(content_path, "wb") as f:
            f.write(resolved_text)
        print(key)

    return 0


if __name__ == "__main__":
    sys.exit(main())
