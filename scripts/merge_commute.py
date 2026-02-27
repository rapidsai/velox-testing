#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Proof-of-concept: merge B into A and auto-resolve conflicts
# where patches commute (R1 == R2).
#
# Usage:
#   python3 scripts/merge_commute.py <branch-a> <branch-b>
#
# Behavior:
# - Checks out branch-a.
# - Runs `git merge --no-commit --no-ff <branch-b>`.
# - For each conflicted file, computes:
#     R1 = apply(base->ours) then apply(base->theirs)
#     R2 = apply(base->theirs) then apply(base->ours)
#   If R1 == R2, writes the result and stages the file.
# - Fails if any conflicts remain.
#
# Notes:
# - "Safe" here means order-independent textual merge.
# - Leaves the merge in progress for the user to commit or abort.

from __future__ import annotations

import argparse
import difflib
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class Edit:
    side: str
    kind: str
    base_start: int
    base_end: int
    new_lines: tuple[bytes, ...]


def run_git(
    args: Iterable[str],
    *,
    cwd: Optional[str] = None,
    input_bytes: Optional[bytes] = None,
    check: bool = True,
    capture_output: bool = False,
    text: bool = False,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        input=input_bytes,
        check=check,
        capture_output=capture_output,
        text=text,
    )


def git_output_text(args: Iterable[str], cwd: str) -> str:
    return run_git(args, cwd=cwd, capture_output=True, text=True).stdout.strip()


def git_output_bytes(args: Iterable[str], cwd: str) -> bytes:
    return run_git(args, cwd=cwd, capture_output=True).stdout


def blob_exists(repo_root: str, commit: str, path: str) -> bool:
    result = run_git(["cat-file", "-e", f"{commit}:{path}"], cwd=repo_root, check=False)
    return result.returncode == 0


def read_blob(repo_root: str, commit: str, path: str) -> Optional[bytes]:
    if not blob_exists(repo_root, commit, path):
        return None
    return git_output_bytes(["show", f"{commit}:{path}"], cwd=repo_root)


def split_lines(content: Optional[bytes]) -> list[bytes]:
    if content is None:
        return []
    return content.splitlines(keepends=True)


def edits_from_diff(base_lines: list[bytes], other_lines: list[bytes], side: str) -> list[Edit]:
    matcher = difflib.SequenceMatcher(a=base_lines, b=other_lines, autojunk=False)
    edits: list[Edit] = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        if tag == "insert":
            kind = "insert"
        elif tag == "delete":
            kind = "delete"
        else:
            kind = "replace"
        edits.append(
            Edit(
                side=side,
                kind=kind,
                base_start=i1,
                base_end=i2,
                new_lines=tuple(other_lines[j1:j2]),
            )
        )
    return edits


def edits_equivalent(left: Edit, right: Edit) -> bool:
    if left.base_start != right.base_start or left.base_end != right.base_end:
        return False
    if left.new_lines != right.new_lines:
        return False
    if left.kind == right.kind:
        return True
    if left.kind in {"replace", "delete"} and right.kind in {"replace", "delete"}:
        return True
    return False


def safe_merge_by_edits(
    base_content: bytes,
    ours_content: bytes,
    theirs_content: bytes,
    *,
    allow_insert_union: bool,
) -> Optional[bytes]:
    base_lines = split_lines(base_content)
    ours_lines = split_lines(ours_content)
    theirs_lines = split_lines(theirs_content)

    edits_ours = edits_from_diff(base_lines, ours_lines, "ours")
    edits_theirs = edits_from_diff(base_lines, theirs_lines, "theirs")

    if not edits_ours and not edits_theirs:
        return base_content
    if not edits_ours:
        return theirs_content
    if not edits_theirs:
        return ours_content

    duplicates: set[int] = set()
    for idx, theirs_edit in enumerate(edits_theirs):
        for ours_edit in edits_ours:
            if edits_equivalent(ours_edit, theirs_edit):
                duplicates.add(idx)
                break

    def is_insert(edit: Edit) -> bool:
        return edit.kind == "insert" and edit.base_start == edit.base_end

    def ranges_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
        return not (a_end <= b_start or b_end <= a_start)

    def insert_overlaps_range(pos: int, start: int, end: int) -> bool:
        return start < pos < end

    for ours_edit in edits_ours:
        for idx, theirs_edit in enumerate(edits_theirs):
            if idx in duplicates:
                continue
            ours_insert = is_insert(ours_edit)
            theirs_insert = is_insert(theirs_edit)

            if ours_insert and theirs_insert:
                if ours_edit.base_start == theirs_edit.base_start:
                    if not allow_insert_union:
                        return None
                    continue
                continue

            if ours_insert:
                if insert_overlaps_range(ours_edit.base_start, theirs_edit.base_start, theirs_edit.base_end):
                    return None
                continue

            if theirs_insert:
                if insert_overlaps_range(theirs_edit.base_start, ours_edit.base_start, ours_edit.base_end):
                    return None
                continue

            if ranges_overlap(
                ours_edit.base_start,
                ours_edit.base_end,
                theirs_edit.base_start,
                theirs_edit.base_end,
            ):
                return None

    combined_edits = list(edits_ours)
    for idx, edit in enumerate(edits_theirs):
        if idx not in duplicates:
            combined_edits.append(edit)

    side_rank = {"ours": 0, "theirs": 1}
    kind_rank = {"insert": 0, "replace": 1, "delete": 1}

    combined_edits.sort(
        key=lambda edit: (
            edit.base_start,
            kind_rank.get(edit.kind, 1),
            side_rank.get(edit.side, 2),
        )
    )

    result_lines: list[bytes] = []
    cursor = 0
    for edit in combined_edits:
        if edit.base_start < cursor:
            return None
        result_lines.extend(base_lines[cursor : edit.base_start])
        if edit.kind == "insert":
            result_lines.extend(edit.new_lines)
        else:
            result_lines.extend(edit.new_lines)
            cursor = edit.base_end
    result_lines.extend(base_lines[cursor:])
    return b"".join(result_lines)


def commuting_merge_result(
    repo_root: str,
    base: str,
    ours: str,
    theirs: str,
    path: str,
    *,
    allow_insert_union: bool,
) -> tuple[bool, Optional[bytes]]:
    base_content = read_blob(repo_root, base, path)
    ours_content = read_blob(repo_root, ours, path)
    theirs_content = read_blob(repo_root, theirs, path)

    if base_content is None:
        if ours_content is None and theirs_content is None:
            return True, None
        if ours_content is None:
            return True, theirs_content
        if theirs_content is None:
            return True, ours_content
        if ours_content == theirs_content:
            return True, ours_content
        return False, None

    if ours_content is None and theirs_content is None:
        return True, None
    if ours_content is None or theirs_content is None:
        return False, None

    merged = safe_merge_by_edits(
        base_content,
        ours_content,
        theirs_content,
        allow_insert_union=allow_insert_union,
    )
    if merged is None:
        return False, None
    return True, merged


def write_result(repo_root: str, path: str, content: Optional[bytes]) -> None:
    abs_path = os.path.join(repo_root, path)
    if content is None:
        if os.path.exists(abs_path):
            os.remove(abs_path)
        return
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    with open(abs_path, "wb") as handle:
        handle.write(content)


def ensure_ready(repo_root: str, allow_dirty: bool, auto_continue: bool) -> None:
    if not allow_dirty:
        status = git_output_text(["status", "--porcelain"], cwd=repo_root)
        if status:
            raise RuntimeError("working tree is not clean")

    if auto_continue:
        staged = git_output_text(["diff", "--cached", "--name-only"], cwd=repo_root)
        if staged:
            raise RuntimeError("staged changes present; auto-continue would include them")

    in_merge = run_git(
        ["rev-parse", "-q", "--verify", "MERGE_HEAD"],
        cwd=repo_root,
        check=False,
    )
    if in_merge.returncode == 0:
        raise RuntimeError("a merge is already in progress")


def abort_merge(repo_root: str) -> bool:
    in_merge = run_git(
        ["rev-parse", "-q", "--verify", "MERGE_HEAD"],
        cwd=repo_root,
        check=False,
    )
    if in_merge.returncode != 0:
        return True
    result = run_git(["merge", "--abort"], cwd=repo_root, check=False)
    return result.returncode == 0


def continue_merge(repo_root: str) -> None:
    unmerged = git_output_text(["diff", "--name-only", "--diff-filter=U"], cwd=repo_root)
    if unmerged:
        raise RuntimeError("cannot continue; unresolved conflicts remain")

    in_merge = run_git(
        ["rev-parse", "-q", "--verify", "MERGE_HEAD"],
        cwd=repo_root,
        check=False,
    )
    if in_merge.returncode != 0:
        raise RuntimeError("no merge in progress")

    run_git(["commit", "--no-edit"], cwd=repo_root)


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge B into A with commuting-conflict auto-resolution.")
    parser.add_argument("branch_a", help="target branch to merge into")
    parser.add_argument("branch_b", help="source branch to merge from")
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="allow running with a dirty working tree",
    )
    parser.add_argument(
        "--keep-merge",
        action="store_true",
        help="keep merge state on failure for manual resolution",
    )
    parser.add_argument(
        "--auto-continue",
        action="store_true",
        help="run git merge --continue (commit) on success",
    )
    parser.add_argument(
        "--strict-commute",
        action="store_true",
        help="fail when same-position inserts would need ordering",
    )
    args = parser.parse_args()

    repo_root = git_output_text(["rev-parse", "--show-toplevel"], cwd=".")

    try:
        ensure_ready(repo_root, args.allow_dirty, args.auto_continue)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    for br in (args.branch_a, args.branch_b):
        run_git(
            ["rev-parse", "--verify", br],
            cwd=repo_root,
            capture_output=True,
        )

    base = git_output_text(["merge-base", args.branch_a, args.branch_b], cwd=repo_root)

    run_git(["checkout", args.branch_a], cwd=repo_root)
    merge = run_git(
        ["merge", "--no-commit", "--no-ff", args.branch_b],
        cwd=repo_root,
        check=False,
    )

    if merge.returncode == 0:
        if args.auto_continue:
            try:
                continue_merge(repo_root)
            except RuntimeError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 1
            print("merge clean: committed")
            return 0
        print("merge clean: no conflicts (left uncommitted)")
        return 0

    conflicts_raw = git_output_text(["diff", "--name-only", "--diff-filter=U"], cwd=repo_root)
    conflicts = [line for line in conflicts_raw.splitlines() if line]
    if not conflicts:
        print("merge failed without conflicted files", file=sys.stderr)
        return 1

    unresolved: list[str] = []
    try:
        for path in conflicts:
            safe, result = commuting_merge_result(
                repo_root,
                base,
                args.branch_a,
                args.branch_b,
                path,
                allow_insert_union=not args.strict_commute,
            )
            if safe:
                write_result(repo_root, path, result)
                run_git(["add", "-A", "--", path], cwd=repo_root)
            else:
                unresolved.append(path)
    except Exception as exc:
        if not args.keep_merge:
            aborted = abort_merge(repo_root)
            if not aborted:
                print("warning: failed to abort merge", file=sys.stderr)
        raise exc

    if unresolved:
        print("unresolved conflicts remain:", file=sys.stderr)
        for path in unresolved:
            print(path, file=sys.stderr)
        if not args.keep_merge:
            aborted = abort_merge(repo_root)
            if not aborted:
                print("warning: failed to abort merge", file=sys.stderr)
        return 1

    if args.auto_continue:
        try:
            continue_merge(repo_root)
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print("all conflicts resolved and committed")
        return 0

    print("all conflicts resolved by commuting-merge check")
    return 0


if __name__ == "__main__":
    sys.exit(main())
