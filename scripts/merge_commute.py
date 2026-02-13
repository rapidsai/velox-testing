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
import hashlib
import os
import subprocess
import sys
import tempfile
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
    result = run_git(
        ["cat-file", "-e", f"{commit}:{path}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    return result.returncode == 0


def read_blob(repo_root: str, commit: str, path: str) -> Optional[bytes]:
    if not blob_exists(repo_root, commit, path):
        return None
    return git_output_bytes(["show", f"{commit}:{path}"], cwd=repo_root)


def split_lines(content: Optional[bytes]) -> list[bytes]:
    if content is None:
        return []
    return content.splitlines(keepends=True)


def edits_from_diff(
    base_lines: list[bytes], other_lines: list[bytes], side: str
) -> list[Edit]:
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
                if insert_overlaps_range(
                    ours_edit.base_start, theirs_edit.base_start, theirs_edit.base_end
                ):
                    return None
                continue

            if theirs_insert:
                if insert_overlaps_range(
                    theirs_edit.base_start, ours_edit.base_start, ours_edit.base_end
                ):
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


@dataclass(frozen=True)
class ConflictHunk:
    start: int  # byte offset of <<<<<<< line
    end: int    # byte offset after >>>>>>> line
    ours: bytes
    base: bytes
    theirs: bytes


def parse_conflict_hunks(content: bytes) -> list[ConflictHunk]:
    """Parse diff3-style conflict markers into structured hunks."""
    hunks: list[ConflictHunk] = []
    lines = content.splitlines(keepends=True)
    byte_offset = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith(b"<<<<<<<"):
            hunk_start = byte_offset
            byte_offset += len(line)
            i += 1
            # Collect ours section (until ||||||| or =======)
            ours_parts: list[bytes] = []
            while i < len(lines):
                line = lines[i]
                if line.startswith(b"|||||||") or line.startswith(b"======="):
                    break
                ours_parts.append(line)
                byte_offset += len(line)
                i += 1
            # Collect base section (between ||||||| and =======)
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
            # Skip =======
            if i < len(lines) and lines[i].startswith(b"======="):
                byte_offset += len(lines[i])
                i += 1
            # Collect theirs section (until >>>>>>>)
            theirs_parts: list[bytes] = []
            while i < len(lines):
                line = lines[i]
                if line.startswith(b">>>>>>>"):
                    byte_offset += len(line)
                    i += 1
                    hunks.append(ConflictHunk(
                        start=hunk_start,
                        end=byte_offset,
                        ours=b"".join(ours_parts),
                        base=b"".join(base_parts),
                        theirs=b"".join(theirs_parts),
                    ))
                    break
                theirs_parts.append(line)
                byte_offset += len(line)
                i += 1
        else:
            byte_offset += len(line)
            i += 1
    return hunks


def compute_per_hunk_key(hunk: ConflictHunk, filepath: str) -> str:
    """Compute order-independent key for a single conflict hunk.

    Key = sorted(md5(ours), md5(theirs)) + md5(base) + md5(filepath).
    Sorting ours/theirs hashes makes the key independent of merge direction.
    """
    ours_hash = hashlib.md5(hunk.ours).hexdigest()
    theirs_hash = hashlib.md5(hunk.theirs).hexdigest()
    base_hash = hashlib.md5(hunk.base).hexdigest()
    filepath_hash = hashlib.md5(filepath.encode()).hexdigest()
    sides = sorted([ours_hash, theirs_hash])
    return sides[0] + sides[1] + base_hash + filepath_hash


def apply_hunk_resolutions(
    conflict_content: bytes,
    hunks: list[ConflictHunk],
    resolved_hunks: list[Optional[bytes]],
) -> bytes:
    """Replace conflict marker regions with resolved text.

    If a resolved_hunk entry is None, the original conflict markers are kept.
    """
    result = bytearray()
    cursor = 0
    for hunk, resolved in zip(hunks, resolved_hunks):
        result.extend(conflict_content[cursor : hunk.start])
        if resolved is not None:
            result.extend(resolved)
        else:
            result.extend(conflict_content[hunk.start : hunk.end])
        cursor = hunk.end
    result.extend(conflict_content[cursor:])
    return bytes(result)


def reconstruct_conflict_from_index(repo_root: str, path: str) -> Optional[bytes]:
    """Reconstruct conflict file from git index stages using git merge-file --diff3.

    This ensures consistent hunk boundaries with the recording tool
    (compute_hunk_resolution.py), which also uses git merge-file --diff3.
    git merge (ort strategy) can combine adjacent conflicts with small gaps,
    producing different hunk boundaries than git merge-file.
    """
    try:
        base = git_output_bytes(["show", f":1:{path}"], cwd=repo_root)
        ours = git_output_bytes(["show", f":2:{path}"], cwd=repo_root)
        theirs = git_output_bytes(["show", f":3:{path}"], cwd=repo_root)
    except Exception:
        return None

    fd_ours, tmp_ours = tempfile.mkstemp(suffix=".ours")
    fd_base, tmp_base = tempfile.mkstemp(suffix=".base")
    fd_theirs, tmp_theirs = tempfile.mkstemp(suffix=".theirs")
    try:
        os.write(fd_ours, ours)
        os.close(fd_ours)
        os.write(fd_base, base)
        os.close(fd_base)
        os.write(fd_theirs, theirs)
        os.close(fd_theirs)

        subprocess.run(
            ["git", "merge-file", "--diff3",
             "-L", "ours", "-L", "base", "-L", "theirs",
             tmp_ours, tmp_base, tmp_theirs],
            check=False,
            capture_output=True,
        )
        with open(tmp_ours, "rb") as f:
            return f.read()
    finally:
        for p in (tmp_ours, tmp_base, tmp_theirs):
            try:
                os.unlink(p)
            except OSError:
                pass


def lookup_resolution(
    resolutions_dir: str,
    repo_root: str,
    path: str,
    used_keys: Optional[list[str]] = None,
) -> tuple[Optional[bytes], int, int]:
    """Look up per-hunk resolutions from bank.

    Returns (resolved_content, resolved_count, total_count).
    resolved_content is None if no hunks matched.
    If partially resolved, the returned content still has conflict markers
    for unresolved hunks.

    Uses git merge-file --diff3 to reconstruct the conflict from index stages,
    ensuring hunk boundaries match those used during resolution recording.
    If used_keys list is provided, matched keys are appended to it.
    """
    # Reconstruct conflict via git merge-file --diff3 for consistent hunk boundaries
    conflict_content = reconstruct_conflict_from_index(repo_root, path)
    if conflict_content is None:
        return None, 0, 0

    hunks = parse_conflict_hunks(conflict_content)
    if not hunks:
        return None, 0, 0

    resolved_hunks: list[Optional[bytes]] = []
    resolved_count = 0

    for hunk in hunks:
        key = compute_per_hunk_key(hunk, path)
        resolution_path = os.path.join(resolutions_dir, "contents", key)
        if os.path.isfile(resolution_path):
            with open(resolution_path, "rb") as f:
                resolved_hunks.append(f.read())
            resolved_count += 1
            if used_keys is not None:
                used_keys.append(key)
        else:
            print(f"  bank miss: {path} hunk key {key[:32]}...", file=sys.stderr)
            resolved_hunks.append(None)

    if resolved_count == 0:
        return None, 0, len(hunks)

    result = apply_hunk_resolutions(conflict_content, hunks, resolved_hunks)
    return result, resolved_count, len(hunks)


def ensure_ready(repo_root: str, allow_dirty: bool, auto_continue: bool) -> None:
    if not allow_dirty:
        status = git_output_text(["status", "--porcelain"], cwd=repo_root)
        if status:
            raise RuntimeError("working tree is not clean")

    if auto_continue:
        staged = git_output_text(["diff", "--cached", "--name-only"], cwd=repo_root)
        if staged:
            raise RuntimeError(
                "staged changes present; auto-continue would include them"
            )

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
    unmerged = git_output_text(
        ["diff", "--name-only", "--diff-filter=U"], cwd=repo_root
    )
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
    parser = argparse.ArgumentParser(
        description="Merge B into A with commuting-conflict auto-resolution."
    )
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
    parser.add_argument(
        "--resolutions-dir",
        help="path to resolutions bank directory for pre-recorded conflict resolutions",
    )
    parser.add_argument(
        "--bank-only",
        action="store_true",
        help="only use banked resolutions (skip commuting-merge logic)",
    )
    parser.add_argument(
        "--resolution-log",
        help="path to file for logging per-file resolution stats (TSV)",
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

    base = git_output_text(
        ["merge-base", args.branch_a, args.branch_b], cwd=repo_root
    )

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

    conflicts_raw = git_output_text(
        ["diff", "--name-only", "--diff-filter=U"], cwd=repo_root
    )
    conflicts = [line for line in conflicts_raw.splitlines() if line]
    if not conflicts:
        print("merge failed without conflicted files", file=sys.stderr)
        return 1

    if args.resolutions_dir:
        contents_dir = os.path.join(args.resolutions_dir, "contents")
        n_bank_files = len(os.listdir(contents_dir)) if os.path.isdir(contents_dir) else 0
        print(f"resolution bank: {contents_dir} ({n_bank_files} entries)", file=sys.stderr)

    unresolved: list[str] = []
    resolution_stats: list[tuple[str, int, int, int]] = []  # (path, total, bank, commute)
    all_used_keys: list[str] = []
    try:
        for path in conflicts:
            resolved = False
            bank_partial = False
            file_total = 0
            file_bank = 0

            # 1. Try per-hunk bank resolution
            if args.resolutions_dir:
                banked, n_resolved, n_total = lookup_resolution(
                    args.resolutions_dir,
                    repo_root,
                    path,
                    used_keys=all_used_keys,
                )
                file_total = n_total
                if banked is not None and n_resolved == n_total:
                    print(f"resolved from bank: {path} ({n_total} hunks)", file=sys.stderr)
                    write_result(repo_root, path, banked)
                    run_git(["add", "-A", "--", path], cwd=repo_root)
                    file_bank = n_total
                    resolved = True
                elif banked is not None and n_resolved > 0:
                    print(
                        f"partially resolved from bank: {path} ({n_resolved}/{n_total} hunks)",
                        file=sys.stderr,
                    )
                    # Write partial resolution (still has markers for unresolved hunks)
                    write_result(repo_root, path, banked)
                    file_bank = n_resolved
                    bank_partial = True

            # 2. Try commuting-merge on full file from blobs (unless --bank-only)
            if not resolved and not args.bank_only:
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
                    # Commute resolves all hunks; if bank was partial its work is overwritten
                    if file_total == 0:
                        recon = reconstruct_conflict_from_index(repo_root, path)
                        if recon:
                            file_total = len(parse_conflict_hunks(recon))
                    resolution_stats.append((path, file_total, 0, file_total))
                    resolved = True

            if resolved and path not in [s[0] for s in resolution_stats]:
                # Resolved by bank (not commute)
                resolution_stats.append((path, file_total, file_bank, 0))

            if not resolved:
                if file_total == 0:
                    recon = reconstruct_conflict_from_index(repo_root, path)
                    if recon:
                        file_total = len(parse_conflict_hunks(recon))
                resolution_stats.append((path, file_total, file_bank, 0))
                if bank_partial:
                    # Partial bank resolution already written to working tree;
                    # remaining markers will appear in conflict dump.
                    pass
                unresolved.append(path)
    except Exception as exc:
        if not args.keep_merge:
            aborted = abort_merge(repo_root)
            if not aborted:
                print("warning: failed to abort merge", file=sys.stderr)
        raise exc

    # Write resolution stats log
    if args.resolution_log:
        if resolution_stats:
            with open(args.resolution_log, "a") as f:
                for path, total, bank, commute in resolution_stats:
                    f.write(f"{path}\t{total}\t{bank}\t{commute}\n")
        if all_used_keys:
            keys_file = args.resolution_log.replace(".tsv", ".keys")
            with open(keys_file, "a") as f:
                for key in all_used_keys:
                    f.write(key + "\n")

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

    print("all conflicts resolved by auto-resolution")
    return 0


if __name__ == "__main__":
    sys.exit(main())
