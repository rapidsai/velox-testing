# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Post-resolution validators for merge-conflict outputs.

These run after rerere or Claude has produced a candidate resolved file but
before we ``git add`` / ``git commit`` it. The goal is to catch resolutions
that look syntactically clean (no ``<<<<<<< / ======= / >>>>>>>`` markers
remain) but are structurally broken — most commonly because the resolution
kept *both* sides of a conflict hunk side by side, leaving a literal duplicate
of the same code block.

Three families of checks run, each gated by file type and each only firing
when the resolution introduces a duplication that did not exist on either
parent side:

1. CMake target/test uniqueness (``CMakeLists.txt``, ``*.cmake``) — catches
   the canonical ``another target with the same name already exists`` build
   failure caused by two copies of an ``add_executable`` / ``add_library`` /
   ``add_custom_target`` / ``add_test(NAME ...)`` block.
2. Python top-level definition uniqueness (``*.py``) — catches the
   silent-failure case where two ``def foo(...)`` or ``class Foo:`` blocks
   end up at module scope and the second one wins at import time.
3. Generic "kept both sides" duplicate-block detection (everything except
   binary blobs and a few large/lock file types) — catches arbitrary
   side-by-side concatenations of identical N-line runs (e.g. two copies of
   the same C++ function body, the same Bash function, the same YAML
   stanza). The proximity filter avoids flagging unrelated repetition that
   happens elsewhere in the file.

For every check we compare against the "ours" (HEAD / stage 2) and "theirs"
(MERGE_HEAD / stage 3) versions of the file, and only flag when the resolved
count exceeds the per-side maximum. Legitimate patterns where both sides
already had the duplication (e.g. ``if(WIN32) add_executable(foo) else()
add_executable(foo) endif()``, or two unrelated copies of the same boilerplate
that pre-existed the merge) are therefore not false-flagged.
"""

from __future__ import annotations

import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .git_ops import git

_CMAKE_TARGET_RE = re.compile(
    r"\b(?:add_executable|add_library|add_custom_target)\s*\(\s*([A-Za-z0-9_./+-]+)",
    re.IGNORECASE,
)
_CMAKE_TEST_RE = re.compile(
    r"\badd_test\s*\(\s*(?:NAME\s+)?([A-Za-z0-9_./+-]+)",
    re.IGNORECASE,
)
_CMAKE_LINE_COMMENT_RE = re.compile(r"#[^\n]*")

# Module-scope `def foo(...)` / `class Foo(...):` (no leading indentation).
_PY_TOPLEVEL_DEF_RE = re.compile(r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE)
_PY_TOPLEVEL_CLASS_RE = re.compile(r"^class\s+([A-Za-z_][A-Za-z0-9_]*)\s*[\(:]", re.MULTILINE)

# File extensions and basenames where the generic duplicate-block detector is
# unhelpful (binary, lockfiles, large regenerated artefacts, etc.).
_GENERIC_BLOCK_SKIP_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".pdf",
    ".zip",
    ".gz",
    ".tar",
    ".bz2",
    ".xz",
    ".so",
    ".dll",
    ".a",
    ".o",
    ".bin",
    ".lock",
    ".min.js",
    ".min.css",
    ".map",
    ".svg",
    ".ico",
}
_GENERIC_BLOCK_SKIP_BASENAMES = {
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "poetry.lock",
    "Cargo.lock",
    "go.sum",
    "Pipfile.lock",
}

# Tunables for the generic duplicate-block detector.
# A window of N consecutive lines requires at least M non-trivial lines so
# that runs of `}` or blank lines never anchor a match, while still allowing
# normal source code where closing braces are interspersed with substance.
# max_proximity is intentionally tight — "kept both sides" mistakes leave the
# duplicates inside the same hunk region (usually a few dozen lines apart at
# most), whereas legitimate "ours added X here and theirs added X over there"
# duplicates tend to be far enough apart that we don't want to flag them.
_GENERIC_MIN_RUN = 4
_GENERIC_MIN_NON_TRIVIAL = 3
_GENERIC_MAX_PROXIMITY = 80
_GENERIC_MAX_REPORTS_PER_FILE = 5
_GENERIC_MAX_FILE_BYTES = 1_500_000

# Lines too small / too repetitive to anchor a duplicate-block report on.
_TRIVIAL_LINE_RE = re.compile(r"^\s*(?:[\{\}\(\);,]+|//+|#+|/\*+|\*+/?|--+|==+|<+|>+|\*+|\++)?\s*$")


def is_cmake_file(file_path: str) -> bool:
    """Return True for files where CMake duplicate-target checks apply."""
    name = Path(file_path).name
    return name == "CMakeLists.txt" or name.endswith(".cmake")


def is_python_file(file_path: str) -> bool:
    return Path(file_path).suffix == ".py"


def _generic_block_check_applies(file_path: str) -> bool:
    name = Path(file_path).name
    if name in _GENERIC_BLOCK_SKIP_BASENAMES:
        return False
    suffix = "".join(Path(name).suffixes[-2:]) if name.count(".") >= 2 else Path(name).suffix
    if Path(name).suffix in _GENERIC_BLOCK_SKIP_SUFFIXES:
        return False
    if suffix in _GENERIC_BLOCK_SKIP_SUFFIXES:
        return False
    return True


def _strip_cmake_comments(text: str) -> str:
    return _CMAKE_LINE_COMMENT_RE.sub("", text or "")


def _count_cmake_targets(text: str) -> Counter:
    return Counter(m.group(1) for m in _CMAKE_TARGET_RE.finditer(_strip_cmake_comments(text)))


def _count_cmake_tests(text: str) -> Counter:
    return Counter(m.group(1) for m in _CMAKE_TEST_RE.finditer(_strip_cmake_comments(text)))


def _count_python_toplevel_defs(text: str) -> Counter:
    return Counter(m.group(1) for m in _PY_TOPLEVEL_DEF_RE.finditer(text or ""))


def _count_python_toplevel_classes(text: str) -> Counter:
    return Counter(m.group(1) for m in _PY_TOPLEVEL_CLASS_RE.finditer(text or ""))


def _read_git_object(repo: Path, ref: str) -> Optional[str]:
    """Return the contents of a git object (e.g. ``HEAD:path`` or ``:2:path``).

    Returns None when the object does not exist (e.g. file added on only one
    side of an add/add or delete/modify conflict, or stage 2/3 missing).
    """
    try:
        res = subprocess.run(
            ["git", "-C", str(repo), "show", ref],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if res.returncode != 0:
        return None
    return (res.stdout or b"").decode("utf-8", errors="replace")


def _ours_theirs_for_unmerged(repo: Path, file_path: str) -> Tuple[str, str]:
    """Read the conflict stages 2 (ours) and 3 (theirs) for an unmerged file.

    Empty string is returned for any stage that does not exist (e.g. modify/
    delete conflicts where one side is missing).
    """
    ours = _read_git_object(repo, f":2:{file_path}") or ""
    theirs = _read_git_object(repo, f":3:{file_path}") or ""
    return ours, theirs


def _ours_theirs_for_in_progress_merge(repo: Path, file_path: str) -> Tuple[str, str]:
    """Read HEAD (ours) and MERGE_HEAD (theirs) versions of ``file_path``.

    Used after rerere has cleared the unmerged stages but before the merge
    commit is created — at that point ``git show :2:`` no longer works but
    HEAD and MERGE_HEAD still point at the two parents of the in-progress
    merge.
    """
    ours = _read_git_object(repo, f"HEAD:{file_path}") or ""
    theirs = _read_git_object(repo, f"MERGE_HEAD:{file_path}") or ""
    return ours, theirs


def _format_duplicate_names(
    resolved_counts: Dict[str, int],
    ours_counts: Dict[str, int],
    theirs_counts: Dict[str, int],
    *,
    kind: str,
    advice: str,
) -> List[str]:
    """Return human-readable error strings for any names that the resolution
    duplicated beyond what either side already had.

    ``kind`` is the short label used in the message (e.g. ``"CMake target"``,
    ``"Python top-level def"``); ``advice`` is appended verbatim and should
    explain to the reader (and Claude) what to do about it.
    """
    errors: List[str] = []
    for name, count in sorted(resolved_counts.items()):
        if count <= 1:
            continue
        baseline = max(ours_counts.get(name, 0), theirs_counts.get(name, 0))
        if count <= baseline:
            continue
        errors.append(
            f"duplicate {kind} '{name}': {count} occurrences in the resolved file "
            f"(ours had {ours_counts.get(name, 0)}, theirs had {theirs_counts.get(name, 0)}). "
            f"{advice}"
        )
    return errors


def find_cmake_duplicates(
    file_path: str,
    resolved_text: str,
    ours_text: str,
    theirs_text: str,
) -> List[str]:
    """Detect duplicate CMake targets/tests introduced by a conflict resolution."""
    if not is_cmake_file(file_path):
        return []

    resolved_targets = _count_cmake_targets(resolved_text)
    resolved_tests = _count_cmake_tests(resolved_text)
    if not any(c > 1 for c in resolved_targets.values()) and not any(c > 1 for c in resolved_tests.values()):
        return []

    ours_targets = _count_cmake_targets(ours_text)
    theirs_targets = _count_cmake_targets(theirs_text)
    ours_tests = _count_cmake_tests(ours_text)
    theirs_tests = _count_cmake_tests(theirs_text)

    errors: List[str] = []
    errors.extend(
        _format_duplicate_names(
            resolved_targets,
            ours_targets,
            theirs_targets,
            kind="CMake target",
            advice=(
                "The resolution kept both copies of an add_executable / add_library / "
                "add_custom_target block — merge them into a single declaration; "
                "CMake rejects two top-level targets with the same name."
            ),
        )
    )
    errors.extend(
        _format_duplicate_names(
            resolved_tests,
            ours_tests,
            theirs_tests,
            kind="CMake test name",
            advice=(
                "The resolution kept both copies of an add_test(NAME ...) block — "
                "merge them into a single declaration; CMake rejects two tests with "
                "the same NAME in the same directory."
            ),
        )
    )
    return errors


def find_python_definition_duplicates(
    file_path: str,
    resolved_text: str,
    ours_text: str,
    theirs_text: str,
) -> List[str]:
    """Detect duplicate top-level ``def``/``class`` declarations introduced by
    a conflict resolution in a Python file.

    Python silently lets the second definition shadow the first, which makes
    "kept both sides" mistakes invisible until runtime. We catch the cases
    that matter here: the same identifier is declared more than once at
    module scope after the merge but was unique on each parent side.
    """
    if not is_python_file(file_path):
        return []

    resolved_defs = _count_python_toplevel_defs(resolved_text)
    resolved_classes = _count_python_toplevel_classes(resolved_text)
    if not any(c > 1 for c in resolved_defs.values()) and not any(c > 1 for c in resolved_classes.values()):
        return []

    ours_defs = _count_python_toplevel_defs(ours_text)
    theirs_defs = _count_python_toplevel_defs(theirs_text)
    ours_classes = _count_python_toplevel_classes(ours_text)
    theirs_classes = _count_python_toplevel_classes(theirs_text)

    errors: List[str] = []
    errors.extend(
        _format_duplicate_names(
            resolved_defs,
            ours_defs,
            theirs_defs,
            kind="Python top-level def",
            advice=(
                "The resolution kept both copies of `def` block — merge them into a "
                "single function; the second definition would silently shadow the "
                "first at import time."
            ),
        )
    )
    errors.extend(
        _format_duplicate_names(
            resolved_classes,
            ours_classes,
            theirs_classes,
            kind="Python top-level class",
            advice=(
                "The resolution kept both copies of a `class` block — merge them into "
                "a single class; the second definition would silently shadow the first "
                "at import time."
            ),
        )
    )
    return errors


def _line_is_trivial(line: str) -> bool:
    """Return True for lines too short or too generic to anchor a duplicate report.

    These are skipped when forming the N-line windows used by the generic
    duplicate-block detector so that long runs of ``}`` or empty lines do not
    create spurious matches.
    """
    return bool(_TRIVIAL_LINE_RE.match(line))


def _collect_window_positions(text: str, n: int, min_non_trivial: int) -> Dict[str, List[int]]:
    """Return ``{window_text: [start_line_index, ...]}`` for every contiguous
    N-line window in ``text`` whose number of non-trivial lines is at least
    ``min_non_trivial``.

    A pure run of trivial lines (closing braces, blanks, comment markers) is
    skipped because it would otherwise trivially match itself everywhere it
    occurs in the file. Mixed windows where most lines carry substance are
    kept so that real function bodies — which inevitably include some ``}``
    and blank lines — can still anchor a match.
    """
    if not text:
        return {}
    raw_lines = text.splitlines()
    if len(raw_lines) < n:
        return {}
    normalized = [ln.rstrip() for ln in raw_lines]
    triviality = [_line_is_trivial(ln) for ln in normalized]
    windows: Dict[str, List[int]] = {}
    for i in range(len(normalized) - n + 1):
        non_trivial_count = sum(1 for j in range(i, i + n) if not triviality[j])
        if non_trivial_count < min_non_trivial:
            continue
        key = "\n".join(normalized[i : i + n])
        windows.setdefault(key, []).append(i)
    return windows


def _count_close_pairs(positions: List[int], max_gap: int) -> int:
    """Count adjacent pairs ``(p_i, p_{i+1})`` with ``p_{i+1} - p_i <= max_gap``."""
    return sum(1 for i in range(len(positions) - 1) if positions[i + 1] - positions[i] <= max_gap)


def find_duplicate_code_blocks(
    file_path: str,
    resolved_text: str,
    ours_text: str,
    theirs_text: str,
    *,
    min_run: int = _GENERIC_MIN_RUN,
    min_non_trivial: int = _GENERIC_MIN_NON_TRIVIAL,
    max_proximity: int = _GENERIC_MAX_PROXIMITY,
    max_reports: int = _GENERIC_MAX_REPORTS_PER_FILE,
) -> List[str]:
    """Detect contiguous N-line code blocks that the resolution duplicated.

    Returns a list of human-readable error strings, capped at ``max_reports``
    entries per file, describing instances where ``min_run`` consecutive
    lines (with at least ``min_non_trivial`` of them carrying substance)
    appear at two locations within ``max_proximity`` lines of each other in
    the resolved file but did not appear at such close proximity in either
    parent. The proximity filter focuses the check on "kept both sides"
    mistakes (which leave the duplicates inside the same hunk region) and
    avoids flagging unrelated repetitions elsewhere in the file.

    The check is skipped for binary / lockfile / very-large files because
    the heuristic is not meaningful there.
    """
    if not _generic_block_check_applies(file_path):
        return []
    if len(resolved_text) > _GENERIC_MAX_FILE_BYTES:
        return []

    resolved_windows = _collect_window_positions(resolved_text, min_run, min_non_trivial)
    if not any(len(positions) >= 2 for positions in resolved_windows.values()):
        return []

    ours_windows = _collect_window_positions(ours_text, min_run, min_non_trivial)
    theirs_windows = _collect_window_positions(theirs_text, min_run, min_non_trivial)

    errors: List[str] = []
    reported_ranges: List[Tuple[int, int]] = []

    items = sorted(resolved_windows.items(), key=lambda kv: kv[1][0])
    for key, positions in items:
        if len(positions) < 2:
            continue
        close_pairs = [
            (positions[i], positions[i + 1])
            for i in range(len(positions) - 1)
            if positions[i + 1] - positions[i] <= max_proximity
        ]
        if not close_pairs:
            continue

        ours_close = _count_close_pairs(ours_windows.get(key, []), max_proximity)
        theirs_close = _count_close_pairs(theirs_windows.get(key, []), max_proximity)
        if len(close_pairs) <= max(ours_close, theirs_close):
            continue

        first_a, first_b = close_pairs[0]
        if any(start <= first_a < end or start <= first_b < end for start, end in reported_ranges):
            continue
        reported_ranges.append((first_a, first_b + min_run))

        first_line = key.split("\n", 1)[0].strip()
        if len(first_line) > 100:
            first_line = first_line[:97] + "..."
        errors.append(
            f"duplicate code block in {file_path}: identical {min_run}-line run at lines "
            f"{first_a + 1} and {first_b + 1} (gap {first_b - first_a} lines). "
            f"Neither parent had two copies this close together — the resolution likely "
            f"kept both sides of a hunk side by side. First line: {first_line!r}. "
            f"Collapse the duplicate into a single copy."
        )
        if len(errors) >= max_reports:
            break

    return errors


def _run_all_validators(
    file_path: str,
    resolved_text: str,
    ours_text: str,
    theirs_text: str,
) -> List[str]:
    """Run every applicable validator against a candidate resolved file.

    The order is structural-first (CMake, Python definitions) then the
    generic block-duplicate fallback so the most actionable messages appear
    first when both fire on the same file.
    """
    errors: List[str] = []
    errors.extend(find_cmake_duplicates(file_path, resolved_text, ours_text, theirs_text))
    errors.extend(find_python_definition_duplicates(file_path, resolved_text, ours_text, theirs_text))
    if not is_cmake_file(file_path):
        # The CMake check already reports duplicate-block-style failures with
        # better messages; running the generic detector on top would add noise
        # without new information.
        errors.extend(find_duplicate_code_blocks(file_path, resolved_text, ours_text, theirs_text))
    return errors


def validate_unmerged_resolution(repo: Path, file_path: str, resolved_text: str) -> List[str]:
    """Validate a Claude-resolved file while the conflict stages are still in
    the index (i.e. before we ``git add`` it).

    Any unexpected exception from the underlying git/IO calls is swallowed
    and reported as "no validation errors found" so a single pathological
    file cannot abort the surrounding merge pipeline.
    """
    try:
        ours, theirs = _ours_theirs_for_unmerged(repo, file_path)
        return _run_all_validators(file_path, resolved_text, ours, theirs)
    except Exception:
        return []


def validate_in_progress_merge_file(repo: Path, file_path: str) -> List[str]:
    """Validate a file that rerere has already resolved (no longer unmerged)
    using HEAD and MERGE_HEAD as the "ours"/"theirs" baselines.

    Returns an empty list (== "looks fine") on any unexpected error so the
    caller can keep iterating over the rest of the merge's touched files.
    """
    abs_path = repo / file_path
    try:
        resolved_text = abs_path.read_text(encoding="utf-8", errors="replace")
    except (OSError, ValueError):
        return []
    try:
        ours, theirs = _ours_theirs_for_in_progress_merge(repo, file_path)
        return _run_all_validators(file_path, resolved_text, ours, theirs)
    except Exception:
        return []


def list_files_in_progress_merge(repo: Path) -> List[str]:
    """Return paths touched by the in-progress merge, relative to ``repo``.

    Uses ``git diff --name-only HEAD MERGE_HEAD`` (the symmetric set of paths
    changed on either side of the merge) so we can scope post-rerere
    validation to just the files the merge could plausibly have produced.
    Falls back to the empty list when ``MERGE_HEAD`` is not set.
    """
    res = git(
        ["diff", "--name-only", "HEAD", "MERGE_HEAD"],
        cwd=repo,
        check=False,
    )
    if res.returncode != 0:
        return []
    return [line.strip() for line in (res.stdout or "").splitlines() if line.strip()]


def format_validation_errors_for_prompt(file_path: str, errors: List[str]) -> str:
    """Render validator errors as a block to inject into the Claude retry prompt."""
    if not errors:
        return ""
    lines = [
        "VALIDATION FAILED on the previous attempt — the resolved file passed the",
        "marker check but introduced structural duplicates that the build system or",
        "language runtime will reject (or silently break):",
        "",
    ]
    for err in errors:
        lines.append(f"  - {err}")
    lines.extend(
        [
            "",
            f"Re-resolve `{file_path}` so that no symbol or code block is duplicated by the",
            "merge itself. When both sides of a hunk added the same declaration, function,",
            "class, or block of statements, KEEP A SINGLE COPY rather than concatenating",
            "both. Legitimate side-by-side duplicates from before the merge (already present",
            "in either ours or theirs) are fine — only the duplicates that the resolution",
            "itself introduced need to be collapsed.",
            "",
        ]
    )
    return "\n".join(lines)
