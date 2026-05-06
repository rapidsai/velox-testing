# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Step logging helpers and the pairwise compatibility matrix renderer."""

from __future__ import annotations

import sys
from typing import Dict, List, Tuple

_step_count = 0


def log(msg: str = "") -> None:
    print(msg, file=sys.stderr, flush=True)


def step(msg: str) -> None:
    global _step_count
    _step_count += 1
    log(f"==== [{_step_count}] {msg} ====")


def divider(msg: str = "") -> None:
    log(f"---- {msg} ----")


def render_compatibility_matrix(
    pr_list: List[str],
    pair_results: Dict[Tuple[str, str], str],
) -> List[str]:
    """Render the pairwise PR compatibility table as a list of lines.

    pair_results keys are (pr_a, pr_b) with pr_a's index < pr_b's index in pr_list.
    Cell values are typically "OK" or "XX"; missing pairs render as "?".
    """
    if len(pr_list) < 2:
        return []

    col_w = max(7, *(len(p) + 1 for p in pr_list))

    def divider_row() -> str:
        sep = "+" + "-" * (col_w + 2) + "+"
        for _ in pr_list:
            sep += "-" * (col_w + 2) + "+"
        return sep

    def fmt_cell(value: str, *, header: bool = False) -> str:
        if header:
            return f"| {value:<{col_w}} "
        return f"| {value:>{col_w}} "

    lines: List[str] = [divider_row()]
    header = fmt_cell("PR", header=True)
    for pr in pr_list:
        header += fmt_cell(f"#{pr}")
    header += "|"
    lines.append(header)
    lines.append(divider_row())

    for i, pr_row in enumerate(pr_list):
        row = fmt_cell(f"#{pr_row}", header=True)
        for j, pr_col in enumerate(pr_list):
            if i == j:
                cell = "--"
            elif i < j:
                cell = pair_results.get((pr_row, pr_col), "?")
            else:
                cell = pair_results.get((pr_col, pr_row), "?")
            row += fmt_cell(cell)
        row += "|"
        lines.append(row)
        lines.append(divider_row())
    return lines


def render_conflict_pr_table(rows: List[Dict[str, str]]) -> List[str]:
    """Render the 'PRs Involved in Conflicts' table.

    Each row dict needs keys: pr, author, title, url.
    """
    if not rows:
        return []

    fmt = "| {pr:<10} | {author:<20} | {title:<50} | {url:<55} |"
    sep = "| {pr:<10} | {author:<20} | {title:<50} | {url:<55} |".format(
        pr="-" * 10,
        author="-" * 20,
        title="-" * 50,
        url="-" * 55,
    )
    lines = [
        fmt.format(pr="PR", author="Author", title="Title", url="URL"),
        sep,
    ]
    for row in rows:
        title = row.get("title", "")
        if len(title) > 47:
            title = title[:47] + "..."
        lines.append(
            fmt.format(
                pr=row.get("pr", ""),
                author=row.get("author", ""),
                title=title,
                url=row.get("url", ""),
            )
        )
    return lines
