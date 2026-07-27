#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Render perf-script call chains as folded stacks and interactive HTML.

This is the dependency-free fallback for hosts whose perf package omits the
in-tree ``perf script report flamegraph`` Python reporter. It does not use the
legacy Perl FlameGraph stack-collapse or rendering scripts. The generated
self-contained flamegraph supports click-to-zoom without loading JavaScript
or CSS from a CDN.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

FRAME_RE = re.compile(r"^([0-9a-fA-F]+)\s+(.+?)\s+\((.*)\)\s*$")
OFFSET_RE = re.compile(r"\+0x[0-9a-fA-F]+(?:/0x[0-9a-fA-F]+)?$")


def clean_symbol(symbol: str, dso: str) -> str:
    """Normalize a frame so samples at different instruction offsets merge."""

    symbol = OFFSET_RE.sub("", symbol.strip()).replace(";", ":")
    if not symbol or symbol in {"[unknown]", "unknown"}:
        dso_name = Path(dso).name if dso and dso != "[unknown]" else "unknown"
        return f"[unknown:{dso_name}]"
    return symbol


def collapse_perf_script(path: Path) -> Counter[tuple[str, ...]]:
    """Return root-to-leaf stacks and their sample counts."""

    stacks: Counter[tuple[str, ...]] = Counter()
    frames: list[str] = []

    def flush() -> None:
        nonlocal frames
        if frames:
            # perf script emits the sampled leaf first and its callers after it.
            stacks[tuple(reversed(frames))] += 1
            frames = []

    with path.open(errors="replace") as source:
        for raw_line in source:
            if not raw_line.strip():
                flush()
                continue
            if raw_line.startswith("#"):
                continue
            if not raw_line[0].isspace():
                # Some perf versions omit the blank separator between samples.
                flush()
                continue

            match = FRAME_RE.match(raw_line.strip())
            if match:
                _address, symbol, dso = match.groups()
                frames.append(clean_symbol(symbol, dso))
        flush()

    return stacks


def read_folded(path: Path) -> Counter[tuple[str, ...]]:
    """Read an existing folded-stack file for fast HTML-only rerendering."""

    stacks: Counter[tuple[str, ...]] = Counter()
    with path.open(errors="replace") as source:
        for line_number, raw_line in enumerate(source, start=1):
            line = raw_line.rstrip("\n")
            if not line:
                continue
            try:
                stack_text, count_text = line.rsplit(maxsplit=1)
                count = int(count_text)
            except (ValueError, TypeError) as error:
                raise ValueError(
                    f"{path}:{line_number}: invalid folded-stack record"
                ) from error
            stack = tuple(stack_text.split(";"))
            if count <= 0 or not stack or any(not frame for frame in stack):
                raise ValueError(
                    f"{path}:{line_number}: invalid folded-stack record"
                )
            stacks[stack] += count
    return stacks


@dataclass
class Node:
    name: str
    count: int = 0
    children: dict[str, "Node"] = field(default_factory=dict)


def build_tree(stacks: Counter[tuple[str, ...]]) -> tuple[Node, int]:
    root = Node("all")
    max_depth = 0
    for stack, count in stacks.items():
        root.count += count
        node = root
        max_depth = max(max_depth, len(stack))
        for frame in stack:
            node = node.children.setdefault(frame, Node(frame))
            node.count += count
    return root, max_depth


def frame_color(name: str) -> str:
    digest = hashlib.sha256(name.encode(errors="replace")).digest()
    return f"rgb({205 + digest[0] % 45},{75 + digest[1] % 105},{35 + digest[2] % 55})"


def shorten(text: str, width: float) -> str:
    max_chars = max(0, int((width - 6) / 7.0))
    if max_chars < 3:
        return ""
    return text if len(text) <= max_chars else text[: max_chars - 2] + ".."


def render_html(root: Node, max_depth: int, destination: Path, title: str) -> None:
    if root.count <= 0:
        raise ValueError("perf script contained no call-chain samples")

    width = 1600
    frame_height = 18
    left = 12
    right = 12
    header = 78
    bottom = 24
    plot_width = width - left - right
    height = header + max(1, max_depth) * frame_height + bottom
    scale = plot_width / root.count
    elements: list[str] = []

    def draw_children(node: Node, x: float, depth: int, parent_path: str) -> None:
        cursor = x
        for child_index, child in enumerate(
            sorted(node.children.values(), key=lambda item: item.name)
        ):
            child_width = child.count * scale
            y = height - bottom - (depth + 1) * frame_height
            node_path = f"{parent_path}/{child_index}" if parent_path else str(child_index)
            name_attr = html.escape(child.name, quote=True)
            percentage = 100.0 * child.count / root.count
            tooltip = html.escape(
                f"{child.name} — {child.count} samples ({percentage:.2f}%)", quote=True
            )
            if child_width >= 0.3:
                elements.append(
                    f'<g class="frame" data-name="{name_attr}" '
                    f'data-path="{node_path}" data-x="{cursor:.6f}" '
                    f'data-y="{y}" data-width="{child_width:.6f}" '
                    f'data-depth="{depth}" role="button" tabindex="0">'
                    f"<title>{tooltip}</title>"
                    f'<rect x="{cursor:.3f}" y="{y}" width="{child_width:.3f}" '
                    f'height="{frame_height - 1}" fill="{frame_color(child.name)}"/>'
                    f'<text x="{cursor + 3:.3f}" y="{y + 13}">'
                    f'{html.escape(shorten(child.name, child_width))}</text></g>'
                )
            draw_children(child, cursor, depth + 1, node_path)
            cursor += child_width

    draw_children(root, left, 0, "")
    escaped_title = html.escape(title)
    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{escaped_title}</title>
<style>
body {{ margin: 0; padding: 12px; background: #fafafa; font-family: sans-serif; }}
#search {{ width: 34rem; max-width: 90%; padding: 5px 8px; }}
button {{ margin-left: .6rem; padding: 5px 10px; }}
button:disabled {{ cursor: default; opacity: .55; }}
svg {{ width: 100%; height: auto; border: 1px solid #ddd; background: white; }}
.frame {{ cursor: pointer; }}
.frame rect {{ stroke: rgb(120,50,30); stroke-width: .25; }}
.frame text {{ font: 11px monospace; fill: black; pointer-events: none; }}
.frame.match rect {{ stroke: #005fcc; stroke-width: 2.5; }}
.frame.zoom-root rect {{ stroke: #111; stroke-width: 1.5; }}
.frame:focus {{ outline: none; }}
.frame:focus rect {{ stroke: #111; stroke-width: 2.5; }}
#zoom-status {{ margin-left: .7rem; color: #333; }}
</style>
</head>
<body>
<h2>{escaped_title}</h2>
<label>Highlight frames: <input id="search" type="search" placeholder="function substring"></label>
<button id="reset-zoom" type="button" disabled>Reset Zoom</button>
<span id="zoom-status">Showing all frames</span>
<p>{root.count} sampled call chains. Click a frame to zoom; hover for sample count and percentage.</p>
<svg id="flamegraph" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
{''.join(elements)}
</svg>
<script>
const search = document.getElementById('search');
const resetZoom = document.getElementById('reset-zoom');
const zoomStatus = document.getElementById('zoom-status');
const frames = Array.from(document.querySelectorAll('.frame'));
const plotLeft = {left};
const plotWidth = {plot_width};
const frameHeight = {frame_height};

function abbreviate(name, width) {{
  const maxChars = Math.max(0, Math.floor((width - 6) / 7));
  if (maxChars < 3) return '';
  return name.length <= maxChars ? name : name.slice(0, maxChars - 2) + '..';
}}

function placeFrame(frame, x, y, width) {{
  const rect = frame.querySelector('rect');
  const label = frame.querySelector('text');
  rect.setAttribute('x', x.toFixed(3));
  rect.setAttribute('y', y.toFixed(3));
  rect.setAttribute('width', Math.max(0, width).toFixed(3));
  label.setAttribute('x', (x + 3).toFixed(3));
  label.setAttribute('y', (y + 13).toFixed(3));
  label.textContent = abbreviate(frame.dataset.name, width);
}}

function isInSubtree(path, rootPath) {{
  return path === rootPath || path.startsWith(rootPath + '/');
}}

function showAllFrames() {{
  frames.forEach(frame => {{
    frame.style.display = '';
    frame.classList.remove('zoom-root');
    placeFrame(
      frame,
      Number(frame.dataset.x),
      Number(frame.dataset.y),
      Number(frame.dataset.width));
  }});
  resetZoom.disabled = true;
  zoomStatus.textContent = 'Showing all frames';
}}

function zoomTo(frame) {{
  const rootPath = frame.dataset.path;
  const rootX = Number(frame.dataset.x);
  const rootWidth = Number(frame.dataset.width);
  const rootDepth = Number(frame.dataset.depth);
  const zoomScale = plotWidth / rootWidth;

  frames.forEach(candidate => {{
    const visible = isInSubtree(candidate.dataset.path, rootPath);
    candidate.style.display = visible ? '' : 'none';
    candidate.classList.toggle('zoom-root', candidate === frame);
    if (!visible) return;
    placeFrame(
      candidate,
      plotLeft + (Number(candidate.dataset.x) - rootX) * zoomScale,
      Number(candidate.dataset.y) + rootDepth * frameHeight,
      Number(candidate.dataset.width) * zoomScale);
  }});
  resetZoom.disabled = false;
  zoomStatus.textContent = 'Zoomed to: ' + frame.dataset.name;
}}

frames.forEach(frame => {{
  frame.addEventListener('click', () => zoomTo(frame));
  frame.addEventListener('keydown', event => {{
    if (event.key === 'Enter' || event.key === ' ') {{
      event.preventDefault();
      zoomTo(frame);
    }}
  }});
}});
resetZoom.addEventListener('click', showAllFrames);
search.addEventListener('input', () => {{
  const wanted = search.value.toLowerCase();
  frames.forEach(frame => frame.classList.toggle(
    'match', wanted.length > 0 && frame.dataset.name.toLowerCase().includes(wanted)));
}});
</script>
</body>
</html>
"""
    destination.write_text(document)


def write_folded(stacks: Counter[tuple[str, ...]], destination: Path) -> None:
    with destination.open("w") as output:
        for stack, count in sorted(stacks.items(), key=lambda item: item[0]):
            output.write(f"{';'.join(stack)} {count}\n")


def known_sample_count(stacks: Counter[tuple[str, ...]]) -> int:
    """Count samples containing at least one symbolized frame."""

    return sum(
        count
        for stack, count in stacks.items()
        if any(not frame.startswith("[unknown:") for frame in stack)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="perf script text")
    source.add_argument(
        "--input-folded",
        type=Path,
        help="existing folded stacks for fast HTML-only rerendering",
    )
    parser.add_argument(
        "--folded",
        type=Path,
        help="folded-stack output (required with --input)",
    )
    parser.add_argument("--output", type=Path, required=True, help="self-contained HTML output")
    parser.add_argument("--title", default="perf flamegraph")
    parser.add_argument(
        "--min-known-sample-ratio",
        type=float,
        default=0.05,
        help="minimum fraction of call chains with a symbolized frame (default: 0.05)",
    )
    args = parser.parse_args()

    if not 0.0 <= args.min_known_sample_ratio <= 1.0:
        parser.error("--min-known-sample-ratio must be between 0 and 1")
    if args.input is not None and args.folded is None:
        parser.error("--folded is required with --input")
    if args.input_folded is not None and args.folded is not None:
        parser.error("--folded cannot be used with --input-folded")

    try:
        stacks = (
            collapse_perf_script(args.input)
            if args.input is not None
            else read_folded(args.input_folded)
        )
    except ValueError as error:
        parser.error(str(error))
    if not stacks:
        parser.error("no call-chain samples found in input")
    total_samples = sum(stacks.values())
    known_samples = known_sample_count(stacks)
    known_ratio = known_samples / total_samples
    if known_ratio < args.min_known_sample_ratio:
        parser.error(
            "only "
            f"{known_samples}/{total_samples} call chains ({known_ratio:.2%}) "
            "contain a symbolized frame; retain perf.data/perf.script.gz and "
            "use the captured symfs or a matching symbol build"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.folded is not None:
        write_folded(stacks, args.folded)
    root, max_depth = build_tree(stacks)
    render_html(root, max_depth, args.output, args.title)
    print(
        f"samples={root.count} known_samples={known_samples} "
        f"known_ratio={known_ratio:.6f} unique_stacks={len(stacks)} "
        f"max_depth={max_depth}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
