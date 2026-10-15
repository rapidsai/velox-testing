#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assemble the static results site the viewer serves from.

Input is a directory of run directories as produced by run_native_e2e_3arm.sh
(one per run, named by run id), typically unpacked CI artifacts:

    <runs_dir>/2026-10-15-nightly/{control,gpu,gpu-strict}/<Suite>.surefire.xml ...

Output:

    <site>/index.html                 the viewer
    <site>/manifest.json              every run: id, summary, XML files, provenance
    <site>/runs/<id>/summary.json     from compare_arms.py --json (built if absent)
    <site>/runs/<id>/<arm>/*.surefire.xml   copied verbatim
    <site>/runs/<id>/{COMPARE.txt,PROVENANCE.txt}

The viewer renders from the summaries and fetches a run's XML only for a
suite drill-down, so the page stays fast however many runs are published.

    python3 build_site.py --runs-dir artifacts/ --site site/
"""

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
ARMS = ("control", "gpu", "gpu-strict")


def provenance(run_dir: pathlib.Path) -> dict:
    prov: dict = {}
    path = run_dir / "PROVENANCE.txt"
    if path.exists():
        for line in path.read_text().splitlines():
            sep = ": " if ": " in line else "=" if "=" in line else None
            if sep:
                key, value = line.split(sep, 1)
                prov[key.strip()] = value.strip()
    return prov


def publish_run(run_dir: pathlib.Path, site: pathlib.Path) -> dict | None:
    xml_files = sorted(p for arm in ARMS for p in (run_dir / arm).glob("*.surefire.xml"))
    if not xml_files:
        print(f"  {run_dir.name}: no Surefire XML, skipped", file=sys.stderr)
        return None
    dest = site / "runs" / run_dir.name
    for src in xml_files:
        (dest / src.parent.name).mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest / src.parent.name / src.name)
    for name in ("COMPARE.txt", "PROVENANCE.txt"):
        if (run_dir / name).exists():
            shutil.copy2(run_dir / name, dest / name)
    summary = dest / "summary.json"
    if (run_dir / "summary.json").exists():
        shutil.copy2(run_dir / "summary.json", summary)
    else:
        subprocess.run(
            [sys.executable, str(HERE / "compare_arms.py"), "--root", str(run_dir), "--json", str(summary)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    rel = lambda p: str(p.relative_to(site))  # noqa: E731
    return {
        "id": run_dir.name,
        "summary": rel(summary),
        "files": [rel(dest / p.parent.name / p.name) for p in xml_files],
        "meta": provenance(run_dir),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--runs-dir", type=pathlib.Path, required=True, help="directory containing one run directory per run"
    )
    ap.add_argument("--site", type=pathlib.Path, required=True, help="output directory (created)")
    ap.add_argument("--viewer", type=pathlib.Path, default=HERE / "viewer.html")
    args = ap.parse_args()

    args.site.mkdir(parents=True, exist_ok=True)
    shutil.copy2(args.viewer, args.site / "index.html")
    runs = [r for d in sorted(p for p in args.runs_dir.iterdir() if p.is_dir()) if (r := publish_run(d, args.site))]
    (args.site / "manifest.json").write_text(json.dumps({"runs": runs}, indent=1))
    print(f"site: {args.site} — {len(runs)} run(s): {', '.join(r['id'] for r in runs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
