#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Attribute results of the three-arm upstream-suite run (presto/scripts/run_native_e2e_3arm.sh).

    <root>/control/     cuDF off                    Prestissimo baseline
    <root>/gpu/         cuDF on, fallback on        what a user gets
    <root>/gpu-strict/  cuDF on, fallback off       what runs fully on GPU

Per test method:
    GPU failure         passes in control, fails in gpu      -> fails even with CPU fallback allowed
    fails in both       fails in gpu AND control             -> not attributable to the
                                                                GPU; Prestissimo gap or
                                                                harness problem, NOT a
                                                                GPU bug
    runs fully on GPU   passes in gpu-strict                 -> no CPU operator besides
                                                                the allowlisted exchange
    needs CPU fallback  passes in gpu, fails in gpu-strict   -> some operator has no GPU
                                                                path yet (coverage gap)

Headline metric: parity = passing(gpu) / passing(control).

The worker logs saved next to each suite log give the per-operator fallback
histogram for the gpu arm (the operator names in `cuDF fallback:` lines).

    python3 presto/testing/native_e2e/compare_arms.py --root <results dir> [--json out.json]
"""

from __future__ import annotations

import argparse
import collections
import gzip
import json
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

# Bump when the JSON layout changes.
SCHEMA_VERSION = 1

ROOT = None  # --root is required
ARMS = ("control", "gpu", "gpu-strict")

# Surefire failure lines, both shapes:
#   [ERROR]   TestX>AbstractTestY.testFoo:123 ...
#   [ERROR]   TestX.testFoo:123 ...
FAILURE = re.compile(r"^\[ERROR\]\s+(\w+)(?:>\w+)?\.(test\w*)(?::\d+)?\b", re.MULTILINE)
SUMMARY = re.compile(r"Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+)")
# Emitted by velox ToCudf.cpp for every operator left on the CPU that is not
# allowlisted; the allowlisted ones log "stays on CPU (listed in" at INFO.
FALLBACK_LINE = re.compile(r"cuDF fallback: task (\S+) operator (\w+)\[")
ALLOWLISTED_LINE = re.compile(r"cuDF: task (\S+) operator (\w+)\[\d+\] stays on CPU")
# Pre-feature worker builds only print this; kept so an old image still yields
# a histogram.
LEGACY_FALLBACK_LINE = re.compile(r"Replacement Failed Operator: (\w+)\[")


# Data-provider tests carry their arguments in the test name, e.g.
# "testSemiJoin[Session{queryId=2026..., startTime=...}](2)"; the arguments carry
# per-run ids, so keep only the method and invocation index.
PARAMS = re.compile(r"\[.*\]", re.DOTALL)


def parse_suite(path: pathlib.Path) -> tuple[dict, set[str]]:
    """(totals, failing tests) for one suite, from its Surefire XML or, failing that, its Maven log.

    The XML names every invocation, so a method that fails for several data-provider rows counts
    once per row, as in Surefire's own "Tests run" line. The log names failures per method only,
    which overstates passes; it is kept as a fallback for runs without the XML.
    """
    xml = path.with_name(path.stem + ".surefire.xml")
    if xml.exists():
        failing = set()
        run = 0
        for case in ET.parse(xml).getroot().iter("testcase"):
            name = PARAMS.sub("", case.get("name", ""))
            if not name:
                continue
            run += 1
            if case.find("failure") is not None or case.find("error") is not None:
                failing.add(name)
        return {"run": run, "failures": len(failing), "errors": 0, "skipped": 0}, failing
    if not path.exists():
        return {}, set()
    text = path.read_text(errors="replace")
    totals: dict = {}
    m = SUMMARY.findall(text)
    if m:
        run, fail, err, skip = (int(x) for x in m[-1])
        totals = {"run": run, "failures": fail, "errors": err, "skipped": skip}
    failing = {method for _cls, method in FAILURE.findall(text)}
    return totals, failing


def test_matrix(root: pathlib.Path, suite: str) -> dict[str, dict]:
    """{test: {arm: pass|fail|skip, "time": {arm: seconds}}} from the arms' Surefire XML.

    This is what the viewer renders from when it loads a run's summary.json
    instead of the (much larger) XML; the XML is fetched only for a drill-down.
    """
    matrix: dict[str, dict] = {}
    for arm in ARMS:
        xml = root / arm / f"{suite}.surefire.xml"
        if not xml.exists():
            continue
        for case in ET.parse(xml).getroot().iter("testcase"):
            name = PARAMS.sub("", case.get("name", ""))
            if not name:
                continue
            if case.find("skipped") is not None:
                status = "skip"
            elif case.find("failure") is not None or case.find("error") is not None:
                status = "fail"
            else:
                status = "pass"
            entry = matrix.setdefault(name, {"time": {}})
            entry[arm] = status
            entry["time"][arm] = round(entry["time"].get(arm, 0.0) + float(case.get("time") or 0), 3)
    return matrix


def fallback_histogram(worker_dir: pathlib.Path) -> tuple[collections.Counter, collections.Counter]:
    """(declined operators, allowlisted operators) counted over a suite's worker logs."""
    declined: collections.Counter = collections.Counter()
    allowlisted: collections.Counter = collections.Counter()
    if not worker_dir.exists():
        return declined, allowlisted
    for f in sorted(worker_dir.iterdir()):
        opener = gzip.open if f.suffix == ".gz" else open
        with opener(f, "rt", errors="replace") as fh:
            text = fh.read()
        tagged = FALLBACK_LINE.findall(text)
        if tagged:
            declined.update(op for _task, op in tagged)
        else:
            declined.update(LEGACY_FALLBACK_LINE.findall(text))
        allowlisted.update(op for _task, op in ALLOWLISTED_LINE.findall(text))
    return declined, allowlisted


def parse_provenance(path: pathlib.Path) -> dict:
    """PROVENANCE.txt: 'key: value' lines plus 'label=value' lines from docker."""
    prov: dict = {}
    if not path.exists():
        return prov
    for line in path.read_text().splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
        elif "=" in line:
            key, value = line.split("=", 1)
        else:
            continue
        prov[key.strip()] = value.strip()
    return prov


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=pathlib.Path, required=True, help="results dir written by run_native_e2e_3arm.sh")
    ap.add_argument("--json", type=pathlib.Path, help="also write a machine-readable summary")
    args = ap.parse_args()
    root: pathlib.Path = args.root

    suites = sorted({p.stem for arm in ARMS for p in (root / arm).glob("*.log")}) if root.exists() else []
    if not suites:
        print(f"No suite logs under {root}", file=sys.stderr)
        return 1

    report: dict = {
        "schema": SCHEMA_VERSION,
        "run_id": root.name,
        "root": str(root),
        "arms": list(ARMS),
        "provenance": parse_provenance(root / "PROVENANCE.txt"),
        "suites": {},
        "totals": {},
    }
    report["started_at"] = report["provenance"].get("date", "")
    tot = collections.Counter()

    hdr = f"{'suite':<46} {'control':>9} {'gpu':>9} {'strict':>9}   {'GPUfail':>8} {'both':>5} {'fullGPU':>7} {'needFB':>6} {'parity':>7}"
    print(hdr)
    print("-" * len(hdr))
    for suite in suites:
        totals, fails = {}, {}
        for arm in ARMS:
            totals[arm], fails[arm] = parse_suite(root / arm / f"{suite}.log")

        have_control = bool(totals["control"])
        have_gpu = bool(totals["gpu"])
        have_strict = bool(totals["gpu-strict"])
        run = totals["gpu"].get("run") or totals["control"].get("run") or 0

        gpu_specific = (fails["gpu"] - fails["control"]) if (have_gpu and have_control) else set()
        both = (fails["gpu"] & fails["control"]) if (have_gpu and have_control) else set()
        fully_gpu = (run - len(fails["gpu-strict"])) if have_strict else None
        fb_dep = (fails["gpu-strict"] - fails["gpu"]) if (have_gpu and have_strict) else set()

        pass_ctrl = run - len(fails["control"]) if have_control else None
        pass_gpu = run - len(fails["gpu"]) if have_gpu else None
        parity = (pass_gpu / pass_ctrl) if (pass_ctrl and pass_gpu is not None) else None

        declined, allowlisted = fallback_histogram(root / "gpu" / f"{suite}.worker")

        def cell(arm):
            t = totals[arm]
            return f"{len(fails[arm])}/{t['run']}" if t else "—"

        print(
            f"{suite:<46} {cell('control'):>9} {cell('gpu'):>9} {cell('gpu-strict'):>9}   "
            f"{len(gpu_specific):>8} {len(both):>5} "
            f"{(str(fully_gpu) if fully_gpu is not None else '—'):>7} {len(fb_dep):>6} "
            f"{(f'{parity:.0%}' if parity is not None else '—'):>7}"
        )

        report["suites"][suite] = {
            "run": run,
            "failures": {arm: sorted(fails[arm]) for arm in ARMS},
            "totals": totals,
            "gpu_specific": sorted(gpu_specific),
            "fails_in_both": sorted(both),
            "fully_on_gpu": fully_gpu,
            "fallback_dependent": sorted(fb_dep),
            "parity": parity,
            "fallback_operators": dict(declined.most_common()),
            "allowlisted_operators": dict(allowlisted.most_common()),
            "tests": test_matrix(root, suite),
        }
        tot["run"] += run
        if pass_ctrl is not None:
            tot["pass_control"] += pass_ctrl
        if pass_gpu is not None:
            tot["pass_gpu"] += pass_gpu
        if fully_gpu is not None:
            tot["fully_gpu"] += fully_gpu
        tot["gpu_specific"] += len(gpu_specific)
        tot["both"] += len(both)
        tot["fb_dep"] += len(fb_dep)

    print()
    print("=" * len(hdr))
    parity = tot["pass_gpu"] / tot["pass_control"] if tot["pass_control"] else None
    print(
        f"tests: {tot['run']}   passing: control {tot['pass_control']}, gpu {tot['pass_gpu']}, fully-on-GPU {tot['fully_gpu']}"
    )
    print(f"PARITY (gpu passes / control passes): {parity:.1%}" if parity is not None else "PARITY: n/a")
    print(f"GPU failures (pass control, fail gpu w/ fallback):  {tot['gpu_specific']}")
    print(f"Fails in all arms (Prestissimo or test issue):      {tot['both']}")
    print(f"Needs CPU fallback (pass gpu, fail gpu-strict):      {tot['fb_dep']}")
    print("Counts are unique test methods; Surefire's 'Failures: N' counts lines.")
    print("=" * len(hdr))
    report["totals"] = {**tot, "parity": parity}

    print("\nGPU failures — fail with cuDF on even though CPU fallback is allowed:")
    for suite in suites:
        for t in report["suites"][suite]["gpu_specific"]:
            print(f"  {suite:<44} {t}")

    print("\nNeeds CPU fallback — pass on gpu, fail on gpu-strict (operators without a GPU path):")
    for suite in suites:
        for t in report["suites"][suite]["fallback_dependent"][:8]:
            print(f"  {suite:<44} {t}")
        n = len(report["suites"][suite]["fallback_dependent"])
        if n > 8:
            print(f"  {suite:<44} … and {n - 8} more")

    print("\nOperators that fell back to CPU in the gpu arm (events per suite):")
    for suite in suites:
        ops = report["suites"][suite]["fallback_operators"]
        if ops:
            print(f"  {suite:<44} " + ", ".join(f"{k}={v}" for k, v in ops.items()))

    print("\nFails in all arms — Prestissimo or test issue; not a GPU bug:")
    for suite in suites:
        b = report["suites"][suite]["fails_in_both"]
        for t in b[:5]:
            print(f"  {suite:<44} {t}")
        if len(b) > 5:
            print(f"  {suite:<44} … and {len(b) - 5} more")

    if args.json:
        args.json.write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
