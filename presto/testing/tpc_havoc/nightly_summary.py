#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""One Markdown (and Slack) summary of one or more TPC-Havoc runs, e.g. CPU, GPU, GPU without CPU fallback.

The Markdown is printed to stdout (so it ends the CI log), appended to $GITHUB_STEP_SUMMARY and
optionally written to a file. It has:

  * a table per engine: queries that ran OK, wrong results, errors, new and known failures;
  * failures grouped by reason, new failures first: the known-issue reason, else the velox-cudf
    reason from the worker log, else the error. Each group lists the engines and queries, the
    error, the worker log reason, a command to reproduce it, the minimal repro SQL from
    known_issues.yaml if there is one, and the SQL of one query.

--slack-payload writes a short incoming-webhook message: the table, new failures with their
repro commands, and known issues that now pass. run_tpc_havoc.py writes each run's summary.md
with the same code. Standard library only, so it runs without the runner's virtual environment.

Example:
    python nightly_summary.py --run CPU=out/cpu --run GPU=out/gpu --run "GPU no fallback=out/gpu-no-fallback" \\
        --title "TPC-Havoc SF10" --run-url URL --slack-payload slack.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from html import escape
from pathlib import Path

KNOWN_ISSUES_FILE = "presto/testing/tpc_havoc/known_issues.yaml"
MAX_SLACK_GROUPS = 10  # new-failure reasons listed in the Slack message; the rest are in the run summary
START_SCRIPTS = {
    "cpu": "start_native_cpu_presto.sh",
    "java": "start_java_presto.sh",
    "gpu-no-fallback": "start_native_gpu_presto.sh --no-cpu-fallback",
}


def load_run(label: str, run_dir: Path) -> dict:
    """A run's summary.json; a run that wrote none (it stopped early) is kept and reported as missing."""
    path = Path(run_dir) / "summary.json"
    run = json.loads(path.read_text()) if path.exists() else {"missing": True, "queries": []}
    return {**run, "label": label, "dir": Path(run_dir)}


def one_line(text, limit: int = 200) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def normalize_error(message, limit: int = 200) -> str:
    """First line of an error without directories, ids and numbers, so one error in several queries groups together."""
    first = re.sub(r"(?:/[\w.+-]+)+/", "", (message or "").strip().split("\n")[0])
    return one_line(re.sub(r"\d+(?:\.\d+)?", "N", re.sub(r"\b[0-9a-f]{8,}\b", "ID", first)), limit)


def failure_label(record: dict, check: str) -> str:
    """What a failure is grouped by: its known-issue reason; else, for errors, the velox-cudf reason
    category from the worker log or the normalized error; else "Wrong result vs <check>"."""
    known = record["checks"][check].get("known_issue")
    if isinstance(known, str) and known:
        return known
    if check != "execution":
        return f"Wrong result vs {check}"
    error = f"{record.get('error_name')}: {normalize_error(record.get('error_message'))}"
    return record.get("worker_reason_category") or error


def engine_counts(run: dict) -> dict:
    queries = run["queries"]
    return {
        "ok": sum(q["status"] == "ok" for q in queries),
        "wrong": sum(q["status"] == "ok" and any(not f["known"] for f in q.get("failures", [])) for q in queries),
        "errors": sum(q["status"] in ("error", "timeout") for q in queries),
        "new": sum(bool(q.get("has_new_failure")) for q in queries),
        "known": sum(bool(q.get("failures")) and not q.get("has_new_failure") for q in queries),
    }


def group_failures(runs: list[dict]) -> list[dict]:
    """Failures of all runs grouped by failure_label: new first, then wrong results, then by size."""
    groups: dict[str, dict] = {}
    for run in runs:
        for record in run["queries"]:
            for failure in record.get("failures", []):
                label = failure_label(record, failure["check"])
                group = groups.setdefault(
                    label,
                    {
                        "label": label,
                        "new": False,
                        "wrong_result": False,
                        "engines": defaultdict(list),
                        "example": None,
                    },
                )
                group["new"] |= not failure["known"]
                group["wrong_result"] |= failure["check"] != "execution"
                if record["query_id"] not in group["engines"][run["label"]]:
                    group["engines"][run["label"]].append(record["query_id"])
                group["example"] = group["example"] or (run, record, failure["check"])
    return sorted(
        groups.values(),
        key=lambda g: (not g["new"], not g["wrong_result"], -sum(map(len, g["engines"].values())), g["label"]),
    )


def repro_command(run: dict, query_id: str, check: str = "execution") -> str:
    engine = run.get("engine", "unknown")
    start = START_SCRIPTS.get(engine, "start_native_gpu_presto.sh")
    schema = run.get("connection", {}).get("schema", "SCHEMA")
    extra = {"duckdb": " --duckdb-data-dir PARQUET_DIR", "reference": " --reference-dir CPU_RUN_DIR"}.get(check, "")
    return f"presto/scripts/{start} && presto/scripts/run_tpc_havoc.sh -s {schema} -e {engine} -q {query_id}{extra}"


def _sql(run: dict, query_id: str) -> str:
    path = Path(run["dir"]) / "queries" / "presto" / f"{query_id}.sql"
    return path.read_text().strip() if path.exists() else f"-- {path} not found"


def _code(text: str) -> str:
    return "`" + text.replace("`", "'") + "`"


def _status(runs: list[dict], groups: list[dict]) -> tuple[list[str], bool, str]:
    """(runs without results, any new failure, known issues that now pass as "Q (engines), ...")."""
    fixed: dict[str, list[str]] = defaultdict(list)
    for run in runs:
        for query_id in sorted({u["query_id"] for u in run.get("unexpected_passes", [])}):
            fixed[query_id].append(run["label"])
    passing = ", ".join(f"{query_id} ({', '.join(labels)})" for query_id, labels in fixed.items())
    return [r["label"] for r in runs if r.get("missing")], any(g["new"] for g in groups), passing


def render_markdown(runs: list[dict], title: str, run_url: str | None = None) -> str:
    groups = group_failures(runs)
    missing, has_new, passing = _status(runs, groups)
    lines = [f"## {escape(title)}", ""] + ([f"Run: {run_url}", ""] if run_url else [])
    lines += [
        "| Engine | Ran OK | Wrong results | Errors | New failures | Known failures |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for run in runs:
        if run.get("missing"):
            lines.append(f"| {run['label']} | no results | | | | |")
            continue
        c = engine_counts(run)
        new = f"**{c['new']}**" if c["new"] else "0"
        lines.append(
            f"| {run['label']} | {c['ok']} / {len(run['queries'])} | {c['wrong']} | {c['errors']} | {new} | {c['known']} |"
        )
    lines.append("")
    if missing:
        lines.append(f"**No results from {', '.join(missing)}**: the run stopped before writing summary.json.")
    if has_new:
        new = sum(engine_counts(r)["new"] for r in runs)
        lines.append(f"**{new} {'query' if new == 1 else 'queries'} with new failures.**")
    elif not missing:
        lines.append(f"No new failures: every failure is listed in `{KNOWN_ISSUES_FILE}`.")
    if passing:
        lines.append(f"Known issues that now pass (remove them from `{KNOWN_ISSUES_FILE}`): {passing}")
    lines += ["", "### Failures by reason", ""] + ([] if groups else ["None.", ""])
    for number, group in enumerate(groups, 1):
        run, record, check = group["example"]
        outcome = record["checks"][check]
        count = len({q for queries in group["engines"].values() for q in queries})
        status = "NEW" if group["new"] else "known"
        kind = "wrong result" if group["wrong_result"] else "error"
        lines += [
            f"#### {number}. [{status}, {kind}] {escape(group['label'])} ({count} quer{'y' if count == 1 else 'ies'})",
            "",
        ]
        engines_by_queries = defaultdict(list)  # one line for engines that fail the same queries
        for label, query_ids in group["engines"].items():
            engines_by_queries[tuple(query_ids)].append(label)
        lines += [f"- {', '.join(labels)}: {', '.join(ids)}" for ids, labels in engines_by_queries.items()]
        if check == "execution":
            error = f"{record.get('error_name')}: {record.get('error_message')}"
            lines.append(f"- Error: {_code(one_line(error, 300))}")
        else:
            lines.append(f"- Mismatch: {_code(one_line(check + ': ' + str(outcome.get('message')), 300))}")
        if record.get("worker_reason"):
            lines.append(f"- Worker log: {_code(one_line(record['worker_reason'], 300))}")
        if outcome.get("known_issue_link"):
            lines.append(f"- Issue: {outcome['known_issue_link']}")
        lines.append(f"- Reproduce: {_code(repro_command(run, record['query_id'], check))}")
        if outcome.get("known_issue_repro"):
            lines += ["- Minimal repro:", "", "```sql", outcome["known_issue_repro"].strip(), "```"]
        summary = f"{record['query_id']} SQL" + ("" if check == "execution" else " and mismatch")
        lines += ["", f"<details><summary>{summary}</summary>", "", "```sql", _sql(run, record["query_id"]), "```"]
        if check != "execution":
            lines += ["", "```", (outcome.get("message") or "").strip()[:2000], "```"]
        lines += ["", "</details>", ""]
    return "\n".join(lines) + "\n"


def _slack_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render_slack(runs: list[dict], title: str, run_url: str | None = None) -> str:
    """Slack mrkdwn: the table, then only what needs action."""
    groups = group_failures(runs)
    missing, has_new, passing = _status(runs, groups)
    icon = ":red_circle:" if has_new or missing else ":white_check_mark:"
    width = max(len(r["label"]) for r in runs)
    lines = [f"{icon} *{_slack_escape(title)}*" + (f" | <{run_url}|run summary>" if run_url else ""), "", "```"]
    lines.append(f"{'Engine':<{width}}  {'OK':>9}  {'Wrong':>5}  {'Errors':>6}  {'New':>3}")
    for run in runs:
        if run.get("missing"):
            lines.append(f"{run['label']:<{width}}  {'no results':>9}")
            continue
        c = engine_counts(run)
        ok = f"{c['ok']}/{len(run['queries'])}"
        lines.append(f"{run['label']:<{width}}  {ok:>9}  {c['wrong']:>5}  {c['errors']:>6}  {c['new']:>3}")
    lines += ["```", ""]
    if missing:
        lines.append(f"*No results from {', '.join(missing)}* (the run stopped before writing summary.json).")
    if has_new:
        lines.append("*New failures (action needed):*")
        new_groups = [g for g in groups if g["new"]]
        for group in new_groups[:MAX_SLACK_GROUPS]:
            run, record, check = group["example"]
            engines = "; ".join(
                f"{label}: {', '.join(q[:6])}{' ...' if len(q) > 6 else ''}" for label, q in group["engines"].items()
            )
            lines.append(f"• {_slack_escape(one_line(group['label'], 150))} ({engines})")
            lines.append(f"   repro: `{repro_command(run, record['query_id'], check)}`")
        if len(new_groups) > MAX_SLACK_GROUPS:
            lines.append(f"... and {len(new_groups) - MAX_SLACK_GROUPS} more reasons in the run summary.")
    elif not missing:
        lines.append("No new failures.")
    if passing:
        lines.append(f"*Known issues now passing (remove from known_issues.yaml):* {passing}")
    known = sum(not g["new"] for g in groups)
    if known:
        lines.append(f"Known issues: {known} reason{'' if known == 1 else 's'}, details in the run summary.")
    return "\n".join(lines) + "\n"


def slack_payload(text: str) -> dict:
    """Block Kit payload, built with the helper the nightly status report uses."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from workflow_status.lib.slack import build_payload

    return build_payload(text)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", action="append", required=True, metavar="LABEL=DIR", help="Run output dir (repeat)")
    parser.add_argument("--title", default="TPC-Havoc")
    parser.add_argument("--run-url", help="Link to the CI run")
    parser.add_argument("--markdown", help="Also write the Markdown summary to this file")
    parser.add_argument("--slack-payload", help="Write a Slack incoming-webhook payload (JSON) to this file")
    parser.add_argument("--no-step-summary", action="store_true", help="Don't append to $GITHUB_STEP_SUMMARY")
    args = parser.parse_args(argv)

    runs = []
    for spec in args.run:
        label, sep, path = spec.partition("=")
        if not sep:
            parser.error(f"--run expects LABEL=DIR, got {spec!r}")
        runs.append(load_run(label.strip(), Path(path)))
    markdown = render_markdown(runs, args.title, args.run_url)
    print(markdown)
    if args.markdown:
        Path(args.markdown).write_text(markdown)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary and not args.no_step_summary:
        with open(step_summary, "a") as fh:
            fh.write(markdown)
    if args.slack_payload:
        text = render_slack(runs, args.title, args.run_url)
        Path(args.slack_payload).write_text(json.dumps(slack_payload(text), indent=2) + "\n")
        print(f"Slack message ({args.slack_payload}):\n{text}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
