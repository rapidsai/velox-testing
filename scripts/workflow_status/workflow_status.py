#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Workflow run status analyser.

Accepts a GitHub Actions workflow ``--run-id``, inspects every job in
that run, and generates a status table plus detailed failure analysis
(stacktrace, AI root-cause / fix, upstream issues).  Optionally sends
the report to Slack via ``--slack``.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from lib.ai import TokenTracker, analyze_block
from lib.config import Config, load_from_args
from lib.formatting import (
    EMOJI_HOURGLASS,
    OutputBuffer,
    build_job_table,
    format_failure_detail,
    status_emoji,
)
from lib.gh import detect_repo, fetch_job_log, fetch_jobs, fetch_run
from lib.logs import (
    extract_relevant_failures,
    filter_log_for_job,
    pick_display_snippet,
    split_into_blocks,
)
from lib.similarity import (
    ERROR_SIMILARITY_THRESHOLD,
    compute_error_tokens,
    error_similarity,
    group_similar_blocks,
)
from lib.slack import build_payload, send_webhook
from lib.upstream import classify_repos, find_related_github_items

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Workflow run status analyser — inspects every job in a GitHub "
            "Actions workflow run, builds a status table, and produces detailed "
            "failure analysis (stacktrace, AI cause/fix, upstream issues/PRs)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
environment variables:
  REPO=owner/repo              repo to query (default: auto-detect via gh)
  SLACK_WEBHOOK_URL=url        Slack webhook URL (required when --slack is set)
  LOG_TAIL_LINES=N             log tail lines per failure (default: 150)
  GH_RETRIES=N                 gh CLI retry count (default: 5)
  GH_RETRY_SLEEP_SECONDS=N     initial retry backoff in seconds (default: 2)
  GH_HTTP_TIMEOUT=N            gh HTTP timeout in seconds (default: 60)

  CLAUDE_BIN                   Claude CLI binary (default: claude)
  CLAUDE_MODEL                 Claude model to use (default: opus)
  ANTHROPIC_API_KEY            API key for Claude analysis

examples:
  python %(prog)s --run-id 23729840879
  python %(prog)s --run-id 23729840879 --job-id 12345678
  python %(prog)s --run-id 23729840879 --slack --output report.json
  python %(prog)s --run-id 23729840879 --no-cause --no-fix
""",
    )
    p.add_argument("--run-id", type=int, required=True, help="GitHub Actions workflow run ID to analyse")
    p.add_argument("--job-id", type=int, default=None, help="analyse only this specific job ID within the run")
    p.add_argument("--repo", default="", help="GitHub repo (owner/repo). Auto-detected if omitted")
    p.add_argument("--output", default="", help="write Slack Block Kit JSON payload to this file")
    p.add_argument("--slack", action="store_true", default=False, help="send the report to Slack via SLACK_WEBHOOK_URL")
    p.add_argument("--print-logs", action="store_true", default=False, help="print failed log tails for each failure")
    p.add_argument("--no-cause", action="store_true", default=False, help="disable AI cause analysis")
    p.add_argument("--no-fix", action="store_true", default=False, help="disable AI fix suggestions")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Per-job failure processing
# ---------------------------------------------------------------------------


def _get_failed_steps(job: dict) -> list[dict]:
    return [s for s in job.get("steps", []) if s.get("conclusion", "") not in ("success", "skipped")]


def _process_failed_job(
    job: dict,
    idx: int,
    log_out: str | None,
    run_url: str,
    wf_name: str,
    config: Config,
    tracker: TokenTracker,
) -> tuple[str, list[tuple[str, str, str]]]:
    """Analyse a single failed job.

    Returns ``(formatted_mrkdwn, [(stacktrace, cause, fix), ...])``.
    """
    job_name = job.get("name", "unknown")
    failed_steps = _get_failed_steps(job)

    job_log_content = ""
    if log_out:
        job_log_content = extract_relevant_failures(filter_log_for_job(log_out, job_name))

    related_items = ""
    if config.analyze_cause and job_log_content.strip():
        repos = classify_repos(job_name)
        repo = repos[0] if repos else ""
        try:
            since_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        except Exception:
            since_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        related_items = find_related_github_items(
            repo,
            job_log_content,
            since_date,
            config,
            prefix="    \u2022 ",
        )

    stacktraces: list[tuple[str, str, str]] = []

    if not job_log_content.strip():
        stacktraces.append(("(no logs available)", "Unable to fetch logs for this job", "Check the run link above"))
    else:
        blocks = split_into_blocks(job_log_content)
        block_groups = group_similar_blocks(blocks)
        rep_indices = [grp[0] for grp in block_groups]

        ai_results: dict[int, tuple[str, str, str]] = {}
        if config.analyze_cause and rep_indices:
            with ThreadPoolExecutor(max_workers=config.max_ai_workers) as ai_pool:
                futs = {
                    ai_pool.submit(
                        analyze_block,
                        blocks[bidx],
                        job_name,
                        wf_name,
                        related_items,
                        config,
                        tracker,
                    ): bidx
                    for bidx in rep_indices
                }
                for fut in as_completed(futs):
                    bidx = futs[fut]
                    try:
                        ai_results[bidx] = fut.result()
                    except Exception:
                        ai_results[bidx] = ("", "Unable to determine cause", "Pending investigation")

        for grp in block_groups:
            rep_bidx = grp[0]
            block = blocks[rep_bidx]
            ai_st, cause, fix = ai_results.get(rep_bidx, ("", "", ""))
            display_st = ai_st if ai_st else pick_display_snippet(block)
            stacktraces.append((display_st, cause, fix))

            if len(grp) > 1:
                tracker.record_dedup_skip(len(grp) - 1)

    detail = format_failure_detail(
        idx=idx,
        job=job,
        failed_steps=failed_steps,
        stacktraces=stacktraces,
        related_items=related_items,
        run_url=run_url,
        analyze_cause=config.analyze_cause,
        analyze_fix=config.analyze_fix,
    )
    return detail, stacktraces


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    if not shutil.which("gh"):
        print("ERROR: missing required command: gh", file=sys.stderr)
        sys.exit(2)

    config = load_from_args(args)

    if config.analyze_cause and not shutil.which(config.claude_bin):
        print(
            f"ERROR: missing required command: {config.claude_bin} (needed for AI analysis; use --no-cause to skip)",
            file=sys.stderr,
        )
        sys.exit(2)
    if not config.repo:
        config.repo = detect_repo(config)
    if not config.repo:
        print("ERROR: could not determine repo. Set --repo or REPO env var.", file=sys.stderr)
        sys.exit(2)

    tracker = TokenTracker()
    out = OutputBuffer()

    # ---- Fetch run metadata ------------------------------------------------
    run = fetch_run(config.run_id, config)
    if not run:
        print(f"ERROR: could not fetch run {config.run_id}", file=sys.stderr)
        sys.exit(1)

    run_url = run.get("url", "")
    wf_name = run.get("workflowName", "")
    conclusion = run.get("conclusion", "")
    status = run.get("status", "")

    # ---- Fetch jobs --------------------------------------------------------
    jobs = fetch_jobs(config.run_id, config)
    if not jobs:
        print(f"ERROR: no jobs found for run {config.run_id}", file=sys.stderr)
        sys.exit(1)

    if config.job_id:
        jobs = [j for j in jobs if j.get("databaseId") == config.job_id]
        if not jobs:
            print(f"ERROR: job {config.job_id} not found in run {config.run_id}", file=sys.stderr)
            sys.exit(1)

    # ---- Header ------------------------------------------------------------
    emoji = status_emoji(conclusion, status)
    out.print(f"*\U0001f50d Workflow Run Status — {wf_name}*")
    out.print()
    out.print(f"\u2022 *Run:* {run_url}")
    out.print(f"\u2022 *Status:* {status}")
    out.print(f"\u2022 *Conclusion:* {conclusion} {emoji}")
    out.print()

    # ---- Status table ------------------------------------------------------
    out.print("*Status Summary:*")
    out.print()
    out.print("```")
    for line in build_job_table(jobs):
        out.print(line)
    out.print("```")

    # ---- Identify failed / in-progress / skipped jobs ----------------------
    failed_jobs = [
        j for j in jobs if j.get("status") == "completed" and j.get("conclusion", "") not in ("success", "skipped")
    ]
    inprog_jobs = [j for j in jobs if j.get("status") != "completed"]

    # ---- Failure details ---------------------------------------------------
    out.print()
    out.print("---")
    out.print()

    if not failed_jobs and not inprog_jobs:
        out.print("*\u2705 All jobs passed.*")
    elif not failed_jobs:
        out.print(f"*{EMOJI_HOURGLASS} No failures — {len(inprog_jobs)} job(s) still in progress.*")
        for j in inprog_jobs:
            out.print(f"  \u2022 `{j.get('name', 'unknown')}` — {j.get('status', 'unknown')}")
    else:
        out.print(f"*\U0001f534 Failure Details ({len(failed_jobs)} failed job(s)):*")

        # Fetch logs per-job in parallel. Logs are needed for the stacktrace
        # display itself, similarity-based grouping, and (optionally) AI
        # cause/fix analysis -- so we always fetch when there are failures,
        # regardless of --no-cause / --no-fix / --print-logs.
        #
        # We use per-job ``gh run view --job <id> --log`` because the
        # run-level ``--log[-failed]`` path is rejected for matrices large
        # enough to trip gh's safety throttle:
        #     too many API requests needed to fetch logs;
        #     try narrowing down to a specific job with the --job option
        # which used to leave every failure stuck on "(no logs available)".
        job_log_by_id: dict[int, str] = {}
        with ThreadPoolExecutor(max_workers=config.max_gh_workers) as log_pool:
            log_futs = {
                log_pool.submit(fetch_job_log, j.get("databaseId"), config): j
                for j in failed_jobs
                if j.get("databaseId")
            }
            for fut in as_completed(log_futs):
                j = log_futs[fut]
                try:
                    job_log_by_id[j["databaseId"]] = fut.result() or ""
                except Exception as exc:
                    print(
                        f"WARN: failed to fetch log for job {j.get('databaseId')} ({j.get('name', '?')}): {exc}",
                        file=sys.stderr,
                    )
                    job_log_by_id[j["databaseId"]] = ""

        # Per-job extracted failure lines, used downstream for similarity
        # grouping. Each ``jlog`` string already covers a single job, so
        # ``filter_log_for_job`` is a cheap no-op safety net.
        job_logs: list[str] = []
        for job in failed_jobs:
            jname = job.get("name", "unknown")
            raw = job_log_by_id.get(job.get("databaseId", -1), "")
            jlog = extract_relevant_failures(filter_log_for_job(raw, jname)) if raw else ""
            job_logs.append(jlog)

        job_token_sets = [compute_error_tokens(jl) for jl in job_logs]

        error_groups: list[list[int]] = []
        for i, tokens in enumerate(job_token_sets):
            placed = False
            for grp in error_groups:
                if error_similarity(tokens, job_token_sets[grp[0]]) >= ERROR_SIMILARITY_THRESHOLD:
                    grp.append(i)
                    placed = True
                    break
            if not placed:
                error_groups.append([i])

        # Process representative jobs concurrently
        display_idx = 0
        results_map: dict[int, str] = {}

        with ThreadPoolExecutor(max_workers=config.max_gh_workers) as pool:
            fut_map: dict = {}
            for member_indices in error_groups:
                rep_idx = member_indices[0]
                display_idx += 1
                rep_job = failed_jobs[rep_idx]
                rep_log = job_log_by_id.get(rep_job.get("databaseId", -1), "") or None
                fut = pool.submit(
                    _process_failed_job,
                    rep_job,
                    display_idx,
                    rep_log,
                    run_url,
                    wf_name,
                    config,
                    tracker,
                )
                fut_map[fut] = (display_idx, member_indices)

            for fut in as_completed(fut_map):
                didx, member_indices = fut_map[fut]
                try:
                    detail_text, _ = fut.result()
                except Exception as exc:
                    detail_text = f"\n*{didx}. (error processing failure: {exc})*\n"

                dup_indices = member_indices[1:]
                if dup_indices:
                    tracker.record_dedup_skip(len(dup_indices))
                    dup_buf = OutputBuffer()
                    dup_buf.print()
                    dup_buf.print("  _Same error also appears in:_")
                    for di in dup_indices:
                        dup_job = failed_jobs[di]
                        dup_name = dup_job.get("name", "unknown")
                        dup_job_id = dup_job.get("databaseId", "")
                        if dup_job_id and run_url:
                            dup_url = f"{run_url}/job/{dup_job_id}"
                            dup_buf.print(f"  \u2022 `{dup_name}` \u2192 {dup_url}")
                        else:
                            dup_buf.print(f"  \u2022 `{dup_name}`")
                    detail_text += dup_buf.text()

                results_map[didx] = detail_text

        first = True
        for didx in sorted(results_map):
            if not first:
                out.print()
                out.print("---")
            first = False
            out.print(results_map[didx], end="")

        # In-progress jobs (if any alongside failures)
        if inprog_jobs:
            out.print()
            out.print("---")
            out.print()
            out.print(f"*{EMOJI_HOURGLASS} In-Progress Jobs ({len(inprog_jobs)}):*")
            for j in inprog_jobs:
                out.print(f"  \u2022 `{j.get('name', 'unknown')}` — {j.get('status', 'unknown')}")

    # ---- Output ------------------------------------------------------------
    out.flush_to_stdout()

    if config.output:
        out.flush_to_file(config.output)

    if config.slack:
        payload = build_payload(out.text())
        send_webhook(payload, config.slack_webhook_url)

    token_summary = tracker.summary(config)
    if token_summary:
        print(f"\n{token_summary}", file=sys.stderr)


if __name__ == "__main__":
    main()
