#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Nightly workflow status helper (Python port of check_nightly_status.sh).

Uses ThreadPoolExecutor for concurrent GitHub API calls and AI analysis
to significantly reduce wall-clock time vs the sequential bash version.

Requirements:
  - gh (GitHub CLI) authenticated (e.g. `gh auth login`)
  - Python 3.8+

Usage:
  python scripts/check_nightly_status.py [OPTIONS]

Options:
  --print-logs    Print failed log tails for each failure (default: disabled)
  --slack         Output in Slack-formatted style with mrkdwn
  --cause         Use AI to analyze logs and determine failure cause (default: enabled)
  --no-cause      Disable AI cause analysis
  --fix           Use AI to suggest a fix, implies --cause (default: enabled)
  --no-fix        Disable AI fix suggestions
  --claude        Use Claude CLI for analysis (default: enabled)
  --no-claude     Use NVIDIA LLM instead of Claude for analysis
  --date YYYY-MM-DD  Fetch nightly status for a specific date (default: today UTC)
  -h, --help      Show this help message

If your network is slow, run:
  GH_HTTP_TIMEOUT=180 GH_RETRIES=8 python scripts/check_nightly_status.py

Optional env:
  REPO=owner/repo              (default: current gh repo)
  TODAY_UTC=YYYY-MM-DD         (default: today's UTC date)
  LOG_TAIL_LINES=N             (default: 150)
  STATUS_FILE=path/to/file     (default: status.txt)
  GH_RETRIES=N                 (default: 5)
  GH_RETRY_SLEEP_SECONDS=N     (default: 2, exponential backoff)
  GH_HTTP_TIMEOUT=N            (default: 60)

AI-powered cause/fix analysis (requires --cause or --fix):
  LLM_API_KEY or NVIDIA_API_KEY
  LLM_API_URL                  (ex: https://integrate.api.nvidia.com/v1/chat/completions)
  LLM_MODEL                    (ex: nvdev/nvidia/llama-3.3-nemotron-super-49b-v1)
  LLM_TIMEOUT                  (default: 30)

Claude AI analysis (requires --cause or --fix with --claude):
  CLAUDE_BIN                   (default: claude)
  CLAUDE_MODEL                 (default: opus)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BLOCK_SEP = "__VELOX_FAILURE_BLOCK__"

EMOJI_DASH = "\u2796"  # heavy minus sign
EMOJI_HOURGLASS = "\u23f3"  # hourglass
EMOJI_CHECK = "\u2705"  # check mark
EMOJI_CROSS = "\u274c"  # cross mark

# Max threads for concurrent gh CLI calls.  gh itself rate-limits, so we keep
# this modest to avoid hammering the GitHub API while still getting a big
# speed-up over sequential execution.
MAX_GH_WORKERS = 12
MAX_AI_WORKERS = 4

# ---------------------------------------------------------------------------
# Configuration (populated from args + env in main)
# ---------------------------------------------------------------------------


class Config:
    print_logs: bool = False
    slack_format: bool = True
    analyze_cause: bool = True
    analyze_fix: bool = True
    use_claude: bool = True
    repo: str = ""
    today_utc: str = ""
    display_date: str = ""
    log_tail_lines: int = 150
    status_file: str = "status.txt"
    gh_retries: int = 5
    gh_retry_sleep: int = 2
    gh_http_timeout: int = 60
    llm_api_key: str = ""
    llm_api_url: str = ""
    llm_model: str = ""
    llm_timeout: int = 30
    claude_bin: str = "claude"
    claude_model: str = "opus"


CFG = Config()

# ---------------------------------------------------------------------------
# Row definitions (mirrors bash associative arrays)
# ---------------------------------------------------------------------------

ROW_DEFS: List[Dict[str, str]] = [
    {
        "name": "Velox Build CPU",
        "display": "Velox Build (CPU)",
        "upstream": "velox-nightly-upstream.yml",
        "staging": "velox-nightly-staging.yml",
        "stable": "",
        "job_filter": "cpu",
    },
    {
        "name": "Velox Build GPU",
        "display": "Velox Build (GPU)",
        "upstream": "velox-nightly-upstream.yml",
        "staging": "velox-nightly-staging.yml",
        "stable": "",
        "job_filter": "gpu",
    },
    {
        "name": "Velox Benchmark",
        "display": "Velox Benchmark",
        "upstream": "",
        "staging": "velox-benchmark-nightly-staging.yml",
        "stable": "",
        "job_filter": "",
    },
    {
        "name": "Presto Java",
        "display": "Presto (Java)",
        "upstream": "presto-nightly-upstream.yml",
        "staging": "presto-nightly-staging.yml",
        "stable": "presto-nightly-pinned.yml",
        "job_filter": "java",
    },
    {
        "name": "Presto CPU",
        "display": "Presto (CPU)",
        "upstream": "presto-nightly-upstream.yml",
        "staging": "presto-nightly-staging.yml",
        "stable": "presto-nightly-pinned.yml",
        "job_filter": "native-cpu",
    },
    {
        "name": "Presto GPU",
        "display": "Presto (GPU)",
        "upstream": "presto-nightly-upstream.yml",
        "staging": "presto-nightly-staging.yml",
        "stable": "presto-nightly-pinned.yml",
        "job_filter": "native-gpu",
    },
]

# ---------------------------------------------------------------------------
# Utility: subprocess / gh CLI with retry
# ---------------------------------------------------------------------------

_TRANSIENT_PATTERNS = re.compile(
    r"TLS handshake timeout|i/o timeout|timeout|temporarily unavailable"
    r"|connection reset|EOF",
    re.IGNORECASE,
)


def run_gh(*args: str, retries: Optional[int] = None, timeout: Optional[int] = None) -> str:
    """Run a gh CLI command with retry on transient errors.

    Returns stdout on success, raises ``RuntimeError`` on persistent failure.
    """
    retries = retries if retries is not None else CFG.gh_retries
    sleep_s = CFG.gh_retry_sleep
    env = {**os.environ, "GH_HTTP_TIMEOUT": str(CFG.gh_http_timeout)}
    cmd = ["gh", *args]

    attempt = 1
    while True:
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout or (CFG.gh_http_timeout + 30),
                env=env,
            )
        except subprocess.TimeoutExpired:
            combined = "subprocess timed out"
        else:
            if proc.returncode == 0:
                return proc.stdout
            combined = (proc.stdout + proc.stderr).strip()

        if _TRANSIENT_PATTERNS.search(combined) and attempt < retries:
            print(
                f"WARN: transient GitHub API error (attempt {attempt}/{retries}), "
                f"retrying in {sleep_s}s...\n      {combined}",
                file=sys.stderr,
            )
            time.sleep(sleep_s)
            attempt += 1
            sleep_s *= 2
            continue

        raise RuntimeError(f"gh command failed: {' '.join(cmd)}\n{combined}")


def run_gh_safe(*args: str, **kwargs) -> Optional[str]:
    """Like ``run_gh`` but returns ``None`` on any failure."""
    try:
        return run_gh(*args, **kwargs)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------

_RUN_LIST_FIELDS = (
    "databaseId,attempt,createdAt,startedAt,updatedAt,conclusion,status,url,workflowName,displayTitle,number"
)


def get_today_run_json(workflow_file: str) -> Optional[dict]:
    """Return the latest run matching today_utc for *workflow_file*, or None."""
    out = run_gh_safe(
        "run",
        "list",
        "-R",
        CFG.repo,
        "--workflow",
        workflow_file,
        "--limit",
        "100",
        "--json",
        _RUN_LIST_FIELDS,
    )
    if not out:
        return None
    try:
        runs = json.loads(out)
    except json.JSONDecodeError:
        return None
    today = CFG.today_utc
    matching = [
        r
        for r in runs
        if (r.get("createdAt") or "").startswith(today)
        or (r.get("startedAt") or "").startswith(today)
        or (r.get("updatedAt") or "").startswith(today)
    ]
    if not matching:
        return None
    matching.sort(key=lambda r: (r.get("createdAt", ""), r.get("attempt", 1)))
    return matching[-1]


def get_latest_run_json(workflow_file: str) -> Optional[dict]:
    out = run_gh_safe(
        "run",
        "list",
        "-R",
        CFG.repo,
        "--workflow",
        workflow_file,
        "--limit",
        "100",
        "--json",
        _RUN_LIST_FIELDS,
    )
    if not out:
        return None
    try:
        runs = json.loads(out)
    except json.JSONDecodeError:
        return None
    if not runs:
        return None
    runs.sort(key=lambda r: (r.get("createdAt", ""), r.get("attempt", 1)))
    return runs[-1]


def get_first_attempt_for_run_number(workflow_file: str, run_number: int) -> Optional[dict]:
    out = run_gh_safe(
        "run",
        "list",
        "-R",
        CFG.repo,
        "--workflow",
        workflow_file,
        "--limit",
        "200",
        "--json",
        _RUN_LIST_FIELDS,
    )
    if not out:
        return None
    try:
        runs = json.loads(out)
    except json.JSONDecodeError:
        return None
    matching = [r for r in runs if r.get("number") == run_number and r.get("attempt", 1) == 1]
    if not matching:
        return None
    matching.sort(key=lambda r: r.get("createdAt", ""))
    return matching[-1]


def _fetch_run(workflow_file: str) -> Tuple[str, Optional[dict]]:
    """Fetch today's run, falling back to latest.  Returns (wf_file, run)."""
    run = get_today_run_json(workflow_file)
    if run is None:
        run = get_latest_run_json(workflow_file)
    return (workflow_file, run)


# ---------------------------------------------------------------------------
# Jobs cache and filtered job status
# ---------------------------------------------------------------------------

_jobs_cache: Dict[int, dict] = {}
_jobs_cache_lock = __import__("threading").Lock()


def _fetch_jobs_json(run_id: int) -> Optional[dict]:
    with _jobs_cache_lock:
        if run_id in _jobs_cache:
            return _jobs_cache[run_id]
    out = run_gh_safe("run", "view", "-R", CFG.repo, str(run_id), "--json", "jobs")
    if not out:
        return None
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return None
    with _jobs_cache_lock:
        _jobs_cache[run_id] = data
    return data


def cell_for_run(run: Optional[dict]) -> str:
    if run is None:
        return EMOJI_DASH
    status = run.get("status", "")
    conclusion = run.get("conclusion", "")
    if status != "completed":
        return EMOJI_HOURGLASS
    if conclusion == "success":
        return EMOJI_CHECK
    return EMOJI_CROSS


def cell_for_filtered_jobs(run: Optional[dict], job_filter: str) -> str:
    if run is None:
        return EMOJI_DASH
    run_id = run.get("databaseId")
    if run_id is None:
        return EMOJI_DASH
    jobs_data = _fetch_jobs_json(run_id)
    if not jobs_data:
        return EMOJI_DASH
    jobs = jobs_data.get("jobs", [])
    filt_lower = job_filter.lower()
    filtered = [j for j in jobs if filt_lower in j.get("name", "").lower()]
    if not filtered:
        return EMOJI_DASH
    for j in filtered:
        if j.get("status") != "completed":
            return EMOJI_HOURGLASS
    for j in filtered:
        c = j.get("conclusion", "")
        if c not in ("success", "skipped"):
            return EMOJI_CROSS
    return EMOJI_CHECK


def _prefetch_jobs_for_runs(runs: List[Optional[dict]]):
    """Fetch jobs JSON for all unique run IDs concurrently."""
    ids = set()
    for r in runs:
        if r and r.get("databaseId"):
            ids.add(r["databaseId"])
    with _jobs_cache_lock:
        ids -= set(_jobs_cache.keys())
    if not ids:
        return
    with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
        list(pool.map(_fetch_jobs_json, ids))


# ---------------------------------------------------------------------------
# Log extraction helpers
# ---------------------------------------------------------------------------

_GH_LOG_PREFIX = re.compile(r"^[^\t]+\t[^\t]+\t")
_TIMESTAMP_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\s?")


def _strip_gh_log_prefix(line: str) -> str:
    """Strip the GitHub Actions job/step/timestamp prefix from a log line."""
    parts = line.split("\t")
    if len(parts) >= 4:
        return "\t".join(parts[3:])
    if len(parts) >= 3:
        return _TIMESTAMP_PREFIX.sub("", parts[2])
    return line


def _strip_log_prefixes(raw: str) -> str:
    return "\n".join(_strip_gh_log_prefix(line) for line in raw.splitlines())


_GTEST_RUN = re.compile(r"^\[ RUN\s+\]")
_GTEST_FAILED = re.compile(r"^\[  FAILED  \]")
_GTEST_OK = re.compile(r"^\[\s+(OK|DISABLED|SKIPPED)\s+\]")
_GTEST_FAILED_NAME = re.compile(r"^\[  FAILED  \] ([A-Z][A-Za-z0-9_]*\.[A-Za-z][A-Za-z0-9_]*)")
_CTEST_SUMMARY = re.compile(r"^\d+%.*tests passed")
_CTEST_FAILED_LIST = re.compile(r"^The following tests FAILED:")

_ERROR_PATTERNS = [
    re.compile(r"[Ee]rror[: \[]"),
    re.compile(r"FAILED"),
    re.compile(r"[Ff]atal"),
    re.compile(r"ninja: build stopped"),
    re.compile(r"make.*\*\*\*"),
    re.compile(r"^failed to solve:"),
    re.compile(r"##\[error\]"),
    re.compile(r"^------$"),
    re.compile(r"^--------------------$"),
    re.compile(r"\.dockerfile:\d+"),
    re.compile(r"^\s*\d+ \|"),
    re.compile(r"^\s*>>>"),
    re.compile(r"undefined reference"),
    re.compile(r"cannot find -l"),
    re.compile(r"[Cc]onfiguring incomplete"),
]


def extract_relevant_failures(raw_log: str, max_fallback: int = 500) -> str:
    """Port of the bash extract_relevant_failures function."""
    content = _strip_log_prefixes(raw_log)
    lines = content.splitlines()

    has_gtest = any(_GTEST_FAILED.match(ln) or _GTEST_RUN.match(ln) for ln in lines)
    if has_gtest:
        blocks = _extract_gtest_blocks(lines)
        if blocks:
            return blocks

    return _extract_error_lines(lines)


def _extract_gtest_blocks(lines: List[str]) -> str:
    """Extract GTest [ RUN ] … [ FAILED ] blocks, CTest summary."""
    result_parts: List[str] = []
    in_block = False
    block_lines: List[str] = []
    seen_tests: set = set()
    in_ctest = False

    for line in lines:
        if _GTEST_RUN.match(line):
            in_block = True
            block_lines = [line]
            continue
        if in_block:
            block_lines.append(line)
            if _GTEST_FAILED.match(line):
                m = _GTEST_FAILED_NAME.match(line)
                if m:
                    seen_tests.add(m.group(1))
                tname = line.lstrip("[  FAILED  ] ").split(" (")[0].strip()
                if tname:
                    seen_tests.add(tname)
                result_parts.append("\n".join(block_lines))
                result_parts.append(BLOCK_SEP)
                in_block = False
                block_lines = []
                continue
            if _GTEST_OK.match(line):
                in_block = False
                block_lines = []
                continue
            continue

        if _GTEST_FAILED.match(line):
            m = _GTEST_FAILED_NAME.match(line)
            tname = m.group(1) if m else ""
            if tname and tname in seen_tests:
                continue
            result_parts.append(line)
            result_parts.append(BLOCK_SEP)
            continue

        if _CTEST_SUMMARY.match(line):
            result_parts.append(line)
            continue
        if _CTEST_FAILED_LIST.match(line):
            in_ctest = True
        if in_ctest:
            result_parts.append(line)
            continue

    return "\n".join(result_parts) if result_parts else ""


def _extract_error_lines(lines: List[str], context_before: int = 5, context_after: int = 3, tail: int = 15) -> str:
    """Extract error-relevant lines with surrounding context."""
    n = len(lines)
    if n == 0:
        return ""
    marked = set()
    for i, line in enumerate(lines):
        if any(p.search(line) for p in _ERROR_PATTERNS):
            for offset in range(max(0, i - context_before), min(n, i + context_after + 1)):
                marked.add(offset)
    for i in range(max(0, n - tail), n):
        marked.add(i)

    if not marked:
        return "\n".join(lines[-30:])

    result: List[str] = []
    prev = -2
    for i in sorted(marked):
        if prev >= 0 and i - prev > 1:
            result.append("...")
        result.append(lines[i])
        prev = i
    return "\n".join(result)


# ---------------------------------------------------------------------------
# Identifier / search-query extraction
# ---------------------------------------------------------------------------

_GTEST_FAILED_NAMES_RE = re.compile(r"(?<=\[  FAILED  \] )[A-Z][A-Za-z0-9_]*\.[A-Za-z][A-Za-z0-9_]*")
_CPP_QUALIFIED = re.compile(r"[A-Z][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)+")
_JAVA_METHOD = re.compile(r"[A-Z][A-Za-z0-9_]*\.[a-z][A-Za-z0-9_]*")
_SOURCE_FILE = re.compile(r"[A-Z][A-Za-z0-9_]+\.(?:cpp|h|cu|cuh|java)")
_ERROR_INDICATOR = re.compile(r"error|undefined|unresolved|FAILED|fatal", re.IGNORECASE)


def extract_gtest_test_names(text: str) -> List[str]:
    return sorted(set(_GTEST_FAILED_NAMES_RE.findall(text)))[:5]


def extract_error_identifiers(text: str) -> List[str]:
    error_lines = [ln for ln in text.splitlines() if _ERROR_INDICATOR.search(ln)]
    blob = "\n".join(error_lines)
    ids: set = set()
    ids.update(_CPP_QUALIFIED.findall(blob)[:5])
    ids.update(_JAVA_METHOD.findall(blob)[:5])
    ids.update(_SOURCE_FILE.findall(blob)[:5])
    return sorted(ids)[:8]


def extract_search_query(text: str) -> str:
    for line in text.splitlines():
        if not line.strip():
            continue
        m = _GTEST_FAILED_NAMES_RE.search(line)
        if m:
            return m.group(0).replace(":", " ")[:120]
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^\[[^\]]*\]\s*", "", line)
        line = line.replace('"', "").replace("'", "")
        return line.replace(":", " ")[:120]
    return ""


def upstream_repo_for_label(label: str) -> str:
    ll = label.lower()
    if "velox" in ll:
        return "facebookincubator/velox"
    if "presto" in ll:
        return "prestodb/presto"
    return ""


# ---------------------------------------------------------------------------
# GitHub search for related issues / PRs (concurrent)
# ---------------------------------------------------------------------------


def _search_gh_items(repo: str, term: str, kind: str, since_date: str) -> List[Tuple[str, str]]:
    """Search GitHub *kind* ('issues' or 'prs') returning [(display, url), ...]."""
    out = run_gh_safe(
        "search",
        kind,
        "--repo",
        repo,
        "--limit",
        "3",
        "--json",
        "title,url,number",
        f"{term} created:>={since_date}",
    )
    if not out:
        return []
    try:
        items = json.loads(out)
    except json.JSONDecodeError:
        return []
    results = []
    for it in items:
        display = f"#{it['number']} {it['title']} ({it['url']})"
        results.append((display, it["url"]))
    return results


def find_related_github_items(
    repo: str,
    stacktrace: str,
    since_date: str,
    prefix: str = "    - ",
) -> str:
    if not repo:
        return ""

    search_terms: List[str] = []
    for name in extract_gtest_test_names(stacktrace):
        search_terms.append(name)
        method = name.split(".")[-1] if "." in name else ""
        if method and method != name:
            search_terms.append(method)
    for ident in extract_error_identifiers(stacktrace):
        search_terms.append(ident.replace("::", " "))
    if not search_terms:
        q = extract_search_query(stacktrace)
        if q:
            search_terms.append(q)

    capped = [t for t in search_terms if t][:3]
    if not capped:
        return ""

    # Fire all searches concurrently (terms x kinds)
    seen_urls: set = set()
    all_items: List[str] = []

    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
        for term in capped:
            for kind in ("issues", "prs"):
                tasks.append(pool.submit(_search_gh_items, repo, term, kind, since_date))
        for fut in as_completed(tasks):
            try:
                for display, url in fut.result():
                    if url not in seen_urls:
                        seen_urls.add(url)
                        all_items.append(f"{prefix}{display}")
            except Exception:
                pass

    return "\n".join(all_items[:15])


def search_related_github_prs(identifiers: List[str]) -> str:
    """Search velox and presto repos for related issues/PRs."""
    try:
        since_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    except Exception:
        since_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not identifiers:
        return ""

    search_terms: List[str] = []
    for ident in identifiers:
        term = ident.replace("::", " ")
        search_terms.append(term)
        if "." in term:
            method = term.rsplit(".", 1)[-1]
            if method:
                search_terms.append(method)

    repos = ["facebookincubator/velox", "prestodb/presto"]
    seen_urls: set = set()
    all_items: List[str] = []

    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
        for term in search_terms:
            if not term:
                continue
            for repo in repos:
                for kind in ("issues", "prs"):
                    tasks.append(pool.submit(_search_gh_items, repo, term, kind, since_date))
        for fut in as_completed(tasks):
            try:
                for display, url in fut.result():
                    if url not in seen_urls:
                        seen_urls.add(url)
                        all_items.append(f"  - {display}")
            except Exception:
                pass

    return "\n".join(all_items[:20])


# ---------------------------------------------------------------------------
# AI analysis
# ---------------------------------------------------------------------------

_AI_PROMPT_TEMPLATE = textwrap.dedent("""\
    You are analyzing a CI/CD build failure log. Your task is to find the ROOT CAUSE of the failure, not just the final symptom.

    IMPORTANT ANALYSIS RULES:
    - For compilation failures: Look for the FIRST 'error:' message with actual error details (type mismatches, undefined references, missing includes, etc.)
    - DO NOT report generic messages like 'make failed', 'ninja: build stopped', 'exit code 1' as the cause - these are symptoms, not causes
    - Look for specific error patterns: type conversion errors, missing symbols, API mismatches, test assertion failures
    - Include the specific file name, class name, or function name involved in the error
    - For type errors, mention what type was expected vs what was provided
    - For TEST FAILURES: Always include the EXACT test case name(s) that failed (e.g., 'TestClassName.testMethodName', 'test_function_name')
    - For test failures, mention the assertion that failed or the error message from the test

    Job: {job_name}
    Workflow: {workflow_name}

    Log output:
    {log_content}{related_items_section}

    Based on your analysis, provide:
    1. STACKTRACE: Extract the relevant error stacktrace or error messages from the log (the actual error output, compiler errors, test failures, or exception traces - NOT the entire log, just the key error portion)
    2. CAUSE: The specific root cause (mention file/class/function names, exact error like type mismatch, missing symbol, failed test names, etc.)
    3. FIX: A concrete suggested fix or investigation step{fix_extra}

    Respond in exactly this format (no markdown except for STACKTRACE which can be multiline):
    STACKTRACE:<the relevant error stacktrace or error messages, can span multiple lines, end with END_STACKTRACE on its own line>
    END_STACKTRACE
    CAUSE:<your specific root cause description - single line>
    FIX:<your fix suggestion - single line{fix_format_extra}>""")


def _truncate_log(content: str, max_chars: int = 30000) -> str:
    if len(content) <= max_chars:
        return content
    return f"[...truncated...]\n{content[-max_chars:]}"


def analyze_logs_with_ai(log_content: str, job_name: str, workflow_name: str) -> str:
    """Analyze via NVIDIA LLM API (HTTP)."""
    api_key = CFG.llm_api_key
    if not api_key:
        return (
            "STACKTRACE:Unable to extract - API key not set\n"
            "CAUSE:Unable to analyze - LLM_API_KEY or NVIDIA_API_KEY not set\n"
            "FIX:Set API key to enable AI-powered log analysis"
        )

    truncated = _truncate_log(log_content)
    prompt = _AI_PROMPT_TEMPLATE.format(
        job_name=job_name,
        workflow_name=workflow_name,
        log_content=truncated,
        related_items_section="",
        fix_extra="",
        fix_format_extra="",
    )

    try:
        import urllib.error
        import urllib.request

        payload = json.dumps(
            {
                "model": CFG.llm_model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1024,
                "temperature": 0.2,
                "top_p": 0.7,
            }
        ).encode()

        req = urllib.request.Request(
            CFG.llm_api_url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=CFG.llm_timeout) as resp:
            body = json.loads(resp.read().decode())
        content = body.get("choices", [{}])[0].get("message", {}).get("content", "")
        if content:
            return content
        err = body.get("error", {}).get("message", "")
        return (
            f"STACKTRACE:Unable to extract - API error\n"
            f"CAUSE:API error - {err or 'unknown'}\n"
            f"FIX:Check API key and quota"
        )
    except Exception as exc:
        return (
            f"STACKTRACE:Unable to extract - API request failed\n"
            f"CAUSE:Unable to analyze - {exc}\n"
            f"FIX:Check network connectivity and API key, or increase LLM_TIMEOUT"
        )


def analyze_logs_with_claude(
    log_content: str,
    job_name: str,
    workflow_name: str,
    prefetched_items: str = "",
) -> str:
    """Analyze via Claude Code CLI."""
    if not shutil.which(CFG.claude_bin):
        return (
            "STACKTRACE:Unable to extract - claude CLI not found\n"
            "CAUSE:Unable to analyze - claude CLI not installed or not in PATH\n"
            "FIX:Install Claude Code CLI or set CLAUDE_BIN to the correct path"
        )

    truncated = _truncate_log(log_content)

    related_items = prefetched_items
    related_section = ""
    if not related_items:
        idents = extract_error_identifiers(truncated)
        gtest = extract_gtest_test_names(truncated)
        all_ids = sorted(set(idents + gtest))
        if all_ids:
            related_items = search_related_github_prs(all_ids)
    if related_items:
        related_section = (
            "\n\nRELATED GITHUB ISSUES AND PRs (found by searching error "
            "identifiers in velox/presto repos):\n"
            f"{related_items}\n\n"
            "When suggesting a FIX, reference any relevant issue or PR from "
            "above that may have introduced or fixed the issue. Include the "
            "number and URL."
        )

    prompt = _AI_PROMPT_TEMPLATE.format(
        job_name=job_name,
        workflow_name=workflow_name,
        log_content=truncated,
        related_items_section=related_section,
        fix_extra=(
            ". If any of the RELATED GITHUB ISSUES AND PRs above are "
            "relevant to the failure, mention them with their number and URL"
            if related_items
            else ""
        ),
        fix_format_extra=(", include relevant PR links if applicable" if related_items else ""),
    )

    try:
        proc = subprocess.run(
            [
                CFG.claude_bin,
                "--print",
                "--model",
                CFG.claude_model,
                "--no-session-persistence",
                "--allowedTools",
                "",
            ],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=120,
        )
        content = proc.stdout.strip()
        if content:
            return content
    except Exception:
        pass

    return (
        "STACKTRACE:Unable to extract - Claude CLI returned no output\n"
        "CAUSE:Unable to analyze - Claude CLI failed "
        "(check authentication with 'claude --print \"hello\"')\n"
        "FIX:Run 'claude' interactively once to authenticate, "
        "or check ANTHROPIC_API_KEY"
    )


# ---------------------------------------------------------------------------
# Failure signature (for dedup in Slack output)
# ---------------------------------------------------------------------------


def compute_failure_signature(run_id: int, job_filter: str) -> str:
    jobs_data = _fetch_jobs_json(run_id)
    if not jobs_data:
        return f"unique_{run_id}"
    jobs = jobs_data.get("jobs", [])
    sig_parts = []
    for j in jobs:
        name = j.get("name", "")
        if job_filter and job_filter.lower() not in name.lower():
            continue
        conc = j.get("conclusion", "")
        if conc in ("success", "skipped"):
            continue
        failed_steps = sorted(
            s.get("name", "") for s in j.get("steps", []) if s.get("conclusion", "") not in ("success", "skipped")
        )
        sig_parts.append(f"{name}|{'+'.join(failed_steps)}")
    sig_parts.sort()
    return ":".join(sig_parts) or f"unique_{run_id}"


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


class OutputBuffer:
    """Collects output lines; written to stdout + file at the end."""

    def __init__(self):
        self._lines: List[str] = []

    def print(self, *args, **kwargs):
        import io

        buf = io.StringIO()
        print(*args, file=buf, **kwargs)
        self._lines.append(buf.getvalue())

    def text(self) -> str:
        return "".join(self._lines)

    def flush_to(self, path: str):
        full = self.text()
        sys.stdout.write(full)
        sys.stdout.flush()
        with open(path, "w", encoding="utf-8") as f:
            f.write(full)


OUT = OutputBuffer()

# ---------------------------------------------------------------------------
# Job-level log extraction + per-job analysis helpers
# ---------------------------------------------------------------------------


def _filter_raw_log_for_job(full_log: str, job_name: str) -> str:
    """Return only lines belonging to *job_name* from GH Actions log output.

    Tries exact prefix match first, then falls back to substring matching
    (handles matrix suffixes like ' (ubuntu-latest)' or calling-workflow
    prefixes).  If nothing matches, returns the full log so callers always
    have *something* to analyse.
    """
    lines = full_log.splitlines()

    # 1. Exact prefix match: "job_name\t"
    escaped = re.escape(job_name)
    exact_pat = re.compile(rf"^{escaped}\t")
    selected = []
    for line in lines:
        if exact_pat.match(line):
            selected.append(line)
            if "Post job cleanup." in line:
                break
    if selected:
        return "\n".join(selected)

    # 2. Substring / fuzzy match — the job name appears anywhere before the
    #    first tab (covers matrix suffixes and calling-workflow prefixes).
    name_lower = job_name.lower()
    selected = []
    for line in lines:
        first_field = line.split("\t", 1)[0].lower()
        if name_lower in first_field:
            selected.append(line)
            if "Post job cleanup." in line:
                break
    if selected:
        return "\n".join(selected)

    # 3. Fallback: return the entire log so callers can still extract errors.
    print(
        f"WARN: could not match job '{job_name}' in log output, using full log ({len(lines)} lines)",
        file=sys.stderr,
    )
    return full_log


def _get_failed_jobs(jobs_data: dict, job_filter: str) -> List[dict]:
    """Return list of failed job dicts from jobs_data."""
    jobs = jobs_data.get("jobs", [])
    result = []
    for j in jobs:
        if job_filter and job_filter.lower() not in j.get("name", "").lower():
            continue
        conc = j.get("conclusion", "")
        if conc not in ("success", "skipped"):
            result.append(j)
    return result


def _get_failed_steps(job: dict) -> List[dict]:
    return [s for s in job.get("steps", []) if s.get("conclusion", "") not in ("success", "skipped")]


def _split_into_blocks(content: str) -> List[str]:
    """Split *content* on BLOCK_SEP into individual failure blocks."""
    blocks: List[str] = []
    current: List[str] = []
    for line in content.splitlines():
        if line == BLOCK_SEP:
            text = "\n".join(current).strip()
            if text:
                blocks.append(text)
            current = []
        else:
            current.append(line)
    text = "\n".join(current).strip()
    if text:
        blocks.append(text)
    return blocks


def _analyze_block(block: str, job_name: str, wf_name: str, related_items: str) -> Tuple[str, str]:
    """Run AI analysis on one failure block.  Returns (cause, fix)."""
    if CFG.use_claude:
        resp = analyze_logs_with_claude(block, job_name, wf_name, related_items)
    else:
        resp = analyze_logs_with_ai(block, job_name, wf_name)
    cause = ""
    fix = ""
    for line in resp.splitlines():
        if line.upper().startswith("CAUSE:"):
            cause = re.sub(r"^CAUSE:\s*", "", line, flags=re.IGNORECASE)
        elif line.upper().startswith("FIX:"):
            fix = re.sub(r"^FIX:\s*", "", line, flags=re.IGNORECASE)
    return cause, fix


# ---------------------------------------------------------------------------
# Print helpers - Slack format
# ---------------------------------------------------------------------------


def print_slack_header():
    try:
        dt = datetime.strptime(CFG.today_utc, "%Y-%m-%d")
        formatted = dt.strftime("%B %d, %Y")
    except Exception:
        formatted = CFG.today_utc
    OUT.print(f"*\U0001f319 Nightly Jobs Status - {formatted}*")
    OUT.print()
    OUT.print("*Status Summary:*")
    OUT.print()


def print_slack_table_header():
    OUT.print("| *NO* | *Job Name*         | *Staging* | *Upstream* | *Stable* |")
    OUT.print("|------|--------------------| --------- | ---------- | -------- |")


def print_slack_table_row(row_no: int, display: str, st_cell: str, up_cell: str, sb_cell: str):
    OUT.print(f"| {row_no:<4} | {display:<18} | {st_cell:<9} | {up_cell:<10} | {sb_cell:<8} |")


def _print_blocks_with_analysis(
    job_log_content: str,
    job_name: str,
    wf_name: str,
    related_items: str,
    slack: bool,
):
    """Print each failure block's stacktrace + optional AI cause/fix."""
    st_label = "*Stacktrace" if slack else "- Stacktrace"
    cause_label = "*Cause:*" if slack else "- Cause:"
    fix_label = "*Fix:*" if slack else "- Fix:"

    if not job_log_content.strip():
        OUT.print(f"    {st_label}: _Unavailable_")
        if CFG.analyze_cause:
            OUT.print(f"    {cause_label} _Unable to fetch logs for this job_")
            if CFG.analyze_fix:
                OUT.print(f"    {fix_label} _Check the run link above for details_")
        return

    blocks = _split_into_blocks(job_log_content)
    n_blocks = len(blocks)

    # Run AI analysis for all blocks concurrently
    ai_results: Dict[int, Tuple[str, str]] = {}
    if CFG.analyze_cause and blocks:
        with ThreadPoolExecutor(max_workers=MAX_AI_WORKERS) as pool:
            futs = {
                pool.submit(_analyze_block, b, job_name, wf_name, related_items): idx for idx, b in enumerate(blocks)
            }
            for fut in as_completed(futs):
                idx = futs[fut]
                try:
                    ai_results[idx] = fut.result()
                except Exception:
                    ai_results[idx] = ("Unable to determine cause", "Pending investigation")

    for idx, block in enumerate(blocks):
        suffix = f" {idx + 1}/{n_blocks}" if n_blocks > 1 else ""
        if slack:
            OUT.print(f"    {st_label}{suffix}:*")
        else:
            OUT.print(f"    {st_label}{suffix}:")
        OUT.print("```")
        for line in block.splitlines():
            if line.strip():
                OUT.print(line)
        OUT.print("```")

        if CFG.analyze_cause:
            cause, fix = ai_results.get(idx, ("Unable to determine cause", "Pending investigation"))
            OUT.print(f"    {cause_label} _{cause or 'Unable to determine cause'}_")
            if CFG.analyze_fix:
                OUT.print(f"    {fix_label} _{fix or 'Pending investigation'}_")


def _process_single_failure(
    label: str,
    run: dict,
    job_filter: str,
    slack: bool,
    idx: int,
    extra_affects: str = "",
) -> str:
    """Process one failure entry.  Returns formatted output as a string.

    This function is designed to run inside a thread so multiple failures
    can be analyzed concurrently.
    """
    local_out = OutputBuffer()

    run_id = run["databaseId"]
    run_url = run["url"]
    wf_name = run.get("workflowName", "")
    conclusion = run.get("conclusion", "")
    try:
        since_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    except Exception:
        since_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if slack:
        local_out.print()
        local_out.print(f"*{idx}. {label}*")
        local_out.print(f"\u2022 *Workflow:* {wf_name}")
        local_out.print(f"\u2022 *Run:* {run_url}")
        if extra_affects:
            parts = []
            for line in extra_affects.strip().splitlines():
                cols = line.split("\t", 1)
                if len(cols) == 2:
                    parts.append(f"{cols[0]} ({cols[1]})")
            if parts:
                local_out.print(f"\u2022 *Also fails in:* {', '.join(parts)}")
        local_out.print(f"\u2022 *Conclusion:* {conclusion}")
    else:
        local_out.print()
        local_out.print(f"### {label}")
        local_out.print(f"- Workflow: {wf_name}")
        local_out.print(f"- Run: {run_url}")
        local_out.print(f"- Title: {run.get('displayTitle', '')}")
        local_out.print(f"- Conclusion: {conclusion}")

    # Fetch log and jobs concurrently
    log_out: Optional[str] = None
    jobs_data: Optional[dict] = None

    with ThreadPoolExecutor(max_workers=2) as pool:
        log_fut = None
        if CFG.print_logs or CFG.analyze_cause:
            log_fut = pool.submit(
                run_gh_safe,
                "run",
                "view",
                "-R",
                CFG.repo,
                str(run_id),
                "--log-failed",
            )
        jobs_fut = pool.submit(_fetch_jobs_json, run_id)
        if log_fut:
            log_out = log_fut.result()
        jobs_data = jobs_fut.result()

    # Fallback: if --log-failed returned nothing, try --log (full log)
    if (CFG.print_logs or CFG.analyze_cause) and not log_out:
        print(
            f"WARN: --log-failed returned empty for run {run_id}, falling back to --log",
            file=sys.stderr,
        )
        log_out = run_gh_safe(
            "run",
            "view",
            "-R",
            CFG.repo,
            str(run_id),
            "--log",
        )

    if not jobs_data:
        if slack:
            local_out.print(
                "  \u25e6 _WARN: failed to fetch job/step details (network/API timeout). See run link above._"
            )
        else:
            local_out.print("  - WARN: failed to fetch job/step details (network/API timeout). See run link above.")
        return local_out.text()

    failed_jobs = _get_failed_jobs(jobs_data, job_filter)
    for job in failed_jobs:
        job_name = job.get("name", "unknown")
        job_conclusion = job.get("conclusion", "unknown")
        failed_steps = _get_failed_steps(job)

        if slack:
            local_out.print(f"  \u25e6 Job: `{job_name}` ({job_conclusion})")
            for s in failed_steps:
                local_out.print(f"    \u25aa\ufe0e Step: {s.get('name', '?')} ({s.get('conclusion', 'unknown')})")
        else:
            local_out.print(f"  - Job: {job_name} ({job_conclusion})")
            for s in failed_steps:
                local_out.print(f"    - Step: {s.get('name', '?')} ({s.get('conclusion', 'unknown')})")

        # Extract job-specific logs
        job_log_content = ""
        if log_out:
            raw_job_log = _filter_raw_log_for_job(log_out, job_name)
            job_log_content = extract_relevant_failures(raw_job_log)

        # Find related GitHub items
        related_items = ""
        if CFG.analyze_cause:
            repo = upstream_repo_for_label(label)
            prefix = "    \u2022 " if slack else "    - "
            related_items = find_related_github_items(repo, job_log_content, since_date, prefix)

        st_label = "*Stacktrace" if slack else "- Stacktrace"
        cause_label = "*Cause:*" if slack else "- Cause:"
        fix_label = "*Fix:*" if slack else "- Fix:"

        if not job_log_content.strip():
            local_out.print(f"    {st_label}: _Unavailable_")
            if CFG.analyze_cause:
                local_out.print(f"    {cause_label} _Unable to fetch logs for this job_")
                if CFG.analyze_fix:
                    local_out.print(f"    {fix_label} _Check the run link above for details_")
        else:
            blocks = _split_into_blocks(job_log_content)
            n_blocks = len(blocks)

            ai_results: Dict[int, Tuple[str, str]] = {}
            if CFG.analyze_cause and blocks:
                with ThreadPoolExecutor(max_workers=MAX_AI_WORKERS) as ai_pool:
                    futs = {
                        ai_pool.submit(_analyze_block, b, job_name, wf_name, related_items): bidx
                        for bidx, b in enumerate(blocks)
                    }
                    for fut in as_completed(futs):
                        bidx = futs[fut]
                        try:
                            ai_results[bidx] = fut.result()
                        except Exception:
                            ai_results[bidx] = (
                                "Unable to determine cause",
                                "Pending investigation",
                            )

            for bidx, block in enumerate(blocks):
                suffix = f" {bidx + 1}/{n_blocks}" if n_blocks > 1 else ""
                if slack:
                    local_out.print(f"    {st_label}{suffix}:*")
                else:
                    local_out.print(f"    {st_label}{suffix}:")
                local_out.print("```")
                for line in block.splitlines():
                    if line.strip():
                        local_out.print(line)
                local_out.print("```")

                if CFG.analyze_cause:
                    cause, fix = ai_results.get(bidx, ("Unable to determine cause", "Pending investigation"))
                    local_out.print(f"    {cause_label} _{cause or 'Unable to determine cause'}_")
                    if CFG.analyze_fix:
                        local_out.print(f"    {fix_label} _{fix or 'Pending investigation'}_")

        if related_items:
            local_out.print()
            if slack:
                local_out.print("    *Related issues/PRs (last 7 days):*")
            else:
                local_out.print("  Related issues/PRs (last 7 days):")
            local_out.print(related_items)

    # Combined logs
    if CFG.print_logs:
        local_out.print()
        if slack:
            local_out.print("*Failed test stacktraces:*")
        else:
            local_out.print("  Failed test stacktraces:")
        if log_out:
            content = extract_relevant_failures(log_out)
            if slack:
                local_out.print("```")
                local_out.print(content)
                local_out.print("```")
            else:
                for line in content.splitlines():
                    local_out.print(f"    {line}")
        else:
            msg = "Unable to fetch logs (network/API timeout). Open the run link above for full logs."
            local_out.print(f"_({msg})_" if slack else f"    (WARN) {msg}")

    return local_out.text()


def _process_in_progress(
    label: str,
    workflow_file: str,
    run: dict,
    slack: bool,
    idx: int,
) -> str:
    local_out = OutputBuffer()
    run_url = run["url"]
    wf_name = run.get("workflowName", "")
    status = run.get("status", "")
    attempt = run.get("attempt", 1)
    run_number = run.get("number")

    if slack:
        local_out.print()
        local_out.print(f"*{idx}. {label} (in progress)*")
        local_out.print(f"\u2022 *Workflow:* {wf_name}")
        local_out.print(f"\u2022 *Run:* {run_url}")
        local_out.print(f"\u2022 *Status:* {status} (attempt: {attempt})")
    else:
        local_out.print()
        local_out.print(f"### {label} (in progress)")
        local_out.print(f"- Workflow: {wf_name}")
        local_out.print(f"- Run: {run_url}")
        local_out.print(f"- Title: {run.get('displayTitle', '')}")
        local_out.print(f"- Status: {status} (attempt: {attempt})")

    if attempt > 1 and run_number:
        first = get_first_attempt_for_run_number(workflow_file, run_number)
        if first:
            first_status = first.get("status", "")
            first_conclusion = first.get("conclusion", "")
            first_url = first.get("url", "")
            first_id = first.get("databaseId")
            if slack:
                local_out.print()
                local_out.print("  _First attempt details:_")
                local_out.print(f"  \u2022 *Run:* {first_url}")
                local_out.print(f"  \u2022 *Status:* {first_status}")
                local_out.print(f"  \u2022 *Conclusion:* {first_conclusion or 'n/a'}")
            else:
                local_out.print()
                local_out.print("  First attempt details:")
                local_out.print(f"  - Run: {first_url}")
                local_out.print(f"  - Status: {first_status}")
                local_out.print(f"  - Conclusion: {first_conclusion or 'n/a'}")

            if first_status == "completed" and first_conclusion != "success" and CFG.print_logs:
                log_out = run_gh_safe(
                    "run",
                    "view",
                    "-R",
                    CFG.repo,
                    str(first_id),
                    "--log-failed",
                )
                if slack:
                    local_out.print()
                    local_out.print("*First attempt failed test stacktraces:*")
                else:
                    local_out.print()
                    local_out.print("  First attempt failed test stacktraces:")
                if log_out:
                    content = extract_relevant_failures(log_out)
                    if slack:
                        local_out.print("```")
                        local_out.print(content)
                        local_out.print("```")
                    else:
                        for line in content.splitlines():
                            local_out.print(f"    {line}")
                else:
                    msg = "Unable to fetch logs. Open the run link above for full logs."
                    local_out.print(f"_{msg}_" if slack else f"    (WARN) {msg}")

    return local_out.text()


# ---------------------------------------------------------------------------
# Plain format helpers
# ---------------------------------------------------------------------------


def print_plain_header():
    try:
        dt = datetime.strptime(CFG.today_utc, "%Y-%m-%d")
        dd = dt.strftime("%m-%d-%Y")
    except Exception:
        dd = CFG.today_utc
    OUT.print(f"1. Nightly Jobs Status Table - current date ({dd})")
    OUT.print()
    OUT.print(f"{'SNO':<4} | {'Job Name':<18} | {'Staging':<8} | {'Upstream':<8} | {'Stable':<8}")
    OUT.print(f"{'----':<4}-|-{'------------------':<18}-|-{'--------':<8}-|-{'--------':<8}-|-{'--------':<8}")


def print_plain_table_row(row_no: int, display: str, st_cell: str, up_cell: str, sb_cell: str):
    OUT.print(f"{row_no:<4} | {display:<18} | {st_cell:<8} | {up_cell:<8} | {sb_cell:<8}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Nightly workflow status helper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--print-logs", action="store_true", default=False)
    p.add_argument("--slack", action="store_true", default=True)
    p.add_argument("--cause", action="store_true", default=None)
    p.add_argument("--no-cause", action="store_true", default=False)
    p.add_argument("--fix", action="store_true", default=None)
    p.add_argument("--no-fix", action="store_true", default=False)
    p.add_argument("--claude", action="store_true", default=None)
    p.add_argument("--no-claude", action="store_true", default=False)
    p.add_argument("--date", type=str, default=None)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Initialisation helpers
# ---------------------------------------------------------------------------


def _get_last_nightly_run_date() -> str:
    out = run_gh_safe(
        "run",
        "list",
        "-R",
        CFG.repo,
        "--workflow",
        "velox-nightly-upstream.yml",
        "--limit",
        "1",
        "--json",
        "createdAt",
        "--jq",
        ".[0].createdAt // empty",
    )
    if out:
        date_part = out.strip().split("T")[0]
        if date_part:
            return date_part
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _detect_repo() -> str:
    out = run_gh_safe("repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner")
    if out:
        return out.strip()
    return ""


def init_config(args: argparse.Namespace):
    """Populate CFG from args and environment."""
    CFG.print_logs = args.print_logs
    CFG.slack_format = args.slack

    CFG.analyze_cause = True
    CFG.analyze_fix = True
    CFG.use_claude = True

    if args.no_cause:
        CFG.analyze_cause = False
        CFG.analyze_fix = False
    if args.cause:
        CFG.analyze_cause = True
    if args.no_fix:
        CFG.analyze_fix = False
    if args.fix:
        CFG.analyze_fix = True
        CFG.analyze_cause = True
    if args.no_claude:
        CFG.use_claude = False
    if args.claude:
        CFG.use_claude = True

    CFG.repo = os.environ.get("REPO", "") or _detect_repo()
    if not CFG.repo:
        print(
            "ERROR: could not determine repo. Set REPO=owner/repo or run within a gh-authenticated repo.",
            file=sys.stderr,
        )
        sys.exit(2)

    if args.date:
        CFG.today_utc = args.date
    else:
        CFG.today_utc = os.environ.get("TODAY_UTC", "") or _get_last_nightly_run_date()

    try:
        dt = datetime.strptime(CFG.today_utc, "%Y-%m-%d")
        CFG.display_date = dt.strftime("%d-%m-%Y")
    except Exception:
        CFG.display_date = CFG.today_utc

    CFG.log_tail_lines = int(os.environ.get("LOG_TAIL_LINES", "150"))
    CFG.status_file = os.environ.get("STATUS_FILE", "status.txt")
    CFG.gh_retries = int(os.environ.get("GH_RETRIES", "5"))
    CFG.gh_retry_sleep = int(os.environ.get("GH_RETRY_SLEEP_SECONDS", "2"))
    CFG.gh_http_timeout = int(os.environ.get("GH_HTTP_TIMEOUT", "60"))

    CFG.llm_api_key = os.environ.get("LLM_API_KEY") or os.environ.get("NVIDIA_API_KEY", "")
    CFG.llm_api_url = os.environ.get(
        "LLM_API_URL",
        "https://integrate.api.nvidia.com/v1/chat/completions",
    )
    CFG.llm_model = os.environ.get(
        "LLM_MODEL",
        "nvdev/nvidia/llama-3.3-nemotron-super-49b-v1",
    )
    CFG.llm_timeout = int(os.environ.get("LLM_TIMEOUT", "30"))
    CFG.claude_bin = os.environ.get("CLAUDE_BIN", "claude")
    CFG.claude_model = os.environ.get("CLAUDE_MODEL", "opus")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if not shutil.which("gh"):
        print("ERROR: missing required command: gh", file=sys.stderr)
        sys.exit(2)

    args = parse_args()
    init_config(args)

    # ----- Phase 1: Fetch all workflow runs concurrently -----
    # Collect unique (non-empty) workflow files to fetch
    wf_to_fetch: set = set()
    for row in ROW_DEFS:
        for col in ("upstream", "staging", "stable"):
            wf = row[col]
            if wf:
                wf_to_fetch.add(wf)

    run_map: Dict[str, Optional[dict]] = {}
    with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
        futures = {pool.submit(_fetch_run, wf): wf for wf in wf_to_fetch}
        for fut in as_completed(futures):
            wf_file, run = fut.result()
            run_map[wf_file] = run

    # ----- Phase 2: Prefetch jobs for all runs that need job-level filtering -----
    all_runs_needing_jobs = []
    for row in ROW_DEFS:
        if row["job_filter"]:
            for col in ("upstream", "staging", "stable"):
                wf = row[col]
                if wf:
                    r = run_map.get(wf)
                    if r:
                        all_runs_needing_jobs.append(r)
    _prefetch_jobs_for_runs(all_runs_needing_jobs)

    # ----- Phase 3: Build status table -----
    if CFG.slack_format:
        print_slack_header()
        print_slack_table_header()
    else:
        print_plain_header()

    # Each row: collect cells + failures/in-progress
    fail_entries: List[Dict[str, Any]] = []  # label, run, filter, wf
    inprog_entries: List[Dict[str, Any]] = []

    for row_no, row in enumerate(ROW_DEFS, 1):
        up_run = run_map.get(row["upstream"]) if row["upstream"] else None
        st_run = run_map.get(row["staging"]) if row["staging"] else None
        sb_run = run_map.get(row["stable"]) if row["stable"] else None
        jf = row["job_filter"]

        if jf:
            up_cell = cell_for_filtered_jobs(up_run, jf)
            st_cell = cell_for_filtered_jobs(st_run, jf)
            sb_cell = cell_for_filtered_jobs(sb_run, jf)
        else:
            up_cell = cell_for_run(up_run)
            st_cell = cell_for_run(st_run)
            sb_cell = cell_for_run(sb_run)

        display = row["display"]
        if CFG.slack_format:
            print_slack_table_row(row_no, display, st_cell, up_cell, sb_cell)
        else:
            print_plain_table_row(row_no, display, st_cell, up_cell, sb_cell)

        for col_name, wf_key, run_obj, cell in [
            ("Staging", "staging", st_run, st_cell),
            ("Upstream", "upstream", up_run, up_cell),
            ("Stable", "stable", sb_run, sb_cell),
        ]:
            wf = row[wf_key]
            if run_obj and wf:
                if cell == EMOJI_CROSS:
                    fail_entries.append(
                        {
                            "label": f"{row['name']} / {col_name}",
                            "run": run_obj,
                            "filter": jf,
                            "wf": wf,
                        }
                    )
                elif cell == EMOJI_HOURGLASS:
                    inprog_entries.append(
                        {
                            "label": f"{row['name']} / {col_name}",
                            "run": run_obj,
                            "filter": jf,
                            "wf": wf,
                        }
                    )

    # ----- Phase 4: Failure details -----
    if CFG.slack_format:
        OUT.print()
        OUT.print("---")
        OUT.print()
        OUT.print("*\U0001f534 Failure Details:*")

        if not fail_entries:
            OUT.print()
            OUT.print(f"_No failures detected for {CFG.today_utc}._")
            OUT.flush_to(CFG.status_file)
            return

        # Dedup by failure signature
        sigs: List[str] = []
        for entry in fail_entries:
            rid = entry["run"]["databaseId"]
            sig = compute_failure_signature(rid, entry["filter"])
            sigs.append(sig)

        seen_sigs: set = set()
        ordered_failures: List[Tuple[int, str]] = []  # (original_index, extra_affects)
        for i, sig in enumerate(sigs):
            if sig in seen_sigs:
                continue
            seen_sigs.add(sig)
            extra_parts = []
            for j, sig2 in enumerate(sigs):
                if j != i and sig2 == sig:
                    e = fail_entries[j]
                    extra_parts.append(f"{e['label']}\t{e['run']['url']}")
            ordered_failures.append((i, "\n".join(extra_parts)))

        # Process failures concurrently
        with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
            fut_map = {}
            for display_idx, (orig_i, extra) in enumerate(ordered_failures, 1):
                e = fail_entries[orig_i]
                fut = pool.submit(
                    _process_single_failure,
                    e["label"],
                    e["run"],
                    e["filter"],
                    True,
                    display_idx,
                    extra,
                )
                fut_map[fut] = display_idx

            # Collect results in display order
            results_by_idx: Dict[int, str] = {}
            for fut in as_completed(fut_map):
                didx = fut_map[fut]
                try:
                    results_by_idx[didx] = fut.result()
                except Exception as exc:
                    results_by_idx[didx] = f"\n*{didx}. (error processing failure: {exc})*\n"

        first = True
        for didx in sorted(results_by_idx):
            if not first:
                OUT.print()
                OUT.print("---")
            first = False
            OUT.print(results_by_idx[didx], end="")

        # In-progress
        if inprog_entries:
            OUT.print()
            OUT.print("---")
            OUT.print()
            OUT.print("*\u23f3 In-Progress Details:*")
            with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
                ip_futs = {}
                for ip_idx, entry in enumerate(inprog_entries, 1):
                    fut = pool.submit(
                        _process_in_progress,
                        entry["label"],
                        entry["wf"],
                        entry["run"],
                        True,
                        ip_idx,
                    )
                    ip_futs[fut] = ip_idx
                ip_results: Dict[int, str] = {}
                for fut in as_completed(ip_futs):
                    ip_results[ip_futs[fut]] = fut.result()
            for ip_idx in sorted(ip_results):
                OUT.print(ip_results[ip_idx], end="")

    else:
        # Plain format
        OUT.print()
        OUT.print("2. Failure Details:")

        if not fail_entries:
            OUT.print()
            OUT.print(f"(No failures detected for {CFG.today_utc}.)")
            OUT.flush_to(CFG.status_file)
            return

        with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
            fut_map = {}
            for fi, entry in enumerate(fail_entries, 1):
                fut = pool.submit(
                    _process_single_failure,
                    entry["label"],
                    entry["run"],
                    entry["filter"],
                    False,
                    fi,
                )
                fut_map[fut] = fi
            results_by_idx: Dict[int, str] = {}
            for fut in as_completed(fut_map):
                fi = fut_map[fut]
                try:
                    results_by_idx[fi] = fut.result()
                except Exception as exc:
                    results_by_idx[fi] = f"\n### (error processing failure: {exc})\n"

        for fi in sorted(results_by_idx):
            OUT.print(results_by_idx[fi], end="")

        if inprog_entries:
            OUT.print()
            OUT.print("3. In-progress Details:")
            with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
                ip_futs = {}
                for ip_idx, entry in enumerate(inprog_entries, 1):
                    fut = pool.submit(
                        _process_in_progress,
                        entry["label"],
                        entry["wf"],
                        entry["run"],
                        False,
                        ip_idx,
                    )
                    ip_futs[fut] = ip_idx
                ip_results: Dict[int, str] = {}
                for fut in as_completed(ip_futs):
                    ip_results[ip_futs[fut]] = fut.result()
            for ip_idx in sorted(ip_results):
                OUT.print(ip_results[ip_idx], end="")

    OUT.flush_to(CFG.status_file)


if __name__ == "__main__":
    main()
