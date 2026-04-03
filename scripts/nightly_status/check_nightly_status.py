#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Nightly workflow status helper — generates a status report for CI nightly runs."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import threading
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


class _TokenTracker:
    """Thread-safe accumulator for AI token usage and cost estimation."""

    def __init__(self):
        self._pricing = {
            "opus": (15.0, 75.0),
            "sonnet": (3.0, 15.0),
            "haiku": (0.25, 1.25),
        }
        self._default_pricing = (0.15, 0.60)
        self._lock = threading.Lock()
        self.api_calls = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.skipped_by_dedup = 0

    def record(self, input_toks: int, output_toks: int):
        with self._lock:
            self.api_calls += 1
            self.input_tokens += input_toks
            self.output_tokens += output_toks

    def record_dedup_skip(self, count: int = 1):
        with self._lock:
            self.skipped_by_dedup += count

    def _estimate_tokens(self, text: str) -> int:
        return max(1, len(text) // 4)

    def record_estimate(self, prompt: str, response: str):
        self.record(self._estimate_tokens(prompt), self._estimate_tokens(response))

    def summary(self) -> str:
        if self.api_calls == 0 and self.skipped_by_dedup == 0:
            return ""
        model = CFG.claude_model if CFG.use_claude else CFG.llm_model
        inp_price, out_price = self._pricing.get(model, self._default_pricing)
        cost_input = self.input_tokens * inp_price / 1_000_000
        cost_output = self.output_tokens * out_price / 1_000_000
        total = cost_input + cost_output
        lines = [
            "AI Token Usage Summary:",
            f"  Model:            {model}",
            f"  API calls:        {self.api_calls}",
            f"  Skipped (dedup):  {self.skipped_by_dedup}",
            f"  Input tokens:     ~{self.input_tokens:,}",
            f"  Output tokens:    ~{self.output_tokens:,}",
            f"  Est. cost:        ~${total:.4f} (input ${cost_input:.4f} + output ${cost_output:.4f})",
        ]
        return "\n".join(lines)


TOKEN_TRACKER = _TokenTracker()


# ---------------------------------------------------------------------------
# Workflow and row definitions
# ---------------------------------------------------------------------------

NIGHTLY_WORKFLOWS: Dict[str, str] = {
    "velox": "velox-nightly.yml",
    "presto": "presto-nightly.yml",
}

PIPELINE_VARIANTS: Dict[str, List[str]] = {
    "velox": ["upstream", "staging"],
    "presto": ["upstream", "staging", "pinned"],
}

VARIANT_COLUMNS = ["upstream", "staging", "pinned"]

ROW_DEFS: List[Dict[str, str]] = [
    {"pipeline": "velox", "display": "Deps Build", "job_pattern": "velox-deps ("},
    {"pipeline": "velox", "display": "Image Build", "job_pattern": "velox-build ("},
    {"pipeline": "velox", "display": "Test (CPU)", "job_pattern": "test-velox-cpu"},
    {"pipeline": "velox", "display": "Test (GPU)", "job_pattern": "test-velox-gpu"},
    {"pipeline": "velox", "display": "Benchmark", "job_pattern": "benchmark-velox-gpu"},
    {"pipeline": "presto", "display": "Deps Build", "job_pattern": "presto-deps ("},
    {"pipeline": "presto", "display": "Coordinator Build", "job_pattern": "presto-coordinator ("},
    {"pipeline": "presto", "display": "Image Build", "job_pattern": "presto-build ("},
    {"pipeline": "presto", "display": "Test (Smoke)", "job_pattern": "test-presto-smoke"},
    {"pipeline": "presto", "display": "Test (Integration)", "job_pattern": "integration-test"},
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


def cell_for_variant_jobs(run: Optional[dict], variant: str, job_pattern: str) -> str:
    """Compute cell status for jobs matching a variant prefix and job name pattern.

    Job names from nested reusable workflows follow the convention:
        ``{variant} / {calling_job} / {leaf_job} ({matrix params})``
    e.g. ``upstream / velox-build / velox-deps (cuda12.9, amd64)``
    """
    if run is None:
        return EMOJI_DASH
    run_id = run.get("databaseId")
    if run_id is None:
        return EMOJI_DASH
    jobs_data = _fetch_jobs_json(run_id)
    if not jobs_data:
        return EMOJI_DASH
    jobs = jobs_data.get("jobs", [])
    prefix = f"{variant} / ".lower()
    pat = job_pattern.lower()
    filtered = [j for j in jobs if j.get("name", "").lower().startswith(prefix) and pat in j.get("name", "").lower()]
    if not filtered:
        return EMOJI_DASH
    for j in filtered:
        if j.get("status") != "completed":
            return EMOJI_HOURGLASS
    if all(j.get("conclusion") == "skipped" for j in filtered):
        return EMOJI_DASH
    for j in filtered:
        if j.get("conclusion", "") not in ("success", "skipped"):
            return EMOJI_CROSS
    return EMOJI_CHECK


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
    - For download/archive failures: ALWAYS include the line that shows the command and the EXACT file name that failed (e.g., 'tar -xz ... -f fizz.tar.gz'), not just the generic error like 'gzip: stdin: not in gzip format'
    - For Docker build failures: Include the Dockerfile line reference and the specific command that failed

    Job: {job_name}
    Workflow: {workflow_name}

    Log output:
    {log_content}{related_items_section}

    Based on your analysis, provide:
    1. STACKTRACE: Extract ONLY the 3-5 most important lines that show the root cause error (e.g., the first compiler error, the failed assertion, the exception message). Do NOT include surrounding context, build system output, or symptom lines like 'make failed' or 'exit code 1'. Maximum 5 lines.
    2. CAUSE: The specific root cause (mention file/class/function names, exact error like type mismatch, missing symbol, failed test names, etc.)
    3. FIX: A concrete suggested fix or investigation step{fix_extra}

    Respond in exactly this format (no markdown except for STACKTRACE which can be multiline):
    STACKTRACE:<3-5 most relevant root-cause error lines only, end with END_STACKTRACE on its own line>
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
        usage = body.get("usage", {})
        if usage:
            TOKEN_TRACKER.record(
                usage.get("prompt_tokens", 0),
                usage.get("completion_tokens", 0),
            )
        else:
            TOKEN_TRACKER.record_estimate(prompt, body.get("choices", [{}])[0].get("message", {}).get("content", ""))
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
            TOKEN_TRACKER.record_estimate(prompt, content)
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


def compute_failure_signature(run_id: int, job_filter: str, variant: str = "") -> str:
    jobs_data = _fetch_jobs_json(run_id)
    if not jobs_data:
        return f"unique_{run_id}"
    jobs = jobs_data.get("jobs", [])
    prefix = f"{variant} / ".lower() if variant else ""
    sig_parts = []
    for j in jobs:
        name = j.get("name", "")
        name_lower = name.lower()
        if prefix and not name_lower.startswith(prefix):
            continue
        if job_filter and job_filter.lower() not in name_lower:
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


def _get_failed_jobs(jobs_data: dict, job_filter: str, variant: str = "") -> List[dict]:
    """Return list of failed job dicts from jobs_data."""
    jobs = jobs_data.get("jobs", [])
    prefix = f"{variant} / ".lower() if variant else ""
    result = []
    for j in jobs:
        name_lower = j.get("name", "").lower()
        if prefix and not name_lower.startswith(prefix):
            continue
        if job_filter and job_filter.lower() not in name_lower:
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


def _analyze_block(block: str, job_name: str, wf_name: str, related_items: str) -> Tuple[str, str, str]:
    """Run AI analysis on one failure block.  Returns (stacktrace, cause, fix)."""
    if CFG.use_claude:
        resp = analyze_logs_with_claude(block, job_name, wf_name, related_items)
    else:
        resp = analyze_logs_with_ai(block, job_name, wf_name)
    stacktrace = ""
    cause = ""
    fix = ""

    # Parse STACKTRACE:...END_STACKTRACE block
    st_match = re.search(
        r"STACKTRACE:\s*(.*?)\s*END_STACKTRACE",
        resp,
        re.DOTALL | re.IGNORECASE,
    )
    if st_match:
        st_lines = [ln for ln in st_match.group(1).strip().splitlines() if ln.strip()]
        stacktrace = "\n".join(st_lines[:5])

    for line in resp.splitlines():
        if line.upper().startswith("CAUSE:"):
            cause = re.sub(r"^CAUSE:\s*", "", line, flags=re.IGNORECASE)
        elif line.upper().startswith("FIX:"):
            fix = re.sub(r"^FIX:\s*", "", line, flags=re.IGNORECASE)
    return stacktrace, cause, fix


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
    OUT.print("| *NO* | *Pipeline* | *Phase*              | *Upstream* | *Staging* | *Pinned* |")
    OUT.print("|------|------------|----------------------|------------|-----------|----------|")


def print_slack_table_row(row_no: int, pipeline: str, phase: str, up_cell: str, st_cell: str, pn_cell: str):
    OUT.print(f"| {row_no:<4} | {pipeline:<8} | {phase:<20} | {up_cell:<10} | {st_cell:<9} | {pn_cell:<8} |")


_DOCKER_STEP_PREFIX = re.compile(r"^#\d+\s+[\d.]+\s*")

_ERROR_SIMILARITY_THRESHOLD = 0.45


def _normalize_log_line(line: str) -> str:
    """Aggressively normalise a single log line for comparison.

    Strips every component that commonly varies across architectures, CUDA
    versions, or matrix entries so that the *structure* of the error is
    preserved but platform-specific details are erased.
    """
    s = line.strip()
    s = _DOCKER_STEP_PREFIX.sub("", s)
    s = _TIMESTAMP_PREFIX.sub("", s)
    s = re.sub(r":\d+:\d+:", ":", s)
    s = re.sub(r":\d+:", ":", s)
    s = re.sub(r"/[\w./_-]+/", "", s)
    s = re.sub(r"'[^']{12,}'", "'_'", s)
    s = re.sub(r'"[^"]{12,}"', '"_"', s)
    s = re.sub(r"`[^`]{12,}`", "`_`", s)
    s = re.sub(r"0x[0-9a-fA-F]+", "0xN", s)
    s = re.sub(r"\b\d{4,}\b", "N", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _compute_error_tokens(log_content: str) -> frozenset:
    """Return a set of normalised lines from error log content."""
    if not log_content.strip():
        return frozenset()
    lines: set = set()
    for raw in log_content.splitlines():
        norm = _normalize_log_line(raw)
        if norm and len(norm) >= 5:
            lines.add(norm)
    return frozenset(lines)


def _error_similarity(a: frozenset, b: frozenset) -> float:
    """Overlap coefficient between two normalised-line sets.

    Returns the fraction of the *smaller* set that is shared with the
    larger one.  This is more forgiving than Jaccard for small sets where
    a couple of variable lines (e.g. different build-target filenames)
    would otherwise break grouping even though the core error is identical.
    """
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def _process_single_failure(
    label: str,
    run: dict,
    job_filter: str,
    slack: bool,
    idx: int,
    extra_affects: str = "",
    variant: str = "",
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

    failed_jobs = _get_failed_jobs(jobs_data, job_filter, variant)

    # -- Pre-compute logs & fingerprints, then group jobs by error ----------
    job_info: List[Tuple[dict, str]] = []  # (job, log_content)
    for job in failed_jobs:
        jname = job.get("name", "unknown")
        jlog = ""
        if log_out:
            jlog = extract_relevant_failures(_filter_raw_log_for_job(log_out, jname))
        job_info.append((job, jlog))

    job_token_sets = [_compute_error_tokens(jlog) for _, jlog in job_info]

    error_groups: List[List[int]] = []
    for i, tokens in enumerate(job_token_sets):
        placed = False
        for grp in error_groups:
            if _error_similarity(tokens, job_token_sets[grp[0]]) >= _ERROR_SIMILARITY_THRESHOLD:
                grp.append(i)
                placed = True
                break
        if not placed:
            error_groups.append([i])

    st_label = "*Stacktrace" if slack else "- Stacktrace"
    cause_label = "*Cause:*" if slack else "- Cause:"
    fix_label = "*Fix:*" if slack else "- Fix:"

    for member_indices in error_groups:
        rep_idx = member_indices[0]
        dup_indices = member_indices[1:]

        job, job_log_content = job_info[rep_idx]
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

        related_items = ""
        if CFG.analyze_cause:
            repo = upstream_repo_for_label(label)
            prefix_str = "    \u2022 " if slack else "    - "
            related_items = find_related_github_items(repo, job_log_content, since_date, prefix_str)

        if not job_log_content.strip():
            local_out.print(f"    {st_label}: _Unavailable_")
            if CFG.analyze_cause:
                local_out.print(f"    {cause_label} _Unable to fetch logs for this job_")
                if CFG.analyze_fix:
                    local_out.print(f"    {fix_label} _Check the run link above for details_")
        else:
            blocks = _split_into_blocks(job_log_content)
            n_blocks = len(blocks)

            ai_results: Dict[int, Tuple[str, str, str]] = {}
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
                                "",
                                "Unable to determine cause",
                                "Pending investigation",
                            )

            for bidx, block in enumerate(blocks):
                suffix = f" {bidx + 1}/{n_blocks}" if n_blocks > 1 else ""
                ai_st, cause, fix = ai_results.get(bidx, ("", "", ""))
                display_st = ai_st if ai_st else "\n".join(ln for ln in block.splitlines() if ln.strip())[:5]

                if slack:
                    local_out.print(f"    {st_label}{suffix}:*")
                else:
                    local_out.print(f"    {st_label}{suffix}:")
                local_out.print("```")
                for line in display_st.splitlines()[:5]:
                    if line.strip():
                        local_out.print(line)
                local_out.print("```")

                if CFG.analyze_cause:
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

        if dup_indices:
            TOKEN_TRACKER.record_dedup_skip(len(dup_indices))
            local_out.print()
            if slack:
                local_out.print("  _Same error also appears in:_")
            else:
                local_out.print("  Same error also appears in:")
            for di in dup_indices:
                dup_job, _ = job_info[di]
                dup_name = dup_job.get("name", "unknown")
                dup_job_id = dup_job.get("databaseId", "")
                if dup_job_id:
                    dup_url = f"{run_url}/job/{dup_job_id}"
                    if slack:
                        local_out.print(f"  \u2022 `{dup_name}` \u2192 {dup_url}")
                    else:
                        local_out.print(f"  - {dup_name} -> {dup_url}")
                else:
                    if slack:
                        local_out.print(f"  \u2022 `{dup_name}`")
                    else:
                        local_out.print(f"  - {dup_name}")

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
    OUT.print(f"{'SNO':<4} | {'Pipeline':<8} | {'Phase':<20} | {'Upstream':<8} | {'Staging':<8} | {'Pinned':<8}")
    OUT.print(
        f"{'----':<4}-|-{'--------':<8}-|-{'--------------------':<20}-|-{'--------':<8}-|-{'--------':<8}-|-{'--------':<8}"
    )


def print_plain_table_row(row_no: int, pipeline: str, phase: str, up_cell: str, st_cell: str, pn_cell: str):
    OUT.print(f"{row_no:<4} | {pipeline:<8} | {phase:<20} | {up_cell:<8} | {st_cell:<8} | {pn_cell:<8}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Nightly workflow status helper — generates a status report for CI nightly runs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
environment variables:
  REPO=owner/repo              repo to query (default: auto-detect via gh)
  TODAY_UTC=YYYY-MM-DD         report date (default: last nightly run date)
  LOG_TAIL_LINES=N             log tail lines per failure (default: 150)
  STATUS_FILE=path/to/file     output file path (default: status.txt)
  GH_RETRIES=N                 gh CLI retry count (default: 5)
  GH_RETRY_SLEEP_SECONDS=N     initial retry backoff in seconds (default: 2)
  GH_HTTP_TIMEOUT=N            gh HTTP timeout in seconds (default: 60)

  LLM_API_KEY or NVIDIA_API_KEY  API key for NVIDIA LLM analysis
  LLM_API_URL                    NVIDIA LLM endpoint URL
  LLM_MODEL                     NVIDIA LLM model name
  LLM_TIMEOUT                   NVIDIA LLM timeout in seconds (default: 30)

  CLAUDE_BIN                   Claude CLI binary (default: claude)
  CLAUDE_MODEL                 Claude model to use (default: opus)
  ANTHROPIC_API_KEY            API key for Claude analysis

examples:
  python %(prog)s
  python %(prog)s --no-cause --no-fix
  GH_HTTP_TIMEOUT=180 GH_RETRIES=8 python %(prog)s
""",
    )
    p.add_argument("--print-logs", action="store_true", default=False, help="print failed log tails for each failure")
    p.add_argument(
        "--no-cause", action="store_true", default=False, help="disable AI cause analysis (enabled by default)"
    )
    p.add_argument(
        "--no-fix", action="store_true", default=False, help="disable AI fix suggestions (enabled by default)"
    )
    p.add_argument(
        "--no-claude", action="store_true", default=False, help="use NVIDIA LLM instead of Claude for analysis"
    )
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
        "velox-nightly.yml",
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
    CFG.slack_format = True

    CFG.analyze_cause = True
    CFG.analyze_fix = True
    CFG.use_claude = True

    if args.no_cause:
        CFG.analyze_cause = False
        CFG.analyze_fix = False
    if args.no_fix:
        CFG.analyze_fix = False
    if args.no_claude:
        CFG.use_claude = False

    CFG.repo = os.environ.get("REPO", "") or _detect_repo()
    if not CFG.repo:
        print(
            "ERROR: could not determine repo. Set REPO=owner/repo or run within a gh-authenticated repo.",
            file=sys.stderr,
        )
        sys.exit(2)

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


def _print_inprog_section(entries: List[Dict[str, Any]], slack: bool):
    """Print in-progress details, deduped by (run_id, variant)."""
    if not entries:
        return
    seen: set = set()
    deduped: List[Dict[str, Any]] = []
    for entry in entries:
        key = (entry["run"]["databaseId"], entry.get("variant", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(entry)

    if slack:
        OUT.print()
        OUT.print("---")
        OUT.print()
        OUT.print("*\u23f3 In-Progress Details:*")
    else:
        OUT.print()
        OUT.print("In-progress Details:")

    with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
        ip_futs = {}
        for ip_idx, entry in enumerate(deduped, 1):
            fut = pool.submit(
                _process_in_progress,
                entry["label"],
                entry["wf"],
                entry["run"],
                slack,
                ip_idx,
            )
            ip_futs[fut] = ip_idx
        ip_results: Dict[int, str] = {}
        for fut in as_completed(ip_futs):
            ip_results[ip_futs[fut]] = fut.result()
    for ip_idx in sorted(ip_results):
        OUT.print(ip_results[ip_idx], end="")


def main():
    args = parse_args()

    if not shutil.which("gh"):
        print("ERROR: missing required command: gh", file=sys.stderr)
        sys.exit(2)
    init_config(args)

    # ----- Phase 1: Fetch nightly workflow runs concurrently -----
    run_map: Dict[str, Optional[dict]] = {}
    with ThreadPoolExecutor(max_workers=MAX_GH_WORKERS) as pool:
        futures = {pool.submit(_fetch_run, wf): wf for wf in NIGHTLY_WORKFLOWS.values()}
        for fut in as_completed(futures):
            wf_file, run = fut.result()
            run_map[wf_file] = run

    # ----- Phase 2: Prefetch jobs for all runs -----
    all_runs = [r for r in run_map.values() if r]
    _prefetch_jobs_for_runs(all_runs)

    # ----- Phase 3: Build status table -----
    if CFG.slack_format:
        print_slack_header()
        for _pipeline, wf_file in NIGHTLY_WORKFLOWS.items():
            run = run_map.get(wf_file)
            if run:
                emoji = cell_for_run(run)
                OUT.print(f"\u2022 *{wf_file}:* {run['url']} {emoji}")
            else:
                OUT.print(f"\u2022 *{wf_file}:* _no run found for {CFG.today_utc}_")
        OUT.print()
        OUT.print("```")
        print_slack_table_header()
    else:
        print_plain_header()

    fail_entries: List[Dict[str, Any]] = []
    inprog_entries: List[Dict[str, Any]] = []

    row_no = 0
    prev_pipeline = ""
    for row in ROW_DEFS:
        pipeline = row["pipeline"]
        wf_file = NIGHTLY_WORKFLOWS[pipeline]
        run = run_map.get(wf_file)
        variants = PIPELINE_VARIANTS.get(pipeline, [])

        cells: Dict[str, str] = {}
        for variant in VARIANT_COLUMNS:
            if variant in variants and run:
                cells[variant] = cell_for_variant_jobs(run, variant, row["job_pattern"])
            else:
                cells[variant] = EMOJI_DASH

        if prev_pipeline and pipeline != prev_pipeline:
            if CFG.slack_format:
                OUT.print("|------|----------|----------------------|------------|-----------|----------|")
            else:
                OUT.print(
                    f"{'----':<4}-|-{'--------':<8}-|-{'--------------------':<20}-|-{'--------':<8}-|-{'--------':<8}-|-{'--------':<8}"
                )
        prev_pipeline = pipeline

        row_no += 1
        up_cell = cells["upstream"]
        st_cell = cells["staging"]
        pn_cell = cells["pinned"]

        if CFG.slack_format:
            print_slack_table_row(row_no, pipeline.title(), row["display"], up_cell, st_cell, pn_cell)
        else:
            print_plain_table_row(row_no, pipeline.title(), row["display"], up_cell, st_cell, pn_cell)

        for variant in variants:
            cell = cells[variant]
            if not run:
                continue
            entry = {
                "label": f"{pipeline.title()} {row['display']} / {variant}",
                "run": run,
                "filter": row["job_pattern"],
                "variant": variant,
                "wf": wf_file,
            }
            if cell == EMOJI_CROSS:
                fail_entries.append(entry)
            elif cell == EMOJI_HOURGLASS:
                inprog_entries.append(entry)

    if CFG.slack_format:
        OUT.print("```")

    # ----- Phase 4: Failure details -----
    if CFG.slack_format:
        OUT.print()
        OUT.print("---")
        OUT.print()
        OUT.print("*\U0001f534 Failure Details:*")

        if not fail_entries:
            OUT.print()
            OUT.print(f"_No failures detected for {CFG.today_utc}._")
            _print_inprog_section(inprog_entries, slack=True)
            OUT.flush_to(CFG.status_file)
            return

        sigs: List[str] = []
        for entry in fail_entries:
            rid = entry["run"]["databaseId"]
            sig = compute_failure_signature(rid, entry["filter"], entry.get("variant", ""))
            sigs.append(sig)

        seen_sigs: set = set()
        ordered_failures: List[Tuple[int, str]] = []
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
                    e.get("variant", ""),
                )
                fut_map[fut] = display_idx

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

        _print_inprog_section(inprog_entries, slack=True)

    else:
        OUT.print()
        OUT.print("2. Failure Details:")

        if not fail_entries:
            OUT.print()
            OUT.print(f"(No failures detected for {CFG.today_utc}.)")
            _print_inprog_section(inprog_entries, slack=False)
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
                    "",
                    entry.get("variant", ""),
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

        _print_inprog_section(inprog_entries, slack=False)

    OUT.flush_to(CFG.status_file)

    token_summary = TOKEN_TRACKER.summary()
    if token_summary:
        print(f"\n{token_summary}", file=sys.stderr)


if __name__ == "__main__":
    main()
