#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Nightly failure root cause analysis — searches upstream repos for related issues/PRs.

Reads a Slack Block Kit JSON payload (from check_nightly_status.py) and
outputs a Slack Block Kit JSON payload with the RCA report.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Stacktrace:
    text: str
    cause: str = ""
    fix: str = ""


@dataclass
class Failure:
    number: int
    title: str
    pipeline: str = ""  # "velox" or "presto" — parsed from structured label
    variant: str = ""  # "upstream", "staging", or "pinned"
    workflow: str = ""
    run_url: str = ""
    also_fails_in: str = ""
    conclusion: str = ""
    job_name: str = ""
    step_name: str = ""
    stacktraces: list[Stacktrace] = field(default_factory=list)


@dataclass
class SearchResult:
    repo: str
    number: int
    title: str
    url: str
    kind: str  # "issue" or "pr"
    state: str = ""


# ---------------------------------------------------------------------------
# Status payload parser
# ---------------------------------------------------------------------------

_FAILURE_HEADING = re.compile(r"^\*(\d+)\.\s+(.+)\*$")
_METADATA_RE = {
    "workflow": re.compile(r"^•\s*\*Workflow:\*\s*(.+)$"),
    "run_url": re.compile(r"^•\s*\*Run:\*\s*(https?://\S+)"),
    "also_fails_in": re.compile(r"^•\s*\*Also fails in:\*\s*(.+)$"),
    "conclusion": re.compile(r"^•\s*\*Conclusion:\*\s*(\S+)"),
}
_JOB_RE = re.compile(r"^\s*◦\s*Job:\s*`(.+?)`")
_STEP_RE = re.compile(r"^\s*▪︎\s*Step:\s*(.+?)(?:\s*\((?:failure|error)\))?$")
_CAUSE_RE = re.compile(r"^\s*\*Cause:\*\s*_(.+?)_?\s*$")
_FIX_RE = re.compile(r"^\s*\*Fix:\*\s*_(.+?)_?\s*$")


def _extract_mrkdwn_from_payload(payload: dict) -> str:
    """Reconstruct mrkdwn text from a Slack Block Kit payload.

    Section blocks contribute their text content; divider blocks become
    '---' separators — matching the format originally parsed from status.txt.
    """
    parts: list[str] = []
    for block in payload.get("blocks", []):
        if block.get("type") == "divider":
            parts.append("---")
        elif block.get("type") == "section":
            text_obj = block.get("text", {})
            parts.append(text_obj.get("text", ""))
    return "\n".join(parts)


def _parse_mrkdwn(text: str) -> list[Failure]:
    """Parse failure entries from mrkdwn text (with --- separators)."""
    sections = re.split(r"^---\s*$", text, flags=re.MULTILINE)
    failures: list[Failure] = []

    for section in sections:
        lines = section.strip().splitlines()
        if not lines:
            continue

        heading_match = None
        for i, line in enumerate(lines):
            heading_match = _FAILURE_HEADING.match(line.strip())
            if heading_match:
                lines = lines[i:]
                break
        if not heading_match:
            continue

        raw_title = heading_match.group(2)
        pipeline = ""
        variant = ""
        if " / " in raw_title:
            phase_part, variant = raw_title.rsplit(" / ", 1)
            variant = variant.strip()
            first_word = phase_part.split()[0].lower() if phase_part else ""
            if first_word in ("velox", "presto"):
                pipeline = first_word

        fail = Failure(
            number=int(heading_match.group(1)),
            title=raw_title,
            pipeline=pipeline,
            variant=variant,
        )

        in_stacktrace = False
        stacktrace_lines: list[str] = []

        for line in lines[1:]:
            stripped = line.strip()

            if stripped.startswith("```"):
                if in_stacktrace:
                    fail.stacktraces.append(Stacktrace(text="\n".join(stacktrace_lines)))
                    stacktrace_lines = []
                    in_stacktrace = False
                else:
                    in_stacktrace = True
                continue

            if in_stacktrace:
                stacktrace_lines.append(line)
                continue

            for key, pattern in _METADATA_RE.items():
                m = pattern.match(stripped)
                if m:
                    setattr(fail, key, m.group(1))
                    break

            m = _JOB_RE.match(stripped)
            if m:
                fail.job_name = m.group(1)
                continue

            m = _STEP_RE.match(stripped)
            if m:
                fail.step_name = m.group(1)
                continue

            m = _CAUSE_RE.match(stripped)
            if m:
                if fail.stacktraces:
                    fail.stacktraces[-1].cause = m.group(1)
                continue

            m = _FIX_RE.match(stripped)
            if m:
                if fail.stacktraces:
                    fail.stacktraces[-1].fix = m.group(1)
                continue

        if fail.stacktraces or fail.title:
            failures.append(fail)

    return failures


def parse_status_file(path: str) -> list[Failure]:
    """Parse failures from a Slack Block Kit JSON payload file."""
    with open(path, encoding="utf-8") as f:
        raw = f.read()
    try:
        payload = json.loads(raw)
        text = _extract_mrkdwn_from_payload(payload)
    except (json.JSONDecodeError, KeyError):
        text = raw
    return _parse_mrkdwn(text)


# ---------------------------------------------------------------------------
# Keyword extraction
# ---------------------------------------------------------------------------

_GTEST_FAILED = re.compile(r"\[\s*FAILED\s*\]\s+(\w+\.\w+)")
_CPP_ERROR = re.compile(r"error:\s+'(\w+)'\s+is not a member of\s+'([\w:]+)'")
_CPP_GENERIC_ERROR = re.compile(r"error:\s+(.{10,80})")
_PACKAGE_INSTALL = re.compile(r"(?:dnf|yum|apt-get)\s+install\s+-y\s+([\w.-]+)")
_FAILED_TARGET = re.compile(r"^FAILED:\s+(\S+)", re.MULTILINE)
_FUNCTION_SIG = re.compile(r"Scalar function signature is not supported:\s+(\w+)\(([^)]+)\)")
_TAR_ARCHIVE = re.compile(r"(?:tar\s+.*|extracting|downloading)\s+([\w.-]+\.tar(?:\.gz|\.bz2|\.xz)?)", re.IGNORECASE)
_ARCHIVE_FILE = re.compile(r"([\w.-]{3,})\.(?:tar\.gz|tgz|tar\.bz2|tar\.xz|zip)\b")
_UNDEFINED_REF = re.compile(r"undefined reference to\s+[`']([^`']+)[`']")


def _classify_repos(failure: Failure) -> list[str]:
    """Determine which upstream repos to search based on failure context.

    Uses the parsed ``pipeline`` field from the structured label
    (e.g. "Velox Deps Build / upstream") with a substring fallback for
    status files produced by older versions of check_nightly_status.py.

    - presto pipeline → prestodb/presto + facebookincubator/velox
    - velox pipeline  → facebookincubator/velox only
    - Fallback        → both upstream repos
    """
    if failure.pipeline == "presto":
        return ["prestodb/presto", "facebookincubator/velox"]
    if failure.pipeline == "velox":
        return ["facebookincubator/velox"]

    title_lower = failure.title.lower()
    if "presto" in title_lower:
        return ["prestodb/presto", "facebookincubator/velox"]
    if "velox" in title_lower:
        return ["facebookincubator/velox"]

    return ["facebookincubator/velox", "prestodb/presto"]


def extract_search_queries(failure: Failure) -> list[str]:
    """Extract meaningful search queries from a failure's stacktraces.

    Priority order (highest → lowest signal):
      1. GTest failed test names
      2. C++ compile errors (symbol / namespace)
      3. Undefined reference symbols
      4. Function signature mismatches
      5. Failed archive names (tar/gzip errors)
      6. Failed build targets
      7. Package install failures
      8. Cause-text keywords (backtick-quoted identifiers)
    """
    queries: list[str] = []
    all_stacktrace_text = "\n".join(st.text for st in failure.stacktraces)

    # 1. GTest failed test names — highest signal
    for m in _GTEST_FAILED.finditer(all_stacktrace_text):
        test_name = m.group(1)
        queries.append(test_name)
        if "." in test_name:
            queries.append(test_name.split(".")[-1])

    # 2. C++ compile errors: 'X' is not a member of 'Y'
    for m in _CPP_ERROR.finditer(all_stacktrace_text):
        symbol, namespace = m.group(1), m.group(2)
        queries.append(f"{namespace} {symbol}")

    # 3. Undefined reference errors
    for m in _UNDEFINED_REF.finditer(all_stacktrace_text):
        queries.append(m.group(1))

    # 4. Function signature mismatches (Velox-specific)
    for m in _FUNCTION_SIG.finditer(all_stacktrace_text):
        queries.append(f"{m.group(1)} {m.group(2)}")

    # 5. Failed archive / tar / gzip errors — use specific queries, not bare names
    for m in _ARCHIVE_FILE.finditer(all_stacktrace_text):
        component = m.group(1)
        if len(component) > 2:
            queries.append(f"{component} download")
            queries.append(f"{component} build")

    # 6. Failed build targets (e.g. grpc_ep)
    for m in _FAILED_TARGET.finditer(all_stacktrace_text):
        target = m.group(1).split("/")[-1].split("-")[0]
        if len(target) > 3:
            queries.append(target)

    # 7. Package install failures
    for m in _PACKAGE_INSTALL.finditer(all_stacktrace_text):
        queries.append(m.group(1))

    # 8. From the cause text — extract backtick-quoted identifiers
    for st in failure.stacktraces:
        if st.cause:
            cause_keywords = _extract_cause_keywords(st.cause)
            queries.extend(cause_keywords)

    # Deduplicate while preserving order, filtering out low-quality queries
    seen: set[str] = set()
    unique: list[str] = []
    for q in queries:
        q = q.strip()
        if not q or q.lower() in seen:
            continue
        if q.startswith("/") or q.startswith(":") or q.startswith("http"):
            continue
        if len(q) < 4:
            continue
        if q.count("/") > 2:
            continue
        # Skip bare dockerfile names — too broad, matches any PR touching the file
        if q.endswith(".dockerfile") or q.endswith(".Dockerfile"):
            continue
        # Skip common shell commands / generic terms that produce noisy results
        if q.lower() in (
            "tar -xz",
            "tar -xzf",
            "curl -l",
            "curl -sl",
            "make",
            "cmake",
            "gzip",
            "stdin",
            "mkdir",
            "bash",
            "build",
            "error",
        ):
            continue
        seen.add(q.lower())
        unique.append(q)

    return unique[:8]


def _extract_cause_keywords(cause: str) -> list[str]:
    """Extract searchable terms from AI-generated cause text."""
    keywords: list[str] = []

    # Look for backtick-quoted identifiers
    for m in re.finditer(r"`([^`]{3,60})`", cause):
        val = m.group(1)
        if not val.startswith("http") and not val.startswith("/"):
            keywords.append(val)

    # Look for error-related noun phrases
    for m in re.finditer(r"(?:API|version)\s+(?:incompatibility|mismatch)", cause, re.IGNORECASE):
        keywords.append(m.group(0))

    return keywords[:3]


# ---------------------------------------------------------------------------
# GitHub search
# ---------------------------------------------------------------------------


def _run_gh(*args: str, timeout: int = 30) -> str:
    try:
        proc = subprocess.run(
            ["gh", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return ""


def _search_gh(
    repo: str,
    query: str,
    kind: str,
    since_date: str,
    max_results: int,
) -> list[SearchResult]:
    """Search a single repo for issues or PRs matching the query.

    Tries `gh search` first (global search), then falls back to
    `gh issue/pr list --search` (repo-scoped search) which often
    has better results for short or code-like queries.
    """
    results: list[SearchResult] = []

    # Strategy 1: gh search (global)
    gh_kind = "issues" if kind == "issue" else "prs"
    raw = _run_gh(
        "search",
        gh_kind,
        "--repo",
        repo,
        "--limit",
        str(max_results),
        "--json",
        "title,url,number,state",
        f"{query} created:>={since_date}",
        timeout=60,
    )
    if raw:
        try:
            for it in json.loads(raw):
                results.append(
                    SearchResult(
                        repo=repo,
                        number=it.get("number", 0),
                        title=it.get("title", ""),
                        url=it.get("url", ""),
                        kind=kind,
                        state=it.get("state", "").upper(),
                    )
                )
        except json.JSONDecodeError:
            pass

    # Strategy 2: gh issue/pr list --search (repo-scoped, often better)
    if len(results) < max_results:
        list_cmd = "issue" if kind == "issue" else "pr"
        raw2 = _run_gh(
            list_cmd,
            "list",
            "--repo",
            repo,
            "--search",
            f"{query} created:>={since_date}",
            "--limit",
            str(max_results),
            "--json",
            "title,url,number,state",
            timeout=60,
        )
        if raw2:
            try:
                seen = {r.url for r in results}
                for it in json.loads(raw2):
                    url = it.get("url", "")
                    if url not in seen:
                        seen.add(url)
                        results.append(
                            SearchResult(
                                repo=repo,
                                number=it.get("number", 0),
                                title=it.get("title", ""),
                                url=url,
                                kind=kind,
                                state=it.get("state", "").upper(),
                            )
                        )
            except json.JSONDecodeError:
                pass

    return results[:max_results]


def search_upstream(
    failure: Failure,
    repos: list[str],
    since_date: str,
    max_results: int,
    extra_repos: list[str] | None = None,
) -> list[SearchResult]:
    """Search all relevant repos for issues/PRs related to a failure."""
    all_repos = list(repos)
    if extra_repos:
        for r in extra_repos:
            if r not in all_repos:
                all_repos.append(r)

    queries = extract_search_queries(failure)
    if not queries:
        return []

    seen_urls: set[str] = set()
    all_results: list[SearchResult] = []

    tasks = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for query in queries[:5]:
            for repo in all_repos:
                for kind in ("issue", "pr"):
                    tasks.append(pool.submit(_search_gh, repo, query, kind, since_date, max_results))
        for fut in as_completed(tasks):
            try:
                for result in fut.result():
                    if result.url not in seen_urls:
                        seen_urls.add(result.url)
                        all_results.append(result)
            except Exception:
                pass

    return all_results


# ---------------------------------------------------------------------------
# Output formatting (Slack mrkdwn)
# ---------------------------------------------------------------------------


def _state_badge(state: str) -> str:
    s = state.upper()
    if s in ("OPEN",):
        return "OPEN"
    if s in ("CLOSED", "MERGED"):
        return "MERGED" if s == "MERGED" else "CLOSED"
    return s


def format_rca_report(
    failures: list[Failure],
    results_by_failure: dict[int, list[SearchResult]],
    date_str: str,
) -> str:
    """Produce Slack-formatted RCA report."""
    lines: list[str] = []

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        formatted_date = dt.strftime("%B %d, %Y")
    except Exception:
        formatted_date = date_str

    lines.append(f"*🔍 Nightly Failure Root Cause Analysis — {formatted_date}*")
    lines.append("")
    lines.append("*Upstream issues and PRs associated with each nightly failure.*")
    lines.append("")
    lines.append("---")

    for fail in failures:
        lines.append("")
        lines.append(f"*{fail.number}. {fail.title}*")

        if fail.pipeline or fail.variant:
            parts = []
            if fail.pipeline:
                parts.append(f"pipeline=`{fail.pipeline}`")
            if fail.variant:
                parts.append(f"variant=`{fail.variant}`")
            lines.append(f"• *Source:* {', '.join(parts)}")
        if fail.workflow:
            lines.append(f"• *Workflow:* {fail.workflow}")
        if fail.run_url:
            lines.append(f"• *Run:* {fail.run_url}")
        if fail.also_fails_in:
            lines.append(f"• *Also fails in:* {fail.also_fails_in}")

        # Error summary from stacktraces
        error_summary = _build_error_summary(fail)
        if error_summary:
            lines.append(f"• *Error:* {error_summary}")

        # Search results grouped by repo
        results = results_by_failure.get(fail.number, [])
        if results:
            by_repo: dict[str, list[SearchResult]] = {}
            for r in results:
                by_repo.setdefault(r.repo, []).append(r)

            lines.append("")
            lines.append("• *Related Upstream Issues/PRs:*")
            for repo, repo_results in sorted(by_repo.items()):
                lines.append(f"  _`{repo}`_:")
                for r in repo_results[:3]:
                    kind_label = "PR" if r.kind == "pr" else "Issue"
                    state_label = _state_badge(r.state)
                    lines.append(f"  ◦ [{kind_label}] #{r.number} — {r.title} ({state_label})")
                    lines.append(f"    {r.url}")
        else:
            lines.append("")
            lines.append(
                "• *Related Upstream Issues/PRs:* _No matching issues/PRs found. This may need a new bug report._"
            )

        # Include the AI-generated cause/fix from status.txt
        causes = [st.cause for st in fail.stacktraces if st.cause and "Unable to" not in st.cause]
        fixes = [st.fix for st in fail.stacktraces if st.fix and "Unable to" not in st.fix]

        if causes:
            lines.append("")
            cause_text = causes[0]
            if len(cause_text) > 1000:
                cause_text = cause_text[:997] + "..."
            lines.append(f"• *Root Cause:* _{cause_text}_")

        if fixes:
            fix_text = fixes[0]
            if len(fix_text) > 1000:
                fix_text = fix_text[:997] + "..."
            lines.append(f"• *Suggested Fix:* _{fix_text}_")

        lines.append("")
        lines.append("---")

    return "\n".join(lines)


SLACK_BLOCK_TEXT_LIMIT = 3000


def _split_mrkdwn_sections(content: str) -> list[str]:
    """Split mrkdwn content on '---' lines into individual sections."""
    sections: list[str] = []
    current: list[str] = []
    for line in content.splitlines(keepends=True):
        if re.match(r"^\s*---\s*$", line):
            text = "".join(current).strip()
            if text:
                sections.append(text)
            current = []
        else:
            current.append(line)
    trailing = "".join(current).strip()
    if trailing:
        sections.append(trailing)
    return sections


def _split_code_blocks(section: str) -> list[str]:
    """Split a section into alternating text and code-fenced fragments."""
    fragments: list[str] = []
    current: list[str] = []
    in_code = False
    for line in section.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("```"):
            if in_code:
                current.append(line)
                fragments.append("".join(current).strip())
                current = []
                in_code = False
            else:
                text_before = "".join(current).strip()
                if text_before:
                    fragments.append(text_before)
                current = [line]
                in_code = True
        else:
            current.append(line)
    trailing = "".join(current).strip()
    if trailing:
        if in_code:
            trailing += "\n```"
        fragments.append(trailing)
    return [f for f in fragments if f]


def _chunk_text(text: str, max_len: int = SLACK_BLOCK_TEXT_LIMIT) -> list[str]:
    """Split text into chunks that fit within Slack's block text limit."""
    if len(text) <= max_len:
        return [text]
    is_code = text.lstrip().startswith("```")
    chunks: list[str] = []
    lines = text.splitlines(keepends=True)
    current: list[str] = []
    current_len = 0
    for line in lines:
        if current_len + len(line) > max_len and current:
            chunk = "".join(current).rstrip()
            if is_code and not chunk.rstrip().endswith("```"):
                chunk += "\n```"
            chunks.append(chunk)
            current = []
            current_len = 0
            if is_code:
                current.append("```\n")
                current_len = 4
        current.append(line)
        current_len += len(line)
    if current:
        chunks.append("".join(current).rstrip())
    return chunks


def _build_slack_payload(mrkdwn_text: str) -> dict:
    """Convert mrkdwn text (with --- separators) into a Slack Block Kit payload."""
    sections = _split_mrkdwn_sections(mrkdwn_text)
    blocks: list[dict] = []
    for i, section in enumerate(sections):
        if i > 0:
            blocks.append({"type": "divider"})
        for fragment in _split_code_blocks(section):
            for chunk in _chunk_text(fragment):
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": chunk}})
    fallback = sections[0][:200] if sections else ""
    return {"text": fallback, "blocks": blocks}


def _build_error_summary(fail: Failure) -> str:
    """Build a one-line error summary from the stacktraces."""
    summaries: list[str] = []
    for st in fail.stacktraces:
        text = st.text
        if not text.strip():
            continue

        # GTest failure
        m = _GTEST_FAILED.search(text)
        if m:
            summaries.append(f"`{m.group(1)}`")
            continue

        # C++ compile error
        m = _CPP_ERROR.search(text)
        if m:
            summaries.append(f"`{m.group(1)}` not a member of `{m.group(2)}`")
            continue

        # Generic error line
        m = _CPP_GENERIC_ERROR.search(text)
        if m:
            summaries.append(f"`{m.group(1).strip()}`")
            continue

        # First meaningful error line
        for line in text.splitlines():
            line = line.strip()
            if any(kw in line.lower() for kw in ("error", "failed", "fatal")):
                clean = re.sub(r"^#\d+\s+[\d.]+\s*", "", line).strip()
                if clean and len(clean) > 10:
                    summaries.append(f"`{clean[:120]}`")
                    break

    seen: set[str] = set()
    unique = []
    for s in summaries:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    return ", ".join(unique[:3]) if unique else ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Nightly failure root cause analysis — parses a Slack Block Kit "
            "JSON payload (from check_nightly_status.py), extracts error "
            "signatures, and searches upstream GitHub repos for related issues/PRs. "
            "Outputs a Slack Block Kit JSON payload."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Failure labels from check_nightly_status.py follow the format
"{Pipeline} {Phase} / {variant}" (e.g. "Velox Deps Build / upstream").
This script parses the pipeline and variant from those labels for
accurate upstream-repo classification and reporting.

requirements:
  gh (GitHub CLI), authenticated
  Python 3.9+

examples:
  python %(prog)s -i status-payload.json -o rca-payload.json
  python %(prog)s --days 14 --max-results 3
  python %(prog)s --repos rapidsai/cudf facebookincubator/velox
""",
    )
    p.add_argument(
        "-i",
        "--input",
        default="status-payload.json",
        help="path to the status payload JSON (default: status-payload.json)",
    )
    p.add_argument(
        "-o",
        "--output",
        default="rca-payload.json",
        help="path to write the RCA payload JSON (default: rca-payload.json)",
    )
    p.add_argument(
        "--days",
        type=int,
        default=30,
        help="Search issues/PRs created within the last N days (default: 30)",
    )
    p.add_argument(
        "--max-results",
        type=int,
        default=5,
        help="Max results per search query per repo (default: 5)",
    )
    p.add_argument(
        "--repos",
        nargs="+",
        default=[],
        help="Additional repos to search (e.g. rapidsai/cudf)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    print(f"Parsing {args.input}...", file=sys.stderr)
    failures = parse_status_file(args.input)
    if not failures:
        print("No failures found in status file.", file=sys.stderr)
        sys.exit(0)

    print(f"Found {len(failures)} failure(s). Searching upstream repos...", file=sys.stderr)

    since_date = (datetime.now(timezone.utc) - timedelta(days=args.days)).strftime("%Y-%m-%d")
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    results_by_failure: dict[int, list[SearchResult]] = {}

    for fail in failures:
        repos = _classify_repos(fail)

        queries = extract_search_queries(fail)
        query_preview = ", ".join(f'"{q}"' for q in queries[:3])
        repo_preview = ", ".join(repos)
        variant_tag = f" [{fail.variant}]" if fail.variant else ""
        print(
            f"  [{fail.number}] {fail.title}{variant_tag}\n      Repos: {repo_preview}\n      Queries: {query_preview}",
            file=sys.stderr,
        )

        results = search_upstream(
            failure=fail,
            repos=repos,
            since_date=since_date,
            max_results=args.max_results,
            extra_repos=args.repos,
        )
        results_by_failure[fail.number] = results
        print(f"      → {len(results)} result(s)", file=sys.stderr)

    report_mrkdwn = format_rca_report(failures, results_by_failure, date_str)

    print(report_mrkdwn)

    payload = _build_slack_payload(report_mrkdwn)
    if args.output and args.output != "-":
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        block_count = len(payload.get("blocks", []))
        print(f"\nWrote {args.output} ({block_count} Slack blocks)", file=sys.stderr)


if __name__ == "__main__":
    main()
