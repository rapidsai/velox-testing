#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Upstream issue / PR search: query extraction, repo classification, GitHub search."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from .config import Config
from .gh import run_gh_safe

# ---- Regex patterns for query extraction ----------------------------------

_GTEST_FAILED_NAMES_RE = re.compile(r"(?<=\[  FAILED  \] )[A-Z][A-Za-z0-9_]*\.[A-Za-z][A-Za-z0-9_]*")
_CPP_QUALIFIED = re.compile(r"[A-Z][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)+")
_JAVA_METHOD = re.compile(r"[A-Z][A-Za-z0-9_]*\.[a-z][A-Za-z0-9_]*")
_SOURCE_FILE = re.compile(r"[A-Z][A-Za-z0-9_]+\.(?:cpp|h|cu|cuh|java)")
_ERROR_INDICATOR = re.compile(r"error|undefined|unresolved|FAILED|fatal", re.IGNORECASE)
_UNDEFINED_REF = re.compile(r"undefined reference to\s+[`']([^`']+)[`']")
_ARCHIVE_FILE = re.compile(r"([\w.-]{3,})\.(?:tar\.gz|tgz|tar\.bz2|tar\.xz|zip)\b")
_FAILED_TARGET = re.compile(r"^FAILED:\s+(\S+)", re.MULTILINE)
# Semantic runtime-error signals: the *symbol* that failed and the human reason.
# These point at what actually broke (e.g. the ``substr`` in "Unsupported
# expression: substr") rather than the source file the error was thrown from,
# so they make far better related-issue/PR search terms.
#
# Rather than hard-code one phrasing, match the common shapes of C++/Velox
# semantic errors generically:
#   * "<error-keyword> ... : <symbol>"   e.g. "Unsupported expression: substr",
#                                             "Unknown function: foo",
#                                             "Scalar function not registered: bar",
#                                             "Cannot resolve type: MAP"
#   * "'<symbol>' <error-keyword>"       e.g. "'substr' is not supported",
#                                             "function `foo` not registered"
_ERR_KEYWORDS = (
    r"un(?:supported|known|registered|resolved|defined|implemented|recognized)"
    r"|not\s+(?:supported|implemented|registered|found|defined|recognized)"
    r"|no\s+(?:such|matching)(?:\s+\w+)?"
    r"|cannot\s+(?:find|resolve|open|cast|convert|register)"
    r"|failed\s+to\s+(?:find|resolve|open|load|cast|convert|register)"
    r"|(?:un)?expected|missing|duplicate|ambiguous"
)
# "<keyword> ...: <symbol>"  (colon-terminated; symbol may be quoted)
_ERR_SYMBOL_PREFIX = re.compile(
    rf"\b(?:{_ERR_KEYWORDS})\b[^:\n]{{0,40}}:\s*['\"`]?([A-Za-z_][\w.:]*)",
    re.IGNORECASE,
)
# "'<symbol>' <keyword>"  (symbol quoted to avoid grabbing filler words)
_ERR_SYMBOL_SUFFIX = re.compile(
    rf"['\"`]([A-Za-z_][\w.:]*)['\"`]\s+(?:is\s+|was\s+|are\s+|were\s+)?"
    rf"(?:{_ERR_KEYWORDS})\b",
    re.IGNORECASE,
)
_REASON_LINE = re.compile(r"\bReason:\s*(.+)")
# Enum-ish error codes / severities that are never a useful search symbol.
_ERR_CODE_TOKEN = re.compile(r"^[A-Z][A-Z0-9_]{2,}$")


# ---- Query extraction helpers ---------------------------------------------


def extract_gtest_test_names(text: str) -> list[str]:
    return sorted(set(_GTEST_FAILED_NAMES_RE.findall(text)))[:5]


def extract_error_phrases(text: str) -> list[str]:
    """Extract high-signal semantic search terms from runtime error messages.

    Prioritises the failing *symbol* (e.g. ``substr`` from "Unsupported
    expression: substr") and the error *reason* phrase, both of which match the
    PRs/issues that actually fix the failure far better than the C++/source-file
    identifiers, which only match PRs that happen to touch the throwing file.
    """
    symbols: list[str] = []
    for rx in (_ERR_SYMBOL_PREFIX, _ERR_SYMBOL_SUFFIX):
        for m in rx.finditer(text):
            # Trim wrapping punctuation and normalise C++ qualifiers for search.
            sym = m.group(1).strip().strip("'\"`").rstrip(".,;:)").replace("::", " ").strip()
            if not sym or sym.isdigit():
                continue
            # Skip enum-ish error codes/severities (INVALID_STATE, RUNTIME, ...).
            if all(_ERR_CODE_TOKEN.match(tok) for tok in sym.split()):
                continue
            symbols.append(sym)

    phrases: list[str] = []
    for m in _REASON_LINE.finditer(text):
        # Sanitise for gh free-text search: colons/quotes/braces confuse the
        # search qualifier parser, so strip them and collapse whitespace.
        reason = re.sub(r"[\"'`:{}]", " ", m.group(1))
        reason = re.sub(r"\s+", " ", reason).strip()
        if 3 <= len(reason) <= 80:
            phrases.append(reason)

    out: list[str] = []
    seen: set[str] = set()
    for term in symbols + phrases:  # symbols first — most specific
        key = term.lower()
        if term and key not in seen:
            seen.add(key)
            out.append(term)
    return out[:3]


def extract_error_identifiers(text: str) -> list[str]:
    error_lines = [ln for ln in text.splitlines() if _ERROR_INDICATOR.search(ln)]
    blob = "\n".join(error_lines)
    ids: set[str] = set()
    ids.update(_CPP_QUALIFIED.findall(blob)[:5])
    ids.update(_JAVA_METHOD.findall(blob)[:5])
    ids.update(_SOURCE_FILE.findall(blob)[:5])
    return sorted(ids)[:8]


def _extract_search_query(text: str) -> str:
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


# ---- Repo classification --------------------------------------------------


def classify_repos(label: str) -> list[str]:
    """Determine upstream repos to search based on a job/failure label."""
    ll = label.lower()
    if "presto" in ll:
        return ["prestodb/presto", "facebookincubator/velox"]
    if "velox" in ll:
        return ["facebookincubator/velox"]
    return ["facebookincubator/velox", "prestodb/presto"]


# ---- GitHub search ---------------------------------------------------------


def _search_gh_items(
    repo: str,
    term: str,
    kind: str,
    since_date: str,
    config: Config,
) -> list[tuple[str, str]]:
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
        config=config,
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


def _run_item_searches(
    repo: str,
    terms: list[str],
    since_date: str,
    config: Config,
    prefix: str,
) -> list[str]:
    """Run issue+PR searches for each term in parallel, de-duplicated by URL."""
    seen_urls: set[str] = set()
    all_items: list[str] = []
    tasks = []
    with ThreadPoolExecutor(max_workers=config.max_gh_workers) as pool:
        for term in terms:
            for kind in ("issues", "prs"):
                tasks.append(pool.submit(_search_gh_items, repo, term, kind, since_date, config))
        for fut in as_completed(tasks):
            try:
                for display, url in fut.result():
                    if url not in seen_urls:
                        seen_urls.add(url)
                        all_items.append(f"{prefix}{display}")
            except Exception:
                pass
    return all_items


def find_related_github_items(
    repo: str,
    stacktrace: str,
    since_date: str,
    config: Config,
    prefix: str = "    - ",
) -> str:
    """Search for related issues / PRs based on a stacktrace.

    Uses a two-tier strategy:

    * **Tier 1 (semantic)** — the failing symbol / reason phrase extracted from
      the error message. These are high-signal, so if they match anything we
      return *only* those hits and skip the broader search below.
    * **Tier 2 (fallback)** — gtest test names, C++/source-file identifiers, or a
      generic error line. This is only used when Tier 1 finds nothing, because
      file-name searches tend to surface loosely-related or wrong items.
    """
    if not repo:
        return ""

    # Tier 1: high-signal semantic error symbol/reason.
    primary_terms = [t for t in extract_error_phrases(stacktrace) if t][:3]
    if primary_terms:
        items = _run_item_searches(repo, primary_terms, since_date, config, prefix)
        if items:
            return "\n".join(items[:15])

    # Tier 2: fall back to structural identifiers only when Tier 1 found nothing.
    fallback_terms: list[str] = []
    for name in extract_gtest_test_names(stacktrace):
        fallback_terms.append(name)
        method = name.split(".")[-1] if "." in name else ""
        if method and method != name:
            fallback_terms.append(method)
    for ident in extract_error_identifiers(stacktrace):
        fallback_terms.append(ident.replace("::", " "))
    if not fallback_terms:
        q = _extract_search_query(stacktrace)
        if q:
            fallback_terms.append(q)

    capped = [t for t in fallback_terms if t][:3]
    if not capped:
        return ""

    items = _run_item_searches(repo, capped, since_date, config, prefix)
    return "\n".join(items[:15])


def search_related_github_prs(identifiers: list[str], config: Config) -> str:
    """Search velox and presto repos for related issues/PRs."""
    try:
        since_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    except Exception:
        since_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not identifiers:
        return ""

    search_terms: list[str] = []
    for ident in identifiers:
        term = ident.replace("::", " ")
        search_terms.append(term)
        if "." in term:
            method = term.rsplit(".", 1)[-1]
            if method:
                search_terms.append(method)

    repos = ["facebookincubator/velox", "prestodb/presto"]
    seen_urls: set[str] = set()
    all_items: list[str] = []

    tasks = []
    with ThreadPoolExecutor(max_workers=config.max_gh_workers) as pool:
        for term in search_terms:
            if not term:
                continue
            for repo in repos:
                for kind in ("issues", "prs"):
                    tasks.append(pool.submit(_search_gh_items, repo, term, kind, since_date, config))
        for fut in as_completed(tasks):
            try:
                for display, url in fut.result():
                    if url not in seen_urls:
                        seen_urls.add(url)
                        all_items.append(f"  - {display}")
            except Exception:
                pass

    return "\n".join(all_items[:20])
