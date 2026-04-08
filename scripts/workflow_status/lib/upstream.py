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


# ---- Query extraction helpers ---------------------------------------------


def extract_gtest_test_names(text: str) -> list[str]:
    return sorted(set(_GTEST_FAILED_NAMES_RE.findall(text)))[:5]


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


def find_related_github_items(
    repo: str,
    stacktrace: str,
    since_date: str,
    config: Config,
    prefix: str = "    - ",
) -> str:
    """Search for related issues / PRs based on a stacktrace."""
    if not repo:
        return ""

    search_terms: list[str] = []
    for name in extract_gtest_test_names(stacktrace):
        search_terms.append(name)
        method = name.split(".")[-1] if "." in name else ""
        if method and method != name:
            search_terms.append(method)
    for ident in extract_error_identifiers(stacktrace):
        search_terms.append(ident.replace("::", " "))
    if not search_terms:
        q = _extract_search_query(stacktrace)
        if q:
            search_terms.append(q)

    capped = [t for t in search_terms if t][:3]
    if not capped:
        return ""

    seen_urls: set[str] = set()
    all_items: list[str] = []

    tasks = []
    with ThreadPoolExecutor(max_workers=config.max_gh_workers) as pool:
        for term in capped:
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

    return "\n".join(all_items[:15])


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
