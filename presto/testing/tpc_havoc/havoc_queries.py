# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""TPC-Havoc query generation via BenchBox.

Base TPC-H queries come from BenchBox's qgen wrapper with the official TPC-H
default substitution parameters (``qgen -d``; i.e. no seed), and the variants
are BenchBox's static TPC-Havoc rewrites of those same queries. The output is
therefore fully deterministic for a given BenchBox version and scale factor,
so CPU and GPU runs (and DuckDB) execute identical query semantics.

Query IDs used by the runner:
    Q<n>       base TPC-H query n
    Q<n>_V<m>  TPC-Havoc variant m of query n
"""

from __future__ import annotations

import functools
import hashlib
import json
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from importlib.metadata import distribution
from pathlib import Path

BASE_DIALECT = "netezza"  # BenchBox's TPC base dialect (see get_tpc_base_dialect)

_ID_RE = re.compile(r"^Q?(\d+)(?:_V(\d+))?$", re.IGNORECASE)


@dataclass
class HavocQuery:
    query_id: str  # e.g. "Q1" or "Q1_V3"
    query_number: int
    variant: int | None  # None for the base query
    description: str
    sql: dict[str, str] = field(default_factory=dict)  # dialect -> SQL text

    @property
    def base_id(self) -> str:
        return f"Q{self.query_number}"

    @property
    def is_base(self) -> bool:
        return self.variant is None

    def sql_hash(self, dialect: str) -> str:
        return hashlib.sha256(self.sql[dialect].encode()).hexdigest()[:16]


def parse_query_id(token: str) -> tuple[int, int | None]:
    match = _ID_RE.match(token.strip())
    if not match:
        raise ValueError(f"Invalid TPC-Havoc query id: {token!r} (expected N, QN or QN_VM)")
    return int(match.group(1)), (int(match.group(2)) if match.group(2) else None)


def sort_key(query_id: str) -> tuple[int, int]:
    number, variant = parse_query_id(query_id)
    return number, variant or 0


def select_query_ids(all_ids: list[str], filter_spec: str | None) -> list[str]:
    """Resolve a --queries filter to an ordered list of query ids.

    Tokens: ``N`` or ``QN`` select the whole group (base + all variants);
    ``QN_VM`` selects one variant. The base query of every selected variant is
    always included, since variants are validated against it.
    """
    if not filter_spec:
        return sorted(all_ids, key=sort_key)
    known = set(all_ids)
    selected: set[str] = set()
    for token in filter_spec.split(","):
        token = token.strip()
        if not token:
            continue
        number, variant = parse_query_id(token)
        if variant is None:
            group = {qid for qid in known if parse_query_id(qid)[0] == number}
            if not group:
                raise ValueError(f"No TPC-Havoc queries found for {token!r}")
            selected |= group
        else:
            qid = f"Q{number}_V{variant}"
            if qid not in known:
                raise ValueError(f"Unknown TPC-Havoc query {token!r}")
            selected |= {qid, f"Q{number}"}
    return sorted(selected, key=sort_key)


def benchbox_version() -> dict[str, str | None]:
    """BenchBox version and the commit the SQL comes from: the pinned source archive, a VCS
    install, or the git checkout of an editable (--benchbox-path) install."""
    import benchbox
    import sqlglot

    info = {"version": getattr(benchbox, "__version__", None), "commit": None, "sqlglot": sqlglot.__version__}
    try:
        direct_url = json.loads(distribution("benchbox").read_text("direct_url.json") or "{}")
        info["source"] = direct_url.get("url")
        archive = re.search(r"/archive/([0-9a-f]{40})", direct_url.get("url", ""))
        info["commit"] = (direct_url.get("vcs_info") or {}).get("commit_id") or (archive and archive.group(1))
    except Exception:
        pass
    if info["commit"] is None:
        repo = str(Path(benchbox.__file__).resolve().parent.parent)
        try:
            git = functools.partial(subprocess.run, capture_output=True, text=True, check=True)
            info["commit"] = git(["git", "-C", repo, "rev-parse", "HEAD"]).stdout.strip()
            dirty = git(["git", "-C", repo, "status", "--porcelain", "--untracked-files=no"]).stdout.strip()
            info["source"] = repo + (" (dirty)" if dirty else "")
        except Exception:
            pass
    return info


def generate_queries(scale_factor: float, dialects: list[str]) -> dict[str, HavocQuery]:
    """Generate the base queries and all TPC-Havoc variants for the given dialects."""
    # The core benchmark class is what BenchBox's platform runner (`benchbox run`) uses.
    from benchbox.core.tpchavoc.benchmark import TPCHavocBenchmark

    with tempfile.TemporaryDirectory(prefix="tpc_havoc_") as tmp:
        benchmark = TPCHavocBenchmark(scale_factor=scale_factor, output_dir=tmp)
        queries: dict[str, HavocQuery] = {}
        for number in sorted(benchmark.get_implemented_queries()):
            base = HavocQuery(f"Q{number}", number, None, "TPC-H base query")
            for dialect in dialects:
                base.sql[dialect] = benchmark.get_query(number, dialect=dialect, base_dialect=BASE_DIALECT)
            queries[base.query_id] = base
            for variant in sorted(benchmark.get_all_variants_info(number)):
                qid = f"Q{number}_V{variant}"
                queries[qid] = HavocQuery(qid, number, variant, benchmark.get_variant_description(number, variant))

        # Variants: use get_queries() exactly as the BenchBox platform runner does.
        for dialect in dialects:
            for key, sql in benchmark.get_queries(dialect=dialect, base_dialect=BASE_DIALECT).items():
                number, variant = key.split("_v")
                qid = f"Q{int(number)}_V{int(variant)}"
                if qid in queries:
                    queries[qid].sql[dialect] = sql

    missing = [qid for qid, q in queries.items() if any(d not in q.sql for d in dialects)]
    for qid in missing:
        del queries[qid]
    return queries
