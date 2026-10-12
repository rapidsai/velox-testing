# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the TPC-Havoc runner helpers (no Presto, DuckDB or BenchBox needed).

Run with: python -m pytest presto/testing/tpc_havoc/tpc_havoc_runner_test.py
"""

import datetime
import decimal
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import havoc_queries
import nightly_summary
import result_utils
import run_tpc_havoc

ALL_IDS = ["Q1", "Q1_V1", "Q1_V2", "Q2", "Q2_V1", "Q10", "Q10_V1"]


def test_select_all_sorted_numerically():
    assert havoc_queries.select_query_ids(ALL_IDS, None) == ["Q1", "Q1_V1", "Q1_V2", "Q2", "Q2_V1", "Q10", "Q10_V1"]


def test_select_group_and_variant_adds_base():
    assert havoc_queries.select_query_ids(ALL_IDS, "2, Q10_V1") == ["Q2", "Q2_V1", "Q10", "Q10_V1"]
    assert havoc_queries.select_query_ids(ALL_IDS, "q1_v2") == ["Q1", "Q1_V2"]


@pytest.mark.parametrize("spec", ["Q3", "Q1_V9", "foo"])
def test_select_rejects_unknown(spec):
    with pytest.raises(ValueError):
        havoc_queries.select_query_ids(ALL_IDS, spec)


def test_infer_scale_factor():
    assert run_tpc_havoc.infer_scale_factor("tpch_sf100") == 100
    assert run_tpc_havoc.infer_scale_factor("sf1_v2_float") == 1
    assert run_tpc_havoc.infer_scale_factor("sf_0_1") == 0.1
    assert run_tpc_havoc.infer_scale_factor("bench") is None


def test_normalize_presto_decimal_and_date_vs_duckdb():
    presto = result_utils.presto_rows_to_frame(
        [["A", "12.50", "1995-03-15", 3]], ["flag", "revenue", "d", "n"], ["varchar", "decimal(15,2)", "date", "bigint"]
    )
    assert isinstance(presto.iloc[0, 1], decimal.Decimal)
    duck = pd.DataFrame({"flag": ["A"], "revenue": [12.5], "d": [pd.Timestamp("1995-03-15")], "n": [3]})
    a = result_utils.normalize_frame(presto, ["varchar", "decimal(15,2)", "date", "bigint"])
    b = result_utils.normalize_frame(duck)
    assert list(a.iloc[0]) == list(b.iloc[0]) == ["A", 12.5, "1995-03-15", 3.0]
    c = result_utils.normalize_frame(pd.DataFrame({"d": [datetime.date(1995, 3, 15)]}))
    assert c.iloc[0, 0] == "1995-03-15"


def test_compare_is_order_insensitive_without_order_by():
    a = pd.DataFrame({"k": ["x", "y"], "v": [1.0, 2.0]})
    b = pd.DataFrame({"k": ["y", "x"], "v": [2.0, 1.0 + 1e-9]})
    assert result_utils.compare(a, b, "SELECT k, v FROM t")[0]
    ok, msg = result_utils.compare(a, pd.DataFrame({"k": ["x", "y"], "v": [1.0, 2.5]}), "SELECT k, v FROM t")
    assert not ok and "mismatch" in msg


def test_variant_without_limit_is_cut_to_base_top_n():
    base_sql = 'SELECT "k", "v" FROM t ORDER BY "v" DESC NULLS FIRST, "k" LIMIT 2'
    base = pd.DataFrame({"k": ["c", "b"], "v": [30.0, 20.0]})
    variant = pd.DataFrame({"kk": ["a", "b", "c"], "vv": [10.0, 20.0, 30.0]})  # unordered, no LIMIT
    assert result_utils.compare_variant_to_base(variant, base, base_sql) == (True, None)
    wrong = pd.DataFrame({"kk": ["a", "b", "c"], "vv": [10.0, 21.0, 30.0]})
    assert not result_utils.compare_variant_to_base(wrong, base, base_sql)[0]


def test_variant_with_extra_rows_and_no_base_limit_fails():
    base = pd.DataFrame({"k": ["a"]})
    variant = pd.DataFrame({"k": ["a", "b"]})
    ok, msg = result_utils.compare_variant_to_base(variant, base, "SELECT k FROM t")
    assert not ok and "Row count mismatch" in msg


ISSUES = [
    {"query": "Q16_V6", "check": "base", "match": "mismatch", "reason": "benchbox"},
    {"query": "Q13_*", "check": "execution", "engines": ["gpu"], "reason": "gpu"},
    {"query": ["Q2_V1", "Q2_V2"], "scale_factors": [100], "reason": "sf100"},
]


def test_known_issue_matching():
    match = run_tpc_havoc.match_known_issue
    assert match(ISSUES, "Q16_V6", "base", "Found 5 mismatch(es)", "cpu", 10)["reason"] == "benchbox"
    assert match(ISSUES, "Q16_V6", "duckdb", "Found 5 mismatch(es)", "cpu", 10) is None
    assert match(ISSUES, "Q16_V6", "base", "Row count", "cpu", 10) is None
    assert match(ISSUES, "Q13_V4", "execution", "boom", "gpu", 10)["reason"] == "gpu"
    assert match(ISSUES, "Q13_V4", "execution", "boom", "cpu", 10) is None
    assert match(ISSUES, "Q2_V2", "reference", "x", "cpu", 100)["reason"] == "sf100"
    assert match(ISSUES, "Q2_V2", "reference", "x", "cpu", 10) is None


def test_engine_glob_covers_gpu_fallback_modes():
    issues = [
        {"query": "Q8_V10", "engines": ["gpu*"], "reason": "both"},
        {"query": "Q1", "engines": ["gpu-no-fallback"], "reason": "no-fallback only"},
    ]
    match = run_tpc_havoc.match_known_issue
    assert match(issues, "Q8_V10", "execution", "x", "gpu", 10)["reason"] == "both"
    assert match(issues, "Q8_V10", "execution", "x", "gpu-no-fallback", 10)["reason"] == "both"
    assert match(issues, "Q8_V10", "execution", "x", "cpu", 10) is None
    assert match(issues, "Q1", "execution", "x", "gpu-no-fallback", 10)["reason"] == "no-fallback only"
    assert match(issues, "Q1", "execution", "x", "gpu", 10) is None


def _record(qid, checks):
    return {"query_id": qid, "checks": {k: dict(v) for k, v in checks.items()}}


def test_classify_failures_new_known_and_unexpected_pass():
    rec = _record("Q16_V6", {"execution": {"status": "passed"}, "base": {"status": "failed", "message": "mismatch"}})
    run_tpc_havoc._classify_failures(rec, ISSUES, "cpu", 10)
    assert rec["failures"] == [{"check": "base", "known": True}] and not rec["has_new_failure"]

    rec = _record("Q13_V1", {"execution": {"status": "failed", "message": "ERROR"}})
    run_tpc_havoc._classify_failures(rec, ISSUES, "cpu", 10)
    assert rec["has_new_failure"]

    rec = _record("Q13_V1", {"execution": {"status": "passed"}, "base": {"status": "passed"}})
    run_tpc_havoc._classify_failures(rec, ISSUES, "gpu", 10)
    assert not rec["failures"] and rec["unexpected_passes"] == ["gpu"]


def test_known_issues_file_parses():
    issues = run_tpc_havoc.load_known_issues(run_tpc_havoc.DEFAULT_KNOWN_ISSUES)
    assert isinstance(issues, list)


CUDF_LOG = """\
W1 OperatorAdapters.cpp:224] Validation failed at function: canRunOnGPU, reason: Operation is not supported in cuDF \
execution: FilterProject projections cannot be evaluated by cuDF, PlanNode id: 2
W2 ExpressionEvaluator.cpp:336] Validation failed at function: canBeEvaluatedByCudf, reason: Operation is not \
supported in cuDF execution: switch(presto.default.eq("a",1),1,0)
W3 ToCudf.cpp:236] Replacement Failed Operator: FilterProject[2] 2
"""


def test_extract_worker_reason_prefers_expression_then_operator():
    lines = CUDF_LOG.splitlines()
    assert run_tpc_havoc.extract_worker_reason(CUDF_LOG) == 'switch(presto.default.eq("a",1),1,0)'
    assert run_tpc_havoc.extract_worker_reason(lines[0]) == "FilterProject projections cannot be evaluated by cuDF"
    assert run_tpc_havoc.extract_worker_reason(lines[2]) == "FilterProject operator cannot run on cuDF"
    assert run_tpc_havoc.extract_worker_reason("I1 Unsupported expression by AST: x") is None


@pytest.mark.parametrize(
    ("reason", "category"),
    [
        ('switch(presto.default.eq("a",1),1,0)', "multi-branch CASE (switch) expression not supported"),
        ("0.0000", "decimal constant expression not supported"),
        ('presto.default.element_at({1 => 1},"p_size")', "element_at() not supported"),
        ("avg does not support input type DECIMAL(15, 2)", "avg does not support input type DECIMAL(15, 2)"),
    ],
)
def test_worker_reason_category(reason, category):
    assert run_tpc_havoc.worker_reason_category(reason) == f"velox-cudf: {category}"


def test_worker_log_tail_reads_only_lines_logged_after_mark(tmp_path):
    log = tmp_path / "worker_0.log"
    log.write_text("Validation failed at function: canRunOnGPU, reason: earlier query\n")
    tail = run_tpc_havoc.WorkerLogTail(str(tmp_path / "worker_*.log"))
    tail.mark()
    assert tail.reason(wait_s=0) is None
    with log.open("a") as fh:
        fh.write("Validation failed at function: canRunOnGPU, reason: this query, PlanNode id: 7\n")
    assert tail.reason(wait_s=0) == "this query"
    assert run_tpc_havoc.WorkerLogTail(None).reason() is None


def test_summary_counts_skipped_queries_and_groups_failures_by_reason(monkeypatch):
    monkeypatch.setattr(havoc_queries, "benchbox_version", dict)
    skipped = {"query_id": "Q2", "status": "skipped", "checks": {}}
    error = {"query_id": "Q2_V7", "status": "error", "error_name": "SYNTAX_ERROR", "error_message": "line 1:26: x"}
    error["checks"] = {"execution": {"status": "failed", "message": "x"}}
    for record in (skipped, error):
        run_tpc_havoc._classify_failures(record, [], "cpu", 10)
    args = run_tpc_havoc.parse_args(["--schema-name", "tpch_sf10"])
    now = datetime.datetime.now(datetime.timezone.utc)
    summary = run_tpc_havoc._build_summary(args, [skipped, error], 10.0, now, Path("."), ["presto"])
    assert summary["totals"]["skipped"] == 1 and summary["totals"]["new_failures"] == 1
    group = {"check": "execution", "label": "SYNTAX_ERROR: line N:N: x", "count": 1, "queries": ["Q2_V7"]}
    assert summary["failure_groups"] == [group]


def _failed(query_id, check="execution", known=None, **fields):
    """A classified record whose only failure is ``check`` (a known issue if ``known`` is its reason)."""
    record = {"query_id": query_id, "status": "error" if check == "execution" else "ok", **fields}
    record["checks"] = {check: {"status": "failed", "message": f"{check} failed\ndetails", "known_issue": known}}
    record.update(failures=[{"check": check, "known": known is not None}], has_new_failure=known is None)
    return record


def _summary_run(label, engine, records, fixed=()):
    queries = [{"query_id": "Q1", "status": "ok", "checks": {}, "failures": [], "has_new_failure": False}, *records]
    unexpected = [{"query_id": q, "known_issue": "x"} for q in fixed]
    return {
        "label": label,
        "engine": engine,
        "dir": Path("/nonexistent"),
        "connection": {"schema": "tpch_sf10"},
        "queries": queries,
        "unexpected_passes": unexpected,
    }


DIALECT = _failed("Q1_V7", known="Presto: dialect", error_name="SYNTAX_ERROR", error_message="line 1:62: list")
CASE = _failed(
    "Q1_V10",
    error_name="GENERIC_INTERNAL_ERROR",
    error_message="!isPureCpuOperator Replacement with cuDF operator failed",
    worker_reason="switch(x)",
    worker_reason_category="velox-cudf: switch",
)


def test_failure_label_prefers_known_reason_then_worker_reason():
    label = nightly_summary.failure_label
    assert label(DIALECT, "execution") == "Presto: dialect"
    assert label(CASE, "execution") == "velox-cudf: switch"
    assert label(_failed("Q2", error_name="E", error_message="line 1:2: x\nmore"), "execution") == "E: line N:N: x"
    cudf_error = "failed for [plan node ID: 2004.0]: CUDF failure at: /build/cudf/copy.cu:388: Both inputs differ"
    expected = "E: failed for [plan node ID: N]: CUDF failure at: copy.cu:N: Both inputs differ"
    assert label(_failed("Q9_V10", error_name="E", error_message=cudf_error), "execution") == expected
    assert label(_failed("Q3_V1", check="duckdb"), "duckdb") == "Wrong result vs duckdb"


def test_nightly_summary_groups_new_failures_first():
    runs = [_summary_run("CPU", "cpu", [DIALECT]), _summary_run("GPU", "gpu-no-fallback", [DIALECT, CASE])]
    groups = nightly_summary.group_failures(runs)
    assert [(g["label"], g["new"]) for g in groups] == [("velox-cudf: switch", True), ("Presto: dialect", False)]
    assert dict(groups[1]["engines"]) == {"CPU": ["Q1_V7"], "GPU": ["Q1_V7"]}
    markdown = nightly_summary.render_markdown(runs, "TPC-Havoc SF10")
    assert "| GPU | 1 / 3 | 0 | 2 | **1** | 1 |" in markdown and "**1 query with new failures.**" in markdown
    assert markdown.index("1. [NEW, error] velox-cudf: switch") < markdown.index("2. [known, error] Presto: dialect")
    assert "- CPU, GPU: Q1_V7" in markdown and "- Worker log: `switch(x)`" in markdown


def test_nightly_summary_slack_lists_only_what_needs_action():
    runs = [_summary_run("CPU", "cpu", [DIALECT], fixed=["Q16_V6"]), _summary_run("GPU", "gpu-no-fallback", [CASE])]
    runs.append({"label": "GPU2", "missing": True, "queries": []})
    text = nightly_summary.render_slack(runs, "TPC-Havoc <SF10>", "https://ci/run")
    assert text.startswith(":red_circle: *TPC-Havoc &lt;SF10&gt;* | <https://ci/run|run summary>")
    assert "*No results from GPU2*" in text and "GPU2  no results" in text
    assert (
        "• velox-cudf: switch (GPU: Q1_V10)\n   repro: `presto/scripts/start_native_gpu_presto.sh --no-cpu-fallback && "
        "presto/scripts/run_tpc_havoc.sh -s tpch_sf10 -e gpu-no-fallback -q Q1_V10`" in text
    )
    assert "Presto: dialect" not in text and "Known issues: 1 reason," in text
    assert "*Known issues now passing (remove from known_issues.yaml):* Q16_V6 (CPU)" in text


def test_nightly_summary_main_writes_step_summary_and_slack_payload(tmp_path, monkeypatch):
    run_dir = tmp_path / "cpu"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(json.dumps({**_summary_run("x", "cpu", [DIALECT]), "dir": None}))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "step.md"))
    argv = ["--run", f"CPU={run_dir}", "--run", f"GPU={tmp_path / 'gpu'}", "--slack-payload", str(tmp_path / "s.json")]
    assert nightly_summary.main(argv) == 0
    step = (tmp_path / "step.md").read_text()
    assert "| CPU | 1 / 2 | 0 | 1 | 0 | 1 |" in step and "| GPU | no results |" in step
    payload = json.loads((tmp_path / "s.json").read_text())
    assert payload["text"].startswith(":red_circle:") and payload["blocks"]
