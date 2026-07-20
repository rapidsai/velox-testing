# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pandas as pd
import pytest

from common.testing.validate_results import validate


def test_validator_reads_distributed_parquet_dataset(tmp_path: Path):
    results_dir = tmp_path / "actual"
    query_dir = results_dir / "q1"
    query_dir.mkdir(parents=True)
    # Distributed writer files do not preserve the query's global row order.
    pd.DataFrame({"value": [3, 4]}).to_parquet(query_dir / "part-00000.parquet", index=False)
    pd.DataFrame({"value": [1, 2]}).to_parquet(query_dir / "part-00001.parquet", index=False)

    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    pd.DataFrame({"value": [1, 2, 3, 4]}).to_parquet(expected_dir / "q01.parquet", index=False)

    result = validate(results_dir, expected_dir, {"Q1": "SELECT value FROM source ORDER BY value"})

    assert result["overall_status"] == "passed"
    assert result["queries"]["q1"]["status"] == "passed"


def test_validator_accepts_empty_distributed_q15_dataset(tmp_path: Path):
    results_dir = tmp_path / "actual"
    query_dir = results_dir / "q15"
    query_dir.mkdir(parents=True)
    (query_dir / ".prestoSchema").write_text("{}")

    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    pd.DataFrame({"supplier_no": [1]}).to_parquet(expected_dir / "q15.parquet", index=False)

    result = validate(results_dir, expected_dir, {"Q15": "SELECT supplier_no FROM source"})

    assert result["overall_status"] == "expected-failure"
    assert result["queries"]["q15"]["status"] == "expected-failure"


def test_validator_keeps_single_file_result_order_sensitive(tmp_path: Path):
    results_dir = tmp_path / "actual"
    results_dir.mkdir()
    pd.DataFrame({"value": [2, 1]}).to_parquet(results_dir / "q1.parquet", index=False)

    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    pd.DataFrame({"value": [1, 2]}).to_parquet(expected_dir / "q01.parquet", index=False)

    result = validate(results_dir, expected_dir, {"Q1": "SELECT value FROM source ORDER BY value"})

    assert result["overall_status"] == "failed"
    assert "Engine violated ORDER BY" in result["queries"]["q1"]["message"]


@pytest.mark.parametrize("query_numbers", [None, [1]])
def test_validator_rejects_file_and_dataset_for_same_query(tmp_path: Path, query_numbers):
    results_dir = tmp_path / "actual"
    query_dir = results_dir / "q1"
    query_dir.mkdir(parents=True)
    pd.DataFrame({"value": [1]}).to_parquet(query_dir / "part-00000.parquet", index=False)
    pd.DataFrame({"value": [1]}).to_parquet(results_dir / "q1.parquet", index=False)

    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    pd.DataFrame({"value": [1]}).to_parquet(expected_dir / "q01.parquet", index=False)

    result = validate(results_dir, expected_dir, {"Q1": "SELECT value FROM source"}, query_numbers=query_numbers)

    assert result["overall_status"] == "failed"
    assert result["queries"]["q1"] == {
        "status": "failed",
        "message": "multiple result representations found: q1, q1.parquet",
    }
