# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pandas as pd

from common.testing.validate_results import validate


def test_validator_accepts_empty_q15_result(tmp_path: Path):
    results_dir = tmp_path / "actual"
    results_dir.mkdir()
    pd.DataFrame().to_parquet(results_dir / "q15.parquet", index=False)

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
