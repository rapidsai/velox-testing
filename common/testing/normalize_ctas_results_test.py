# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pandas as pd

from common.testing.normalize_ctas_results import normalize_ctas_results


def test_normalize_ctas_results_merges_parts_and_restores_order(tmp_path: Path):
    scratch = tmp_path / "scratch"
    query_dir = scratch / "q1"
    query_dir.mkdir(parents=True)
    # CTAS assigns positional storage names, while ORDER BY still refers to
    # the original SELECT output name.
    pd.DataFrame({"c1": [3, 1]}).to_parquet(query_dir / "part-00000.parquet", index=False)
    pd.DataFrame({"c1": [4, 2]}).to_parquet(query_dir / "part-00001.parquet", index=False)

    output = tmp_path / "output"
    normalized = normalize_ctas_results(
        scratch,
        output,
        {"Q1": "SELECT value FROM source ORDER BY value"},
    )

    assert normalized == [output / "q1.parquet"]
    assert pd.read_parquet(normalized[0])["c1"].tolist() == [1, 2, 3, 4]
    assert not query_dir.exists()


def test_normalize_ctas_results_promotes_single_unordered_part(tmp_path: Path):
    scratch = tmp_path / "scratch"
    query_dir = scratch / "q6"
    query_dir.mkdir(parents=True)
    expected = pd.DataFrame({"revenue": [42.0]})
    expected.to_parquet(query_dir / "part-00000.parquet", index=False)

    output = tmp_path / "output"
    normalize_ctas_results(scratch, output, {"Q6": "SELECT revenue FROM source"})

    pd.testing.assert_frame_equal(pd.read_parquet(output / "q6.parquet"), expected)
    assert not query_dir.exists()


def test_normalize_ctas_results_writes_empty_result(tmp_path: Path):
    scratch = tmp_path / "scratch"
    query_dir = scratch / "q15"
    query_dir.mkdir(parents=True)
    (query_dir / ".prestoSchema").write_text("{}")

    output = tmp_path / "output"
    normalize_ctas_results(scratch, output, {"Q15": "SELECT supplier_no FROM source"})

    assert pd.read_parquet(output / "q15.parquet").empty
    assert not query_dir.exists()
