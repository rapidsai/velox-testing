# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
# This entire module tests only local E2E profiling instrumentation.

import importlib.util
import json
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


OBSERVER_PATH = (
    Path(__file__).resolve().parents[2]
    / "rg_analysis"
    / "current"
    / "tools"
    / "end-to-end"
    / "real_cli_observer.py"
)
SPEC = importlib.util.spec_from_file_location("real_cli_observer_test_module", OBSERVER_PATH)
observer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = observer
SPEC.loader.exec_module(observer)


def _write_fixture(tmp_path, convert_decimals_to_floats):
    data_dir = tmp_path / "data"
    table_dir = data_dir / "synthetic"
    table_dir.mkdir(parents=True)
    if convert_decimals_to_floats:
        table = pa.table({"amount": pa.array([1.25, 2.5], type=pa.float64())})
    else:
        table = pa.table(
            {"amount": pa.array([Decimal("1.25"), Decimal("2.50")], type=pa.decimal128(10, 2))}
        )
    pq.write_table(table, table_dir / "part.parquet", row_group_size=1)
    (data_dir / "metadata.json").write_text(json.dumps({"scale_factor": 1.0}))
    timing_path = tmp_path / "timing.json"
    timing_path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "result": "success",
                "benchmark": {
                    "type": "tpcds",
                    "scale_factor": 1.0,
                    "convert_decimals_to_floats": convert_decimals_to_floats,
                },
                "duckdb_settings_after_export_config": {
                    "threads": "256",
                    "memory_limit": "1.0 TiB",
                    "preserve_insertion_order": "false",
                },
                "requested_writers": 32,
                "export_lifecycle": {
                    "mode": "same_connection",
                    "reopened_read_only": False,
                    "prewarm_bytes": 0,
                },
                "export": {
                    "source_rows_by_table": {"synthetic": 2},
                    "source_files": 1,
                },
                "published": {"files": 1},
            }
        )
    )
    expected = {
        "threads": "256",
        "writers": 32,
        "memory_limit": "1TiB",
        "memory_limit_bytes": 1024**4,
        "preserve_insertion_order": "false",
        "convert_decimals_to_floats": str(convert_decimals_to_floats).lower(),
        "export_lifecycle": "same_connection",
    }
    return data_dir, timing_path, expected


@pytest.mark.parametrize("convert_decimals_to_floats", [False, True])
def test_validation_records_and_enforces_schema_mode(tmp_path, convert_decimals_to_floats):
    data_dir, timing_path, expected = _write_fixture(tmp_path, convert_decimals_to_floats)

    result = observer._validate_outputs(0, data_dir, timing_path, 1.0, expected)

    assert result["valid"], result["reasons"]
    assert result["convert_decimals_to_floats"] is convert_decimals_to_floats
    assert result["export_lifecycle"]["mode"] == "same_connection"
    assert result["parquet"]["row_groups_by_table"] == {"synthetic": 2}
    assert result["parquet"]["compressed_bytes_by_table"]["synthetic"] > 0
    assert result["parquet"]["uncompressed_bytes_by_table"]["synthetic"] > 0
    assert len(result["parquet"]["schema_fingerprints_by_table"]["synthetic"]) == 1
    assert len(result["parquet"]["file_summaries"]) == 1
    if convert_decimals_to_floats:
        assert result["parquet"]["decimal_columns_by_table"] == {}
    else:
        assert result["parquet"]["decimal_columns_by_table"] == {"synthetic": ["amount"]}

    mismatched = dict(expected)
    mismatched["convert_decimals_to_floats"] = str(
        not convert_decimals_to_floats
    ).lower()
    mismatch_result = observer._validate_outputs(
        0, data_dir, timing_path, 1.0, mismatched
    )
    assert not mismatch_result["valid"]
    assert any(
        "convert_decimals_to_floats" in reason for reason in mismatch_result["reasons"]
    )

    lifecycle_mismatch = dict(expected)
    lifecycle_mismatch["export_lifecycle"] = "reopen"
    lifecycle_result = observer._validate_outputs(
        0, data_dir, timing_path, 1.0, lifecycle_mismatch
    )
    assert not lifecycle_result["valid"]
    assert any("export_lifecycle" in reason for reason in lifecycle_result["reasons"])


def test_conversion_expectation_is_required():
    parser = observer._parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--run-id",
                "test",
                "--scale-factor",
                "1",
                "--data-dir",
                "/tmp/data",
                "--evidence-dir",
                "/tmp/evidence",
                "--",
                "true",
            ]
        )


# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
