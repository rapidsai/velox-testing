# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hive.publish_output_files import DestinationLocation, FileSystemDestination
from hive.rewrite_tpch_dataset import (
    DateEstimate,
    SizeCalibration,
    build_date_ranges,
    load_codec_definitions,
    ordered_source_row_groups,
    parse_args,
    partition_path,
    validate_basic_output,
)


def estimate(day: date, rows: int, uncompressed_bytes: int) -> DateEstimate:
    return DateEstimate(day=day, rows=rows, uncompressed_bytes=uncompressed_bytes)


def test_date_ranges_reach_target_and_never_cross_months():
    ranges = build_date_ranges(
        [
            estimate(date(1994, 1, 30), 40, 40),
            estimate(date(1994, 1, 31), 40, 40),
            estimate(date(1994, 2, 1), 40, 40),
            estimate(date(1994, 2, 2), 70, 70),
        ],
        compression_ratio=1.0,
        target_file_bytes=70,
        planning_budget_bytes=120,
    )

    assert [(item.partition_value, item.minimum_date, item.maximum_date, item.estimated_rows) for item in ranges] == [
        ("1994-01", date(1994, 1, 30), date(1994, 1, 31), 80),
        ("1994-02", date(1994, 2, 1), date(1994, 2, 2), 110),
    ]


def test_date_ranges_split_month_at_gpu_budget():
    ranges = build_date_ranges(
        [
            estimate(date(1994, 1, 1), 40, 40),
            estimate(date(1994, 1, 2), 40, 40),
            estimate(date(1994, 1, 3), 40, 40),
        ],
        compression_ratio=0.5,
        target_file_bytes=100,
        planning_budget_bytes=80,
    )

    assert [(item.minimum_date, item.maximum_date) for item in ranges] == [
        (date(1994, 1, 1), date(1994, 1, 2)),
        (date(1994, 1, 3), date(1994, 1, 3)),
    ]


def test_date_range_rejects_one_day_above_budget():
    with pytest.raises(ValueError, match="One day"):
        build_date_ranges(
            [estimate(date(1994, 1, 1), 100, 101)],
            compression_ratio=1.0,
            target_file_bytes=100,
            planning_budget_bytes=100,
        )


def test_size_calibration_uses_sample_then_completed_files():
    calibration = SizeCalibration(target_bytes=512, observed_rows=4_000, observed_bytes=400)

    assert calibration.predicted_rows() == 5_120
    calibration.observe(rows=6_000, physical_bytes=600)
    assert calibration.predicted_rows() == 5_120


def test_flat_source_row_groups_are_ordered_by_key_range(tmp_path):
    high = tmp_path / "customer-1.parquet"
    low = tmp_path / "customer-2.parquet"
    pq.write_table(pa.table({"c_custkey": [3, 4]}), high)
    pq.write_table(pa.table({"c_custkey": [1, 2]}), low)

    assert ordered_source_row_groups([high, low], ("c_custkey",)) == [(low, 0), (high, 0)]


def test_flat_source_rejects_overlapping_key_ranges(tmp_path):
    first = tmp_path / "part-1.parquet"
    second = tmp_path / "part-2.parquet"
    pq.write_table(pa.table({"p_partkey": [1, 3]}), first)
    pq.write_table(pa.table({"p_partkey": [2, 4]}), second)

    with pytest.raises(RuntimeError, match="overlap"):
        ordered_source_row_groups([first, second], ("p_partkey",))


def test_partition_path_and_destination_parsing(tmp_path):
    relative = partition_path("orders", "o_ordermonth", "1994-03", 7)

    assert relative == "orders/o_ordermonth=1994-03/part-00007.parquet"
    assert DestinationLocation.parse("s3://bucket/prefix").join(relative) == (
        "s3://bucket/prefix/orders/o_ordermonth=1994-03/part-00007.parquet"
    )
    assert DestinationLocation.parse(tmp_path / "output").join(relative) == str(tmp_path / "output" / relative)
    with pytest.raises(ValueError, match="must be relative"):
        DestinationLocation.parse("s3://bucket/prefix").join("../escape")


def test_filesystem_destination_publishes_atomically(tmp_path):
    source = tmp_path / "temporary.parquet"
    source.write_bytes(b"parquet bytes")
    destination = FileSystemDestination(DestinationLocation.parse(tmp_path / "output"))

    entry = destination.publish_file(source, "orders/part.parquet")

    assert entry["physical_bytes"] == len(b"parquet bytes")
    assert (tmp_path / "output/orders/part.parquet").read_bytes() == b"parquet bytes"
    assert not list((tmp_path / "output/orders").glob("*.tmp"))


def test_basic_output_validation_checks_schema_and_rows(tmp_path):
    output = tmp_path / "part.parquet"
    table = pa.table({"o_orderkey": [1, 2]})
    pq.write_table(table, output)

    assert validate_basic_output(output, table.schema, 2)["rows"] == 2
    with pytest.raises(RuntimeError, match="row count"):
        validate_basic_output(output, table.schema, 3)


def test_codec_definitions_are_loaded_by_table(tmp_path):
    path = tmp_path / "codecs.json"
    path.write_text(
        json.dumps(
            {
                "tables": [
                    {
                        "name": "orders",
                        "compression": "SNAPPY",
                        "columns": [
                            {
                                "name": "o_orderkey",
                                "encoding": "DELTA_BINARY_PACKED",
                                "dictionary": False,
                            }
                        ],
                    }
                ]
            }
        )
    )

    codecs = load_codec_definitions(path)

    assert codecs["orders"]["columns"][0]["encoding"] == "DELTA_BINARY_PACKED"


def test_cli_exposes_first_version_controls(tmp_path):
    args = parse_args(
        [
            "--source",
            str(tmp_path / "source"),
            "--destination",
            "s3://bucket/tpch",
            "--staging",
            str(tmp_path / "staging"),
            "--target-file-bytes",
            "512",
            "--row-group-bytes",
            "128",
            "--gpu-read-chunk-bytes",
            "256",
            "--gpu-memory-bytes",
            "1024",
            "--tables",
            "orders",
            "--codec-definitions",
            str(tmp_path / "codecs.json"),
            "--overwrite",
        ]
    )

    assert args.destination == "s3://bucket/tpch"
    assert args.target_file_bytes == 512
    assert args.gpu_memory_bytes == 1024
    assert args.tables == ["orders"]
    assert args.overwrite
