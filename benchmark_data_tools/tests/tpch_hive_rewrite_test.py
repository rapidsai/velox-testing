# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
from argparse import Namespace
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tpch_hive_rewrite import (
    DateEstimate,
    DestinationURI,
    LocalDestination,
    Manifest,
    OutputRange,
    S3Destination,
    SizeCalibration,
    build_output_ranges,
    build_work_windows,
    gpu_planning_budget,
    hive_partition_path,
    ordered_source_row_groups,
    parse_args,
    publish_completion_files,
    schema_json,
    sorted_run_compression_ratio,
    sorted_run_rows_per_byte,
    source_identity,
    validate_resume,
)


def estimate(day: date, rows: int, uncompressed_bytes: int) -> DateEstimate:
    return DateEstimate(day=day, rows=rows, uncompressed_bytes=uncompressed_bytes)


def test_output_ranges_prefer_whole_dates_and_never_cross_months():
    ranges = build_output_ranges(
        [
            estimate(date(1994, 1, 30), 40, 40),
            estimate(date(1994, 1, 31), 40, 40),
            estimate(date(1994, 2, 1), 40, 40),
            estimate(date(1994, 2, 2), 70, 70),
        ],
        compressed_bytes_per_uncompressed_byte=1.0,
        target_file_bytes=100,
    )

    assert [(item.partition_value, item.minimum_date, item.maximum_date, item.estimated_rows) for item in ranges] == [
        ("1994-01", "1994-01-30", "1994-01-31", 80),
        ("1994-02", "1994-02-01", "1994-02-01", 40),
        ("1994-02", "1994-02-02", "1994-02-02", 70),
    ]


def test_output_range_records_indivisible_day():
    ranges = build_output_ranges(
        [estimate(date(1994, 1, 1), 101, 101)],
        compressed_bytes_per_uncompressed_byte=1.0,
        target_file_bytes=100,
    )

    assert ranges[0].size_exception == "indivisible-day"


def test_work_windows_do_not_split_hive_months():
    ranges = [
        OutputRange("range-00000", "1994-01", "1994-01-31", "1994-01-31", 10, 40, 80),
        OutputRange("range-00001", "1994-02", "1994-02-01", "1994-02-01", 10, 40, 80),
        OutputRange("range-00002", "1994-02", "1994-02-02", "1994-02-02", 10, 40, 80),
    ]

    windows = build_work_windows(ranges, gpu_window_bytes=170)

    assert windows[0].range_ids == ("range-00000",)
    assert (windows[0].minimum_date, windows[0].maximum_date) == ("1994-01-31", "1994-01-31")
    assert windows[1].range_ids == ("range-00001", "range-00002")
    assert (windows[1].minimum_date, windows[1].maximum_date) == ("1994-02-01", "1994-02-02")
    assert gpu_planning_budget(400) == 33


def test_work_window_rejects_one_file_estimate_above_budget():
    output_range = OutputRange("range-00000", "1994-01", "1994-01-01", "1994-01-01", 10, 40, 180)

    with pytest.raises(ValueError, match="exceeds GPU window budget"):
        build_work_windows([output_range], gpu_window_bytes=170)


def test_sorted_run_calibration_uses_final_writer_measurements():
    runs = [
        {
            "rows": 1_000,
            "physical_bytes": 100,
            "row_groups": [{"bytes": 250, "columns": []}],
        },
        {
            "rows": 2_000,
            "physical_bytes": 200,
            "row_groups": [{"bytes": 750, "columns": []}],
        },
    ]

    assert sorted_run_compression_ratio(runs) == pytest.approx(0.3)
    assert sorted_run_rows_per_byte(runs) == pytest.approx(10.0)


def test_size_calibration_scales_retries_and_learns_from_accepted_files():
    calibration = SizeCalibration(target_bytes=512, seed_rows_per_byte=10.0)

    assert calibration.predicted_rows() == 5_120
    assert calibration.adjusted_rows(candidate_rows=5_120, candidate_bytes=400) == 6_554

    calibration.accept(rows=6_000, physical_bytes=500)
    assert calibration.rows_per_byte == pytest.approx(12.0)
    assert calibration.predicted_rows() == 6_144


def test_flat_source_row_groups_are_ordered_by_key_range(tmp_path):
    high = tmp_path / "customer-1.parquet"
    low = tmp_path / "customer-2.parquet"
    pq.write_table(pa.table({"c_custkey": [3, 4]}), high)
    pq.write_table(pa.table({"c_custkey": [1, 2]}), low)

    groups = ordered_source_row_groups([high, low], ("c_custkey",))

    assert groups == [(low, 0), (high, 0)]


def test_flat_source_rejects_overlapping_key_ranges(tmp_path):
    first = tmp_path / "part-1.parquet"
    second = tmp_path / "part-2.parquet"
    pq.write_table(pa.table({"p_partkey": [1, 3]}), first)
    pq.write_table(pa.table({"p_partkey": [2, 4]}), second)

    with pytest.raises(RuntimeError, match="overlap"):
        ordered_source_row_groups([first, second], ("p_partkey",))


def test_hive_path_destination_parsing_and_joining(tmp_path):
    relative = hive_partition_path("orders", "o_ordermonth", "1994-03", 7)

    assert relative == "orders/o_ordermonth=1994-03/part-00007.parquet"
    assert DestinationURI.parse("s3://bucket/prefix").join(relative) == (
        "s3://bucket/prefix/orders/o_ordermonth=1994-03/part-00007.parquet"
    )
    assert DestinationURI.parse(tmp_path / "output").join(relative) == str(tmp_path / "output" / relative)
    with pytest.raises(ValueError, match="must be relative"):
        DestinationURI.parse("s3://bucket/prefix").join("../escape")


def test_local_destination_publishes_atomically_and_validates_size(tmp_path):
    source = tmp_path / "temporary.parquet"
    source.write_bytes(b"parquet bytes")
    destination = LocalDestination(DestinationURI.parse(tmp_path / "output"))

    entry = destination.publish_file(source, "orders/part.parquet")

    assert destination.validate("orders/part.parquet", entry)
    assert (tmp_path / "output/orders/part.parquet").read_bytes() == b"parquet bytes"
    assert not list((tmp_path / "output/orders").glob("*.tmp"))


def test_completion_files_are_idempotent_on_resume(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    destination = LocalDestination(DestinationURI.parse(tmp_path / "output"))
    manifest = Manifest(staging / "work.json", {"state": "complete", "completed_at": "fixed"})
    args = Namespace(staging=staging)

    publish_completion_files(args, destination, manifest)
    publish_completion_files(args, destination, manifest)

    assert (tmp_path / "output/_SUCCESS").read_bytes() == b""
    assert json.loads((tmp_path / "output/rewrite_manifest.json").read_text()) == manifest.data


def test_s3_destination_uses_injected_publisher_and_optional_object_metadata(tmp_path):
    source = tmp_path / "part.parquet"
    source.write_bytes(b"12345")
    uploads = []

    destination = S3Destination(
        DestinationURI.parse("s3://bucket/dataset"),
        uploader=lambda path, uri: uploads.append((path, uri)) or {"etag": "test"},
        head_object=lambda _uri: {"ContentLength": 5} if uploads else None,
        prefix_has_objects=lambda _uri: False,
    )
    entry = destination.publish_file(source, "orders/part.parquet")

    assert uploads == [(source, "s3://bucket/dataset/orders/part.parquet")]
    assert entry["object_metadata"] == {"etag": "test"}
    assert destination.validate("orders/part.parquet", entry)


def test_source_identity_uses_manifest_path_size_and_footer_facts_not_content_hashes(tmp_path):
    source = tmp_path / "source"
    table_dir = source / "orders"
    table_dir.mkdir(parents=True)
    parquet_path = table_dir / "part.parquet"
    pq.write_table(pa.table({"o_orderdate": [date(1994, 1, 1)], "o_orderkey": [1]}), parquet_path)
    generation_manifest = tmp_path / "generation.json"
    generation_manifest.write_text(json.dumps({"dataset_id": "tpch-sf1000-v2", "verified": True}))

    identity = source_identity(
        source,
        source_manifest=generation_manifest,
        source_identity_value="generation-42",
        tables=["orders"],
    )

    assert identity["identity"] == "generation-42"
    assert identity["files"][0]["path"] == "orders/part.parquet"
    assert identity["files"][0]["physical_bytes"] == parquet_path.stat().st_size
    assert identity["files"][0]["rows"] == 1
    assert "schema_digest" in identity["files"][0]
    assert "sha256" not in json.dumps(identity)


def test_decimal_schema_records_exact_precision():
    schema = pa.schema([pa.field("o_totalprice", pa.decimal128(15, 2))])

    assert schema_json(schema)[0] == {
        "name": "o_totalprice",
        "type": "decimal128(15, 2)",
        "nullable": True,
        "precision": 15,
        "scale": 2,
    }


def test_manifest_save_is_atomic_and_resume_compares_cheap_identity(tmp_path):
    path = tmp_path / "rewrite_manifest.json"
    destination = DestinationURI.parse(tmp_path / "output")
    identity = {"metadata_digest": "footer-facts-only"}
    args = Namespace(
        source=tmp_path / "source",
        s3_region=None,
        target_file_bytes=512,
        file_size_tolerance=0.05,
        max_size_retries=6,
        row_group_bytes=128,
        gpu_read_chunk_bytes=256,
        gpu_window_bytes=1024,
        tables=["orders"],
    )
    data = {
        "source": str(args.source),
        "destination": {"scheme": destination.scheme, "root": destination.root},
        "s3_region": None,
        "source_identity": identity,
        "target_file_bytes": 512,
        "file_size_tolerance": 0.05,
        "max_size_retries": 6,
        "row_group_bytes": 128,
        "gpu_read_chunk_bytes": 256,
        "gpu_window_bytes": 1024,
        "gpu_working_set_multiplier": 3,
        "gpu_planning_safety_percent": 25,
        "tables_requested": ["orders"],
    }

    Manifest(path, data).save()
    loaded = Manifest.load(path)
    validate_resume(loaded, args, identity, destination)

    assert loaded.data == data
    assert list(tmp_path.iterdir()) == [path]
    args.gpu_window_bytes = 2048
    with pytest.raises(ValueError, match="gpu_window_bytes"):
        validate_resume(loaded, args, identity, destination)


def test_cli_exposes_generic_operational_controls(tmp_path):
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
            "--gpu-window-bytes",
            "1024",
            "--tables",
            "orders",
            "--source-identity",
            "generation-42",
            "--source-manifest",
            str(tmp_path / "source-manifest.json"),
            "--dry-run",
        ]
    )

    assert args.destination == "s3://bucket/tpch"
    assert args.file_size_tolerance == 0.05
    assert args.max_size_retries == 6
    assert args.gpu_read_chunk_bytes == 256
    assert args.gpu_window_bytes == 1024
    assert args.tables == ["orders"]
    assert args.source_identity == "generation-42"
    assert args.dry_run


def test_cli_rejects_resume_with_overwrite(tmp_path):
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--source",
                str(tmp_path / "source"),
                "--destination",
                str(tmp_path / "destination"),
                "--staging",
                str(tmp_path / "staging"),
                "--resume",
                "--overwrite",
            ]
        )
