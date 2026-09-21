# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
from argparse import Namespace
from pathlib import Path, PurePosixPath

import pytest
from hive.flatten_tpch_dataset import MANIFEST_NAME, flattened_path, run


def arguments(source: Path, destination: Path, **overrides) -> Namespace:
    values = {
        "source": str(source),
        "destination": str(destination),
        "s3_region": None,
        "workers": 2,
        "resume": False,
        "dry_run": False,
    }
    values.update(overrides)
    return Namespace(**values)


def make_source(root: Path) -> dict[str, bytes]:
    files = {
        "orders/o_ordermonth=1992-01/part-00000.parquet": b"orders-january",
        "orders/o_ordermonth=1992-02/part-00000.parquet": b"orders-february",
        "lineitem/l_shipmonth=1992-01/part-00000.parquet": b"lineitem-january",
        "customer/part-00000.parquet": b"customer",
        "part/part-00000.parquet": b"part",
        "partsupp/part-00000.parquet": b"partsupp",
        "supplier/part-00000.parquet": b"supplier",
        "nation/part-00000.parquet": b"nation",
        "region/part-00000.parquet": b"region",
    }
    for relative, contents in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
    return files


def test_flattened_path_preserves_partition_provenance():
    assert flattened_path(PurePosixPath("orders/o_ordermonth=1994-03/part-00007.parquet")) == PurePosixPath(
        "orders/o_ordermonth=1994-03__part-00007.parquet"
    )
    assert flattened_path(PurePosixPath("customer/part-00000.parquet")) == PurePosixPath("customer/part-00000.parquet")


@pytest.mark.parametrize(
    "relative, message",
    [
        ("orders/part-00000.parquet", "table/partition/file"),
        ("orders/wrong=1994-03/part-00000.parquet", "Invalid orders partition"),
        ("customer/nested/part-00000.parquet", "table/file"),
        ("unknown/part-00000.parquet", "Unknown TPC-H table"),
        ("part/README", "non-Parquet"),
    ],
)
def test_flattened_path_rejects_unexpected_layout(relative, message):
    with pytest.raises(ValueError, match=message):
        flattened_path(PurePosixPath(relative))


def test_run_copies_byte_identical_files_and_writes_manifest(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    files = make_source(source)

    summary = run(arguments(source, destination))

    assert summary["orders"] == {
        "objects": 2,
        "physical_bytes": len(b"orders-january") + len(b"orders-february"),
    }
    assert (destination / "orders/o_ordermonth=1992-01__part-00000.parquet").read_bytes() == b"orders-january"
    assert (destination / "lineitem/l_shipmonth=1992-01__part-00000.parquet").read_bytes() == b"lineitem-january"
    for relative, contents in files.items():
        assert (source / relative).read_bytes() == contents

    manifest = json.loads((destination / MANIFEST_NAME).read_text())
    assert manifest["source"] == str(source)
    assert manifest["destination"] == str(destination)
    assert len(manifest["entries"]) == len(files)


def test_resume_validates_existing_files_and_completes_missing_files(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    make_source(source)
    run(arguments(source, destination))
    missing = destination / "orders/o_ordermonth=1992-02__part-00000.parquet"
    missing.unlink()

    run(arguments(source, destination, resume=True))

    assert missing.read_bytes() == b"orders-february"


def test_nonempty_destination_requires_resume(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    make_source(source)
    destination.mkdir()
    (destination / "unexpected.parquet").write_bytes(b"unexpected")

    with pytest.raises(FileExistsError, match="unexpected objects"):
        run(arguments(source, destination))


def test_dry_run_does_not_create_destination(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    make_source(source)

    run(arguments(source, destination, dry_run=True))

    assert not destination.exists()


def test_dry_run_checks_destination_state(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    make_source(source)
    destination.mkdir()
    (destination / "unexpected.parquet").write_bytes(b"unexpected")

    with pytest.raises(FileExistsError, match="unexpected objects"):
        run(arguments(source, destination, dry_run=True))
