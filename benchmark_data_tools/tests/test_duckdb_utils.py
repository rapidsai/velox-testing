import json
from pathlib import Path
import sys

import pytest

# Add repo 'velox-testing' root to sys.path to import modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root (velox-testing)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # benchmark_data_tools dir for 'duckdb_utils'

from benchmark_data_tools.duckdb_utils import is_decimal_column
from benchmark_data_tools.generate_data_files import (
    write_metadata,
    rearrange_directory,
    get_column_projection,
)


def test_is_decimal_column():
    assert is_decimal_column("DECIMAL(10,2)")
    assert is_decimal_column("DECIMAL(38,18)")
    assert not is_decimal_column("DOUBLE")
    assert not is_decimal_column("VARCHAR")


def test_write_metadata(tmp_path):
    write_metadata(str(tmp_path), 0.01)
    p = tmp_path / "metadata.json"
    assert p.exists()
    meta = json.loads(p.read_text())
    assert meta["scale_factor"] == 0.01


def test_rearrange_directory_moves_partitions(tmp_path):
    raw = tmp_path / "raw"
    (raw / "part-1").mkdir(parents=True)
    # Simulate two tables
    (raw / "part-1" / "orders.parquet").write_bytes(b"")
    (raw / "part-1" / "customer.parquet").write_bytes(b"")

    rearrange_directory(str(raw), 1)

    assert not (raw / "part-1").exists()
    assert (raw / "orders" / "orders-1.parquet").exists()
    assert (raw / "customer" / "customer-1.parquet").exists()


def test_get_column_projection_converts_decimal():
    # column metadata rows from duckdb DESCRIBE: (name, type, ...)
    dec_col = ("price", "DECIMAL(10,2)")
    dbl_col = ("qty", "DOUBLE")
    assert (
        get_column_projection(dec_col, True)
        == "CAST(price AS DOUBLE) AS price"
    )
    assert get_column_projection(dbl_col, True) == "qty"

