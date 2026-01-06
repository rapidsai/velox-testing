import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
import sys as _sys

# Ensure module import from benchmark_data_tools directory
_sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _script_path(name: str) -> str:
    return str(Path(__file__).resolve().parents[1] / name)


def test_help_exits_zero():
    script = _script_path("generate_table_schemas.py")
    proc = subprocess.run([sys.executable, script, "-h"], text=True, stdout=subprocess.PIPE)
    assert proc.returncode == 0
    assert "Generate benchmark table schemas" in proc.stdout or "usage" in proc.stdout


def test_generate_schemas_tpch_not_null(tmp_path, monkeypatch):
    # Import module to test its functions directly
    import generate_table_schemas as gts

    data_dir = tmp_path / "data"
    schemas_dir = tmp_path / "schemas"
    # Simulate two table directories in data_dir
    (data_dir / "orders").mkdir(parents=True)
    (data_dir / "customer").mkdir(parents=True)

    # Monkeypatch duck utils to create simple in-memory tables with NOT NULL
    def fake_create_not_null_table(table_name, data_path):
        duckdb.sql(f"DROP TABLE IF EXISTS {table_name}")
        duckdb.sql(
            f"CREATE TABLE {table_name} (id BIGINT NOT NULL, val DOUBLE NOT NULL)"
        )

    def fake_create_table(table_name, data_path):
        duckdb.sql(f"DROP TABLE IF EXISTS {table_name}")
        duckdb.sql(f"CREATE TABLE {table_name} (id BIGINT, val DOUBLE)")

    monkeypatch.setattr(gts.duck, "create_not_null_table", fake_create_not_null_table)
    monkeypatch.setattr(gts.duck, "create_table", fake_create_table)

    # Generate schema files for tpch (expects NOT NULL columns)
    gts.generate_table_schemas("tpch", str(schemas_dir), str(data_dir), verbose=False)

    # Check that schema files were written and include NOT NULL
    for tbl in ["orders", "customer"]:
        p = schemas_dir / f"{tbl}.sql"
        assert p.exists()
        sql = p.read_text()
        assert "CREATE TABLE hive.{schema}." in sql
        assert "NOT NULL" in sql


def test_generate_schemas_tpcds_nullable(tmp_path, monkeypatch):
    import generate_table_schemas as gts

    data_dir = tmp_path / "data"
    schemas_dir = tmp_path / "schemas"
    (data_dir / "store_sales").mkdir(parents=True)

    # Ensure clean state (previous test may have created tables)
    for (tbl,) in duckdb.sql("SHOW TABLES").fetchall():
        duckdb.sql(f"DROP TABLE IF EXISTS {tbl}")

    def fake_create_table(table_name, data_path):
        duckdb.sql(f"DROP TABLE IF EXISTS {table_name}")
        duckdb.sql(f"CREATE TABLE {table_name} (k INTEGER, v VARCHAR)")

    monkeypatch.setattr(gts.duck, "create_table", fake_create_table)
    # tpch path uses create_not_null_table, tpcds uses create_table
    gts.generate_table_schemas("tpcds", str(schemas_dir), str(data_dir), verbose=False)

    p = schemas_dir / "store_sales.sql"
    assert p.exists()
    sql = p.read_text()
    # Columns should not be forced NOT NULL for tpcds
    assert "NOT NULL" not in sql

