import json
import subprocess
import sys
from pathlib import Path

import pytest
from types import SimpleNamespace
import sys as _sys

# Allow direct imports of the module under test
_sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _script_path(name: str) -> str:
    return str(Path(__file__).resolve().parents[1] / name)


def _duckdb_ext_available(ext: str) -> bool:
    try:
        import duckdb  # noqa: F401
        subprocess.run(
            [
                sys.executable,
                "-c",
                f"import duckdb; duckdb.sql('INSTALL {ext}; LOAD {ext};')",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True
    except Exception:
        return False


def test_help_exits_zero():
    script = _script_path("generate_data_files.py")
    proc = subprocess.run([sys.executable, script, "-h"], text=True, stdout=subprocess.PIPE)
    assert proc.returncode == 0
    assert "Generate benchmark parquet data files" in proc.stdout or "usage" in proc.stdout


@pytest.mark.skipif(not _duckdb_ext_available("tpch"), reason="duckdb tpch extension not available")
def test_generate_tpch_duckdb_small(tmp_path):
    script = _script_path("generate_data_files.py")
    out = tmp_path / "tpch_sf0001"
    args = [
        sys.executable,
        script,
        "-b",
        "tpch",
        "-d",
        str(out),
        "-s",
        "0.001",
        "--use-duckdb",
        "-j",
        "1",
    ]
    proc = subprocess.run(args, text=True, capture_output=True)
    assert proc.returncode == 0
    # Expect metadata and at least one table dir
    meta = out / "metadata.json"
    assert meta.exists()
    data = json.loads(meta.read_text())
    assert float(data["scale_factor"]) == pytest.approx(0.001)
    # Find any subdir containing parquet
    has_any = False
    for p in out.iterdir():
        if p.is_dir() and any(x.suffix == ".parquet" for x in p.glob("*.parquet")):
            has_any = True
            break
    assert has_any, "expected at least one parquet file to be written"


@pytest.mark.skipif(not _duckdb_ext_available("tpch"), reason="duckdb tpch extension not available")
def test_verbose_and_overwrite(tmp_path):
    script = _script_path("generate_data_files.py")
    out = tmp_path / "tpch_sf0001"
    out.mkdir(parents=True)
    # Pre-create a file that should be removed since script recreates directory
    (out / "old.txt").write_text("old")
    args = [
        sys.executable,
        script,
        "-b",
        "tpch",
        "-d",
        str(out),
        "-s",
        "0.001",
        "--use-duckdb",
        "-v",
    ]
    proc = subprocess.run(args, text=True, capture_output=True)
    assert proc.returncode == 0
    # Directory should exist and old file should be gone
    assert out.exists()
    assert not (out / "old.txt").exists()
    # Verbose path prints "generating with duckdb"
    assert "generating with duckdb" in (proc.stdout + proc.stderr)


@pytest.mark.skipif(
    not (_duckdb_ext_available("tpch") and pytest.importorskip("pyarrow", reason="pyarrow required")),
    reason="duckdb tpch extension or pyarrow not available",
)
def test_convert_decimals_to_floats_no_decimal_types(tmp_path):
    import pyarrow.parquet as pq

    script = _script_path("generate_data_files.py")
    out = tmp_path / "tpch_sf0001"
    args = [
        sys.executable,
        script,
        "-b",
        "tpch",
        "-d",
        str(out),
        "-s",
        "0.001",
        "--use-duckdb",
        "-c",
    ]
    proc = subprocess.run(args, text=True, capture_output=True)
    assert proc.returncode == 0
    # Inspect a known table with DECIMALs in TPCH (e.g., lineitem)
    lineitem = out / "lineitem" / "lineitem.parquet"
    # Some small scales might not include all tables; fall back to any table
    target = lineitem if lineitem.exists() else next(out.glob("*/*.parquet"))
    schema = pq.read_schema(target)
    # Ensure no decimal types remain after conversion
    assert all("decimal" not in str(f.type).lower() for f in schema)


@pytest.mark.skipif(not _duckdb_ext_available("tpcds"), reason="duckdb tpcds extension not available")
def test_tpcds_schema_with_zero_scale():
    import duckdb

    # Generate only schema with zero scale (fast); do not write files
    duckdb.sql("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0);")
    tables = duckdb.sql("SHOW TABLES").fetchall()
    # Expect a reasonable number of tables present
    assert len(tables) >= 5
    # Check that DESCRIBE works for one known table
    table_name = tables[0][0]
    desc = duckdb.sql(f"DESCRIBE {table_name}").fetchall()
    assert len(desc) > 0


def test_invalid_missing_required_args(tmp_path):
    script = _script_path("generate_data_files.py")
    # Missing benchmark type
    proc = subprocess.run(
        [sys.executable, script, "-d", str(tmp_path / "x"), "-s", "0.1"],
        text=True,
        capture_output=True,
    )
    assert proc.returncode != 0
    # Missing data dir
    proc = subprocess.run(
        [sys.executable, script, "-b", "tpch", "-s", "0.1"],
        text=True,
        capture_output=True,
    )
    assert proc.returncode != 0
    # Missing scale factor
    proc = subprocess.run(
        [sys.executable, script, "-b", "tpch", "-d", str(tmp_path / "y")],
        text=True,
        capture_output=True,
    )
    assert proc.returncode != 0


@pytest.mark.skipif(not _duckdb_ext_available("tpch"), reason="duckdb tpch extension not available")
def test_extra_options_accepted(tmp_path):
    script = _script_path("generate_data_files.py")
    out = tmp_path / "tpch_sf0001"
    # Options --max-rows-per-file and -j are relevant to tpchgen path, but should be accepted with duckdb
    proc = subprocess.run(
        [
            sys.executable,
            script,
            "-b",
            "tpch",
            "-d",
            str(out),
            "-s",
            "0.001",
            "--use-duckdb",
            "--max-rows-per-file",
            "1000",
            "-j",
            "2",
        ],
        text=True,
        capture_output=True,
    )
    assert proc.returncode == 0


def test_tpchgen_partitions_count_monkeypatched(tmp_path, monkeypatch):
    # Import the module under test for monkeypatching
    import generate_data_files as gdf
    from pathlib import Path as _Path

    out_dir = tmp_path / "tpch_partitions"

    # Provide a fixed partition mapping to avoid duckdb dependency
    monkeypatch.setattr(
        gdf,
        "get_table_sf_ratios",
        lambda scale_factor, max_rows: {"orders": 3, "customer": 2},
    )

    # Replace the partition generator to create placeholder parquet files
    def fake_generate_partition(table, partition, raw_data_path, scale_factor, num_partitions, verbose):
        pdir = _Path(raw_data_path) / f"part-{partition}"
        pdir.mkdir(parents=True, exist_ok=True)
        (_Path(pdir) / f"{table}.parquet").write_text("")

    monkeypatch.setattr(gdf, "generate_partition", fake_generate_partition)

    args = SimpleNamespace(
        data_dir_path=str(out_dir),
        scale_factor=1,
        max_rows_per_file=1_000_000,
        num_threads=2,
        verbose=False,
        convert_decimals_to_floats=False,
        benchmark_type="tpch",
    )

    gdf.generate_data_files_with_tpchgen(args)

    # After rearrange_directory, each table dir should contain one file per partition
    orders = list((out_dir / "orders").glob("*.parquet"))
    customer = list((out_dir / "customer").glob("*.parquet"))
    assert len(orders) == 3
    assert len(customer) == 2


