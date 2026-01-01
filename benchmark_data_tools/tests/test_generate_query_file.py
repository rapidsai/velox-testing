import json
import subprocess
import sys
from pathlib import Path

import pytest


def _script_path(name: str) -> str:
    return str(Path(__file__).resolve().parents[1] / name)


def _duckdb_ext_available(ext: str) -> bool:
    try:
        import duckdb  # noqa: F401
        subprocess.run(
            [sys.executable, "-c", f"import duckdb; duckdb.sql('INSTALL {ext}; LOAD {ext};')"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True
    except Exception:
        return False


@pytest.mark.parametrize("benchmark,expected_count", [("tpch", 22), ("tpcds", 99)])
def test_generate_queries_counts(tmp_path, benchmark, expected_count):
    if not _duckdb_ext_available(benchmark):
        pytest.skip(f"duckdb {benchmark} extension not available")
    script = _script_path("generate_query_file.py")
    out_dir = tmp_path / f"queries_{benchmark}"
    proc = subprocess.run(
        [sys.executable, script, "--benchmark-type", benchmark, "--queries-dir-path", str(out_dir)],
        text=True,
    )
    assert proc.returncode == 0
    qf = out_dir / "queries.json"
    assert qf.exists()
    data = json.loads(qf.read_text())
    assert len(data) == expected_count
    # Keys are Q1..Qn
    assert all(k.startswith("Q") for k in data.keys())


def test_help_exits_zero():
    script = _script_path("generate_query_file.py")
    proc = subprocess.run([sys.executable, script, "-h"], text=True, stdout=subprocess.PIPE)
    assert proc.returncode == 0
    assert "Usage" in proc.stdout or "usage" in proc.stdout



