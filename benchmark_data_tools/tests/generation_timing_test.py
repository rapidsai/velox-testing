# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
# This entire module exists only for local E2E generation profiling/timing.
# Remove this file before upstreaming.

import json
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
import generate_data_files as generator
import pytest
import row_group_sizing

from .common_fixtures import DataGenArgs


@pytest.mark.parametrize("convert_decimals_to_floats", [False, True])
@pytest.mark.parametrize("copy_start_gate", [False, True])
@pytest.mark.parametrize(
    "export_lifecycle",
    [
        "same_connection",
        "reopen_read_write",
        "reopen_read_only",
    ],
)
def test_duckdb_generation_writes_timing_metrics(
    tmp_path,
    monkeypatch,
    convert_decimals_to_floats,
    copy_start_gate,
    export_lifecycle,
):
    data_dir = tmp_path / "data"
    timing_path = tmp_path / "metrics" / "generation.json"
    args = _args(data_dir, timing_path)
    args.convert_decimals_to_floats = convert_decimals_to_floats
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", export_lifecycle)
    monkeypatch.setenv("VELOX_TESTING_COPY_START_GATE", "1" if copy_start_gate else "0")
    real_connect = duckdb.connect

    class InstallConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def sql(self, _statement):
            return None

    def connect(*connect_args, **connect_kwargs):
        if not connect_args and not connect_kwargs:
            return InstallConnection()
        return real_connect(*connect_args, **connect_kwargs)

    @contextmanager
    def probe(*_args, metrics=None, **_kwargs):
        if metrics is not None:
            metrics.update(
                {
                    "submit_seconds": 0.0,
                    "wait_seconds": 0.0,
                    "complete_seconds": 0.0,
                    "child": {
                        "generation_seconds": 0.0,
                        "probe_seconds": 0.0,
                        "total_seconds": 0.0,
                        "settings": {},
                        "rss_hwm_bytes": 1,
                    },
                }
            )
        yield lambda: {"synthetic": 2}

    def materialize(_benchmark_type, _scale_factor, conn):
        conn.sql("CREATE TABLE synthetic AS SELECT i AS value FROM range(9) AS rows(i)")

    monkeypatch.setattr(generator.duckdb, "connect", connect)
    monkeypatch.setattr(generator, "row_group_row_count_probe", probe)
    monkeypatch.setattr(generator, "init_benchmark_tables", materialize)

    generator.generate_data_files(args)

    document = json.loads(timing_path.read_text())
    assert document["schema_version"] == 5
    assert document["result"] == "success"
    assert document["benchmark"] == {
        "type": "tpcds",
        "scale_factor": 1.0,
        "convert_decimals_to_floats": convert_decimals_to_floats,
    }
    assert document["requested_writers"] == 2
    assert document["export"]["planned_tasks"] == 3
    assert document["export"]["planned_writers"] == 2
    assert document["export"]["source_rows"] == 9
    assert document["export"]["source_files"] == 1
    assert 1 <= document["export"]["actual_max_concurrent_copy"] <= 2
    assert document["export"]["copy_duration_sum_seconds"] >= 0
    assert len(document["export"]["copy_tasks"]) == 3
    assert all(task["duration_seconds"] >= 0 for task in document["export"]["copy_tasks"])
    assert document["export"]["copy_start_gate_enabled"] is copy_start_gate
    assert document["export"]["task_submission_seconds"] >= 0
    assert len(document["export"]["task_submissions"]) == 3
    for task in document["export"]["copy_tasks"]:
        assert task["queue_wait_seconds"] >= 0
        assert task["gate_wait_seconds"] >= 0
        assert task["cursor_open_seconds"] >= 0
        assert task["pre_copy_seconds"] >= task["cursor_open_seconds"]
        assert task["cursor_close_seconds"] >= 0
        assert task["publish_seconds"] >= 0
        assert task["worker_total_seconds"] >= task["duration_seconds"]
        assert task["error_stage"] is None
        assert task["cursor_open_end_offset_seconds"] <= task["start_offset_seconds"]
        assert task["start_offset_seconds"] <= task["copy_end_offset_seconds"]
        assert task["copy_end_offset_seconds"] <= task["worker_end_offset_seconds"]
    assert document["published"]["files"] == 3
    assert document["published"]["bytes"] > 0
    assert document["main_rss_hwm_bytes"] > 0
    assert document["host"]["cpu_count"] > 0
    assert document["versions"]["python"]
    assert document["versions"]["duckdb"]
    assert document["duckdb_settings_before_materialization"]["preserve_insertion_order"] == "true"
    assert document["duckdb_settings_after_export_config"]["preserve_insertion_order"] == "false"
    assert document["export_lifecycle"]["mode"] == export_lifecycle
    assert document["export_lifecycle"]["reopened"] is (
        export_lifecycle != "same_connection"
    )
    assert document["export_lifecycle"]["reopened_read_only"] is (
        export_lifecycle == "reopen_read_only"
    )
    assert document["export_lifecycle"]["prewarm_bytes"] == 0
    assert set(document["lifecycle_boundaries"]) == {
        "materialization_end",
        "after_export_connection_prepare",
        "copy_start",
        "copy_end",
    }
    for boundary in document["lifecycle_boundaries"].values():
        assert boundary["offset_seconds"] >= 0
        assert boundary["process"]["memory_bytes"]["VmRSS"] > 0
        assert boundary["process"]["minor_faults"] >= 0
        assert "duckdb_memory_by_tag" in boundary["database"]

    expected_intervals = {
        "output_prepare",
        "duckdb_install",
        "duckdb_connect",
        "duckdb_setup",
        "target_materialization",
        "checkpoint",
        "proxy_wait",
        "export_connection_close",
        "database_prewarm",
        "export_connection_reopen",
        "export_config",
        "export_planning",
        "parquet_copy_publish_wall",
        "final_export_connection_close",
        "intermediate_database_delete",
        "db_close_temp_cleanup",
        "metadata",
        "total_cli",
    }
    assert expected_intervals <= document["intervals_seconds"].keys()
    assert all(document["intervals_seconds"][name] >= 0 for name in expected_intervals)
    assert expected_intervals <= document["phase_intervals"].keys()
    for name in expected_intervals:
        interval = document["phase_intervals"][name]
        assert interval["start_offset_seconds"] >= 0
        assert interval["duration_seconds"] == document["intervals_seconds"][name]
    assert "_monotonic_origin" not in document
    assert not list(timing_path.parent.glob(f".{timing_path.name}.*"))


def test_failure_metrics_do_not_mask_generation_error(tmp_path, monkeypatch):
    args = _args(tmp_path / "data", tmp_path / "timing.json")

    def fail_generation(_args, _metrics):
        raise RuntimeError("original generation failure")

    monkeypatch.setattr(generator, "generate_data_files_with_duckdb", fail_generation)
    monkeypatch.setattr(
        generator,
        "_write_json_atomically",
        lambda *_args: (_ for _ in ()).throw(OSError("metrics failure")),
    )

    with pytest.raises(RuntimeError, match="original generation failure"):
        generator.generate_data_files(args)


def test_generation_failure_writes_best_effort_metrics(tmp_path, monkeypatch):
    timing_path = tmp_path / "timing.json"
    args = _args(tmp_path / "data", timing_path)

    def fail_generation(_args, _metrics):
        raise RuntimeError("injected failure")

    monkeypatch.setattr(generator, "generate_data_files_with_duckdb", fail_generation)

    with pytest.raises(RuntimeError, match="injected failure"):
        generator.generate_data_files(args)

    document = json.loads(timing_path.read_text())
    assert document["result"] == "failure"
    assert document["error"] == {"type": "RuntimeError", "message": "injected failure"}
    assert document["intervals_seconds"]["total_cli"] >= 0


def test_writer_observer_tracks_overlap_and_copy_time():
    observer = generator._WriterObserver()
    task = generator._ExportTask("table", "SELECT 1", "/tmp/table-1.parquet", 1)
    started = time.monotonic()
    observer.enter(task, started)
    observer.enter(task, started)
    observer.exit(task, started, time.monotonic() - started)
    observer.exit(task, started, time.monotonic() - started)

    snapshot = observer.snapshot()
    assert snapshot["actual_max_concurrent_copy"] == 2
    assert snapshot["copy_duration_sum_seconds"] >= 0
    assert len(snapshot["copy_tasks"]) == 2


def test_proxy_child_metrics_are_optional(tmp_path, monkeypatch):
    def materialize(_benchmark_type, _scale_factor, conn):
        conn.sql("CREATE OR REPLACE TABLE synthetic AS SELECT i FROM range(3) AS rows(i)")

    monkeypatch.setattr(row_group_sizing, "init_benchmark_tables", materialize)
    monkeypatch.setattr(row_group_sizing, "_measure_row_group_rows", lambda *_args: 2048)
    call_args = ("tpcds", 1024, 1.0, False, str(tmp_path))

    assert row_group_sizing.get_row_group_row_counts(*call_args) == {"synthetic": 2048}
    row_counts, metrics = row_group_sizing.get_row_group_row_counts(*call_args, include_metrics=True)

    assert row_counts == {"synthetic": 2048}
    assert metrics["generation_seconds"] >= 0
    assert metrics["probe_seconds"] >= 0
    assert metrics["total_seconds"] >= 0
    assert metrics["settings"]["threads"] == "1"
    assert metrics["settings"]["memory_limit"]
    assert metrics["rss_hwm_bytes"] > 0
    assert set(metrics["phase_intervals"]) == {
        "proxy_generation",
        "proxy_probe",
        "proxy_child_total",
    }
    assert all(interval["duration_seconds"] >= 0 for interval in metrics["phase_intervals"].values())


def _args(data_dir: Path, timing_path: Path):
    return DataGenArgs(
        benchmark_type="tpcds",
        data_dir_path=str(data_dir),
        scale_factor=1.0,
        convert_decimals_to_floats=False,
        use_duckdb=True,
        num_threads=2,
        verbose=False,
        max_rows_per_file=4,
        keep_original_dataset=True,
        approx_row_group_bytes=1024 * 1024,
        timing_json=str(timing_path),
    )
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
