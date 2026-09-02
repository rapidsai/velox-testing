# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import time

import generate_data_files as generator
import pytest


def test_export_lifecycle_defaults_to_same_connection(monkeypatch):
    monkeypatch.delenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", raising=False)

    assert generator._export_lifecycle() == "same_connection"


def test_invalid_export_lifecycle_is_rejected(monkeypatch):
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", "invalid")

    with pytest.raises(ValueError, match="VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE"):
        generator._export_lifecycle()


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
def test_research_initial_config_is_allowlisted(monkeypatch):
    monkeypatch.setenv(
        "VELOX_TESTING_DUCKDB_INITIAL_CONFIG_JSON",
        '{"pin_threads":"off","external_threads":32}',
    )
    monkeypatch.setattr(generator, "_DUCKDB_MEMORY_LIMIT", "48GiB")

    assert generator._duckdb_connection_config() == {
        "memory_limit": "48GiB",
        "pin_threads": "off",
        "external_threads": "32",
    }

    monkeypatch.setenv("VELOX_TESTING_DUCKDB_INITIAL_CONFIG_JSON", '{"memory_limit":"1GiB"}')
    with pytest.raises(ValueError, match="unsupported"):
        generator._duckdb_connection_config()


def test_copy_start_gate_is_explicit_boolean(monkeypatch):
    monkeypatch.delenv("VELOX_TESTING_COPY_START_GATE", raising=False)
    assert generator._copy_start_gate_enabled() is False

    monkeypatch.setenv("VELOX_TESTING_COPY_START_GATE", "1")
    assert generator._copy_start_gate_enabled() is True

    monkeypatch.setenv("VELOX_TESTING_COPY_START_GATE", "true")
    with pytest.raises(ValueError, match="must be 0 or 1"):
        generator._copy_start_gate_enabled()
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, False), ("false", False), ("FALSE", False), ("true", True), ("TRUE", True)],
)
def test_preserve_insertion_order_environment(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("VELOX_TESTING_DUCKDB_PRESERVE_INSERTION_ORDER", raising=False)
    else:
        monkeypatch.setenv("VELOX_TESTING_DUCKDB_PRESERVE_INSERTION_ORDER", value)

    assert generator._duckdb_preserve_insertion_order() is expected


def test_invalid_preserve_insertion_order_is_rejected(monkeypatch):
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_PRESERVE_INSERTION_ORDER", "1")

    with pytest.raises(ValueError, match="must be true or false"):
        generator._duckdb_preserve_insertion_order()


@pytest.mark.parametrize("value", ["1", "2048", "100000000"])
def test_row_group_rows_environment_accepts_positive_integer(monkeypatch, value):
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_ROW_GROUP_ROWS", value)

    assert generator._duckdb_row_group_rows() == int(value)


def test_row_group_rows_environment_defaults_to_probe(monkeypatch):
    monkeypatch.delenv("VELOX_TESTING_DUCKDB_ROW_GROUP_ROWS", raising=False)

    assert generator._duckdb_row_group_rows() is None


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "invalid", ""])
def test_row_group_rows_environment_rejects_non_positive_integer(monkeypatch, value):
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_ROW_GROUP_ROWS", value)

    with pytest.raises(ValueError, match="must be a positive integer"):
        generator._duckdb_row_group_rows()


@pytest.mark.parametrize(
    ("lifecycle", "expected_read_only"),
    [("reopen_read_write", False), ("reopen_read_only", True)],
)
def test_export_connection_reopens_with_requested_access_mode(
    tmp_path,
    monkeypatch,
    lifecycle,
    expected_read_only,
):
    class Connection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    source = Connection()
    reopened = Connection()
    connect_calls = []

    def connect(path, **kwargs):
        connect_calls.append((path, kwargs))
        return reopened

    metrics = {
        "_monotonic_origin": time.monotonic(),
        "intervals_seconds": {},
        "phase_intervals": {},
    }
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", lifecycle)
    monkeypatch.setattr(generator.duckdb, "connect", connect)

    result = generator._prepare_export_connection(source, tmp_path / "data.duckdb", metrics, False)

    assert result is reopened
    assert source.closed
    assert connect_calls[0][1]["read_only"] is expected_read_only
    assert metrics["export_lifecycle"]["reopened_read_only"] is expected_read_only


def test_export_connection_reopen_failure_leaves_writable_connection_closed(tmp_path, monkeypatch):
    class Connection:
        closed = False

        def close(self):
            self.closed = True

    source = Connection()
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", "reopen_read_only")
    monkeypatch.setattr(
        generator.duckdb,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("injected reopen failure")),
    )

    with pytest.raises(OSError, match="injected reopen failure"):
        generator._prepare_export_connection(source, tmp_path / "data.duckdb", None, False)

    assert source.closed


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
def test_same_connection_trim_keeps_connection_and_restores_memory_limit(monkeypatch):
    class Connection:
        def __init__(self):
            self.closed = False
            self.statements = []

        def close(self):
            self.closed = True

        def execute(self, statement):
            self.statements.append(statement)

    source = Connection()
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", "same_connection_trim")
    monkeypatch.setattr(generator, "_DUCKDB_MEMORY_LIMIT", "48GiB")

    result = generator._prepare_export_connection(source, "unused.duckdb", None, False)

    assert result is source
    assert source.closed is False
    assert source.statements == [
        "CHECKPOINT",
        "SET memory_limit='256MiB'",
        "SELECT 1",
        "SET memory_limit='48GiB'",
    ]


def test_reopen_drop_file_cache_advises_only_the_database_file(tmp_path, monkeypatch):
    class Connection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    source = Connection()
    reopened = Connection()
    advised = []
    database_path = tmp_path / "intermediate.duckdb"
    database_path.write_bytes(b"duckdb")

    def connect(_path, **kwargs):
        assert kwargs["read_only"] is False
        return reopened

    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", "reopen_drop_file_cache")
    monkeypatch.setattr(generator.duckdb, "connect", connect)
    monkeypatch.setattr(
        generator,
        "_drop_database_file_cache",
        lambda path: advised.append(str(path)),
    )

    result = generator._prepare_export_connection(source, database_path, None, False)

    assert result is reopened
    assert source.closed
    assert advised == [str(database_path)]


def test_reopen_keep_instance_holds_keeper_until_new_connection_exists(monkeypatch):
    events = []

    class Keeper:
        def close(self):
            events.append("keeper_close")

    class Connection:
        def cursor(self):
            events.append("keeper_open")
            return Keeper()

        def close(self):
            events.append("source_close")

    source = Connection()
    reopened = Connection()

    def connect(*_args, **_kwargs):
        events.append("reopen")
        return reopened

    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", "reopen_keep_instance")
    monkeypatch.setattr(generator.duckdb, "connect", connect)

    result = generator._prepare_export_connection(source, "data.duckdb", None, False)

    assert result is reopened
    assert events == ["keeper_open", "source_close", "reopen", "keeper_close"]


def test_page_cache_repopulate_uses_explicit_numa_node(monkeypatch):
    commands = []
    monkeypatch.setattr(
        generator.subprocess,
        "run",
        lambda command, check: commands.append((command, check)),
    )

    generator._repopulate_database_file_cache("/tmp/data.duckdb", "1")

    command, check = commands[0]
    assert command[:3] == ["numactl", "--cpunodebind=1", "--membind=1"]
    assert command[-1] == "/tmp/data.duckdb"
    assert check is True

    with pytest.raises(ValueError, match="must be 0 or 1"):
        generator._repopulate_database_file_cache("/tmp/data.duckdb", "all")


# BEGIN PER-TASK DATABASEINSTANCE EXPERIMENT — DELETE AS ONE BLOCK
def test_per_task_lifecycle_is_allowed(monkeypatch):
    monkeypatch.setenv("VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE", "per_task_processes")
    assert generator._export_lifecycle() == "per_task_processes"


def test_weighted_budget_covers_one_slot_per_task():
    weights = {f"t{index}": 100_000_000 if index < 87 else 1 for index in range(198)}
    threads = generator._allocate_weighted_budget(weights, 384, 1)
    memory = generator._allocate_weighted_budget(weights, 1536, 1)
    assert sum(threads.values()) == 384
    assert min(threads.values()) >= 1
    assert max(threads.values()) > min(threads.values())
    assert sum(memory.values()) == 1536
    assert min(memory.values()) >= 1
# END PER-TASK DATABASEINSTANCE EXPERIMENT — DELETE AS ONE BLOCK
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
