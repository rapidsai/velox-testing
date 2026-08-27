# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import math
import multiprocessing
import os

# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
import platform
import resource

# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
import shutil
import subprocess
import sys
import tempfile

# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
import threading
import time

# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import NamedTuple

import duckdb
from duckdb_utils import copy_to_parquet, get_select_query, init_benchmark_tables
from row_group_sizing import row_group_row_count_probe

_INTEGER_TYPES = frozenset(("INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT", "INT"))
_HIGH_CARD_NDV_THRESHOLD = 0.99
_SAMPLE_SF = 0.01
# --- Experiment knobs ------------------------------------------------------
# Each default below is the original behaviour. The environment variables are
# for local performance experiments only: they are deliberately not CLI
# arguments, and nothing here derives a value from the host.
#   VELOX_TESTING_DUCKDB_MEMORY_LIMIT    unset -> 128GiB, the original constant
#   VELOX_TESTING_DUCKDB_EXPORT_THREADS  unset -> DuckDB's own default
#   VELOX_TESTING_DUCKDB_ROW_GROUP_ROWS  unset -> per-table probe estimate
#   VELOX_TESTING_DUCKDB_PRESERVE_INSERTION_ORDER unset -> false
# Concurrent Parquet writers are already tunable through -j/--num-threads.
_DUCKDB_MEMORY_LIMIT = os.environ.get("VELOX_TESTING_DUCKDB_MEMORY_LIMIT", "128GiB")
_DUCKDB_EXPORT_THREADS = os.environ.get("VELOX_TESTING_DUCKDB_EXPORT_THREADS")
_DUCKDB_ROW_GROUP_ROWS_ENV = "VELOX_TESTING_DUCKDB_ROW_GROUP_ROWS"
_DUCKDB_PRESERVE_INSERTION_ORDER_ENV = "VELOX_TESTING_DUCKDB_PRESERVE_INSERTION_ORDER"
# Explicit opt-in export connection reset. Same-connection remains the default;
# no hostname or runtime heuristic selects a lifecycle automatically.
_EXPORT_LIFECYCLE_ENV = "VELOX_TESTING_DUCKDB_EXPORT_LIFECYCLE"
_EXPORT_LIFECYCLES = frozenset(
    ("same_connection", "reopen_read_write", "reopen_read_only")
)
# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
_RESEARCH_EXPORT_LIFECYCLES = frozenset(
    (
        "same_connection_trim",
        "reopen_drop_file_cache",
        "reopen_keep_instance",
        "reopen_fresh_process",
    )
)
_RESEARCH_INITIAL_CONFIG_ENV = "VELOX_TESTING_DUCKDB_INITIAL_CONFIG_JSON"
_RESEARCH_PAGE_CACHE_NODE_ENV = "VELOX_TESTING_DUCKDB_PAGE_CACHE_NODE"
_RESEARCH_COPY_START_GATE_ENV = "VELOX_TESTING_COPY_START_GATE"
_RESEARCH_INITIAL_CONFIG_KEYS = frozenset(
    (
        "pin_threads",
        "external_threads",
        "async_threads",
        "scheduler_process_partial",
        "allocator_background_threads",
        "allocator_flush_threshold",
        "allocator_bulk_deallocation_flush_threshold",
    )
)
_TIMING_SCHEMA_VERSION = 5
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
# ---------------------------------------------------------------------------


def _duckdb_row_group_rows():
    value = os.environ.get(_DUCKDB_ROW_GROUP_ROWS_ENV)
    if value is None:
        return None
    try:
        rows = int(value)
    except ValueError as error:
        raise ValueError(f"{_DUCKDB_ROW_GROUP_ROWS_ENV} must be a positive integer") from error
    if rows <= 0:
        raise ValueError(f"{_DUCKDB_ROW_GROUP_ROWS_ENV} must be a positive integer")
    return rows


def _duckdb_preserve_insertion_order():
    value = os.environ.get(_DUCKDB_PRESERVE_INSERTION_ORDER_ENV, "false").lower()
    if value not in ("false", "true"):
        raise ValueError(f"{_DUCKDB_PRESERVE_INSERTION_ORDER_ENV} must be true or false")
    return value == "true"


def generate_partition(
    table,
    partition,
    raw_data_path,
    scale_factor,
    num_partitions,
    verbose,
    approx_row_group_bytes,
    convert_decimals_to_floats,
    codec_defs,
):
    if verbose:
        print(f"Generating '{table}' partition: {partition}")
    Path(f"{raw_data_path}/part-{partition}").mkdir(parents=True, exist_ok=True)
    command = [
        "tpchgen-cli",
        "-T",
        table,
        "-s",
        str(scale_factor),
        "--output-dir",
        str(f"{raw_data_path}/part-{partition}"),
        "--parts",
        str(num_partitions),
        "--part",
        str(partition),
        "--format",
        "parquet",
        "--parquet-version",
        "2",
        "--parquet-row-group-bytes",
        str(approx_row_group_bytes),
    ]

    if convert_decimals_to_floats:
        command.extend(["--decimal-column-type", "f64"])

    command.extend(get_tpchgen_codec_args(codec_defs, table))

    try:
        subprocess.run(command, check=True, stderr=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr or ""
        if "--parquet-compression" in stderr:
            bad_value = next(t["compression"] for t in codec_defs["tables"] if t["name"] == table)
            raise ValueError(
                f"Invalid 'compression' value '{bad_value}' for table '{table}' in codec definitions. "
                f"See codec_definition_template.json for valid values."
            ) from e
        if stderr:
            sys.stderr.write(stderr)
        raise


def generate_data_files(args):
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    timing_path = getattr(args, "timing_json", None)
    metrics = _new_metrics(args) if timing_path else None
    total_started = time.monotonic()
    try:
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        if args.codec_definitions:
            if args.benchmark_type != "tpch":
                raise ValueError("--codec-definitions is currently only supported for TPC-H benchmarks")
            if args.use_duckdb:
                raise ValueError("--codec-definitions is not supported with --use-duckdb")
            codec_defs = load_codec_definitions(args.codec_definitions)
        elif args.benchmark_type == "tpch" and not args.use_duckdb:
            codec_defs = build_default_codec_defs()
        else:
            codec_defs = None

        if args.use_duckdb or args.benchmark_type != "tpch":
            # Validate DuckDB export environment before expensive materialization.
            _duckdb_row_group_rows()
            _duckdb_preserve_insertion_order()

        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        if os.path.exists(args.data_dir_path):
            shutil.rmtree(args.data_dir_path)
        Path(f"{args.data_dir_path}").mkdir(parents=True, exist_ok=True)
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "output_prepare", started)
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)

        # tpchgen is much faster, but is exclusive to generating tpch data.  Use duckdb as a fallback.
        if args.benchmark_type == "tpch" and not args.use_duckdb:
            if args.verbose:
                print("generating with tpchgen")
            generate_data_files_with_tpchgen(args, codec_defs)
        else:
            if args.verbose:
                print("generating with duckdb")
            generate_data_files_with_duckdb(
                args,
                # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                metrics,
                # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            )

        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        write_metadata(args)
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "metadata", started)
        if metrics is not None:
            metrics["result"] = "success"
            metrics["published"] = _published_stats(args.data_dir_path)
    except BaseException as error:
        if metrics is not None:
            metrics["result"] = "failure"
            metrics["error"] = {"type": type(error).__name__, "message": str(error)}
            metrics["published"] = _published_stats(args.data_dir_path)
        raise
    finally:
        if metrics is not None:
            _record_interval(metrics, "total_cli", total_started)
            metrics["main_rss_hwm_bytes"] = _rss_hwm_bytes()
            if sys.exc_info()[0] is None:
                _write_json_atomically(timing_path, metrics)
            else:
                try:
                    _write_json_atomically(timing_path, metrics)
                except BaseException:
                    # Metrics are diagnostic and must never replace a generation error.
                    pass
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
def _new_metrics(args):
    affinity = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    origin = time.monotonic()
    return {
        "_monotonic_origin": origin,
        "schema_version": _TIMING_SCHEMA_VERSION,
        "result": "running",
        "benchmark": {
            "type": args.benchmark_type,
            "scale_factor": args.scale_factor,
            "convert_decimals_to_floats": bool(args.convert_decimals_to_floats),
        },
        "requested_writers": args.num_threads,
        "host": {
            "cpu_count": os.cpu_count(),
            "cpu_affinity": affinity,
            "platform": platform.platform(),
        },
        "versions": {"python": platform.python_version(), "duckdb": duckdb.__version__},
        "intervals_seconds": {},
        "phase_intervals": {},
    }


def _record_interval(metrics, name, started):
    if metrics is not None:
        duration = time.monotonic() - started
        metrics["intervals_seconds"][name] = duration
        metrics["phase_intervals"][name] = {
            "start_offset_seconds": started - metrics["_monotonic_origin"],
            "duration_seconds": duration,
        }


def _rss_hwm_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def _read_text(path):
    try:
        return Path(path).read_text()
    except (OSError, UnicodeError):
        return None


def _key_value_lines(text):
    values = {}
    if text is None:
        return values
    for line in text.splitlines():
        fields = line.replace(":", " ").split()
        if len(fields) >= 2:
            try:
                values[fields[0]] = int(fields[1])
            except ValueError:
                values[fields[0]] = " ".join(fields[1:])
    return values


def _process_snapshot():
    status = _key_value_lines(_read_text("/proc/self/status"))
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # /proc reports these fields in KiB.
    memory = {
        name: status.get(name, 0) * 1024
        for name in ("VmRSS", "RssAnon", "RssFile", "RssShmem", "VmSwap", "VmHWM")
        if isinstance(status.get(name), int)
    }
    return {
        "memory_bytes": memory,
        "minor_faults": usage.ru_minflt,
        "major_faults": usage.ru_majflt,
        "user_cpu_seconds": usage.ru_utime,
        "system_cpu_seconds": usage.ru_stime,
    }


def _numa_snapshot():
    node_pages = {}
    mapped_pages = 0
    text = _read_text("/proc/self/numa_maps")
    if text is None:
        return {"available": False}
    for token in text.split():
        if token.startswith("N") and "=" in token and token[1 : token.index("=")].isdigit():
            node, pages = token.split("=", 1)
            try:
                pages = int(pages)
            except ValueError:
                continue
            node_pages[node] = node_pages.get(node, 0) + pages
            mapped_pages += pages
    return {
        "available": True,
        "page_size_bytes": os.sysconf("SC_PAGE_SIZE"),
        "mapped_pages": mapped_pages,
        "node_pages": node_pages,
    }


def _host_resource_snapshot():
    cpu_line = (_read_text("/proc/stat") or "").splitlines()
    cpu_fields = cpu_line[0].split() if cpu_line else []
    meminfo = _key_value_lines(_read_text("/proc/meminfo"))
    cgroup_root = Path("/sys/fs/cgroup")
    cgroup = {}
    for name in ("memory.current", "memory.peak", "memory.swap.current"):
        value = _read_text(cgroup_root / name)
        if value is not None:
            try:
                cgroup[name.replace(".", "_")] = int(value.strip())
            except ValueError:
                pass
    for name in ("cpu.stat", "memory.events", "io.stat"):
        value = _read_text(cgroup_root / name)
        if value is not None:
            cgroup[name.replace(".", "_")] = value.strip()
    return {
        "cpu_jiffies": {
            name: int(value)
            for name, value in zip(
                ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"),
                cpu_fields[1:],
            )
        },
        "memory_bytes": {
            name: meminfo[name] * 1024
            for name in ("MemAvailable", "Dirty", "Writeback", "SwapFree")
            if isinstance(meminfo.get(name), int)
        },
        "cgroup": cgroup,
        "diskstats": (_read_text("/proc/diskstats") or "").strip(),
    }


def _query_dicts(conn, query):
    cursor = conn.execute(query)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _database_snapshot(conn, database_path=None):
    snapshot = {}
    try:
        snapshot["duckdb_memory_by_tag"] = _query_dicts(conn, "SELECT * FROM duckdb_memory()")
    except duckdb.Error as error:
        snapshot["duckdb_memory_error"] = str(error)
    try:
        snapshot["database_size"] = _query_dicts(conn, "PRAGMA database_size")
    except duckdb.Error as error:
        snapshot["database_size_error"] = str(error)
    if database_path is None:
        try:
            databases = _query_dicts(conn, "PRAGMA database_list")
            database_path = next(
                (
                    row.get("file")
                    for row in databases
                    if row.get("file") and row.get("file") != ":memory:"
                ),
                None,
            )
        except duckdb.Error:
            database_path = None
    if database_path:
        database_path = Path(database_path)
        snapshot["files"] = {
            "database_bytes": database_path.stat().st_size if database_path.exists() else 0,
            "wal_bytes": (
                Path(f"{database_path}.wal").stat().st_size
                if Path(f"{database_path}.wal").exists()
                else 0
            ),
        }
    return snapshot


def _capture_boundary(metrics, name, conn, database_path=None, observer=None):
    if metrics is None:
        return
    metrics.setdefault("lifecycle_boundaries", {})[name] = {
        "offset_seconds": time.monotonic() - metrics["_monotonic_origin"],
        "process": _process_snapshot(),
        "numa": _numa_snapshot(),
        "host": _host_resource_snapshot(),
        "database": _database_snapshot(conn, database_path),
        "active_writers": observer.current_concurrency() if observer is not None else 0,
    }


def _write_json_atomically(path, document):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    public_document = {key: value for key, value in document.items() if not key.startswith("_")}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=destination.parent, prefix=f".{destination.name}.", delete=False
        ) as file:
            temporary = Path(file.name)
            json.dump(public_document, file, indent=2, sort_keys=True)
            file.write("\n")
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _published_stats(data_dir_path):
    paths = list(Path(data_dir_path).rglob("*.parquet")) if Path(data_dir_path).exists() else []
    return {"files": len(paths), "bytes": sum(path.stat().st_size for path in paths)}
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def _export_lifecycle():
    lifecycle = os.environ.get(_EXPORT_LIFECYCLE_ENV, "same_connection")
    allowed = set(_EXPORT_LIFECYCLES)
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    allowed.update(_RESEARCH_EXPORT_LIFECYCLES)
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    if lifecycle not in allowed:
        raise ValueError(
            f"{_EXPORT_LIFECYCLE_ENV} must be one of {sorted(allowed)}, not {lifecycle!r}"
        )
    return lifecycle


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
def _duckdb_connection_config():
    config = {"memory_limit": _DUCKDB_MEMORY_LIMIT}
    encoded = os.environ.get(_RESEARCH_INITIAL_CONFIG_ENV)
    if not encoded:
        return config
    values = json.loads(encoded)
    if not isinstance(values, dict):
        raise ValueError(f"{_RESEARCH_INITIAL_CONFIG_ENV} must encode a JSON object")
    unsupported = set(values) - _RESEARCH_INITIAL_CONFIG_KEYS
    if unsupported:
        raise ValueError(
            f"{_RESEARCH_INITIAL_CONFIG_ENV} contains unsupported keys: {sorted(unsupported)}"
        )
    config.update({str(name): str(value) for name, value in values.items()})
    return config


def _copy_start_gate_enabled():
    value = os.environ.get(_RESEARCH_COPY_START_GATE_ENV, "0")
    if value not in ("0", "1"):
        raise ValueError(f"{_RESEARCH_COPY_START_GATE_ENV} must be 0 or 1")
    return value == "1"


def _repopulate_database_file_cache(database_path, node):
    """Repopulate the database file cache under one explicit NUMA policy."""
    if str(node) not in ("0", "1"):
        raise ValueError(f"{_RESEARCH_PAGE_CACHE_NODE_ENV} must be 0 or 1")
    helper = """
import os, sys
path = sys.argv[1]
with open(path, 'rb', buffering=0) as handle:
    os.posix_fadvise(handle.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    while handle.read(64 * 1024 * 1024):
        pass
"""
    subprocess.run(
        [
            "numactl",
            f"--cpunodebind={node}",
            f"--membind={node}",
            sys.executable,
            "-c",
            helper,
            str(database_path),
        ],
        check=True,
    )
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def _prepare_export_connection(
    conn,
    database_path,
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    metrics,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    verbose,
):
    lifecycle = _export_lifecycle()
    page_cache_node = os.environ.get(_RESEARCH_PAGE_CACHE_NODE_ENV)
    reopened_read_only = lifecycle == "reopen_read_only"
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    buffer_trim = lifecycle == "same_connection_trim"
    file_cache_dropped = lifecycle == "reopen_drop_file_cache"
    instance_preserved = lifecycle == "reopen_keep_instance"
    reopened = lifecycle not in ("same_connection", "same_connection_trim")
    if metrics is not None:
        metrics["export_lifecycle"] = {
            "mode": lifecycle,
            "reopened": reopened,
            "reopened_read_only": reopened_read_only,
            "buffer_trim": buffer_trim,
            "file_cache_dropped": file_cache_dropped,
            "instance_preserved_by_keeper": instance_preserved,
            "prewarm_bytes": 0,
            "page_cache_repopulate_node": page_cache_node,
        }
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    if lifecycle == "same_connection":
        if page_cache_node is not None:
            started = time.monotonic()
            _repopulate_database_file_cache(database_path, page_cache_node)
            _record_interval(metrics, "database_file_cache_repopulate", started)
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        for interval_name in (
            "export_connection_close",
            "database_prewarm",
            "export_connection_reopen",
        ):
            _record_interval(metrics, interval_name, time.monotonic())
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        return conn

    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    if lifecycle == "same_connection_trim":
        if verbose:
            print("Trimming DuckDB buffer manager before export", flush=True)
        started = time.monotonic()
        conn.execute("CHECKPOINT")
        conn.execute("SET memory_limit='256MiB'")
        conn.execute("SELECT 1")
        conn.execute(f"SET memory_limit='{_DUCKDB_MEMORY_LIMIT}'")
        _record_interval(metrics, "buffer_trim", started)
        for interval_name in (
            "export_connection_close",
            "database_prewarm",
            "export_connection_reopen",
        ):
            _record_interval(metrics, interval_name, time.monotonic())
        return conn
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)

    if verbose:
        print("Closing writable DuckDB connection before export", flush=True)
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    keeper = conn.cursor() if instance_preserved else None
    started = time.monotonic()
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    conn.close()
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    _record_interval(metrics, "export_connection_close", started)
    _record_interval(metrics, "database_prewarm", time.monotonic())
    if lifecycle == "reopen_drop_file_cache":
        started = time.monotonic()
        _drop_database_file_cache(database_path)
        _record_interval(metrics, "file_cache_drop", started)
    if page_cache_node is not None:
        started = time.monotonic()
        _repopulate_database_file_cache(database_path, page_cache_node)
        _record_interval(metrics, "database_file_cache_repopulate", started)
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)

    if verbose:
        access_mode = "read-only" if reopened_read_only else "read-write"
        print(f"Reopening DuckDB database {access_mode} for export", flush=True)
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    started = time.monotonic()
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    try:
        reopened = duckdb.connect(
            str(database_path),
            read_only=reopened_read_only,
            config=_duckdb_connection_config(),
        )
    finally:
        if keeper is not None:
            keeper.close()
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    _record_interval(metrics, "export_connection_reopen", started)
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    return reopened


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
def _drop_database_file_cache(database_path):
    """Evict only this database file from the OS page cache. Research-only."""
    if not hasattr(os, "posix_fadvise"):
        raise RuntimeError("posix_fadvise is required for reopen_drop_file_cache")
    with Path(database_path).open("rb") as handle:
        os.posix_fadvise(handle.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def generate_data_files_with_tpchgen(args, codec_defs):
    local_installs_bin = Path(__file__).resolve().parent / ".local_installs" / "bin"
    if local_installs_bin.exists():
        os.environ["PATH"] = os.pathsep.join([str(local_installs_bin), os.environ["PATH"]])

    tables_sf_ratio = get_table_sf_ratios(args.scale_factor, args.max_rows_per_file)
    raw_data_path = args.data_dir_path

    max_partitions = 1
    with ThreadPoolExecutor(args.num_threads) as executor:
        futures = []

        for table, num_partitions in tables_sf_ratio.items():
            if args.verbose:
                print(f"Generating TPC-H data for table '{table}' with {num_partitions} partitions")
            for partition in range(1, num_partitions + 1):
                futures.append(
                    executor.submit(
                        generate_partition,
                        table,
                        partition,
                        raw_data_path,
                        args.scale_factor,
                        num_partitions,
                        args.verbose,
                        args.approx_row_group_bytes,
                        args.convert_decimals_to_floats,
                        codec_defs,
                    )
                )
            max_partitions = num_partitions if num_partitions > max_partitions else max_partitions

        for future in futures:
            future.result()

    rearrange_directory(raw_data_path, max_partitions)

    if args.verbose:
        print(f"Raw data created at: {raw_data_path}")


# This dictionary maps each table to the number of partitions it should have based on it's
# expected file size relative to the SF.
# We generate a small sample benchmark (sf-0.01) to sample the ratio of how many rows are generated.
def get_table_sf_ratios(scale_factor, max_rows):
    int_scale_factor = int(scale_factor)
    int_scale_factor = 1 if int_scale_factor < 1 else int_scale_factor
    tables_sf_ratio = {}
    init_benchmark_tables("tpch", 0.01)
    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table in tables:
        stripped_table = table[0].strip("'")
        num_rows = duckdb.sql(f"SELECT COUNT (*) FROM {stripped_table}").fetchall()
        tables_sf_ratio[stripped_table] = math.ceil(int_scale_factor / (max_rows / (int(num_rows[0][0]) * 100)))
    return tables_sf_ratio


def rearrange_directory(raw_data_path, num_partitions):
    # When we generate partitioned data it will have the form <data_dir>/<partition>/<table_name>/<table_name>.parquet.
    # We want to re-arrange it to have the form <data_dir>/<table_name>/<table_name>-<partition>.parquet
    tables = os.listdir(f"{raw_data_path}/part-1")

    for table in tables:
        Path(f"{raw_data_path}/{table}").mkdir(parents=True, exist_ok=True)

    # Move the partitioned data into the new directory structure.
    for partition in range(1, num_partitions + 1):
        for table in tables:
            part_file_path = f"{raw_data_path}/part-{partition}/{table}/{table}.{partition}.parquet"
            if os.path.exists(part_file_path):
                shutil.move(part_file_path, f"{raw_data_path}/{table}/{table}-{partition}.parquet")
        part_dir_path = f"{raw_data_path}/part-{partition}"
        for dir_name in os.listdir(part_dir_path):
            os.rmdir(f"{part_dir_path}/{dir_name}")
        os.rmdir(part_dir_path)


def write_metadata(args):
    with open(f"{args.data_dir_path}/metadata.json", "w") as file:
        metadata = {
            "scale_factor": args.scale_factor,
            "approx_row_group_bytes": args.approx_row_group_bytes,
        }
        json.dump(metadata, file, indent=2)
        file.write("\n")


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
def _fresh_process_export_worker(
    database_path,
    args,
    row_group_rows,
    metrics_path,
    monotonic_origin,
):
    """Spawn target: open a new process/DatabaseInstance and export."""
    child_metrics = (
        {
            "_monotonic_origin": monotonic_origin,
            "intervals_seconds": {},
            "phase_intervals": {},
            "lifecycle_boundaries": {},
        }
        if metrics_path is not None
        else None
    )
    conn = duckdb.connect(str(database_path), config=_duckdb_connection_config())
    try:
        _capture_boundary(
            child_metrics,
            "after_export_connection_prepare",
            conn,
            database_path,
        )
        started = time.monotonic()
        configure_duckdb_export(conn)
        if child_metrics is not None:
            child_metrics["duckdb_settings_after_export_config"] = _duckdb_settings(conn)
        _record_interval(child_metrics, "export_config", started)
        _export_tables(args, row_group_rows, conn, child_metrics)
    finally:
        conn.close()
    if metrics_path is not None:
        _write_json_atomically(metrics_path, child_metrics)


def _export_in_fresh_process(database_path, args, row_group_rows, metrics):
    metrics_path = Path(database_path).with_name("fresh-process-export-metrics.json")
    metrics_path.unlink(missing_ok=True)
    origin = metrics["_monotonic_origin"] if metrics is not None else time.monotonic()
    started = time.monotonic()
    process = multiprocessing.get_context("spawn").Process(
        target=_fresh_process_export_worker,
        args=(
            str(database_path),
            args,
            row_group_rows,
            str(metrics_path) if metrics is not None else None,
            origin,
        ),
    )
    process.start()
    process.join()
    if process.exitcode != 0:
        raise RuntimeError(f"fresh export process exited with status {process.exitcode}")
    if metrics is not None:
        child_metrics = json.loads(metrics_path.read_text())
        for name in (
            "export",
            "duckdb_settings_after_export_config",
            "lifecycle_boundaries",
        ):
            if name in child_metrics:
                if name == "lifecycle_boundaries":
                    metrics.setdefault(name, {}).update(child_metrics[name])
                else:
                    metrics[name] = child_metrics[name]
        metrics["intervals_seconds"].update(child_metrics["intervals_seconds"])
        metrics["phase_intervals"].update(child_metrics["phase_intervals"])
        metrics["intervals_seconds"]["fresh_process_export_total"] = time.monotonic() - started
        metrics["export_lifecycle"] = {
            "mode": "reopen_fresh_process",
            "reopened": True,
            "reopened_read_only": False,
            "buffer_trim": False,
            "file_cache_dropped": False,
            "instance_preserved_by_keeper": False,
            "fresh_process": True,
            "prewarm_bytes": 0,
        }
    metrics_path.unlink(missing_ok=True)
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def generate_data_files_with_duckdb(
    args,
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    metrics=None,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
):
    """Materialize the dataset into an intermediate DuckDB file, then export Parquet from it."""
    work_directory = tempfile.mkdtemp(prefix=".duckdb-main-", dir=args.data_dir_path)
    conn = None
    try:
        database_path = Path(work_directory) / "intermediate.duckdb"
        # Avoid concurrent first-time extension installation across processes.
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        with duckdb.connect() as install_conn:
            install_conn.sql(f"INSTALL {args.benchmark_type}")
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "duckdb_install", started)
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)

        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        conn = duckdb.connect(str(database_path), config=_duckdb_connection_config())
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "duckdb_connect", started)

        started = time.monotonic()
        if metrics is not None:
            metrics["duckdb_settings_before_materialization"] = _duckdb_settings(conn)
        _record_interval(metrics, "duckdb_setup", started)
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)

        if metrics is None:
            row_group_rows = _materialize_tables(args, conn)
            # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            if _export_lifecycle() == "reopen_fresh_process":
                conn.close()
                conn = None
                _export_in_fresh_process(database_path, args, row_group_rows, metrics)
                return
            conn = _prepare_export_connection(conn, database_path, metrics, args.verbose)
            # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            configure_duckdb_export(conn)
            _export_tables(args, row_group_rows, conn)
        else:
            # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            row_group_rows = _materialize_tables(args, conn, metrics)
            _capture_boundary(metrics, "materialization_end", conn, database_path)
            if _export_lifecycle() == "reopen_fresh_process":
                started = time.monotonic()
                conn.close()
                conn = None
                _record_interval(metrics, "export_connection_close", started)
                _export_in_fresh_process(database_path, args, row_group_rows, metrics)
                return
            conn = _prepare_export_connection(conn, database_path, metrics, args.verbose)
            _capture_boundary(metrics, "after_export_connection_prepare", conn, database_path)
            started = time.monotonic()
            # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            configure_duckdb_export(conn)
            # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            metrics["duckdb_settings_after_export_config"] = _duckdb_settings(conn)
            _record_interval(metrics, "export_config", started)
            # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            _export_tables(
                args,
                row_group_rows,
                conn,
                # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                metrics,
                # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            )
    finally:
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        if conn is not None:
            # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            connection_close_started = time.monotonic()
            # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            conn.close()
            # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
            _record_interval(metrics, "final_export_connection_close", connection_close_started)
            # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        database_delete_started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        shutil.rmtree(work_directory, ignore_errors=True)
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "intermediate_database_delete", database_delete_started)
        _record_interval(metrics, "db_close_temp_cleanup", started)
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def configure_duckdb_export(conn):
    """Apply the shipping DuckDB settings used immediately before Parquet export."""
    if _DUCKDB_EXPORT_THREADS:
        conn.execute(f"SET threads={_DUCKDB_EXPORT_THREADS}")
    preserve_insertion_order = str(_duckdb_preserve_insertion_order()).lower()
    conn.execute(f"SET preserve_insertion_order={preserve_insertion_order}")


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
def _duckdb_settings(conn):
    names = (
        "'threads','memory_limit','preserve_insertion_order',"
        "'pin_threads','external_threads','async_threads',"
        "'scheduler_process_partial','allocator_background_threads',"
        "'allocator_flush_threshold','allocator_bulk_deallocation_flush_threshold'"
    )
    return dict(
        conn.execute(
            f"SELECT name, value FROM duckdb_settings() WHERE name IN ({names}) ORDER BY name"
        ).fetchall()
    )
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def _materialize_tables(
    args,
    conn,
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    metrics=None,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
):
    """Generate the target dataset, returning the probed rows per row group per table."""
    if args.verbose:
        print("Starting row-group proxy probe", flush=True)

    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    proxy_metrics = {} if metrics is not None else None
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    probe = row_group_row_count_probe(
        args.benchmark_type,
        args.approx_row_group_bytes,
        args.scale_factor,
        args.convert_decimals_to_floats,
        args.data_dir_path,
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        metrics=proxy_metrics,
        monotonic_origin=metrics["_monotonic_origin"] if metrics is not None else None,
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    )
    with probe as probed_row_counts:
        if args.verbose:
            print(
                f"Materializing {args.benchmark_type.upper()} SF{args.scale_factor:g} into a storage-backed database",
                flush=True,
            )
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        init_benchmark_tables(args.benchmark_type, args.scale_factor, conn)
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "target_materialization", started)
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        if args.verbose:
            print("Materialization completed", flush=True)

        # Let the export read one settled state.
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        conn.sql("CHECKPOINT")
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "checkpoint", started)
        started = time.monotonic()
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        row_counts = probed_row_counts()
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        _record_interval(metrics, "proxy_wait", started)
        if metrics is not None:
            metrics["proxy"] = proxy_metrics
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        return row_counts


class _ExportTask(NamedTuple):
    """One Parquet file to write: a whole table, or one rowid range of a split table."""

    table_name: str
    query: str
    file_path: str
    rows_per_row_group: int
    estimated_rows: int = 0


def _export_tables(
    args,
    row_group_rows,
    conn,
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    metrics=None,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
):
    """Write every materialized table out as Parquet.

    Parts from all tables share one queue, so a table with few parts no longer
    leaves writers idle, and num_threads caps the writers for the whole export.
    """
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    started = time.monotonic()
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    tasks = _plan_export_tasks(
        args,
        row_group_rows,
        conn,
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        metrics,
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    )
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    task_order = os.environ.get("VELOX_TESTING_COPY_TASK_ORDER", "fifo")
    if task_order == "largest_first":
        tasks.sort(key=lambda task: (-task.estimated_rows, task.file_path))
    elif task_order != "fifo":
        raise ValueError(
            "VELOX_TESTING_COPY_TASK_ORDER must be 'fifo' or 'largest_first', "
            f"not {task_order!r}"
        )
    _record_interval(metrics, "export_planning", started)
    observer = _WriterObserver(metrics["_monotonic_origin"]) if metrics is not None else None
    start_gate_enabled = _copy_start_gate_enabled()
    if metrics is not None:
        metrics["export"]["planned_tasks"] = len(tasks)
        metrics["export"]["planned_writers"] = min(args.num_threads, len(tasks))
        metrics["export"]["task_order"] = task_order
        metrics["export"]["copy_start_gate_enabled"] = start_gate_enabled
        _capture_boundary(metrics, "copy_start", conn, observer=observer)

    started = time.monotonic()
    try:
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        if args.num_threads == 1 or len(tasks) <= 1:
            for task in tasks:
                # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                submitted_at = time.monotonic() if observer is not None else None
                # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                _write_part(
                    task,
                    conn,
                    None,
                    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                    observer,
                    submitted_at,
                    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                )
        else:
            with ThreadPoolExecutor(max_workers=min(args.num_threads, len(tasks))) as executor:
                start_gate = threading.Event() if start_gate_enabled else None
                # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                submission_started = time.monotonic()
                submission_details = []
                # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                futures = []
                try:
                    previous_submit_returned = submission_started
                    for task_index, task in enumerate(tasks):
                        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                        submitted_at = time.monotonic()
                        gap_before_submit = submitted_at - previous_submit_returned
                        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                        futures.append(
                            executor.submit(
                                _write_part,
                                task,
                                conn,
                                start_gate,
                                # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                                observer,
                                submitted_at,
                                # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                            )
                        )
                        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                        submit_returned = time.monotonic()
                        submission_details.append(
                            {
                                "task_index": task_index,
                                "table": task.table_name,
                                "file": Path(task.file_path).name,
                                "submit_start_offset_seconds": (
                                    submitted_at - metrics["_monotonic_origin"]
                                    if metrics is not None
                                    else None
                                ),
                                "submit_end_offset_seconds": (
                                    submit_returned - metrics["_monotonic_origin"]
                                    if metrics is not None
                                    else None
                                ),
                                "submit_call_seconds": submit_returned - submitted_at,
                                "gap_before_submit_seconds": gap_before_submit,
                            }
                        )
                        previous_submit_returned = submit_returned
                        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                finally:
                    if start_gate is not None:
                        start_gate.set()
                # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                if metrics is not None:
                    metrics["export"]["task_submission_seconds"] = (
                        time.monotonic() - submission_started
                    )
                    metrics["export"]["task_submissions"] = submission_details
                    metrics["export"]["copy_start_gate_release_offset_seconds"] = (
                        time.monotonic() - metrics["_monotonic_origin"]
                        if start_gate is not None
                        else None
                    )
                # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
                try:
                    for future in futures:
                        future.result()
                except BaseException:
                    # Drop queued parts; writes already running still finish as the pool closes.
                    for future in futures:
                        future.cancel()
                    raise
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    finally:
        _record_interval(metrics, "parquet_copy_publish_wall", started)
        if metrics is not None:
            metrics["export"].update(observer.snapshot())
            _capture_boundary(metrics, "copy_end", conn, observer=observer)
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def _plan_export_tasks(
    args,
    row_group_rows,
    conn,
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    metrics=None,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
):
    """Describe every Parquet file to write, before any of them is written.

    Catalog queries and directory creation stay here so that writers only run COPY.
    """
    tasks = []
    row_group_rows_override = _duckdb_row_group_rows()
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    source_tables = {}
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    for (table_name,) in conn.sql("SHOW TABLES").fetchall():
        table_data_dir = f"{args.data_dir_path}/{table_name}"
        Path(table_data_dir).mkdir(exist_ok=False)

        select_query = get_select_query(table_name, args.convert_decimals_to_floats, conn)
        row_count = conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        source_tables[table_name] = row_count
        # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
        # The estimate is missing for empty tables and can exceed the target's row count.
        estimated_row_group_rows = (
            row_group_rows_override
            if row_group_rows_override is not None
            else row_group_rows.get(table_name, row_count)
        )
        rows_per_row_group = min(estimated_row_group_rows, row_count)
        num_partitions = max(math.ceil(row_count / args.max_rows_per_file), 1) if args.max_rows_per_file else 1

        # 1-indexed part names, contiguous even when a table is not split.
        for part in range(num_partitions):
            partition_query = select_query
            if num_partitions > 1:
                start_row = part * args.max_rows_per_file
                partition_query += f" WHERE rowid >= {start_row} AND rowid < {start_row + args.max_rows_per_file}"
            tasks.append(
                _ExportTask(
                    table_name=table_name,
                    query=partition_query,
                    file_path=f"{table_data_dir}/{table_name}-{part + 1}.parquet",
                    rows_per_row_group=rows_per_row_group,
                    estimated_rows=min(
                        args.max_rows_per_file if args.max_rows_per_file else row_count,
                        max(row_count - part * (args.max_rows_per_file or row_count), 0),
                    ),
                )
            )
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    if metrics is not None:
        metrics["export"] = {
            "source_rows": sum(source_tables.values()),
            "source_files": len(source_tables),
            "source_rows_by_table": source_tables,
            "row_group_rows_override": row_group_rows_override,
        }
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    return tasks


# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
class _WriterObserver:
    """Thread-safe COPY concurrency and per-task-duration observer."""

    def __init__(self, monotonic_origin=None):
        self._lock = threading.Lock()
        self._monotonic_origin = monotonic_origin if monotonic_origin is not None else time.monotonic()
        self._concurrent = 0
        self._maximum_concurrent = 0
        self._copy_seconds = 0.0
        self._tasks = []

    def enter(self, task, started):
        with self._lock:
            self._concurrent += 1
            self._maximum_concurrent = max(self._maximum_concurrent, self._concurrent)

    def exit(self, task, started, elapsed):
        self.finish_copy(elapsed)
        self.record_task(task, started, elapsed)

    def finish_copy(self, elapsed):
        with self._lock:
            self._copy_seconds += elapsed
            self._concurrent -= 1

    def record_task(self, task, started, elapsed, details=None):
        record = {
            "table": task.table_name,
            "file": Path(task.file_path).name,
            "start_offset_seconds": started - self._monotonic_origin,
            "duration_seconds": elapsed,
        }
        if details:
            record.update(details)
        with self._lock:
            self._tasks.append(record)

    def current_concurrency(self):
        with self._lock:
            return self._concurrent

    def snapshot(self):
        with self._lock:
            return {
                "actual_max_concurrent_copy": self._maximum_concurrent,
                "copy_duration_sum_seconds": self._copy_seconds,
                "copy_tasks": sorted(self._tasks, key=lambda task: task["start_offset_seconds"]),
            }
# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)


def _write_part(
    task,
    root_conn,
    start_gate=None,
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    observer=None,
    submitted_at=None,
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
):
    """Write one Parquet file through its own cursor.

    COPY writes a .partial file that no dataset reader matches, so a failed or
    interrupted export never leaves a truncated Parquet file behind.
    """
    partial_path = f"{task.file_path}.partial"
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    if observer is not None:
        worker_started = time.monotonic()
        submitted_at = submitted_at if submitted_at is not None else worker_started
        gate_wait_started = time.monotonic()
        if start_gate is not None:
            start_gate.wait()
        gate_released = time.monotonic()
        cursor_open_started = time.monotonic()
        cursor_opened = None
        copy_started = None
        copy_completed = None
        cursor_close_started = None
        cursor_closed = None
        publish_started = None
        publish_completed = None
        conn = None
        error_stage = "cursor_open"
        try:
            conn = root_conn.cursor()
            cursor_opened = time.monotonic()
            error_stage = "copy"
            copy_started = time.monotonic()
            observer.enter(task, copy_started)
            try:
                copy_to_parquet(task.query, partial_path, task.rows_per_row_group, conn)
            finally:
                copy_completed = time.monotonic()
                observer.finish_copy(copy_completed - copy_started)
            error_stage = "cursor_close"
            cursor_close_started = time.monotonic()
            conn.close()
            conn = None
            cursor_closed = time.monotonic()
            error_stage = "publish"
            publish_started = time.monotonic()
            os.replace(partial_path, task.file_path)
            publish_completed = time.monotonic()
            error_stage = None
        finally:
            if conn is not None:
                cursor_close_started = cursor_close_started or time.monotonic()
                try:
                    conn.close()
                finally:
                    cursor_closed = time.monotonic()
            worker_completed = time.monotonic()
            observed_copy_start = copy_started if copy_started is not None else worker_started
            observed_copy_seconds = (
                copy_completed - copy_started
                if copy_started is not None and copy_completed is not None
                else 0.0
            )

            def offset(value):
                return value - observer._monotonic_origin if value is not None else None

            def duration(start, end):
                return end - start if start is not None and end is not None else None

            observer.record_task(
                task,
                observed_copy_start,
                observed_copy_seconds,
                {
                    "thread_native_id": threading.get_native_id(),
                    "submitted_offset_seconds": offset(submitted_at),
                    "worker_start_offset_seconds": offset(worker_started),
                    "gate_release_offset_seconds": offset(gate_released),
                    "cursor_open_start_offset_seconds": offset(cursor_open_started),
                    "cursor_open_end_offset_seconds": offset(cursor_opened),
                    "copy_end_offset_seconds": offset(copy_completed),
                    "cursor_close_start_offset_seconds": offset(cursor_close_started),
                    "cursor_close_end_offset_seconds": offset(cursor_closed),
                    "publish_start_offset_seconds": offset(publish_started),
                    "publish_end_offset_seconds": offset(publish_completed),
                    "worker_end_offset_seconds": offset(worker_completed),
                    "queue_wait_seconds": worker_started - submitted_at,
                    "gate_wait_seconds": gate_released - gate_wait_started,
                    "cursor_open_seconds": duration(cursor_open_started, cursor_opened),
                    "pre_copy_seconds": duration(worker_started, copy_started),
                    "cursor_close_seconds": duration(cursor_close_started, cursor_closed),
                    "publish_seconds": duration(publish_started, publish_completed),
                    "worker_total_seconds": worker_completed - worker_started,
                    "error_stage": error_stage,
                },
            )
        return
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)

    if start_gate is not None:
        start_gate.wait()
    conn = root_conn.cursor()
    try:
        copy_to_parquet(task.query, partial_path, task.rows_per_row_group, conn)
    finally:
        conn.close()
    os.replace(partial_path, task.file_path)


def get_tpchgen_codec_args(codec_defs, table_name):
    """Build tpchgen-cli flags from codec definitions for a specific table.

    Returns a list of CLI arguments to append to the tpchgen-cli command.
    """
    if codec_defs is None:
        return []

    table_config = None
    for table in codec_defs.get("tables", []):
        if table["name"] == table_name:
            table_config = table
            break

    if table_config is None:
        return []

    args = []

    table_compression = table_config.get("compression")
    if table_compression:
        args.append(f"--parquet-compression={table_compression.upper()}")

    columns = table_config.get("columns", [])
    if not columns:
        return args

    uncompressed_cols = [column["name"] for column in columns if column.get("compressed") is False]
    if uncompressed_cols:
        args.append(f"--uncompressed-column-overrides={','.join(uncompressed_cols)}")

    for column in columns:
        encoding = column.get("encoding")
        if encoding:
            if encoding.upper() == "RLE_DICTIONARY":
                raise ValueError(
                    "RLE_DICTIONARY cannot be used as a column encoding. To enable dictionary encoding, set "
                    "'dictionary' to true (or omit it) instead."
                )
            args.append(f"--column-encoding={column['name']}={encoding}")

    no_dict_cols = [column["name"] for column in columns if column.get("dictionary") is False]
    if no_dict_cols:
        args.append(f"--disable-dictionary-encoding={','.join(no_dict_cols)}")

    return args


def load_codec_definitions(path):
    """Load and validate a codec definitions JSON file.

    See codec_definition_template.json for the expected schema.
    """
    with open(path) as file:
        codec_defs = json.load(file)

    if "tables" not in codec_defs:
        raise ValueError(f"Codec definitions file must contain a 'tables' key: {path}")

    for table in codec_defs["tables"]:
        if "name" not in table:
            raise ValueError(f"Each table entry must have a 'name' key: {path}")
        for column in table.get("columns", []):
            if "name" not in column:
                raise ValueError(f"Each column entry must have a 'name' key (table '{table['name']}'): {path}")

    return codec_defs


def build_default_codec_defs():
    """Auto-generate codec definitions by introspecting TPC-H schema at a tiny SF.

    Best-performing configuration from TPC-H SF1000 benchmarks (10 iterations,
    22 queries, Presto GPU):
      - All integers:              DELTA_BINARY_PACKED, dictionary off
      - Unique strings (NDV>=99%): PLAIN, dictionary off
      - Everything else:           tpchgen-cli defaults (PLAIN + Snappy + dictionary on)

    Achieved ~11% improvement over baseline with ~15% smaller dataset.
    """
    with duckdb.connect() as conn:
        init_benchmark_tables("tpch", _SAMPLE_SF, conn)

        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        config = {"tables": []}

        for table in sorted(tables):
            total_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if total_rows == 0:
                raise RuntimeError(f"Table '{table}' has no rows at SF {_SAMPLE_SF}")

            columns_meta = conn.execute(f"DESCRIBE {table}").fetchall()
            col_entries = []

            for col_name, col_type, *_ in columns_meta:
                column_type = col_type.upper()

                if column_type in _INTEGER_TYPES:
                    col_entries.append(
                        {
                            "name": col_name,
                            "encoding": "DELTA_BINARY_PACKED",
                            "dictionary": False,
                        }
                    )
                elif column_type == "VARCHAR":
                    ndv = conn.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {table}").fetchone()[0]
                    if ndv / total_rows >= _HIGH_CARD_NDV_THRESHOLD:
                        col_entries.append(
                            {
                                "name": col_name,
                                "encoding": "PLAIN",
                                "dictionary": False,
                            }
                        )

            if col_entries:
                config["tables"].append({"name": table, "columns": col_entries})

    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark parquet data files for a given scale factor. "
        "Only the TPC-H and TPC-DS benchmarks are currently supported."
    )
    parser.add_argument(
        "-b",
        "--benchmark-type",
        type=str,
        required=True,
        choices=["tpch", "tpcds"],
        help="The type of benchmark to generate data for.",
    )
    parser.add_argument(
        "-d",
        "--data-dir-path",
        type=str,
        required=True,
        help="The path to the directory that will contain the benchmark data files. "
        "This directory will be created if it does not already exist.",
    )
    parser.add_argument(
        "-s", "--scale-factor", type=float, required=True, help="The scale factor of the generated dataset."
    )
    parser.add_argument(
        "-c",
        "--convert-decimals-to-floats",
        action="store_true",
        required=False,
        default=False,
        help="Convert all decimal columns to float column type.",
    )
    parser.add_argument(
        "--use-duckdb", action="store_true", required=False, default=False, help="Use duckdb instead of tpchgen"
    )
    parser.add_argument(
        "-j",
        "--num-threads",
        type=int,
        required=False,
        default=4,
        help="Number of concurrent data generation tasks",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", required=False, default=False, help="Extra verbose logging"
    )
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        required=False,
        default=100_000_000,
        help="Limit number of rows in each file (creates more partitions)",
    )
    parser.add_argument(
        "--approx-row-group-bytes",
        type=int,
        required=False,
        default=128 * 1024 * 1024,
        help="Approximate row group size in bytes. 128MB by default.",
    )
    parser.add_argument(
        "--codec-definitions",
        type=str,
        required=False,
        default=None,
        help="Path to a JSON file specifying per-table/per-column encoding, compression, and dictionary settings.",
    )
    # BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    parser.add_argument(
        "--timing-json",
        type=str,
        required=False,
        default=None,
        help="Write versioned generation timing and resource metrics to this JSON path.",
    )
    # END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
    args = parser.parse_args()
    generate_data_files(args)
