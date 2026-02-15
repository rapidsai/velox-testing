#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import decimal
import os
import re
import sys
import time
from array import array

import duckdb
import prestodb

import test_utils


TYPE_ID_INT64 = 4
TYPE_ID_DECIMAL64 = 26
TYPE_ID_DECIMAL128 = 27


DEFAULT_HOST = os.getenv("PRESTO_COORDINATOR_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("PRESTO_COORDINATOR_PORT", "8080"))
DEFAULT_USER = os.getenv("USER", "root")


def _parse_manifest(manifest_path):
    manifest = {}
    with open(manifest_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            manifest[key] = value
    return manifest


def _load_bytes(path):
    with open(path, "rb") as handle:
        return handle.read()


def _load_int64_column(path):
    data = _load_bytes(path)
    values = array("q")
    values.frombytes(data)
    if sys.byteorder != "little":
        values.byteswap()
    return values


def _load_int128_column(path, max_rows=None):
    data = _load_bytes(path)
    values = []
    count = len(data) // 16
    if max_rows is not None:
        count = min(count, max_rows)
    for idx in range(count):
        start = idx * 16
        chunk = data[start : start + 16]
        values.append(int.from_bytes(chunk, byteorder="little", signed=True))
    return values


def _load_null_mask(path):
    if not path:
        return None
    if not os.path.exists(path):
        return None
    data = _load_bytes(path)
    return data if data else None


def _is_valid(mask, idx):
    if mask is None:
        return True
    byte = mask[idx // 8]
    return (byte >> (idx % 8)) & 1


def _format_scaled(value, scale):
    if scale >= 0:
        return f"{value}*10^{scale}"
    scale_digits = -scale
    factor = 10**scale_digits
    sign = "-" if value < 0 else ""
    abs_val = abs(value)
    whole = abs_val // factor
    frac = abs_val % factor
    return f"{sign}{whole}.{frac:0{scale_digits}d}"


def _get_table_external_location(schema_name, table, presto_cursor):
    create_table_text = presto_cursor.execute(
        f"SHOW CREATE TABLE hive.{schema_name}.{table}"
    ).fetchall()
    test_pattern = (
        r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)'"
    )
    user_pattern = (
        r"external_location = 'file:/var/lib/presto/data/hive/data/user_data/(.*)'"
    )

    test_match = re.search(test_pattern, create_table_text[0][0])
    if test_match:
        external_dir = test_utils.get_abs_file_path(f"data/{test_match.group(1)}")
    else:
        user_match = re.search(user_pattern, create_table_text[0][0])
        if not user_match:
            raise RuntimeError(
                "Could not parse external_location from SHOW CREATE TABLE for "
                f"hive.{schema_name}.{table}: {create_table_text[0][0]}"
            )
        presto_data_dir = os.getenv("PRESTO_DATA_DIR")
        if not presto_data_dir:
            raise RuntimeError(
                "PRESTO_DATA_DIR is required for user_data external locations."
            )
        external_dir = f"{presto_data_dir}/{user_match.group(1)}"

    if not os.path.isdir(external_dir):
        raise RuntimeError(
            f"External location '{external_dir}' for hive.{schema_name}.{table} "
            "does not exist."
        )
    return external_dir


def _register_lineitem(lineitem_path):
    duckdb.sql("DROP TABLE IF EXISTS lineitem")
    duckdb.sql(f"CREATE TABLE lineitem AS SELECT * FROM '{lineitem_path}/*.parquet'")


def _compute_dense_sums(keys, values, key_mask, val_mask, max_dense_keys):
    min_key = min(keys)
    max_key = max(keys)
    if min_key < 0:
        raise RuntimeError(f"Negative key values not supported: min_key={min_key}")
    range_size = max_key - min_key + 1
    if range_size > max_dense_keys:
        raise RuntimeError(
            "Dense array would be too large: "
            f"range_size={range_size} max_dense_keys={max_dense_keys}"
        )
    sums = array("q", [0]) * range_size
    for idx, key in enumerate(keys):
        if not _is_valid(key_mask, idx):
            continue
        if not _is_valid(val_mask, idx):
            continue
        sums[key - min_key] += values[idx]
    return sums, min_key, max_key


def _accumulate_dense_sums(keys, values, key_mask, val_mask, sums, min_key, max_key):
    for idx, key in enumerate(keys):
        if not _is_valid(key_mask, idx):
            continue
        if not _is_valid(val_mask, idx):
            continue
        if key < min_key or key > max_key:
            raise RuntimeError(
                f"Key out of range for session aggregate: key={key} "
                f"range={min_key}-{max_key}"
            )
        sums[key - min_key] += values[idx]


def _compare_dump_to_duckdb(
    sums,
    min_key,
    max_key,
    scale,
    batch_size,
):
    if scale > 0:
        raise RuntimeError(f"Positive scale not supported: scale={scale}")
    scale_factor = 10 ** (-scale)
    query = (
        "SELECT l_partkey, "
        f"CAST(sum(l_quantity) * {scale_factor} AS DECIMAL(38,0)) "
        "FROM lineitem "
        f"WHERE l_partkey BETWEEN {min_key} AND {max_key} "
        "GROUP BY l_partkey ORDER BY l_partkey"
    )
    rel = duckdb.sql(query)
    mismatches = 0
    checked = 0
    first = None
    while True:
        rows = rel.fetchmany(batch_size)
        if not rows:
            break
        for key, sum_scaled in rows:
            expected = sums[key - min_key]
            actual = int(sum_scaled) if sum_scaled is not None else 0
            checked += 1
            if expected != actual:
                mismatches += 1
                if first is None:
                    first = (key, expected, actual)
    return mismatches, checked, first


def _resolve_dump_dir(session_dir, entry):
    candidates = []
    if os.path.isabs(entry):
        candidates.append(entry)
    else:
        candidates.append(os.path.join(session_dir, entry))
        candidates.append(os.path.abspath(entry))
    base = os.path.basename(entry)
    if base:
        candidates.append(os.path.join(session_dir, base))
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate
    return None, candidates


def _read_session_index(session_dir):
    index_path = os.path.join(session_dir, "hashagg_dump_index.txt")
    if not os.path.exists(index_path):
        raise RuntimeError(f"hashagg_dump_index.txt not found in {session_dir}")
    dump_dirs = []
    missing = []
    with open(index_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            resolved = _resolve_dump_dir(session_dir, line)
            if isinstance(resolved, tuple):
                missing.append((line, resolved[1]))
            else:
                dump_dirs.append(resolved)
    if not dump_dirs:
        raise RuntimeError(f"No dump paths found in {index_path}")
    if missing:
        details = "; ".join(
            f"entry={entry} tried={candidates}"
            for entry, candidates in missing[:5]
        )
        raise RuntimeError(
            "Some dump paths listed in hashagg_dump_index.txt do not exist after "
            f"copy. session_dir={session_dir} details={details}"
        )
    return dump_dirs


def _read_manifest_summary(dump_dir):
    manifest_path = os.path.join(dump_dir, "manifest.txt")
    if not os.path.exists(manifest_path):
        raise RuntimeError(f"manifest.txt not found in {dump_dir}")

    manifest = _parse_manifest(manifest_path)
    summary = {
        "step": manifest.get("step", ""),
        "key_type_id": int(manifest["key.0.type_id"]),
        "key_size": int(manifest["key.0.size"]),
        "key_scale": int(manifest.get("key.0.scale", "0")),
        "key_data_file": manifest["key.0.data_file"],
        "key_mask_file": manifest.get("key.0.null_mask_file", ""),
        "value_type_id": int(manifest["request.0.type_id"]),
        "value_scale": int(manifest["request.0.scale"]),
        "value_data_file": manifest["request.0.data_file"],
        "value_mask_file": manifest.get("request.0.null_mask_file", ""),
    }
    return summary


def _compare_single_dump(dump_dir, args):
    summary = _read_manifest_summary(dump_dir)
    key_type_id = summary["key_type_id"]
    key_size = summary["key_size"]
    key_scale = summary["key_scale"]
    key_data_file = summary["key_data_file"]
    key_mask_file = summary["key_mask_file"]
    value_type_id = summary["value_type_id"]
    value_scale = summary["value_scale"]
    value_data_file = summary["value_data_file"]
    value_mask_file = summary["value_mask_file"]

    if key_scale != 0:
        raise RuntimeError(
            f"Key scale is not zero (scale={key_scale}) in {dump_dir}"
        )

    key_path = os.path.join(dump_dir, key_data_file)
    val_path = os.path.join(dump_dir, value_data_file)

    start = time.time()
    if key_type_id not in (TYPE_ID_INT64, TYPE_ID_DECIMAL64, TYPE_ID_DECIMAL128):
        raise RuntimeError(f"Unsupported key type_id={key_type_id}")
    if value_type_id not in (TYPE_ID_DECIMAL64, TYPE_ID_DECIMAL128):
        raise RuntimeError(f"Unsupported value type_id={value_type_id}")

    if key_type_id == TYPE_ID_DECIMAL128:
        keys = _load_int128_column(key_path, max_rows=args.max_int128_rows)
    else:
        keys = _load_int64_column(key_path)

    if value_type_id == TYPE_ID_DECIMAL128:
        values = _load_int128_column(val_path, max_rows=args.max_int128_rows)
    else:
        values = _load_int64_column(val_path)

    if len(keys) != len(values):
        raise RuntimeError(
            f"Key/value size mismatch keys={len(keys)} values={len(values)}"
        )
    if len(keys) != key_size:
        raise RuntimeError(
            f"Key size mismatch manifest={key_size} actual={len(keys)}"
        )

    key_mask = _load_null_mask(
        os.path.join(dump_dir, key_mask_file) if key_mask_file else None
    )
    val_mask = _load_null_mask(
        os.path.join(dump_dir, value_mask_file) if value_mask_file else None
    )
    print(
        f"[compare_hashagg_dump] dump={dump_dir} loaded rows={len(keys)} "
        f"key_type_id={key_type_id} value_type_id={value_type_id} "
        f"scale={value_scale} seconds={time.time() - start:.2f}",
        flush=True,
    )

    if value_type_id == TYPE_ID_DECIMAL128 or key_type_id == TYPE_ID_DECIMAL128:
        raise RuntimeError(
            "DECIMAL128 comparison requires a specialized path; "
            "rerun with DECIMAL64 or add a DECIMAL128 compare mode."
        )

    sums, min_key, max_key = _compute_dense_sums(
        keys, values, key_mask, val_mask, args.max_dense_keys
    )
    print(
        f"[compare_hashagg_dump] dump={dump_dir} key_range={min_key}-{max_key} "
        f"scale={value_scale} seconds={time.time() - start:.2f}",
        flush=True,
    )

    mismatches, checked, first = _compare_dump_to_duckdb(
        sums,
        min_key,
        max_key,
        value_scale,
        args.batch_size,
    )

    if first is None:
        print(
            f"[compare_hashagg_dump] dump={dump_dir} "
            f"checked={checked} mismatches=0",
            flush=True,
        )
        return 0

    key, expected, actual = first
    diff = expected - actual
    print(
        "[compare_hashagg_dump] "
        f"dump={dump_dir} checked={checked} mismatches={mismatches} "
        f"first_key={key} expected_scaled={expected} actual_scaled={actual} "
        f"diff_scaled={diff} "
        f"expected={_format_scaled(expected, value_scale)} "
        f"actual={_format_scaled(actual, value_scale)}",
        flush=True,
    )
    return 1


def main():
    parser = argparse.ArgumentParser(
        description="Compare HashAgg dump input sums to DuckDB."
    )
    parser.add_argument("--dump-dir", help="Dump directory to inspect.")
    parser.add_argument(
        "--session",
        help="Session directory containing hashagg_dump_index.txt.",
    )
    parser.add_argument(
        "--steps",
        default="partial,single",
        help="Comma-separated aggregation steps to include for session runs.",
    )
    parser.add_argument(
        "--session-aggregate",
        action="store_true",
        default=False,
        help="Aggregate all session dumps before comparing to DuckDB.",
    )
    parser.add_argument(
        "--schema-name",
        help="Hive schema name to locate lineitem parquet (uses Presto).",
    )
    parser.add_argument(
        "--lineitem-path",
        help="Path to lineitem parquet directory (overrides --schema-name).",
    )
    parser.add_argument("--hostname", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="DuckDB fetch batch size.",
    )
    parser.add_argument(
        "--max-dense-keys",
        type=int,
        default=50000000,
        help="Max range size for dense sum array.",
    )
    parser.add_argument(
        "--max-int128-rows",
        type=int,
        default=None,
        help="Max rows to read for DECIMAL128 values (debug).",
    )
    args = parser.parse_args()

    if not args.dump_dir and not args.session:
        raise RuntimeError("Need --dump-dir or --session.")

    if args.session and args.dump_dir:
        raise RuntimeError("Use only one of --dump-dir or --session.")

    if args.lineitem_path:
        lineitem_path = args.lineitem_path
    else:
        if not args.schema_name:
            raise RuntimeError("Need --schema-name or --lineitem-path.")
        conn = prestodb.dbapi.connect(
            host=args.hostname,
            port=args.port,
            user=args.user,
            catalog="hive",
            schema=args.schema_name,
        )
        cursor = conn.cursor()
        lineitem_path = _get_table_external_location(
            args.schema_name, "lineitem", cursor
        )

    if args.session:
        dump_dirs = _read_session_index(args.session)
    else:
        dump_dirs = [args.dump_dir]

    allowed_steps = {step.strip() for step in args.steps.split(",") if step.strip()}
    filtered = []
    for dump_dir in dump_dirs:
        if not args.session:
            filtered.append(dump_dir)
            continue
        summary = _read_manifest_summary(dump_dir)
        step = summary.get("step", "")
        if allowed_steps and step and step not in allowed_steps:
            print(
                f"[compare_hashagg_dump] skip dump={dump_dir} step={step}",
                flush=True,
            )
            continue
        filtered.append(dump_dir)
    dump_dirs = filtered
    if not dump_dirs:
        raise RuntimeError("No dumps matched requested steps.")

    failures = 0
    _register_lineitem(lineitem_path)
    if args.session and args.session_aggregate:
        first_summary = _read_manifest_summary(dump_dirs[0])
        value_scale = first_summary["value_scale"]
        key_type_id = first_summary["key_type_id"]
        value_type_id = first_summary["value_type_id"]

        if value_type_id == TYPE_ID_DECIMAL128 or key_type_id == TYPE_ID_DECIMAL128:
            raise RuntimeError(
                "DECIMAL128 comparison requires a specialized path; "
                "rerun with DECIMAL64 or add a DECIMAL128 compare mode."
            )

        sums = None
        min_key = None
        max_key = None
        for dump_dir in dump_dirs:
            summary = _read_manifest_summary(dump_dir)
            if summary["value_scale"] != value_scale:
                raise RuntimeError(
                    "Scale mismatch across session dumps: "
                    f"{value_scale} vs {summary['value_scale']}"
                )
            if summary["key_type_id"] != key_type_id:
                raise RuntimeError(
                    "Key type mismatch across session dumps: "
                    f"{key_type_id} vs {summary['key_type_id']}"
                )
            if summary["value_type_id"] != value_type_id:
                raise RuntimeError(
                    "Value type mismatch across session dumps: "
                    f"{value_type_id} vs {summary['value_type_id']}"
                )

            key_path = os.path.join(dump_dir, summary["key_data_file"])
            val_path = os.path.join(dump_dir, summary["value_data_file"])

            if key_type_id == TYPE_ID_DECIMAL128:
                keys = _load_int128_column(key_path, max_rows=args.max_int128_rows)
            else:
                keys = _load_int64_column(key_path)

            if value_type_id == TYPE_ID_DECIMAL128:
                values = _load_int128_column(val_path, max_rows=args.max_int128_rows)
            else:
                values = _load_int64_column(val_path)

            if len(keys) != len(values):
                raise RuntimeError(
                    f"Key/value size mismatch keys={len(keys)} values={len(values)}"
                )
            if len(keys) != summary["key_size"]:
                raise RuntimeError(
                    "Key size mismatch manifest="
                    f"{summary['key_size']} actual={len(keys)}"
                )

            key_mask = _load_null_mask(
                os.path.join(dump_dir, summary["key_mask_file"])
                if summary["key_mask_file"]
                else None
            )
            val_mask = _load_null_mask(
                os.path.join(dump_dir, summary["value_mask_file"])
                if summary["value_mask_file"]
                else None
            )

            if sums is None:
                sums, min_key, max_key = _compute_dense_sums(
                    keys, values, key_mask, val_mask, args.max_dense_keys
                )
            else:
                _accumulate_dense_sums(
                    keys, values, key_mask, val_mask, sums, min_key, max_key
                )

        mismatches, checked, first = _compare_dump_to_duckdb(
            sums,
            min_key,
            max_key,
            value_scale,
            args.batch_size,
        )
        if first is None:
            print(
                "[compare_hashagg_dump] session_aggregate "
                f"checked={checked} mismatches=0 dumps={len(dump_dirs)}",
                flush=True,
            )
            return 0
        key, expected, actual = first
        diff = expected - actual
        print(
            "[compare_hashagg_dump] session_aggregate "
            f"checked={checked} mismatches={mismatches} dumps={len(dump_dirs)} "
            f"first_key={key} expected_scaled={expected} actual_scaled={actual} "
            f"diff_scaled={diff} "
            f"expected={_format_scaled(expected, value_scale)} "
            f"actual={_format_scaled(actual, value_scale)}",
            flush=True,
        )
        return 1

    for dump_dir in dump_dirs:
        failures += _compare_single_dump(dump_dir, args)

    if failures == 0:
        print(
            f"[compare_hashagg_dump] session_failures=0 dumps={len(dump_dirs)}",
            flush=True,
        )
        return 0
    print(
        f"[compare_hashagg_dump] session_failures={failures} dumps={len(dump_dirs)}",
        flush=True,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
