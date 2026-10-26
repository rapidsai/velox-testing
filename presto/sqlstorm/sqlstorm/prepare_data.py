# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Converts the SQLStorm StackOverflow CSV files to Parquet, one directory per table.

The CSVs have no header, use double quotes with doubled-quote escaping, and contain
embedded newlines in text columns. An unquoted empty field is NULL while a quoted
empty field ("") is an empty string. Row counts are checked against an independent
parse with Python's csv module.
"""

import argparse
import csv
import os
import sys
from concurrent.futures import ProcessPoolExecutor

import duckdb

from sqlstorm.schema import load_tables

DUCKDB_TYPES = {
    "SMALLINT": "SMALLINT",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "VARCHAR": "VARCHAR",
}


def count_csv_rows(path):
    csv.field_size_limit(sys.maxsize)
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f))


def convert_table(table, csv_dir, parquet_dir):
    csv_path = os.path.join(csv_dir, f"{table.name}.csv")
    table_dir = os.path.join(parquet_dir, table.name.lower())
    os.makedirs(table_dir, exist_ok=True)
    parquet_path = os.path.join(table_dir, f"{table.name.lower()}.parquet")

    columns = ", ".join(f"'{name}': '{DUCKDB_TYPES[presto_type]}'" for name, presto_type in table.columns)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM read_csv('{csv_path}', columns = {{{columns}}}, header = false,
                                   delim = ',', quote = '"', escape = '"', nullstr = '',
                                   allow_quoted_nulls = false, max_line_size = 100000000)
        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
    )
    parquet_rows = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    csv_rows = count_csv_rows(csv_path)
    return table.name, csv_rows, parquet_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sqlstorm-dir", required=True, help="Checkout of github.com/SQL-Storm/SQLStorm")
    parser.add_argument("--csv-dir", required=True, help="Directory with the extracted StackOverflow CSV files")
    parser.add_argument("--parquet-dir", required=True, help="Output directory, one subdirectory per table")
    args = parser.parse_args()

    tables = load_tables(args.sqlstorm_dir)
    mismatches = []
    with ProcessPoolExecutor(max_workers=len(tables)) as pool:
        futures = [pool.submit(convert_table, table, args.csv_dir, args.parquet_dir) for table in tables]
        for future in futures:
            name, csv_rows, parquet_rows = future.result()
            status = "ok" if csv_rows == parquet_rows else "MISMATCH"
            print(f"{name:20} csv={csv_rows:>10} parquet={parquet_rows:>10} {status}")
            if csv_rows != parquet_rows:
                mismatches.append(name)
    if mismatches:
        sys.exit(f"Row count mismatch for: {', '.join(mismatches)}")


if __name__ == "__main__":
    main()
