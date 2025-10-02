# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import prestodb


def analyze_tables(presto_cursor, schema_name, verbose=False):
    """Analyze all tables in the given schema to collect statistics.
    
    Args:
        presto_cursor: Presto database cursor
        schema_name: Name of the schema containing tables to analyze
        verbose: If True, print detailed progress information
    """
    try:
        if verbose:
            print(f"Discovering tables in schema '{schema_name}'...")
        tables = presto_cursor.execute(f"SHOW TABLES FROM hive.{schema_name}").fetchall()
        table_names = [table_name for table_name, in tables]
        
        if not table_names:
            print(f"Warning: No tables found in schema '{schema_name}'")
            return
        
        if verbose:
            print(f"Found {len(table_names)} table(s): {', '.join(table_names)}")
            print(f"\nStarting table analysis...")
        
        success_count = 0
        failure_count = 0
        
        for i, table_name in enumerate(table_names, 1):
            try:
                if verbose:
                    print(f"  [{i}/{len(table_names)}] Analyzing table '{table_name}'...", end=" ")
                presto_cursor.execute(f"ANALYZE hive.{schema_name}.{table_name}")
                if verbose:
                    print("OK")
                success_count += 1
            except Exception as e:
                if verbose:
                    print("FAILED")
                print(f"Warning: Failed to analyze table '{table_name}': {e}")
                failure_count += 1
        
        if verbose or failure_count > 0:
            print(f"\nAnalysis complete: {success_count} succeeded, {failure_count} failed")
        
    except Exception as e:
        print(f"Error: Could not list tables in schema '{schema_name}': {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze all tables in a Hive schema to collect statistics for query optimization.")
    parser.add_argument("--schema-name", type=str, required=True,
                        help="Name of the schema containing the tables to analyze.")
    parser.add_argument("--host", type=str, default="localhost",
                        help="Presto coordinator hostname (default: localhost)")
    parser.add_argument("--port", type=int, default=8080,
                        help="Presto coordinator port (default: 8080)")
    parser.add_argument("--user", type=str, default="test_user",
                        help="Presto user (default: test_user)")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Enable verbose output")
    args = parser.parse_args()

    conn = prestodb.dbapi.connect(host=args.host, port=args.port, user=args.user, catalog="hive")
    cursor = conn.cursor()
    
    try:
        analyze_tables(cursor, args.schema_name, verbose=args.verbose)
    finally:
        cursor.close()
        conn.close()
 