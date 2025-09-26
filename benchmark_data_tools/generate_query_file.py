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
import duckdb
import json
import re

from pathlib import Path


def generate_queries_json(benchmark_type, queries_dir_path):
    assert benchmark_type in ["tpch", "tpcds"]

    duckdb.sql(f"INSTALL {benchmark_type}; LOAD {benchmark_type};")
    queries = duckdb.sql(f"FROM {benchmark_type}_queries()").fetchall()
    Path(queries_dir_path).mkdir(parents=True, exist_ok=True)

    result = {}
    for query_id, query in queries:
        # Update each query text to be on a single line and remove trailing commas.
        result[f"Q{query_id}"] = re.sub("\\n *", " ", query).strip(" ;")

        # The fraction portion of Q11 is a value that depends on scale factor.
        # Replace it with a placeholder that will be replaced when the query is run.
        if query_id == 11:
            result[f"Q{query_id}"] = re.sub("0.0001000000", "{SF_FRACTION}", result[f"Q{query_id}"])

    with open(f"{queries_dir_path}/queries.json", "w") as file:
        json.dump(result, file, indent=2)
        file.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark queries and store in a JSON file. Only the TPC-H and TPC-DS "
                    "benchmarks are currently supported.")
    parser.add_argument("--benchmark-type", type=str, required=True, choices=["tpch", "tpcds"],
                        help="The type of benchmark to generate queries for.")
    parser.add_argument("--queries-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the queries JSON file. "
                             "This directory will be created if it does not already exist.")
    args = parser.parse_args()

    generate_queries_json(args.benchmark_type, args.queries_dir_path)
