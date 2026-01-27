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

import prestodb
import pytest

from pathlib import Path
from .benchmark_keys import BenchmarkKeys
from .profiler_utils import start_profiler, stop_profiler
from ..common.fixtures import tpch_queries, tpcds_queries


@pytest.fixture(scope="module")
def presto_cursor(request):
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive",
                                  schema=schema)
    return conn.cursor()


@pytest.fixture(scope="session")
def benchmark_result_collector(request):
    benchmark_results = {}
    yield benchmark_results

    request.session.benchmark_results = benchmark_results


@pytest.fixture(scope="module")
def benchmark_queries(request, tpch_queries, tpcds_queries):
    if request.node.obj.BENCHMARK_TYPE == "tpch":
        return tpch_queries
    else:
        assert request.node.obj.BENCHMARK_TYPE == "tpcds"
        return tpcds_queries


@pytest.fixture(scope="module")
def benchmark_query(request, presto_cursor, benchmark_queries, benchmark_result_collector):
    iterations = request.config.getoption("--iterations")
    profile = request.config.getoption("--profile")
    profile_script_path = request.config.getoption("--profile-script-path")
    explain_analyze = request.config.getoption("--explain-analyze")
    explain_analyze_save_mode = request.config.getoption("--explain-analyze-save")
    explain = request.config.getoption("--explain")
    benchmark_type = request.node.obj.BENCHMARK_TYPE

    if profile:
        assert profile_script_path is not None
        bench_output_dir = request.config.getoption("--output-dir")
        profile_output_dir_path = Path(f"{bench_output_dir}/profiles/{benchmark_type}")
        profile_output_dir_path.mkdir(parents=True, exist_ok=True)

    benchmark_result_collector[benchmark_type] = {
        BenchmarkKeys.RAW_TIMES_KEY: {},
        BenchmarkKeys.FAILED_QUERIES_KEY: {},
    }

    benchmark_dict = benchmark_result_collector[benchmark_type]
    raw_times_dict = benchmark_dict[BenchmarkKeys.RAW_TIMES_KEY]
    assert raw_times_dict == {}

    failed_queries_dict = benchmark_dict[BenchmarkKeys.FAILED_QUERIES_KEY]
    assert failed_queries_dict == {}

    def _rows_to_lines(output_rows):
        # prestodb returns a list of rows; for EXPLAIN/EXPLAIN ANALYZE the plan is typically a
        # single VARCHAR column rendered across multiple rows (engine dependent). Serialize robustly.
        lines = []
        for row in output_rows or []:
            if row is None:
                continue
            if isinstance(row, (list, tuple)) and len(row) == 1:
                lines.append(str(row[0]))
            else:
                lines.append(str(row))
        return lines

    def _write_lines(output_path: Path, lines):
        with open(output_path, "w") as file:
            file.write("\n".join(lines))
            file.write("\n")

    def _write_explain_output(
        explain_kind: str,
        query_id: str,
        *,
        sql: str | None = None,
        iteration_index: int | None = None,
        output_rows=None,
        explain_sections=None,
    ):
        """Write explain output artifacts.

        - explain_kind == "explain_analyze": writes per-iteration rows under explain_analyze/<type>/<qid>/iter_N.txt
        - explain_kind == "explain": writes a single per-query file explain/<type>/<qid>_explain.coffee
        """
        # Keep output in the same benchmark output dir (and tag subdir) as other artifacts.
        # Import locally to avoid import-order surprises with pytest plugins.
        from .conftest import get_output_dir  # pylint: disable=import-outside-toplevel

        bench_output_dir = get_output_dir(request.config)

        if explain_kind == "explain_analyze":
            if iteration_index is None:
                raise ValueError("iteration_index is required for explain_analyze output")
            explain_dir = bench_output_dir / "explain_analyze" / str(benchmark_type) / str(query_id)
            explain_dir.mkdir(parents=True, exist_ok=True)

            if iterations > 1 and explain_analyze_save_mode == "all":
                file_name = f"iter_{iteration_index}.txt"
            elif iterations > 1:
                file_name = f"iter_{iterations}.txt"
            else:
                file_name = "iter_1.txt"

            output_path = explain_dir / file_name
            _write_lines(output_path, _rows_to_lines(output_rows))
            return

        if explain_kind == "explain":
            if sql is None or explain_sections is None:
                raise ValueError("sql and explain_sections are required for explain output")
            explain_dir = bench_output_dir / "explain" / str(benchmark_type)
            explain_dir.mkdir(parents=True, exist_ok=True)
            output_path = explain_dir / f"{query_id}_explain.coffee"

            lines = ["==== QUERY ====", sql.strip(), ""]
            for title, statement, rows, error in explain_sections:
                lines.append(f"==== {title} ====")
                if error is not None:
                    lines.append("ERROR:")
                    lines.append(str(error))
                    lines.append("")
                    continue
                lines.extend(_rows_to_lines(rows))
                lines.append("")

            _write_lines(output_path, lines)
            return

        raise ValueError(f"Unknown explain_kind: {explain_kind}")

    def benchmark_query_function(query_id):
        profile_output_file_path = None
        try:
            if profile:
                # Base path without .nsys-rep extension: {dir}/{query_id}
                profile_output_file_path = f"{profile_output_dir_path.absolute()}/{query_id}"
                start_profiler(profile_script_path, profile_output_file_path)
            sql = benchmark_queries[query_id]
            comment_prefix = "--" + str(benchmark_type) + "_" + str(query_id) + "--" + "\n"
            if explain_analyze and explain:
                raise ValueError("Only one of --explain and --explain-analyze may be set.")
            if explain_analyze:
                statement = comment_prefix + "EXPLAIN ANALYZE\n" + sql
            elif explain:
                # EXPLAIN-only mode is non-iterative (we still record one elapsed time).
                statements = [
                    ("EXPLAIN (TYPE LOGICAL)", comment_prefix + "EXPLAIN (TYPE LOGICAL)\n" + sql),
                    ("EXPLAIN (TYPE DISTRIBUTED)", comment_prefix + "EXPLAIN (TYPE DISTRIBUTED)\n" + sql),
                    ("EXPLAIN (TYPE IO)", comment_prefix + "EXPLAIN (TYPE IO)\n" + sql),
                ]
            else:
                statement = comment_prefix + sql

            timings = []
            if explain:
                explain_sections = []
                last_elapsed = None
                for title, stmt in statements:
                    rows = None
                    err = None
                    try:
                        presto_cursor.execute(stmt)
                        last_elapsed = presto_cursor.stats.get("elapsedTimeMillis")
                        rows = presto_cursor.fetchall()
                    except Exception as e:
                        err = e
                    explain_sections.append((title, stmt, rows, err))

                if last_elapsed is None:
                    last_elapsed = 0
                timings = [last_elapsed]
                _write_explain_output("explain", str(query_id), sql=sql, explain_sections=explain_sections)
            elif explain_analyze:
                last_explain_rows = None
                for i in range(1, iterations + 1):
                    presto_cursor.execute(statement)
                    timings.append(presto_cursor.stats["elapsedTimeMillis"])

                    rows = presto_cursor.fetchall()
                    if iterations > 1 and explain_analyze_save_mode == "all":
                        _write_explain_output(
                            "explain_analyze",
                            str(query_id),
                            iteration_index=i,
                            output_rows=rows,
                        )
                    else:
                        last_explain_rows = rows

                if iterations == 1 or explain_analyze_save_mode == "last":
                    _write_explain_output(
                        "explain_analyze",
                        str(query_id),
                        iteration_index=iterations,
                        output_rows=last_explain_rows,
                    )
            else:
                for _ in range(iterations):
                    presto_cursor.execute(statement)
                    timings.append(presto_cursor.stats["elapsedTimeMillis"])

            raw_times_dict[query_id] = timings
        except Exception as e:
            failed_queries_dict[query_id] = f"{e.error_type}: {e.error_name}"
            raw_times_dict[query_id] = None
            raise
        finally:
            if profile and profile_output_file_path is not None:
                stop_profiler(profile_script_path, profile_output_file_path)

    return benchmark_query_function
