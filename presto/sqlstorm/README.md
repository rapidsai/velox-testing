# SQLStorm sweep (proof of concept)

Runs the [SQLStorm](https://github.com/SQL-Storm/SQLStorm) v1.0 StackOverflow queries
(18,251 LLM-generated PostgreSQL queries) against Presto Java, Presto native CPU, and
Presto native GPU with and without CPU fallback, using the public `prestodb` images.
Nothing is built.

For each engine the sweep reports the share of queries that run to completion within
the per-query time limit. A query counts as converted if its translated SQL passes
`EXPLAIN` on Presto Java. All percentages use every considered query as the
denominator, so conversion losses count against every engine.

## Data

The 1GB scale of the SQLStorm StackOverflow benchmark is the
[dba.stackexchange.com](https://dba.stackexchange.com) dump, published by the SQLStorm
authors at `https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz`. The CSV files
have no header, use `,` as the delimiter and `"` for quoting (escaped by doubling), and
contain embedded newlines in text columns. An unquoted empty field is NULL; `""` is an
empty string. Booleans are `0`/`1`. The schema is `v1.0/stackoverflow/schema.sql` in the
SQLStorm repository.

```bash
cd velox-testing/presto/sqlstorm
./download_data.sh -d /data/sqlstorm -s /data/sqlstorm/SQLStorm
pip install -r requirements.txt
python -m sqlstorm.prepare_data --sqlstorm-dir /data/sqlstorm/SQLStorm \
    --csv-dir /data/sqlstorm/stackoverflow_dba --parquet-dir /data/sqlstorm/parquet/stackoverflow_dba
export PRESTO_DATA_DIR=/data/sqlstorm/parquet
```

`prepare_data` writes one Snappy Parquet directory per table with lowercase column names
and checks every table's row count against an independent CSV parse.

## Running

```bash
./run_sqlstorm_sweep.sh -s /data/sqlstorm/SQLStorm -o results -l 10
```

The script starts each engine with `--use-public-images`, registers the Hive tables,
runs `EXPLAIN` on Presto Java for every query, then runs the converted queries on each
engine and writes `results/summary.md`. Per-query results (status, Presto error name,
message, elapsed time) are in `results/run_<engine>.csv`. Rerunning with the same output
directory resumes where the previous run stopped. See `--help` for engine selection,
query ranges, concurrency, and the time limit.

GPU engines run one query at a time. If a query kills the GPU worker, it is recorded as
`crash` and only the worker container is restarted (`restart_worker.sh`).

## Conversion

`sqlstorm/convert.py` translates each query with sqlglot (`postgres` to `presto`) and
then applies rewrites for constructs sqlglot leaves in a form Presto rejects. Only
rewrites that preserve the query's semantics are applied; anything that would need an
approximation (for example `AGE()`, which returns a calendar interval) is reported as
not converted. Converted SQL is not saved.

## Image and host notes

- The public native GPU image defines an `ENTRYPOINT` that ignores the generated config.
  The GPU compose template clears it so the velox-testing launch scripts run.
- The public native images do not include `numactl`; the worker launch script skips NUMA
  pinning when it is missing.
- On kernels with a page size other than 4KB (for example 64KB aarch64 kernels), Velox's
  mmap allocator cannot start; config generation disables it automatically.
- On aarch64, Velox's stack trace capture segfaults when an exception is thrown through
  code built with SVE, which crashes the GPU worker. cuDF's AST support check throws for
  many expressions, and with CPU fallback disabled every unsupported operator throws, so
  images without the fix (including the public nightly images) crash on most queries.
  For those images, set `GPU_EXTRA_WORKER_CONFIG=cudf.ast_expression_enabled=false`.
  This avoids the crashes, but cuDF then cannot evaluate most filters and projections,
  so almost every query falls back to CPU, and with CPU fallback disabled almost every
  query fails. Images that do not crash can be built from the `fix-aarch64-sve-stacktrace`
  branch of GregoryKimball/velox with `ARM_BUILD_TARGET=generic`. Without SVE code,
  folly's exception tracer does not hit the same libunwind bug.
