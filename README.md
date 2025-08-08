# velox-testing
This repository contains infrastructure for Velox and Presto functional and benchmark testing. The scripts in this repository are intended to be usable by CI/CD systems, such as GitHub Actions, as well as usable for local development and testing.
The provided infrastructure is broken down into four categories:
- Velox Testing
- Velox Benchmarking
- Presto Testing
- Presto Benchmarking
Important details about each category is provided below.
## Velox Testing
No test harness is published here yet. Planned: containerized utilities and scenarios built on `velox/` with simple one-command runners.
## Velox Benchmarking
No benchmark harness is published here yet. Planned: reproducible micro/TPCH-style benchmarks using `velox/velox/benchmarks` binaries.
## Presto Testing
A number of docker image build and container services infrastructure (using docker compose) have been added to facilitate and simplify the process of building and deploying presto native CPU and GPU workers for a given snapshot/branch of the [presto](https://github.com/prestodb/presto) and [velox](https://github.com/facebookincubator/velox) repositories. In order to build and deploy presto using this infrastructure, the following directory structure is expected for the involved repositories:
```
├─ base_directory/
  ├─ velox-testing
  ├─ presto
  ├─ velox
```
Specifically, the `velox-testing`, `presto`, and `velox` repositories have to be checked out as sibling directories under the same parent directory. Once that is done, navigate (`cd`) into the `velox-testing/presto/scripts` directory and execute the start up script for the needed presto deployment variant. The following scripts: `start_java_presto.sh`, `start_native_cpu_presto.sh`, and `start_native_gpu_presto.sh` can be used to build/deploy "Presto Java Coordinator + Presto Java Worker", "Presto Java Coordinator + Presto Native CPU Worker", and "Presto Java Coordinator + Presto Native GPU Worker" variants respectively. The presto server can then be accessed at http://localhost:8080.
## Presto Benchmarking
This repo can auto-generate a local TPC-H Parquet dataset and register it in Presto's Hive connector for quick benchmarking.
### Prerequisites
- Docker/Compose installed (and NVIDIA runtime for GPU variant)
- Directory layout:
```
├─ base_directory/
  ├─ velox-testing
  ├─ presto
  ├─ velox
```
### Environment (optional)
- `TPCH_SF`: TPC-H scale factor to generate (default `1`)
- `TPCH_PARQUET_DIR`: Host path to existing TPCH Parquet with subfolders `lineitem/`, `orders/`, ...
  - If unset, data is generated automatically into `velox-testing/presto/docker/data/tpch` using DuckDB
- `HIVE_METASTORE_DIR`: Host path for file-based metastore (default `velox-testing/presto/docker/data/hive-metastore`)
- `GPU`: Set to `ON` only if your machine has CUDA/cuDF support for native GPU worker builds; otherwise leave unset or `OFF`.
### Start a cluster (with auto TPCH data + registration)
From `velox-testing/presto/scripts`:
- Java coordinator + Java worker:
  ```bash
  ./start_java_presto.sh
  ```
- Java coordinator + Native CPU worker:
  ```bash
  # Auto-generates TPCH data if TPCH_PARQUET_DIR is unset
  ./start_native_cpu_presto.sh
  ```
- Java coordinator + Native GPU worker:
  ```bash
  # Enable only if your environment supports CUDA/cuDF
  GPU=ON ./start_native_gpu_presto.sh
  ```
On first run, if `TPCH_PARQUET_DIR` is not set, the scripts will:
1) Generate Parquet using a DuckDB container into `velox-testing/presto/docker/data/tpch`
2) Mount that directory into the Presto services
3) Create `hive.tpch_parquet` external tables over the Parquet files
Presto UI: http://localhost:8080
### Validate tables
You can validate via the REST API, e.g. count rows in `lineitem`:
```bash
curl -sS -X POST \
  -H 'X-Presto-Catalog: hive' \
  -H 'X-Presto-Schema: tpch_parquet' \
  -H 'X-Presto-User: tester' \
  --data 'select count(*) from lineitem' \
  http://localhost:8080/v1/statement | jq
```
### Run TPC-H style queries
Point queries at `hive.tpch_parquet` schema. Example (Q1-like):
```sql
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from hive.tpch_parquet.lineitem
where l_shipdate <= date '1998-09-02'
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus;
```
Use the REST API or any Presto/Trino client to submit queries.
### TPC-H Benchmark Script
A comprehensive TPC-H benchmark script (`tpch_benchmark.sh`) is provided for automated benchmarking:
#### Features
- **Data Generation**: Auto-generates TPC-H Parquet datasets using DuckDB
- **Table Registration**: Automatically registers external tables in Presto
- **Query Execution**: Runs all 22 TPC-H queries with timing and statistics
- **Flexible Configuration**: Supports different scale factors and query subsets
- **Results Export**: Outputs benchmark results in JSON format
#### Usage Examples
```bash
# Complete workflow: generate data, register tables, run all queries
./tpch_benchmark.sh full -s 1
# Generate data only
./tpch_benchmark.sh generate -s 1
# Register tables only (requires existing data)
./tpch_benchmark.sh register -s 1
# Run benchmark only (requires registered tables)
./tpch_benchmark.sh benchmark
# Run specific queries
./tpch_benchmark.sh benchmark -q "1,3,5"
# Clean up tables
./tpch_benchmark.sh clean
```
#### Configuration Options
- `-s, --scale-factor`: TPC-H scale factor (default: 1)
- `-t, --timeout`: Query timeout in seconds (default: 30)
- `-q, --queries`: Comma-separated list of specific queries to run
- `-o, --output`: Output file for results (default: tpch_benchmark_results.json)
#### Environment Variables
- `TPCH_SF`: Default scale factor
- `TPCH_PARQUET_DIR`: Directory for TPC-H Parquet files
- `COORD`: Presto coordinator URL (default: localhost:8080)
- `CATALOG`: Presto catalog (default: hive)
- `SCHEMA`: Presto schema (default: tpch_parquet)
#### Recent Fixes
- **Fixed external table location paths**: Updated table creation to use correct directory structure (`file:/data/tpch/sf${TPCH_SF}/${table}`) to resolve "External location must be a directory" errors
- **Improved error handling**: Enhanced retry logic and error reporting for table registration
