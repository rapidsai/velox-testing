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

### Available Scripts

All scripts in the `presto/scripts/` directory support `--help` for detailed usage information. Here are the main scripts:

#### 1. TPC-H Benchmark Script (`tpch_benchmark.sh`)
A comprehensive TPC-H benchmark script for automated benchmarking:

**Features:**
- **Smart Data Generation**: Auto-generates TPC-H Parquet datasets using DuckDB, with intelligent caching to avoid regenerating existing data
- **Table Registration**: Automatically registers external tables in Presto
- **Query Execution**: Runs all 22 TPC-H queries with timing and statistics
- **Flexible Configuration**: Supports different scale factors and query subsets
- **Results Export**: Outputs benchmark results in JSON format

**Help Output:**
```bash
Usage: ./tpch_benchmark.sh <command> [options]

Commands:
  generate              Generate TPC-H Parquet data
  register              Register TPC-H tables in Presto
  benchmark             Run TPC-H benchmark queries
  full                  Complete workflow (generate + register + benchmark)
  clean                 Clean up TPC-H tables

Options:
  -s, --scale-factor N  TPC-H scale factor (default: 1)
  -t, --timeout N       Query timeout in seconds (default: 30)
  -o, --output FILE     Output file for results (default: tpch_benchmark_results.json)
  -q, --queries LIST    Comma-separated list of specific queries to run (e.g., "1,3,5")
  -f, --force          Force regeneration of TPC-H data (skip existence check)
  -h, --help           Show this help message

Environment Variables:
  TPCH_PARQUET_DIR      Directory for TPC-H Parquet files (default: ./docker/data/tpch)
  COORD                 Presto coordinator URL (default: localhost:8080)
  CATALOG               Presto catalog (default: hive)
  SCHEMA                Presto schema (default: tpch_parquet)

Examples:
  ./tpch_benchmark.sh generate -s 1                    # Generate SF1 data
  ./tpch_benchmark.sh generate -s 10 --force           # Force regenerate SF10 data
  ./tpch_benchmark.sh register                         # Register tables
  ./tpch_benchmark.sh benchmark                        # Run benchmark
  ./tpch_benchmark.sh full -s 1                        # Complete workflow with SF1
  ./tpch_benchmark.sh full -s 100 --force              # Complete workflow with SF100, force regenerate
  ./tpch_benchmark.sh clean                            # Clean up tables
```

#### 2. Presto Memory Manager (`presto_memory_manager.sh`)
Consolidated script for dynamic memory configuration and management:

**Help Output:**
```bash
Usage: ./presto_memory_manager.sh [COMMAND] [OPTIONS]

Commands:
  configure [OPTIONS]     Configure memory settings dynamically
  restart [TYPE]          Restart Presto containers (java|native-cpu|native-gpu)
  benchmark [SF] [TIMEOUT] [QUERIES]  Run TPC-H benchmark
  validate               Validate current configuration
  status                 Show current system and configuration status
  summary                Show TPC-H memory issues summary
  cleanup                Remove backup files

Options for configure:
  -m, --memory GB          Manual memory override (in GB)
  -p, --percent PERCENT    Memory usage percentage (default: 85)
  -f, --force              Skip confirmation prompt
  -c, --cleanup            Remove backup files after update

Environment Variables:
  MEMORY_GB                Manual memory override (in GB)
  USAGE_PERCENT            Memory usage percentage (default: 85)
  FORCE                    Skip confirmation prompt
  CLEANUP                  Remove backup files after update

Examples:
  ./presto_memory_manager.sh configure                    # Auto-configure based on system memory
  ./presto_memory_manager.sh configure -m 64              # Use 64GB total memory
  ./presto_memory_manager.sh configure -p 90              # Use 90% of available memory
  ./presto_memory_manager.sh restart java                 # Restart Java Presto
  ./presto_memory_manager.sh restart native-gpu           # Restart GPU Presto
  ./presto_memory_manager.sh benchmark 10 60              # Run SF10 benchmark with 60s timeout
  ./presto_memory_manager.sh benchmark 10 60 '9,18,21'    # Run specific queries
  ./presto_memory_manager.sh validate                     # Validate configuration
  ./presto_memory_manager.sh status                       # Show current status
  ./presto_memory_manager.sh summary                      # Show TPC-H issues summary
```

#### 3. Presto Startup Scripts

**Java Presto (`start_java_presto.sh`):**
```bash
Usage: ./start_java_presto.sh [OPTIONS] [SCALE_FACTOR]

Options:
  -s, --scale-factor SF    TPC-H scale factor (1, 10, 100)
  --all-sf, --all-scale-factors  Load all scale factors (1, 10, 100) simultaneously
  -h, --help              Show this help message

Scale Factor Shortcuts:
  sf1                     Use scale factor 1 (default)
  sf10                    Use scale factor 10
  sf100                   Use scale factor 100

Environment Variables:
  RUN_TPCH_BENCHMARK=true Run TPC-H benchmark after startup
  TPCH_PARQUET_DIR=path   Use existing TPC-H data directory

Examples:
  ./start_java_presto.sh                      # Start with SF1 (default), no benchmark
  ./start_java_presto.sh sf10                 # Start with SF10 and run benchmark
  ./start_java_presto.sh -s 100               # Start with SF100 and run benchmark
  ./start_java_presto.sh --all-sf             # Load all scale factors and run benchmark
```

**Native CPU Presto (`start_native_cpu_presto.sh`):**
```bash
Usage: ./start_native_cpu_presto.sh [OPTIONS] [SCALE_FACTOR]

Options:
  -s, --scale-factor SF    TPC-H scale factor (1, 10, 100)
  --all-sf, --all-scale-factors  Load all scale factors (1, 10, 100) simultaneously
  -h, --help              Show this help message

Scale Factor Shortcuts:
  sf1                     Use scale factor 1 (default)
  sf10                    Use scale factor 10
  sf100                   Use scale factor 100

Environment Variables:
  RUN_TPCH_BENCHMARK=true Run TPC-H benchmark after startup
  TPCH_PARQUET_DIR=path   Use existing TPC-H data directory

Examples:
  ./start_native_cpu_presto.sh                      # Start with SF1 (default), no benchmark
  ./start_native_cpu_presto.sh sf10                 # Start with SF10 and run benchmark
  ./start_native_cpu_presto.sh -s 100               # Start with SF100 and run benchmark
  ./start_native_cpu_presto.sh --all-sf             # Load all scale factors and run benchmark
```

**Native GPU Presto (`start_native_gpu_presto.sh`):**
```bash
Usage: ./start_native_gpu_presto.sh [OPTIONS] [SCALE_FACTOR]

Options:
  -s, --scale-factor SF    TPC-H scale factor (1, 10, 100)
  --all-sf, --all-scale-factors  Load all scale factors (1, 10, 100) simultaneously
  -h, --help              Show this help message

Scale Factor Shortcuts:
  sf1                     Use scale factor 1 (default)
  sf10                    Use scale factor 10
  sf100                   Use scale factor 100

Environment Variables:
  GPU=ON/OFF              Force GPU or CPU mode
  RUN_TPCH_BENCHMARK=true Run TPC-H benchmark after startup
  TPCH_PARQUET_DIR=path   Use existing TPC-H data directory

Examples:
  ./start_native_gpu_presto.sh                      # Start with SF1 (default), no benchmark
  ./start_native_gpu_presto.sh sf10                 # Start with SF10 and run benchmark
  ./start_native_gpu_presto.sh -s 100               # Start with SF100 and run benchmark
  ./start_native_gpu_presto.sh --all-sf             # Load all scale factors and run benchmark
```

#### 4. Utility Scripts

**Stop Presto (`stop_presto.sh`):**
```bash
# Stops all running Presto containers
./stop_presto.sh
```

**Sanity Test (`all_variants_sanity_test.sh`):**
```bash
# Runs sanity tests on all Presto variants (Java, Native CPU, Native GPU)
./all_variants_sanity_test.sh
```

**Build Dependencies (`build_centos_deps_image.sh`):**
```bash
# Builds CentOS dependencies image for native Presto
./build_centos_deps_image.sh
```

### Quick Reference

| Script | Purpose | Key Options |
|--------|---------|-------------|
| `tpch_benchmark.sh` | TPC-H benchmarking | `-s <scale>`, `-q <queries>`, `-t <timeout>` |
| `presto_memory_manager.sh` | Memory management | `-m <memory>`, `-p <percent>`, `restart <type>` |
| `start_java_presto.sh` | Start Java Presto | `-s <scale>`, `--all-sf` |
| `start_native_cpu_presto.sh` | Start Native CPU Presto | `-s <scale>`, `--all-sf` |
| `start_native_gpu_presto.sh` | Start Native GPU Presto | `-s <scale>`, `--all-sf`, `GPU=ON` |
| `stop_presto.sh` | Stop all Presto containers | None |
| `all_variants_sanity_test.sh` | Test all variants | None |
| `build_centos_deps_image.sh` | Build dependencies | None |

### Recent Fixes
- **Fixed external table location paths**: Updated table creation to use correct directory structure (`file:/data/tpch/sf${TPCH_SF}/${table}`) to resolve "External location must be a directory" errors
- **Improved error handling**: Enhanced retry logic and error reporting for table registration
