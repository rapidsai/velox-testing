# Presto GPU

Welcome to the Presto GPU testing and benchmarking infrastructure! This directory contains everything needed to build, deploy, and test Presto with GPU-accelerated native execution using NVIDIA cuDF and Velox.

## Overview

This infrastructure enables running Presto with GPU-accelerated query execution, leveraging the GPU-native Velox execution engine powered by NVIDIA cuDF. For more details about the technology behind GPU acceleration, see the [NVIDIA Developer Blog: Accelerating Large-Scale Data Analytics with GPU-Native Velox and NVIDIA cuDF](https://developer.nvidia.com/blog/accelerating-large-scale-data-analytics-with-gpu-native-velox-and-nvidia-cudf/).

For general information about this repository, see the [main README](https://github.com/rapidsai/velox-testing/blob/main/README.md).

## Repository Structure and Version Compatibility

Ensure you have the following directory structure:

```
├─ base_directory/
  ├─ velox-testing           # https://github.com/rapidsai/velox-testing
  ├─ presto                  # https://github.com/prestodb/presto
  ├─ velox                   # https://github.com/facebookincubator/velox
```

All three repositories must be checked out as sibling directories. **Important:** Please use compatible commit hashes/branches across these repositories. There is no formal public matrix yet—refer to release notes, this README, and instructions in the dependent repositories for guidance.

## Current Testing Status

**TPC-H** is the primary benchmark suite currently used for testing Presto GPU functionality. The infrastructure supports comprehensive testing including:
- Functional correctness testing
- Performance benchmarking
- CPU vs GPU comparison
- Integration testing with Hive metastore

**TPC-DS** support is also available and under active development.

## Quick Start

### Prerequisites

- Sibling repo checkout (see structure above)
- Docker and docker-compose installed
- NVIDIA GPU, drivers, and compatible CUDA toolkit

### Building and Starting Presto GPU

1. Navigate to the scripts directory:
   ```bash
   cd velox-testing/presto/scripts
   ```

2. Set up your data directory (optional but recommended):
   ```bash
   export PRESTO_DATA_DIR=/path/to/your/benchmark/data
   ```
   If the data is on S3, simply define `PRESTO_DATA_DIR` as `s3://YOUR_BUCKET`. Data for a schema must be contained in a directory inside the bucket.
   > **Tip:** Add this export to your `~/.bashrc` to avoid setting it each time.

3. Build dependencies (first time only):
   ```bash
   ./build_centos_deps_image.sh
   ```
   > **Note:** Only internal team members with credentials can fetch a pre-built image (`./fetch_centos_deps_image.sh`). For most users, building locally is required.

4. Start Presto with GPU workers:
   ```bash
   ./start_native_gpu_presto.sh
   ```

5. Access the Presto web UI at http://localhost:8080

### Running Tests

Run integration test suite:
```bash
cd velox-testing/presto/scripts
./run_integ_test.sh --help              # See all options
./run_integ_test.sh --benchmark-type tpch
```

Or directly via pytest:
```bash
cd velox-testing/presto/testing/integration_tests
pytest tpch_test.py
```

### Running Benchmarks

1. Ensure the Presto GPU instance is running.

2. Set up benchmark tables if needed:
   ```bash
   cd velox-testing/presto/scripts
   ./setup_benchmark_data_and_tables.sh --help  # See all options
   ```

3. **Important:** Run ANALYZE TABLES on CPU Presto first:
   ```bash
   ./analyze_tables.sh
   ```
   > This step is necessary because aggregation for statistics collection is not yet supported on GPU. Collecting statistics improves performance and reduces OOM errors for GPU execution.

4. Run benchmarks:
   ```bash
   ./run_benchmark.sh --help
   ./run_benchmark.sh --benchmark tpch --schema-name <your_schema>
   ```
   > **Note:** The `--schema-name` flag is required to specify your target DB schema for the benchmark.

## Directory Structure

- **`docker/`** - Docker Compose configurations and Dockerfiles for different Presto variants
- **`pbench/`** - Performance benchmarking utilities and TPC-H query definitions
- **`scripts/`** - Shell scripts for building, deploying, and testing Presto
- **`testing/`** - Python-based test framework using pytest
  - `integration_tests/` - Functional correctness tests
  - `performance_benchmarks/` - Performance testing infrastructure
  - `common/` - Shared test utilities and query definitions

## Available Presto Variants

1. **Java Presto** - Pure Java coordinator and workers
   ```bash
   ./start_java_presto.sh
   ```
2. **Native CPU Presto** - Java coordinator with native CPU workers
   ```bash
   ./start_native_cpu_presto.sh
   ```
3. **Native GPU Presto** - Java coordinator with GPU-accelerated native workers
   ```bash
   ./start_native_gpu_presto.sh
   ```

## Benchmark Data

The infrastructure uses Hive-style directory layouts with Parquet files. For TPC-H, the expected structure is:

```
benchmark_data/
└─ tpch/
   ├─ customer/
   ├─ lineitem/
   ├─ nation/
   ├─ orders/
   ├─ part/
   ├─ partsupp/
   ├─ region/
   └─ supplier/
```

Generate new data or set up tables on existing data using the provided scripts.

## Testing Different Scale Factors

There are two common approaches:

1. **Generate new data for your preferred scale factor:**
   ```bash
   cd velox-testing/presto/testing/integration_tests/scripts
   ./generate_test_files.sh --scale-factor 100
   ```
   Then run the tests as shown above.

2. **Switch between pre-generated scale factors with schemas:**
   Register tables for any generated data at any scale factor as a new schema:
   ```bash
   cd velox-testing/presto/scripts
   ./setup_benchmark_data_and_tables.sh --scale-factor 100 --schema-name tpch_sf100
   ./run_integ_test.sh --benchmark-type tpch --schema-name tpch_sf100
   ```
   This allows you to run benchmarks and tests without regenerating data for each scale factor—simply specify the schema name.

## Configuration

Configuration files for Presto are managed through a template system in:
- `docker/config/template/` - Configuration templates
- `docker/config/params.json` - Parameters for template filling

Configuration is generated automatically when starting Presto or running tests. Manual execution of `generate_presto_config.sh` is rarely needed:
```bash
cd velox-testing/presto/scripts
./generate_presto_config.sh   # Only if making custom template changes
```

## Resources

- [Main Repository README](https://github.com/rapidsai/velox-testing/blob/main/README.md)
- [NVIDIA Blog: GPU-Native Velox and cuDF](https://developer.nvidia.com/blog/accelerating-large-scale-data-analytics-with-gpu-native-velox-and-nvidia-cudf/)
- [Presto Official Documentation](https://prestodb.io/)
- [Velox Documentation](https://facebookincubator.github.io/velox/)

## Contributing

See the [CONTRIBUTING.md](https://github.com/rapidsai/velox-testing/blob/main/CONTRIBUTING.md) in the main repository for contribution guidelines.

## License

See [LICENSE](https://github.com/rapidsai/velox-testing/blob/main/LICENSE) in the main repository.
