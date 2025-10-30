# Presto GPU

Welcome to the Presto GPU testing and benchmarking infrastructure! This directory contains everything needed to build, deploy, and test Presto with GPU-accelerated native execution using NVIDIA cuDF and Velox.

## Overview

This infrastructure enables running Presto with GPU-accelerated query execution, leveraging the GPU-native Velox execution engine powered by NVIDIA cuDF. For more details about the technology behind GPU acceleration, see the [NVIDIA Developer Blog: Accelerating Large-Scale Data Analytics with GPU-Native Velox and NVIDIA cuDF](https://developer.nvidia.com/blog/accelerating-large-scale-data-analytics-with-gpu-native-velox-and-nvidia-cudf/).

For general information about the velox-testing repository, please see the [main README](https://github.com/rapidsai/velox-testing/blob/main/README.md).

## Current Testing Status

**TPC-H** is the primary benchmark suite currently used for testing Presto GPU functionality. The infrastructure supports comprehensive testing including:
- Functional correctness testing
- Performance benchmarking
- CPU vs GPU comparison
- Integration testing with Hive metastore

TPC-DS support is also available in the testing infrastructure and is under active development.

## Quick Start

### Prerequisites

Ensure you have the following directory structure:

```
├─ base_directory/
  ├─ velox-testing
  ├─ presto
  ├─ velox
```

All three repositories must be checked out as sibling directories.

### Building and Starting Presto GPU

1. Navigate to the scripts directory:
   ```bash
   cd velox-testing/presto/scripts
   ```

2. Set up your data directory (optional but recommended):
   ```bash
   export PRESTO_DATA_DIR=/path/to/your/benchmark/data
   ```
   
   > **Tip:** Add this export to your `~/.bashrc` to avoid setting it each time.

3. Build dependencies (first time only):
   ```bash
   ./build_centos_deps_image.sh
   # OR fetch a pre-built image (requires credentials)
   ./fetch_centos_deps_image.sh
   ```

4. Start Presto with GPU workers:
   ```bash
   ./start_native_gpu_presto.sh
   ```

5. Access Presto at http://localhost:8080

### Running Tests

Execute integration tests using the provided script:

```bash
cd velox-testing/presto/scripts
./run_integ_test.sh --help  # See all options
./run_integ_test.sh --test-suite tpch
```

Or run tests directly with pytest:

```bash
cd velox-testing/presto/testing/integration_tests
pytest tpch_test.py
```

### Running Benchmarks

1. Start a Presto instance with GPU workers (see above)

2. Set up benchmark tables (if needed):
   ```bash
   cd velox-testing/presto/scripts
   ./setup_benchmark_data_and_tables.sh --help  # See all options
   ```

3. **Important:** Run ANALYZE TABLES on CPU Presto first:
   ```bash
   ./analyze_tables.sh
   ```
   This step is required because aggregation for statistics collection is not yet supported on GPU. The statistics benefit GPU execution by improving performance and reducing OOM failures.

4. Run benchmarks:
   ```bash
   ./run_benchmark.sh --help  # See all options
   ./run_benchmark.sh --benchmark tpch
   ```

## Directory Structure

- **`docker/`** - Docker Compose configurations and Dockerfiles for different Presto variants
- **`pbench/`** - Performance benchmarking utilities and TPC-H query definitions
- **`scripts/`** - Shell scripts for building, deploying, and testing Presto
- **`testing/`** - Python-based test framework using pytest
  - `integration_tests/` - Functional correctness tests
  - `performance_benchmarks/` - Performance testing infrastructure
  - `common/` - Shared test utilities and query definitions

## Available Presto Variants

The infrastructure supports three deployment variants:

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

Use the provided scripts to generate data at various scale factors or set up tables on existing data.

## Testing Different Scale Factors

Generate test files for different scale factors:

```bash
cd velox-testing/presto/testing/integration_tests/scripts
./generate_test_files.sh --scale-factor 100
```

Then run tests normally using the integration test scripts.

## Configuration

Configuration files for Presto are managed through a template system located in:
- `docker/config/template/` - Configuration templates
- `docker/config/params.json` - Parameters for configuration generation

To modify Presto settings, edit the templates and regenerate configurations using:

```bash
cd velox-testing/presto/scripts
./generate_presto_config.sh
```

## Resources

- [Main Repository README](https://github.com/rapidsai/velox-testing/blob/main/README.md)
- [NVIDIA Blog: GPU-Native Velox and cuDF](https://developer.nvidia.com/blog/accelerating-large-scale-data-analytics-with-gpu-native-velox-and-nvidia-cudf/)
- [Presto Official Documentation](https://prestodb.io/)
- [Velox Documentation](https://facebookincubator.github.io/velox/)

## Contributing

Please see the [CONTRIBUTING.md](https://github.com/rapidsai/velox-testing/blob/main/CONTRIBUTING.md) guide in the main repository for information on how to contribute to this project.

## License

See [LICENSE](https://github.com/rapidsai/velox-testing/blob/main/LICENSE) in the main repository.

