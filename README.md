# velox-testing
This repository contains infrastructure for Velox and Presto functional and benchmark testing. The scripts in this repository are intended to be usable by CI/CD systems, such as GitHub Actions, as well as usable for local development and testing.

The provided infrastructure is broken down into four categories:
- Velox Testing
- Velox Benchmarking
- Presto Testing
- Presto Benchmarking

Important details about each category is provided below.

## Velox Testing
A Docker-based build infrastructure has been added to facilitate building Velox with comprehensive configuration options including GPU support, various storage adapters, and CI-mirrored settings. This infrastructure builds Velox libraries and executables only. In order to build Velox using this infrastructure, the following directory structure is expected:

```
├─ base_directory/
  ├─ velox-testing
  ├─ velox
  ├─ presto (optional, not relevant to velox builds)
```

Specifically, the `velox-testing` and `velox` repositories must be checked out as sibling directories under the same parent directory. Once that is done, navigate (`cd`) into the `velox-testing/velox/scripts` directory and execute the build script `build_velox.sh`. After a successful build, the Velox libraries and executables are available in the container at `/opt/velox-build/release`.

## Velox Benchmarking
A Docker-based benchmarking infrastructure has been added to facilitate running Velox benchmarks with support for CPU/GPU execution engines and profiling capabilities. The infrastructure uses a dedicated `velox-benchmark` Docker service with pre-configured volume mounts that automatically sync benchmark data and results. The data follows Hive directory structure, making it compatible with Presto. Currently, only TPC-H is implemented, but the infrastructure is designed to be easily extended to support additional benchmarks in the future.

### Prerequisites
The benchmarking infrastructure requires the same directory structure as Velox Testing, plus benchmark data using Hive directory structure. For TPC-H, the required data layout is shown below.

```
├─ base_directory/
  ├─ velox-testing
  ├─ velox
  └─ velox-benchmark-data/
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

#### Data Layout Options (applies to ALL table directories)

Each table directory can use any of these patterns. Using `lineitem/` as an example:

```
lineitem/
├─ lineitem.parquet                    # Single file
# OR
├─ part-00000.parquet                  # Multiple files
├─ part-00001.parquet
└─ part-00002.parquet
# OR  
├─ year=1992/                          # Partitioned data
│   ├─ part-00000.parquet
│   └─ part-00001.parquet
├─ year=1993/
│   └─ part-00000.parquet
└─ year=1994/
    ├─ part-00000.parquet
    └─ part-00001.parquet
```

#### Supported Data Patterns
- Single files: `customer/customer.parquet`
- Multiple files: `lineitem/part-00000.parquet`, `lineitem/part-00001.parquet`, etc.
- Partitioned data: `orders/year=1992/part-00000.parquet`, `orders/year=1993/part-00000.parquet`, etc.
- Multi-partition: `customer/region=AMERICA/part-00000.parquet`, `customer/region=EUROPE/part-00001.parquet`, etc.
- Mixed partitioning: Some tables partitioned, others with single/multiple files

### Building for Benchmarks
Before running benchmarks, Velox must be built with benchmarking support enabled:

```bash
cd velox-testing/velox/scripts
./build_velox.sh --benchmarks true   # Enables benchmarks and nsys profiling (default)
./build_velox.sh --gpu --benchmarks true   # GPU support with benchmarks (default)
./build_velox.sh --cpu --benchmarks true   # CPU-only with benchmarks
```

For faster builds when benchmarks are not needed:
```bash
./build_velox.sh --benchmarks false  # Disables benchmarks and skips nsys installation
```

### Running Benchmarks
Navigate to the benchmarking scripts directory and execute the benchmark runner:

```bash
cd velox-testing/velox/scripts
./benchmark_velox.sh [OPTIONS]
```

#### Basic Examples:
```bash
# Run all TPC-H queries on both CPU and GPU (using defaults)
./benchmark_velox.sh

# Run TPC-H Q6 on CPU only
./benchmark_velox.sh --queries 6 --device-type cpu

# Run TPC-H Q1 and Q6 on both CPU and GPU
./benchmark_velox.sh --queries "1 6" --device-type "cpu gpu"

# Run TPC-H Q6 on GPU with profiling enabled
./benchmark_velox.sh --queries 6 --device-type gpu --profile true

# Custom output directory for results
./benchmark_velox.sh --queries 6 --device-type gpu --profile true -o ./my-results
```

### Results
The benchmark results are automatically available in the specified output directory and can be analyzed using standard tools like NVIDIA Nsight Systems for the profiling data. Note that NVIDIA Nsight Systems is pre-installed in the Velox container, so profiling data can be examined directly within the container.

## Presto Testing
A number of docker image build and container services infrastructure (using docker compose) have been added to facilitate and simplify the process of building and deploying presto native CPU and GPU workers for a given snapshot/branch of the [presto](https://github.com/prestodb/presto) and [velox](https://github.com/facebookincubator/velox) repositories. In order to build and deploy presto using this infrastructure, the following directory structure is expected for the involved repositories:
```
├─ base_directory/
  ├─ velox-testing
  ├─ presto
  ├─ velox
``` 
Specifically, the `velox-testing`, `presto`, and `velox` repositories have to be checked out as sibling directories under the same parent directory. Once that is done, navigate (`cd`) into the `velox-testing/presto/scripts` directory and execute the start up script for the needed presto deployment variant. The following scripts: `start_java_presto.sh`, `start_native_cpu_presto.sh`, and `start_native_gpu_presto.sh` can be used to build/deploy "Presto Java Coordinator + Presto Java Worker", "Presto Java Coordinator + Presto Native CPU Worker", and "Presto Java Coordinator + Presto Native GPU Worker" variants respectively. The presto server can then be accessed at http://localhost:8080.

### Running Integration Tests
The Presto integration tests are implemented using the [pytest](https://docs.pytest.org/en/stable/) framework. The integration tests can be executed directly by using the `pytest` command e.g. `pytest tpch_test.py` or more conveniently, by using the `run_integ_test.sh` script from within the `velox-testing/presto/scripts` directory (this script handles environment setup for test execution). Execute `./run_integ_test.sh --help` to get more details about script options. An instance of Presto must be deployed and running *before* running the integration tests. This can be done using one of the `start_*` scripts mentioned in the "Presto Testing" section.

#### Testing Different Scale Factors
The integration tests can be executed against tables with different scale factors by navigating (`cd`) into the `velox-testing/presto/testing/integration_tests/scripts` directory and executing the `generate_test_files.sh` script with a `--scale-factor` or `-s` argument. After this, the tests can then be executed using the steps described in the "Running Integration Tests" section.

Note that `velox-testing/presto/testing/integration_tests` and `velox-testing/benchmark_data_tools` are separate projects that are expected to be operated with their own virtual environment.

## Presto Benchmarking
TODO: Add details when related infrastructure is added.
