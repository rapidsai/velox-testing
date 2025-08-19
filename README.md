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
A Docker-based benchmarking infrastructure has been added to facilitate running Velox benchmarks with support for multiple benchmark types, CPU/GPU execution engines, profiling capabilities, and automated result collection. The infrastructure is designed to be modular and extensible, currently supporting TPC-H benchmarks with plans for additional benchmark types.

### Prerequisites
The benchmarking infrastructure requires the same directory structure as Velox Testing, plus benchmark data. For TPC-H, the required data layout and files are shown below.

```
├─ base_directory/
  ├─ velox-testing
  ├─ velox
  ├─ velox-benchmark-data/
    ├─ tpch/
      ├─ customer.parquet
      ├─ lineitem.parquet
      ├─ nation.parquet
      ├─ orders.parquet
      ├─ part.parquet
      ├─ partsupp.parquet
      ├─ region.parquet
      ├─ supplier.parquet
      ├─ customer          # metadata files (can be auto-updated or generated with --fix-metadata)
      ├─ lineitem
      ├─ nation
      ├─ orders
      ├─ part
      ├─ partsupp
      ├─ region
      └─ supplier
```

### Building for Benchmarks
Before running benchmarks, Velox must be built with benchmarking support enabled:

```bash
cd velox-testing/velox/scripts
./build_velox.sh --benchmarks  # Enables benchmarks and nsys profiling (default)
./build_velox.sh --gpu --benchmarks  # GPU support with benchmarks (default)
./build_velox.sh --cpu --benchmarks  # CPU-only with benchmarks
```

For faster builds when benchmarks are not needed:
```bash
./build_velox.sh --no-benchmarks  # Disables benchmarks and skips nsys installation
```

### Running Benchmarks
Navigate to the benchmarking scripts directory and execute the benchmark runner:

```bash
cd velox-testing/velox/scripts
./benchmark_velox.sh [BENCHMARK_TYPE] [QUERIES] [DEVICES] [PROFILE] [OPTIONS]
```

#### Basic Examples:
```bash
# Run all TPC-H queries on both CPU and GPU
./benchmark_velox.sh

# Run TPC-H Q6 on CPU only
./benchmark_velox.sh tpch 6 cpu

# Run TPC-H Q1 and Q6 on both CPU and GPU
./benchmark_velox.sh tpch "1 6" "cpu gpu"

# Run TPC-H Q6 on GPU with profiling enabled
./benchmark_velox.sh tpch 6 gpu true

# Auto-fix metadata files and run benchmarks
./benchmark_velox.sh tpch 6 gpu false --fix-metadata

# Custom output directory for results
./benchmark_velox.sh tpch 6 gpu true --benchmark-results-output ./my-results
```
The benchmark results are available in the specified output directory and can be analyzed using standard tools like NVIDIA Nsight Systems for the profiling data. Note that NVIDIA Nsight Systems is pre-installed in the Velox container, so profiling data can be examined directly within the container.

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
TODO: Add details when related infrastructure is added.
