# velox-testing
This repository contains infrastructure for Velox, Presto, and Spark Gluten functional and benchmark testing. The scripts in this repository are intended to be usable by CI/CD systems, such as GitHub Actions, as well as usable for local development and testing.

The provided infrastructure is broken down into six categories:
- Velox Testing
- Velox Benchmarking
- Presto Testing
- Presto Benchmarking
- Spark Gluten Testing
- Spark Gluten Benchmarking

Important details about each category is provided below.

## CI/CD Workflows
This repository includes comprehensive GitHub Actions workflows for automated testing and benchmarking. The workflows support nightly testing against upstream and staging branches, benchmark sanity checks, and automated staging branch management.

For detailed information about available workflows, their inputs, and how to use them, see the [Workflows Documentation](.github/workflows/README.md).

## Velox Testing
A Docker-based build infrastructure has been added to facilitate building Velox with comprehensive configuration options including GPU support, various storage adapters, and CI-mirrored settings. This infrastructure builds Velox libraries and executables only. In order to build Velox using this infrastructure, the following directory structure is expected:

```
├─ base_directory/
  ├─ velox-testing
  ├─ velox
  ├─ presto (optional, not relevant to velox builds)
```

Specifically, the `velox-testing` and `velox` repositories must be checked out as sibling directories under the same parent directory. Once that is done, navigate (`cd`) into the `velox-testing/velox/scripts` directory and execute the build script `build_velox.sh`. After a successful build, the Velox libraries and executables are available in the container at `/opt/velox-build/release`.

## `sccache` Usage
`sccache` has been integrated to significantly accelerate both Velox and Presto native builds using remote S3 caching and optional distributed compilation. On cache hits, pre-compiled object files are downloaded from S3 instead of recompiling, significantly speeding up builds across machines and repeat runs.

The fork `rapidsai/sccache` is integrated and configured for use with the `rapidsai` GitHub organization. The sccache scripts are located in `scripts/sccache/` and shared by both Velox and Presto build pipelines.

### Setup
Set up authentication credentials (required once, valid for 12 hours):
```bash
cd scripts/sccache
./setup_sccache_auth.sh
```

This creates `~/.sccache-auth/` containing a GitHub token and AWS credentials for S3 bucket access. You can override the directory with `SCCACHE_AUTH_DIR`.

### Velox Builds with sccache
```bash
cd velox-testing/velox/scripts

# Default: Remote S3 cache + local compilation (recommended)
./build_velox.sh --sccache

# Optional: Enable distributed compilation (may cause build differences such as additional warnings)
./build_velox.sh --sccache --sccache-enable-dist

# Pin a specific sccache version
./build_velox.sh --sccache --sccache-version 0.12.0-rapids.1
```

### Presto Builds with sccache
```bash
cd velox-testing/presto/scripts

# GPU native build with sccache
./start_native_gpu_presto.sh --sccache

# CPU native build with sccache
./start_native_cpu_presto.sh --sccache

# Pin a specific sccache version
./start_native_gpu_presto.sh --sccache --sccache-version 0.12.0-rapids.1

# Enable distributed compilation (use with caution)
./start_native_gpu_presto.sh --sccache --sccache-enable-dist
```

### How it Works
When `--sccache` is passed, the build process:
1. Installs the RAPIDS sccache fork inside the Docker build
2. Configures CMake to route all C/C++/CUDA compilations through sccache
3. For each compilation unit, sccache checks the S3 bucket (`rapids-sccache-devs`) for a cached result
4. **Cache hit**: downloads the cached object file (fast)
5. **Cache miss**: compiles locally and uploads the result to S3 for future use
6. Post-build statistics are displayed showing hit/miss rates

By default, distributed compilation is disabled to avoid compiler version differences that can cause build failures.

## Velox Benchmarking
A Docker-based benchmarking infrastructure has been added to facilitate running Velox benchmarks with support for CPU/GPU execution engines and profiling capabilities. The infrastructure uses a dedicated `velox-benchmark` Docker service with pre-configured volume mounts that automatically sync benchmark data and results. The data follows Hive directory structure, making it compatible with Presto. Currently, only TPC-H is implemented, but the infrastructure is designed to be easily extended to support additional benchmarks in the future.

### Prerequisites
The benchmarking infrastructure requires the same directory structure as Velox Testing, plus benchmark data using Hive directory structure. For TPC-H, the required data layout is shown below.

```
  velox-benchmark-data/
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

By default, the data directory is named `velox-benchmark-data`, but you can specify a different directory using a command-line option. The data must follow the Hive-style partition layout backed by Parquet files.

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

# Run TPC-H Q6 with 5 repetitions
./benchmark_velox.sh --queries 6 --device-type cpu --num-repeats 5

# Use custom data directory
./benchmark_velox.sh --queries 6 --device-type cpu --data-dir /path/to/data
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

Note that CPU and GPU builds require a local dependencies/run-time base Docker image (`presto/prestissimo-dependency:centos9`). The `start` scripts will not create this automatically. It must be obtained manually. Use the `build_centos_deps_image.sh` script to build an image locally, or the `fetch_centos_deps_image.sh` script to fetch a pre-built image from an external source. Note that the latter script currently requires additional credentials not available to third-parties.

### Running Integration Tests
The Presto integration tests are implemented using the [pytest](https://docs.pytest.org/en/stable/) framework. The integration tests can be executed directly by using the `pytest` command e.g. `pytest tpch_test.py` or more conveniently, by using the `run_integ_test.sh` script from within the `velox-testing/presto/scripts` directory (this script handles environment setup for test execution). Execute `./run_integ_test.sh --help` to get more details about script options. An instance of Presto must be deployed and running *before* running the integration tests. This can be done using one of the `start_*` scripts mentioned in the "Presto Testing" section.

#### Testing Different Scale Factors
The integration tests can be executed against tables with different scale factors by navigating (`cd`) into the `velox-testing/presto/testing/integration_tests/scripts` directory and executing the `generate_test_files.sh` script with a `--scale-factor` or `-s` argument. After this, the tests can then be executed using the steps described in the "Running Integration Tests" section.

Note that `velox-testing/presto/testing/integration_tests` and `velox-testing/benchmark_data_tools` are separate projects that are expected to be operated with their own virtual environment.

### Setting Up Benchmark Tables
A couple of utility scripts have been added to facilitate the process of setting up benchmark tables either from scratch or on top of existing benchmark data (Parquet) files. Specifically, the `setup_benchmark_tables.sh` script can be used to set up a new schema and tables on top of already generated benchmark data files. Execute `./setup_benchmark_tables.sh --help` to get more details about script options. The `setup_benchmark_data_and_tables.sh` script can be used to generate benchmark data at a specified scale factor and set up a schema and tables on top of the generated data files. Execute `./setup_benchmark_data_and_tables.sh --help` to get more details about script options. Both scripts should be executed from within the `velox-testing/presto/scripts` directory.

> [!TIP]
> Add `export PRESTO_DATA_DIR={path to directory that will contain datasets}` to your `~/.bashrc` file. This avoids having to always set the `PRESTO_DATA_DIR` environment variable when executing the `start_*` scripts and/or the schema/table setup scripts.


## Presto Benchmarking
The Presto benchmarks are implemented using the [pytest](https://docs.pytest.org/en/stable/) framework and builds on top of infrastructure that was implemented for general Presto testing. Specifically, the `start_*` scripts mentioned in the "Presto Testing" section can be used to start up a Presto variant (make sure the `PRESTO_DATA_DIR` environment variable is set appropriately before running the script), and the benchmark can be run by executing the `run_benchmark.sh` script from within the `velox-testing/presto/scripts` directory. Execute `./run_benchmark.sh --help` to get more details about the benchmark script options.
> [!TIP]
ANALYZE TABLES `velox-testing/presto/scripts/analyze_tables.sh` must be run on CPU Presto before GPU benchmarks because aggregation is not yet supported on GPU. Statistics are stored in the Hive metastore and automatically benefit GPU query execution, improving performance and reducing OOM failures.

## Spark Gluten Testing
A Python-based testing infrastructure using [pytest](https://docs.pytest.org/en/stable/) has been added to facilitate functional correctness testing of Spark with Gluten, a columnar execution plugin that leverages Velox for accelerated query processing. The infrastructure supports both TPC-H and TPC-DS benchmark suites and compares Spark Gluten query results against reference results (typically from DuckDB).

### Directory Structure
The build and test scripts expect the following directory layout:
```
├─ base_directory/
  ├─ velox-testing/
  ├─ velox/
  ├─ incubator-gluten/
```
The `velox-testing`, `velox`, and `incubator-gluten` repositories must be checked out as sibling directories under the same parent directory.

### Building Gluten

Three build variants are supported. All builds use Docker and produce images or JAR artifacts that can be used for testing and benchmarking.

#### CPU Static Build
Builds Gluten with the Velox CPU backend using static linking via vcpkg. Produces a standalone JAR file that can be used without a pre-built Docker image.

```bash
cd velox-testing/spark_gluten/scripts

# Basic build (output JAR goes to build_artifacts/cpu_static/)
./build_gluten_static.sh

# Custom output directory
./build_gluten_static.sh -o my_output_dir

# Use 8 threads
./build_gluten_static.sh -j 8

# Force a full rebuild (clear cached build artifacts)
./build_gluten_static.sh --no-cache
```

Execute `./build_gluten_static.sh --help` to get more details about script options.

#### CPU Dynamic Build
Builds Gluten with the Velox CPU backend using dynamic linking. Produces a Docker image containing the Gluten JARs and linked libraries.

```bash
cd velox-testing/spark_gluten/scripts

# Build CPU dynamic image (tagged as apache/gluten:dynamic_cpu_${USER})
./build_gluten_dynamic.sh -d cpu

# Custom image tag
./build_gluten_dynamic.sh -d cpu --image-tag my_cpu_image

# Use 8 threads
./build_gluten_dynamic.sh -d cpu -j 8
```

Execute `./build_gluten_dynamic.sh --help` to get more details about script options.

#### GPU Dynamic Build
Builds Gluten with the Velox GPU backend (cuDF acceleration) using dynamic linking. Produces a Docker image with GPU support.

```bash
cd velox-testing/spark_gluten/scripts

# Build GPU dynamic image (tagged as apache/gluten:dynamic_gpu_${USER})
./build_gluten_dynamic.sh -d gpu

# Specify CUDA architectures (default: auto-detected from host GPU)
./build_gluten_dynamic.sh -d gpu --cuda-arch "80;86;89;90"

# Build for all supported CUDA architectures
./build_gluten_dynamic.sh -d gpu --cuda-arch all

# Force a full rebuild
./build_gluten_dynamic.sh -d gpu --no-cache
```

Execute `./build_gluten_dynamic.sh --help` to get more details about script options.

> [!NOTE]
> Build artifacts are cached across runs using Docker BuildKit cache mounts. This enables fast incremental compilation when only a few source files change. Use `--no-cache` (`-n`) to clear the cache and force a full rebuild.

#### Quick Start (Pre-built JAR)
Alternatively, a pre-built static JAR file for CPU can be [downloaded](https://downloads.apache.org/incubator/gluten/) directly. A convenience script is provided:
```bash
cd velox-testing/spark_gluten/scripts
./download_gluten.sh  # Downloads Gluten JAR to testing/spark-gluten-install/
```

### Running Integration Tests
The Spark Gluten integration tests can be executed using the `run_integ_test.sh` script from within the `velox-testing/spark_gluten/scripts` directory. Tests run inside a Docker container using either a dynamically-built image or a statically-linked JAR. Execute `./run_integ_test.sh --help` to get more details about script options.

#### Basic Examples:
```bash
cd velox-testing/spark_gluten/scripts

# Run all TPC-H integration tests (uses the default dynamic GPU image)
./run_integ_test.sh -b tpch

# Run specific queries
./run_integ_test.sh -b tpch -q "1,2,3"

# Run with a CPU dynamic image
./run_integ_test.sh -b tpch --image-tag dynamic_cpu_${USER}

# Run with a statically-linked JAR
./run_integ_test.sh -b tpch --static-gluten-jar-path /path/to/gluten.jar

# Run with a custom dataset (requires SPARK_DATA_DIR environment variable)
export SPARK_DATA_DIR=/path/to/your/benchmark/data
./run_integ_test.sh -b tpch -d my_dataset_name

# Run GPU tests with GPU-specific Spark configuration and environment variables
./run_integ_test.sh -b tpch \
  --spark-config spark_gluten/testing/config/gpu_default.conf \
  --env-file spark_gluten/testing/config/gpu_default.env

# Store Spark results for later comparison
./run_integ_test.sh -b tpch --store-spark-results

# Show result previews
./run_integ_test.sh -b tpch --show-spark-result-preview --show-reference-result-preview --preview-rows-count 10

# Use custom reference results
./run_integ_test.sh -b tpch -r /path/to/reference/results
```

If `--dataset-name` is not specified, the default test dataset from `common/testing/integration_tests/data/` is used.

> [!TIP]
> Add `export SPARK_DATA_DIR={path to directory that will contain datasets}` to your `~/.bashrc` file. This avoids having to always set the `SPARK_DATA_DIR` environment variable when executing tests with custom datasets.

### Configuration Files
Default Spark configuration and GPU-specific overrides are provided under `spark_gluten/testing/config/`:

| File | Purpose |
|------|---------|
| `default.conf` | Base Spark configuration (memory, shuffle, Gluten settings). Applied to all runs. |
| `gpu_default.conf` | GPU-specific overrides (cuDF acceleration, GPU table scan, GPU shuffle). Use with `--spark-config`. |
| `gpu_default.env` | GPU environment variables (pinned memory pools, device selection). Use with `--env-file`. |

The `--spark-config` file overlays settings on top of `default.conf`. The `--env-file` sets environment variables inside the container (e.g. `CUDA_VISIBLE_DEVICES`, cuDF memory pool sizes, etc.).

## Spark Gluten Benchmarking
The Spark Gluten benchmarks are implemented using the [pytest](https://docs.pytest.org/en/stable/) framework and build on top of the infrastructure implemented for Spark Gluten testing. The benchmarks measure query execution time across multiple iterations and can be used to compare CPU vs GPU performance, different configurations, or track performance over time.

### Prerequisites
The benchmarking infrastructure requires:
- A built Gluten image (see [Building Gluten](#building-gluten)) or a statically-linked JAR.
- Benchmark data organized in Hive-style directory layouts.
- The `SPARK_DATA_DIR` environment variable set to a directory containing your benchmark datasets.

### Running Benchmarks
The Spark Gluten benchmarks can be executed using the `run_benchmark.sh` script from within the `velox-testing/spark_gluten/scripts` directory. Execute `./run_benchmark.sh --help` to get more details about script options.

#### Basic Examples:
```bash
cd velox-testing/spark_gluten/scripts
export SPARK_DATA_DIR=/path/to/your/benchmark/data

# Run all TPC-H queries with the default GPU dynamic image
./run_benchmark.sh -b tpch -d sf10_64mb

# Run specific queries
./run_benchmark.sh -b tpch -d sf10_64mb -q "1,2,3"

# Run with 10 iterations
./run_benchmark.sh -b tpch -d sf10_64mb -i 10

# Run with a CPU dynamic image
./run_benchmark.sh -b tpch -d sf10_64mb --image-tag dynamic_cpu_${USER} -t cpu_dynamic

# Run with a statically-linked JAR
./run_benchmark.sh -b tpch -d sf10_64mb --static-gluten-jar-path /path/to/gluten.jar -t cpu_static

# Run GPU benchmarks with GPU-specific configuration
./run_benchmark.sh -b tpch -d sf10_64mb \
  --spark-config spark_gluten/testing/config/gpu_default.conf \
  --env-file spark_gluten/testing/config/gpu_default.env \
  -t gpu_dynamic

# Custom output directory
./run_benchmark.sh -b tpch -d sf10_64mb -o ~/benchmark_results

# Skip dropping system caches (caches are dropped by default)
./run_benchmark.sh -b tpch -d sf10_64mb --skip-drop-cache
```

Benchmark results are written in JSON and text formats to the specified output directory (default: `benchmark_output/`). Spark/Velox warnings and stderr are redirected to a `spark_warnings.log` file in the output directory.

> [!TIP]
> Add `export SPARK_DATA_DIR={path to directory that will contain datasets}` to your `~/.bashrc` file. This avoids having to always set the `SPARK_DATA_DIR` environment variable when executing benchmarks.
