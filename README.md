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
`sccache` has been integrated to significantly accelerate builds using remote S3 caching and optional distributed compilation. Currently supported for Velox builds only (not Presto).

The fork `rapidsai/sccache` is integrated and configured for use with the `rapidsai` GitHub organization.

### Setup and Usage
First, set up authentication credentials:
```bash
cd velox-testing/velox/scripts
./setup_sccache_auth.sh
```

Then build Velox with sccache enabled:
```bash
# Default: Remote S3 cache + local compilation (recommended)
./build_velox.sh --sccache

# Optional: Enable distributed compilation (may cause build differences such as additional warnings)
./build_velox.sh --sccache --sccache-enable-dist
```

Authentication files are stored in `~/.sccache-auth/` by default and credentials are valid for 12 hours. By default, distributed compilation is disabled to avoid compiler version differences that can cause build failures.

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

### Quick Start
The fastest way to get started with testing Gluten is to [download](https://downloads.apache.org/incubator/gluten/) a pre-built JAR file. A convenience script `download_gluten.sh` in the `velox-testing/spark_gluten/scripts` directory has been added to facilitate this.
```bash
cd velox-testing/spark_gluten/scripts
./download_gluten.sh  # Optional: downloads Gluten JAR to testing/spark-gluten-install/
```

> [!NOTE]
> Scripts that facilitate builds of custom Gluten JAR files from checked out Velox and Gluten repositories will be added soon.
>

### Running Integration Tests
The Spark Gluten integration tests can be executed using the `run_integ_test.sh` script from within the `velox-testing/spark_gluten/scripts` directory. This script handles environment setup and management for test execution. Execute `./run_integ_test.sh --help` to get more details about script options.

#### Basic Examples:
```bash
# Run all TPC-H integration tests
./run_integ_test.sh -b tpch

# Run specific queries
./run_integ_test.sh -b tpch -q "1,2,3"

# Run with a custom dataset (requires SPARK_DATA_DIR environment variable)
export SPARK_DATA_DIR=/path/to/your/benchmark/data
./run_integ_test.sh -b tpch -d my_dataset_name

# Store Spark results for later comparison
./run_integ_test.sh -b tpch --store-spark-results

# Show result previews
./run_integ_test.sh -b tpch --show-spark-result-preview --show-reference-result-preview --preview-rows-count 10

# Use custom reference results
./run_integ_test.sh -b tpch -r /path/to/reference/results
```

If `--dataset-name` is not specified, the default test dataset from `common/testing/integration_tests/data/` is used. The tests can also be executed directly via pytest:
```bash
cd velox-testing/spark_gluten/testing/integration_tests
pytest tpch_test.py -s -v
```

> [!TIP]
> Add `export SPARK_DATA_DIR={path to directory that will contain datasets}` to your `~/.bashrc` file. This avoids having to always set the `SPARK_DATA_DIR` environment variable when executing tests with custom datasets.

## Spark Gluten Benchmarking
The Spark Gluten benchmarks are implemented using the [pytest](https://docs.pytest.org/en/stable/) framework and build on top of the infrastructure implemented for Spark Gluten testing. The benchmarks measure query execution time and can be used to compare different configurations or track performance over time.

### Prerequisites
The benchmarking infrastructure requires the same setup as Spark Gluten Testing, plus benchmark data organized in Hive-style directory layouts. The `SPARK_DATA_DIR` environment variable must be set to a directory containing your benchmark datasets.

### Running Benchmarks
The Spark Gluten benchmarks can be executed using the `run_benchmark.sh` script from within the `velox-testing/spark_gluten/scripts` directory. This script handles environment setup and management for benchmark execution. Execute `./run_benchmark.sh --help` to get more details about script options.

#### Basic Examples:
```bash
# Run all TPC-H queries (requires SPARK_DATA_DIR and dataset name)
export SPARK_DATA_DIR=/path/to/your/benchmark/data
./run_benchmark.sh -b tpch -d dataset_name

# Run specific queries
./run_benchmark.sh -b tpch -d dataset_name -q "1,2,3"

# Run with 10 iterations
./run_benchmark.sh -b tpch -d dataset_name -i 10

# Run with a custom output directory
./run_benchmark.sh -b tpch -d dataset_name -o ~/benchmark_results

# Tag benchmark runs for organization
./run_benchmark.sh -b tpch -d dataset_name -t my_experiment_tag

# Skip dropping system caches (caches are dropped by default)
./run_benchmark.sh -b tpch -d dataset_name --skip-drop-cache
```

Benchmark results are written in JSON and text formats to the specified output directory (default: `benchmark_output/`).

> [!TIP]
> Add `export SPARK_DATA_DIR={path to directory that will contain datasets}` to your `~/.bashrc` file. This avoids having to always set the `SPARK_DATA_DIR` environment variable when executing benchmarks.
