# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`velox-testing` is an infrastructure repository for building, testing, and benchmarking [Velox](https://github.com/facebookincubator/velox), [Presto](https://github.com/prestodb/presto), and [Spark Gluten](https://github.com/apache/incubator-gluten) — with GPU acceleration via NVIDIA cuDF. It is **not** a library; it is a collection of shell scripts, Python test frameworks, Docker Compose files, and GitHub Actions workflows.

The repo expects sibling directories to exist at the same level (e.g., `../velox`, `../presto`, `../incubator-gluten`).

---

## Code Quality

```bash
# Python linting (configured in pyproject.toml: 120 char lines, Python 3.11+)
ruff check <file>
ruff format <file>
ruff check --fix <file>

# Spell checking
codespell <path>

# Run all CI style checks
./ci/check_style.sh
```

---

## Velox

```bash
# Build
velox/scripts/build_velox.sh                            # GPU build (default)
velox/scripts/build_velox.sh --cpu                      # CPU-only
velox/scripts/build_velox.sh --benchmarks true --sccache

# Test
velox/scripts/test_velox.sh [OPTIONS]

# Benchmark (TPC-H)
velox/scripts/benchmark_velox.sh --queries "1 6" --device-type cpu
velox/scripts/benchmark_velox.sh --queries 6 --device-type gpu --profile true
```

---

## Presto

```bash
# Start clusters
presto/scripts/start_java_presto.sh
presto/scripts/start_native_cpu_presto.sh
presto/scripts/start_native_gpu_presto.sh [--sccache]

# Setup benchmark data (one-time per scale factor)
presto/scripts/setup_benchmark_data_and_tables.sh --scale-factor 100 --schema-name tpch_sf100
presto/scripts/analyze_tables.sh     # Required before GPU benchmarks

# Integration tests (pytest under the hood)
presto/scripts/run_integ_test.sh --benchmark-type tpch

# Benchmarks
presto/scripts/run_benchmark.sh -b tpch -s bench_sf100
presto/scripts/run_benchmark.sh -b tpch -s bench_sf100 -i 10 --profile
```

Python tests live in `presto/testing/integration_tests/` and `presto/testing/performance_benchmarks/`. Both use pytest with shared fixtures from `presto/testing/common/`.

---

## Spark Gluten

```bash
# Build
spark_gluten/scripts/build_gluten_static.sh             # CPU standalone JAR
spark_gluten/scripts/build_gluten_dynamic.sh -d cpu     # CPU Docker image
spark_gluten/scripts/build_gluten_dynamic.sh -d gpu     # GPU Docker image
spark_gluten/scripts/download_gluten.sh                 # Use pre-built JAR

# Integration tests
spark_gluten/scripts/run_integ_test.sh -b tpch
spark_gluten/scripts/run_integ_test.sh -b tpch -q "1,2,3"
spark_gluten/scripts/run_integ_test.sh -b tpch --static-gluten-jar-path /path/to/gluten.jar

# Benchmarks
spark_gluten/scripts/run_benchmark.sh -b tpch -d sf10_64mb
spark_gluten/scripts/run_benchmark.sh -b tpch -d sf10_64mb -i 10
```

---

## Architecture

### Component Layout

Each of the three major components (velox/, presto/, spark_gluten/) follows the same pattern:
- `scripts/` — shell scripts orchestrating Docker Compose for builds, tests, and benchmarks
- `docker/` — Docker Compose files and Dockerfiles
- `testing/integration_tests/` — pytest correctness tests
- `testing/performance_benchmarks/` — pytest performance tests

### Shared Infrastructure

- `scripts/common.sh` — bash utility functions sourced by most shell scripts
- `scripts/cuda_helper.sh` — CUDA architecture detection
- `scripts/py_env_functions.sh` — Python venv setup helpers
- `scripts/sccache/` — sccache authentication and setup
- `common/testing/` — Python test utilities shared across all three components
- `benchmark_data_tools/` — Python scripts for generating TPC-H/TPC-DS Parquet data
- `benchmark_reporting_tools/` — Post-benchmark analysis and expected-result generation
- `template_rendering/render_docker_compose_template.py` — Generates Docker Compose files from templates with custom parameters

### CI/CD (`.github/`)

- **Staging workflows** (`*-create-staging.yml`): merge PRs from upstream repos into staging branches for integrated testing
- **Test workflows** (`*-test.yml`, `*-nightly-*.yml`): run integration and benchmark tests against staging branches
- **CI image workflows** (`ci-images-*.yml`): build and clean up Docker images published to GHCR
- **Reusable actions** (`.github/actions/`): `velox-setup`, `velox-deps-fetch`, `sccache-setup`, `test-benchmark-data-tools`

### Validation Pattern

Integration tests compare query results against a DuckDB reference implementation. Results are validated in Python via pytest fixtures defined in each component's `conftest.py`. The `benchmark_data_tools/duckdb_utils.py` module handles reference computation.

### Docker Compose Architecture

Presto uses composable Docker Compose files:
- `docker-compose.common.yml` — base services (coordinator, metastore)
- `docker-compose.java.yml` / `docker-compose.native-cpu.yml` / `docker-compose.native-gpu.yml` — overlay for worker type

The launch scripts (`presto/docker/launch_*.sh`) are entrypoints executed inside containers.
