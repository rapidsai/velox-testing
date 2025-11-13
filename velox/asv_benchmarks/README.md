# Velox ASV Benchmarks

This directory contains [ASV (Airspeed Velocity)](https://asv.readthedocs.io/) benchmarks for Velox CUDF TPC-H queries. ASV is a tool for tracking performance over time with automated build and benchmarking.

## Quick Start

### Prerequisites

1. **Docker** and Docker Compose installed
2. **TPC-H data** generated in Hive-style Parquet format
3. **sccache authentication** (for faster builds)
4. **Velox patches applied** (automatically handled by scripts)

### Applying Velox Patches

The benchmarking system requires patches to be applied to the Velox repository for TPC-H Python bindings support. These patches are automatically applied when you use the build scripts:

- **`build_asv_image.sh`**: Applies patches before building the ASV image
- **`run_asv_commit_range.sh`**: Applies patches for each commit during range benchmarking

#### Manual Patch Application

If you need to apply patches manually:

```bash
cd velox/scripts

# Apply patches to default Velox repo (../../../velox)
./apply_velox_patches.sh

# Apply patches to custom Velox repo
./apply_velox_patches.sh --velox-repo /path/to/velox
```

The script will:
- Check if patches are already applied (idempotent)
- Apply patches in numerical order
- Skip already-applied patches
- Report any conflicts or errors

#### Available Patches

Located in `velox/patches/`:
1. `0001-make-querybuilder-as-protected-member-so-that-it-can.patch` - Makes QueryBuilder accessible in subclasses
2. `0002-added-tpch-python-bindings.patch` - Adds Python bindings for TPC-H benchmarks
3. `0003-add-python-directory-to-the-cmakelist-for-tpch-bridg.patch` - Updates CMake configuration

### Single Benchmark Run

Run benchmarks for the current code state:

```bash
cd velox-testing/velox/scripts

# Quick benchmark with default settings
./run_asv_benchmarks.sh \
    --data-path /path/to/tpch_data

# Custom configuration
./run_asv_benchmarks.sh \
    --data-path /path/to/tpch_data \
    --results-path ./custom_results \
    --port 9090
```

Access results at `http://localhost:8080` (or your specified port).

### Benchmark Multiple Commits

Track performance changes across commits:

```bash
# Last 5 commits
./scripts/run_asv_commit_range.sh --commits HEAD~5..HEAD

# Between releases
./scripts/run_asv_commit_range.sh --commits v1.0..v2.0

# With custom paths
./scripts/run_asv_commit_range.sh \
    --data-path /path/to/tpch_data \
    --commits HEAD~10..HEAD
```

---

## Table of Contents

- [Single Commit Benchmarking](#single-commit-benchmarking)
  - [Basic Usage](#basic-usage)
  - [Command-Line Options](#command-line-options)
  - [Environment Variables](#environment-variables)
  - [Examples](#single-commit-examples)
- [Commit Range Benchmarking](#commit-range-benchmarking)
  - [Overview](#commit-range-overview)
  - [Usage](#commit-range-usage)
  - [Commit Range Syntax](#commit-range-syntax)
  - [What Happens During Execution](#what-happens-during-execution)
  - [Examples](#commit-range-examples)
- [Directory Structure](#directory-structure)
- [Configuration](#configuration)
- [Viewing Results](#viewing-results)
- [Adding New Benchmarks](#adding-new-benchmarks)
- [Troubleshooting](#troubleshooting)
- [Related Documentation](#related-documentation)

---

## Single Commit Benchmarking

Use `run_asv_benchmarks.sh` to benchmark the current state of the code (single commit or working directory).

### Basic Usage

```bash
cd velox/scripts

# Run all benchmarks with default settings
./run_asv_benchmarks.sh --data-path /path/to/tpch_data

# Run specific benchmark
./run_asv_benchmarks.sh \
    --data-path /path/to/tpch_data \
    --bench "tpch_benchmarks.TimeQuery06"

# Run without preview server
./run_asv_benchmarks.sh \
    --data-path /path/to/tpch_data \
    --no-preview
```

### Command-Line Options

```
Options:
  --data-path PATH           Path to TPC-H data directory (required)
  --results-path PATH        Path to store ASV results (default: ../asv_benchmarks/results/)
  --port PORT                HTTP server port for preview (default: 8080)
  --bench PATTERN            Run specific benchmark(s) matching pattern
  --no-preview               Skip starting the preview server after benchmarks
  --no-publish               Skip publishing HTML (only run benchmarks)
  --publish-existing         Publish existing results without running benchmarks
  --commits RANGE            Benchmark specific commit range (e.g., HEAD~5..HEAD)
  --interleave-rounds        Enable ASV's --interleave-rounds for better averaging
  --no-cache                 Rebuild Docker image without cache
  -i, --interactive          Run interactive bash shell instead of benchmarks
  -h, --help                 Show help message
```

### Environment Variables

Control benchmark behavior with these variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ASV_MACHINE` | `docker-container` | Machine name for this benchmark run |
| `ASV_AUTO_MACHINE` | `false` | Auto-generate unique machine name with timestamp |
| `ASV_RECORD_SAMPLES` | `false` | Record multiple samples for variance analysis |
| `ASV_CLEAR_RESULTS` | `false` | Clear old results before running |
| `ASV_SKIP_SMOKE_TEST` | `false` | Skip initial data validation test |
| `ASV_PREVIEW` | `true` | Start web server after benchmarks |
| `ASV_PORT` | `8080` | Web server port |

### Single Commit Examples

#### Example 1: Quick Benchmark with Defaults

```bash
./run_asv_benchmarks.sh --data-path /data/tpch
```

#### Example 2: Benchmark Specific Query

```bash
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --bench "TimeQuery06"
```

#### Example 3: Generate Graph Data (Multiple Runs)

To generate graphs, run multiple times with unique machine names:

```bash
# Method 1: Manual names
ASV_MACHINE="baseline" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch --no-preview

ASV_MACHINE="optimized" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch

# Method 2: Auto-generated names (loop)
for i in {1..3}; do
    ASV_MACHINE="run$i" ASV_RECORD_SAMPLES=true \
    ./run_asv_benchmarks.sh --data-path /data/tpch --no-preview
done
./run_asv_benchmarks.sh --data-path /data/tpch --no-publish

# Method 3: Auto-generated timestamp-based names
ASV_AUTO_MACHINE=true ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch
```

#### Example 4: Performance Testing with Variance

```bash
# Clear old results and run with variance tracking
ASV_CLEAR_RESULTS=true \
ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch
```

#### Example 5: CI/CD Integration

```bash
# Skip smoke test and preview for automated runs
ASV_SKIP_SMOKE_TEST=true \
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --no-preview \
    --results-path ./ci-results
```

#### Example 6: Rebuild After Code Changes

After modifying C++ files in Velox or the TPC-H wrapper library:

```bash
# Step 1: Rebuild Velox and TPC-H wrapper library
cd velox/scripts
./build_velox.sh \
    --build-type release \
    --sccache \
    --no-cache

# Step 2: Rebuild ASV image and run benchmarks
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --no-cache
```

**Note**: The `--no-cache` flag ensures a fresh build without Docker layer caching.

#### Example 7: Interactive Debugging

```bash
# Drop into shell for debugging
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --interactive
```

### Single Commit Workflow Patterns

#### Development Workflow

```bash
# 1. Make code changes to Velox or TPC-H wrapper
vim velox/exec/MyFile.cpp

# 2. Rebuild Velox and TPC-H wrapper library
cd velox/scripts
./build_velox.sh \
    --build-type release \
    --sccache \
    --no-cache

# 3. Rebuild ASV image and run benchmarks
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --no-cache

# 4. View results at http://localhost:8080
```

#### Before/After Comparison

```bash
# 1. Baseline measurement
ASV_MACHINE="before" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch --no-preview

# 2. Make optimization changes
vim velox/exec/Optimizer.cpp

# 3. Rebuild Velox and TPC-H wrapper
cd velox/scripts
./build_velox.sh \
    --build-type release \
    --sccache \
    --no-cache

# 4. After measurement
ASV_MACHINE="after" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch --no-cache

# 5. Compare in web UI at http://localhost:8080
```

### Understanding What Needs to be Rebuilt

The benchmarking system has several layers. Understanding what needs rebuilding helps optimize your workflow:

#### When You Change Python Code Only

**Files**: `benchmarks/tpch_benchmarks.py` or other Python benchmark files

**Rebuild Required**: ASV image only

```bash
./run_asv_benchmarks.sh --data-path /data/tpch --no-cache
```

**Why**: Python changes only affect the benchmark harness, not the C++ implementation.

#### When You Change C++ Code

**Files**: 
- Velox C++ source (`velox/exec/*.cpp`, `velox/core/*.cpp`, etc.)
- TPC-H wrapper library (`velox/experimental/cudf/benchmarks/python/src/*.cpp`)

**Rebuild Required**: Both Velox and ASV image

```bash
# Step 1: Rebuild Velox + TPC-H wrapper (C++ compilation)
cd velox/scripts
./build_velox.sh --build-type release --sccache --no-cache

# Step 2: Rebuild ASV image (Python bindings installation)
./run_asv_benchmarks.sh --data-path /data/tpch --no-cache
```

**Why**: C++ changes require recompiling Velox and the wrapper library, then reinstalling Python bindings.

#### When You Change Cython Bindings

**Files**: `velox/experimental/cudf/benchmarks/python/src/*.pyx`

**Rebuild Required**: Both Velox and ASV image

```bash
cd velox/scripts
./build_velox.sh --build-type release --sccache --no-cache

./run_asv_benchmarks.sh --data-path /data/tpch --no-cache
```

**Why**: Cython files are compiled to C++ and then to binary extensions.

#### When You Update Patches

**Files**: `velox/patches/*.patch`

**Rebuild Required**: Apply new patches, then rebuild both Velox and ASV image

```bash
cd velox/scripts

# Step 1: Apply updated patches
./apply_velox_patches.sh

# Step 2: Rebuild Velox
./build_velox.sh --build-type release --sccache --no-cache

# Step 3: Rebuild ASV image
./run_asv_benchmarks.sh --data-path /data/tpch --no-cache
```

**Why**: Patches modify Velox source code, requiring full rebuild pipeline.

**Note**: Patches are automatically applied by `build_asv_image.sh` and `run_asv_commit_range.sh`. Manual application is rarely needed unless testing new patches.

#### Using sccache for Faster Rebuilds

The `--sccache` flag enables distributed compilation caching:

- **First build**: Full compilation (~30 minutes)
- **Subsequent builds**: Only changed files recompiled (~5-15 minutes)
- **Requirement**: sccache authentication files in `~/.sccache-auth/`

Without sccache:
```bash
cd velox/scripts
./build_velox.sh --build-type release --no-cache
```

---

## Commit Range Benchmarking

Use `run_asv_commit_range.sh` to benchmark multiple commits and track performance changes over time.

### Commit Range Overview

The script automates the process of benchmarking multiple commits:

1. **For each commit in the range:**
   - Checks out the commit
   - Applies Velox patches (for TPC-H Python bindings)
   - Rebuilds Velox with sccache (using `--no-cache` for fresh builds)
   - Rebuilds ASV benchmark image (using `--no-cache`)
   - Runs ASV benchmarks with a unique machine name
   - Tags the Docker image with the commit hash

2. **After all commits are benchmarked:**
   - Publishes HTML reports for all results
   - Starts ASV preview server
   - Tags the most recent commit's image as `latest`
   - Cleans up intermediate tagged images

### Commit Range Usage

```bash
cd velox/scripts

# Basic usage
./run_asv_commit_range.sh --commits HEAD~5..HEAD

# With all options
./run_asv_commit_range.sh \
    --velox-repo /path/to/velox \
    --data-path /path/to/tpch_data \
    --results-path /path/to/asv_results \
    --sccache-auth-dir /path/to/.sccache-auth \
    --port 8080 \
    --commits HEAD~5..HEAD
```

### Command-Line Options (Commit Range)

- `--velox-repo PATH` - Path to Velox repository (default: current directory)
- `--data-path PATH` - Path to TPC-H data directory (required)
- `--results-path PATH` - Path to store ASV results (default: ../asv_results)
- `--sccache-auth-dir PATH` - Path to sccache auth directory (default: ~/.sccache-auth)
- `--port PORT` - HTTP server port for preview (default: `8080`)
- `--commits RANGE` - Git commit range to benchmark (required)
- `-h, --help` - Show help message

**Note**: `--clear-results` is automatic - results are always cleared at the start for commit range benchmarking.

### Commit Range Syntax

The `--commits` argument accepts standard Git commit range syntax:

#### Last N Commits

```bash
# Last 5 commits
--commits HEAD~5..HEAD

# Last 10 commits
--commits HEAD~10..HEAD
```

#### Between Two Tags

```bash
--commits v1.0..v2.0
```

#### Between Two Commits

```bash
--commits abc123..def456
```

#### Specific Commits

```bash
# Single commit
--commits abc123^!

# Multiple specific commits (space-separated)
--commits "abc123 def456 789ghi"
```

### What Happens During Execution

#### 1. Initialization

- Validates all paths and arguments
- Gets list of commits to benchmark (in chronological order)
- Displays commits and asks for confirmation
- Clears previous results automatically

#### 2. For Each Commit

For each commit in the range (oldest to newest):

1. **Checkout**: Switches to the commit
2. **Apply Patches**: Applies Velox patches for TPC-H Python bindings (idempotent)
3. **Build Velox**: Rebuilds `velox-adapters-build:latest` with sccache and `--no-cache`
4. **Run Benchmarks**: Builds ASV image with `--no-cache` and runs all TPC-H benchmarks
5. **Tag Image**: Tags the Docker image as `velox-adapters-build:<commit-hash>`

Each commit gets a unique machine name: `velox-commit-<hash>-<timestamp>`

#### 3. Publishing and Preview

After all commits are benchmarked:

1. **Publish**: Generates HTML reports from all benchmark results
2. **Preview**: Starts ASV web server on specified port
3. **Cleanup**: Tags most recent commit as `latest`, removes intermediate images

#### 4. Cleanup on Exit

When you stop the preview server (Ctrl+C) or if an error occurs:

1. Tags the most recent commit's image as `velox-adapters-build:latest`
2. Removes all other tagged commit images
3. Restores Git to original branch/commit

### Commit Range Examples

#### Example 1: Benchmark Recent Development

Track performance changes in recent development:

```bash
./scripts/run_asv_commit_range.sh --commits HEAD~10..HEAD
```

#### Example 2: Benchmark Between Releases

Compare performance between two release tags:

```bash
./scripts/run_asv_commit_range.sh --commits v0.0.1..v0.0.2
```

#### Example 3: Benchmark Specific Feature Branch

Benchmark commits from a feature branch:

```bash
cd ../velox
git checkout feature-branch
cd ../velox-tetsing/velox/scripts
./run_asv_commit_range.sh --commits main..feature-branch
```

#### Example 4: Full Custom Configuration

```bash
./scripts/run_asv_commit_range.sh \
    --velox-repo /custom/path/to/velox \
    --data-path /custom/tpch/data \
    --results-path /custom/results \
    --sccache-auth-dir /custom/sccache-auth \
    --port 9090 \
    --commits HEAD~20..HEAD
```

### Build Process Details

#### Why `--no-cache`?

The script always uses `--no-cache` for both Velox and ASV image builds to ensure:

1. **Fresh Builds**: Each commit is built from scratch, eliminating cached layer issues
2. **Accurate Comparisons**: Performance measurements aren't affected by Docker cache artifacts
3. **Reproducibility**: Builds are deterministic and reproducible

#### sccache Acceleration

While Docker cache is disabled, **sccache is still enabled** to speed up C++ compilation:

- sccache caches compiled object files (not Docker layers)
- Provides 2-10x speedup depending on code changes
- Shared across all commits being benchmarked

### Performance Considerations

#### Time Estimates

Benchmarking time depends on:

- **Number of commits**: ~30-60 minutes per commit
- **Velox build time**: ~15-30 minutes (with sccache)
- **Benchmark run time**: ~15-30 minutes (all 22 TPC-H queries)

Example: 10 commits ≈ 5-10 hours total

#### Disk Space

Each commit requires:

- **Velox build**: ~5-10 GB
- **Docker images**: ~3-5 GB per tagged image
- **Benchmark results**: ~100-500 MB per commit

Example: 10 commits ≈ 50-100 GB total

#### sccache Benefits

With sccache enabled:

- **First commit**: Full build (~30 minutes)
- **Subsequent commits**: Incremental builds (~5-15 minutes)
- **Total speedup**: 2-5x faster than without sccache

### Best Practices for Commit Range Benchmarking

#### 1. Use Meaningful Commit Ranges

```bash
# Good: Track recent development
--commits HEAD~10..HEAD

# Good: Compare releases
--commits v1.0..v2.0

# Avoid: Too many commits
--commits HEAD~100..HEAD  # Will take days!
```

#### 2. Monitor System Resources

```bash
# In another terminal, monitor resources
watch -n 5 'docker stats --no-stream && echo && df -h'
```

#### 3. Run During Off-Hours

Since benchmarking takes hours, consider:

- Running overnight
- Using `nohup` or `screen` for long-running sessions
- Monitoring remotely via SSH

```bash
# Run in background with nohup
nohup ./scripts/run_asv_commit_range.sh --commits HEAD~10..HEAD > benchmark.log 2>&1 &
```

#### 4. Document Results

After benchmarking:

1. Take screenshots of interesting trends
2. Export data for further analysis
3. Note any anomalies or unexpected results
4. Archive results for future reference

---

## Directory Structure

```
asv_benchmarks/
├── asv.conf.json              # ASV configuration
├── Dockerfile                 # ASV benchmark Docker image
├── entrypoint.sh             # Container entrypoint script
├── machine.json              # Machine metadata template
├── benchmarks/               # Benchmark implementations
│   └── tpch_benchmarks.py    # TPC-H query benchmarks
├── results/                  # Benchmark results (generated)
│   ├── <machine-name>/       # Per-machine results
│   └── html/                 # Generated HTML reports
└── env/                      # ASV virtualenv (generated)
```

---

## Configuration

### ASV Configuration (`asv.conf.json`)

Key settings in `asv.conf.json`:

```json
{
  "version": 1,
  "project": "velox-cudf-tpch",
  "project_url": "https://github.com/facebookincubator/velox",
  "repo": "/workspace/velox",
  "branches": ["main"],
  "environment_type": "virtualenv",
  "pythons": ["3.9"],
  "install_project": false,
  "build_command": [
    "/bin/bash", "-c",
    "cd /workspace/velox/docker && ./build_velox.sh --sccache"
  ],
  "install_command": [
    "/bin/bash", "-c",
    "cd /workspace/velox/velox/experimental/cudf/benchmarks/python && python -m pip install -e ."
  ]
}
```

**Important Settings:**

- `environment_type: "virtualenv"` - Uses Python virtualenv for isolation
- `install_project: false` - Prevents ASV from auto-installing (we handle it manually)
- `build_command` - Builds Velox C++ components
- `install_command` - Installs Python bindings

### Machine Configuration (`machine.json`)

Machine metadata is auto-generated but can be customized:

```json
{
  "docker-container": {
    "machine": "docker-container",
    "os": "Linux",
    "arch": "x86_64",
    "cpu": "AMD EPYC 7742",
    "ram": "512GB",
    "gpu": "NVIDIA A100 80GB"
  }
}
```

## Understanding Timing Parameters

ASV provides several parameters to control how benchmarks are measured:

### Benchmark Class Attributes

These are set in `benchmarks/tpch_benchmarks.py`:

- **`number`**: How many times to execute the benchmark function in each measurement
  - For queries taking 100ms+: `number = 1` is sufficient
  - For fast operations (<1ms): increase to `number = 100` or `1000`

- **`repeat`**: Number of statistical samples to collect
  - Each repeat runs `number` times and records one measurement
  - Default: `repeat = 3` for good statistics
  - Increase to `repeat = 5-10` for very accurate measurements
  - Trade-off: More repeats = better statistics but longer runtime

- **`rounds`**: Run the entire benchmark suite this many times
  - Used with `--interleave-rounds` to average over long-time variations
  - Helps with: system load changes, CPU thermal throttling, power management
  - Default: `rounds = 1` for quick runs
  - Production: `rounds = 3-5` for stable results

- **`sample_time`**: Minimum time to run adaptively (seconds)
  - Set to `0` for explicit control via `number`/`repeat`
  - Set to `0.1` or `1.0` for adaptive sampling (ASV will adjust `number`)

- **`warmup_time`**: Time to warmup before measurements (seconds)
  - Useful for JIT-compiled code or caches
  - For native C++: `warmup_time = 0` is fine

### Example Configurations

#### Fast Interactive Development (Default)
```python
number = 1
repeat = 1
rounds = 1
sample_time = 0
```
**Use case**: Quick feedback during development  
**Runtime**: ~2-3 minutes for all 22 queries

#### Balanced (Current Configuration)
```python
number = 1
repeat = 3
rounds = 1
sample_time = 0
```
**Use case**: Regular benchmarking with reasonable accuracy  
**Runtime**: ~6-8 minutes for all 22 queries

#### High Accuracy (Production)
```python
number = 1
repeat = 5
rounds = 3
sample_time = 0
```
**Use case**: Release benchmarks, performance tracking  
**Runtime**: ~25-30 minutes for all 22 queries  
**Command**: `asv run --interleave-rounds`

## Library Threading Control

The entrypoint automatically sets these environment variables to reduce timing variability:

```bash
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
```

**Why?** Multithreaded libraries can cause unpredictable performance due to:
- Thread scheduling variability
- CPU core migration
- Cache effects

You can override these by passing custom values:
```bash
OMP_NUM_THREADS=4 ./run_asv_benchmarks.sh --data-path /data/tpch
```

## Adding New Benchmarks

### For Additional TPC-H Queries

Edit `benchmarks/tpch_benchmarks.py` to add new query methods:

```python
def time_query_23(self):
    """Benchmark TPC-H Query 23"""
    result = self.benchmark.run_query(23)
    return result.execution_time_ms
```

### For New Benchmark Suites (e.g., TPC-DS)

1. **Create Python bindings** in `velox/experimental/cudf/benchmarks/python/`
2. **Create benchmark file**: `benchmarks/tpcds_benchmarks.py`
3. **Implement benchmarks**:

```python
import os
import cudf_tpcds_benchmark

class TpcDsBenchmarks:
    def setup(self):
        data_path = os.environ.get('TPCDS_DATA_PATH', '/data/tpcds')
        self.benchmark = cudf_tpcds_benchmark.CudfTpcdsBenchmark(
            data_path=data_path,
            data_format='parquet'
        )
    
    def time_query_01(self):
        result = self.benchmark.run_query(1)
        return result.execution_time_ms
```

4. **Update scripts** to pass `TPCDS_DATA_PATH` environment variable

---

## Troubleshooting

### Patch Application Issues

#### Error: "Patch cannot be applied (conflicts or errors)"

**Cause**: Patches may not apply cleanly due to changes in the Velox repository

**Solutions**:

1. **Check if patches are partially applied**:
   ```bash
   cd velox  # Your sibling velox repo
   git status
   git diff
   ```

2. **Reset to clean state**:
   ```bash
   cd velox
   git reset --hard HEAD
   git clean -fd
   ```

3. **Update patches for current Velox version**:
   - Check which patch failed
   - Manually apply changes
   - Create new patch: `git diff > new-patch.patch`

#### Error: "Failed to apply Velox patches"

**Cause**: Script cannot find patches directory or Velox repository

**Solution**:
```bash
# Verify paths
ls -la velox/patches/           # Patches should exist
ls -la ../../../velox/.git/     # Velox repo should exist

# Apply manually with verbose output
cd velox/scripts
./apply_velox_patches.sh --velox-repo /path/to/velox
```

#### Warning: "Patch already applied (skipping)"

This is normal behavior - patches are idempotent and will be skipped if already applied.

### Build Issues

#### Error: "No such file or directory: build_velox.sh"

**Solution**: Ensure you're running from the correct directory:

```bash
cd velox/scripts
./run_asv_benchmarks.sh --data-path /data/tpch
```

#### Error: "sccache authentication failed"

**Solution**: Check sccache auth files exist:

```bash
ls -la ~/.sccache-auth/
# Should contain: sccache-credentials.json and sccache-token.json
```

Or disable sccache by not passing auth directory.

### Benchmark Issues

#### Error: "No module named 'cudf_tpch_benchmark'"

**Cause**: Python bindings not installed or outdated

**Solution**: 

If you only changed Python code:
```bash
./scripts/run_asv_benchmarks.sh --data-path /data/tpch --no-cache
```

If you changed C++ code in Velox or TPC-H wrapper:
```bash
# Step 1: Rebuild Velox + TPC-H wrapper
cd velox/scripts
./build_velox.sh --build-type release --sccache --no-cache

# Step 2: Rebuild ASV image
cd ../scripts
./scripts/run_asv_benchmarks.sh --data-path /data/tpch --no-cache
```

#### Error: "Smoke test failed"

**Cause**: TPC-H data not accessible or in wrong format

**Solution**:
```bash
# Skip smoke test
ASV_SKIP_SMOKE_TEST=true ./run_asv_benchmarks.sh --data-path /data/tpch

# Or verify data format
ls -la /data/tpch/lineitem/
# Should contain: *.parquet files with Hive-style partitioning
```

#### Error: "No benchmarks found"

**Causes and Solutions**:

1. Python bindings not built - rebuild required:
   ```bash
   # If you changed C++ code
   cd velox/scripts
   ./build_velox.sh --build-type release --sccache --no-cache

   ./run_asv_benchmarks.sh --no-cache --data-path /data/tpch
   ```

2. Data path not set:
   ```bash
   export TPCH_DATA_PATH=/path/to/tpch_data
   ./scripts/run_asv_benchmarks.sh --data-path $TPCH_DATA_PATH
   ```

### Docker Issues

#### Error: "Port already in use"

**Solution**: Use a different port:

```bash
./run_asv_benchmarks.sh --data-path /data/tpch --port 9090
```

#### Error: "Permission denied" on results directory

**Solution**: Use the cleanup script:

```bash
./scripts/clear_asv_results.sh
```

### Git Issues

#### Error: "dubious ownership"

**Solution**: Already handled in entrypoint, but if issues persist:

```bash
git config --global --add safe.directory /workspace/velox
git config --global --add safe.directory /workspace/velox-testing
```

### Results Issues

#### Problem: No graphs appearing

**Cause**: Only one data point exists

**Solution**: Run multiple times with different machine names (see [Example 3](#example-3-generate-graph-data-multiple-runs))

#### Problem: Results not updating

**Cause**: Cached results

**Solution**: Clear results:

```bash
ASV_CLEAR_RESULTS=true ./run_asv_benchmarks.sh --data-path /data/tpch
```

Or manually:

```bash
./scripts/clear_asv_results.sh
```

---

## Related Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Quick start guide for common workflows
- **[TUNING.md](TUNING.md)** - Performance tuning and optimization tips
- **[COMMIT_RANGE_BENCHMARKING.md](COMMIT_RANGE_BENCHMARKING.md)** - Detailed commit range benchmarking guide
- **[ASV Documentation](https://asv.readthedocs.io/)** - Official ASV documentation
- **[Velox Documentation](https://facebookincubator.github.io/velox/)** - Velox project documentation

---

## Support

For issues or questions:

1. Check the [Troubleshooting](#troubleshooting) section above
2. Review related documentation
3. Check script help: `./run_asv_benchmarks.sh --help`
4. Open an issue on [Velox GitHub](https://github.com/facebookincubator/velox/issues)

---

## Summary

This ASV benchmarking system provides two main workflows:

### Single Commit Benchmarking (`run_asv_benchmarks.sh`)

✅ **Fast**: Benchmark current code state in 15-30 minutes
✅ **Flexible**: Run all queries or specific ones
✅ **Interactive**: Immediate web UI feedback
✅ **Development-Friendly**: Quick iteration for code changes

### Commit Range Benchmarking (`run_asv_commit_range.sh`)

✅ **Automated**: Handles checkout, build, benchmark, and cleanup
✅ **Reproducible**: Fresh builds with `--no-cache` for each commit
✅ **Fast**: Uses sccache for C++ compilation acceleration
✅ **Safe**: Always restores Git state and cleans up images
✅ **Visual**: Generates HTML reports with interactive charts
✅ **Robust**: Handles errors gracefully

Choose the right tool for your workflow:
- **Developing/debugging?** Use `run_asv_benchmarks.sh`
- **Tracking performance over time?** Use `run_asv_commit_range.sh`
- **Need graphs?** Run `run_asv_benchmarks.sh` multiple times with unique machine names

Happy benchmarking! 🚀
