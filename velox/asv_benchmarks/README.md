# Velox ASV Benchmarks

This directory contains [ASV (Airspeed Velocity)](https://asv.readthedocs.io/) benchmarks for the Velox project. ASV is a tool for tracking Python package performance over time.

## Directory Structure

```
asv_benchmarks/
├── asv.conf.json           # ASV configuration
├── benchmarks/             # Benchmark implementations
│   └── tpch_benchmarks.py  # TPC-H benchmarks
├── .asv/                   # ASV results and cache (generated)
│   ├── results/            # Benchmark results
│   └── html/               # Generated HTML reports
└── README.md               # This file
```

## Prerequisites

1. **Build Python bindings** (e.g., for TPC-H):
   ```bash
   cd /path/to/velox/velox/experimental/cudf/benchmarks/python
   ./build.sh
   ```

   This builds the C++ wrapper and Python bindings in-place (no installation).

2. **Install ASV**:
   ```bash
   pip install asv
   ```

3. **Set environment variables**:
   ```bash
   export PYTHONPATH=/path/to/velox/velox/experimental/cudf/benchmarks/python:$PYTHONPATH
   export TPCH_DATA_PATH=/path/to/tpch/data
   ```

## Configuration

The `asv.conf.json` file contains ASV configuration. Key settings:

```json
{
  "repo": "/workspace/velox",           # Path to Velox git repository
  "environment_type": "existing",        # Use current Python environment
  "benchmark_dir": "benchmarks",         # Where benchmark files are located
  "results_dir": ".asv/results",         # Where to store results
  "html_dir": ".asv/html"                # Where to generate HTML reports
}
```

**Important**: Update the `repo` path to match your environment:
- Docker: typically `/workspace/velox`
- Host: typically `/home/user/projects/velox` or similar

## Running Benchmarks

From this directory:

```bash
cd /path/to/velox/asv_benchmarks

# Run all benchmarks
asv run

# Run specific benchmark
asv run --bench tpch_benchmarks.TimeQuery06

# Run benchmarks for specific commit
asv run commit_hash^!

# Compare performance between commits
asv continuous main HEAD
```

## Viewing Results

### Generate HTML Reports

```bash
asv publish
asv preview  # Opens http://localhost:8080
```

**Note**: With only one commit benchmarked, graphs won't be available. To see performance trends:

```bash
# Benchmark multiple commits
asv run HEAD~5..HEAD

# Regenerate HTML
asv publish
asv preview
```

### View in Terminal

```bash
asv show
```

### Docker Users

If running in Docker, serve the HTML:

```bash
asv publish
python3 -m http.server 8080 --directory .asv/html
```

Access from host: `http://localhost:8080` (ensure port is mapped)

Or copy results out:

```bash
# From host
docker cp <container_id>:/workspace/velox/asv_benchmarks/.asv/html ./results
```

## Understanding Results

- **Time values**: Displayed in milliseconds (ms), microseconds (µs), or seconds (s)
- **Regressions**: Performance slowdowns compared to previous commits
- **Grid/List view**: Click these in the HTML to see tabular results

## Adding New Benchmarks

### For TPC-H Queries

Edit `benchmarks/tpch_benchmarks.py` to add or modify query benchmarks.

### For New Benchmark Suites (e.g., TPC-DS)

1. Create new Python bindings in `velox/experimental/cudf/benchmarks/python/`
2. Create new benchmark file: `benchmarks/tpcds_benchmarks.py`
3. Import and use your new bindings:

```python
import cudf_tpcds_benchmark

class TimeQuery01:
    def setup(self, data_format):
        self.benchmark = cudf_tpcds_benchmark.CudfTpcdsBenchmark(
            data_path=os.environ['TPCDS_DATA_PATH'],
            data_format=data_format
        )
    
    def time_query_01(self, data_format):
        result = self.benchmark.run_query(1)
        return result.execution_time_ms
```

## Troubleshooting

### "No module named 'cudf_tpch_benchmark'"

Build the Python bindings and set PYTHONPATH:
```bash
cd /path/to/velox/velox/experimental/cudf/benchmarks/python
./build.sh

export PYTHONPATH=/path/to/velox/velox/experimental/cudf/benchmarks/python:$PYTHONPATH
```

### "No benchmarks found"

1. Verify Python bindings are installed: `python -c "import cudf_tpch_benchmark"`
2. Check `TPCH_DATA_PATH` is set and valid
3. Verify `repo` path in `asv.conf.json` is correct

### "dubious ownership" Git errors

In Docker, add the repository as safe:
```bash
git config --global --add safe.directory /workspace/velox
```

### Connector registration errors

The benchmarks use a shared instance to avoid re-registering the Velox connector. This is configured in the benchmark code with `_get_benchmark_instance()`.

## Documentation

For more details, see:
- [Python Bindings README](../velox/experimental/cudf/benchmarks/python/README.md)
- [ASV Documentation](https://asv.readthedocs.io/)
- [Velox Documentation](https://facebookincubator.github.io/velox/)

## Support

For issues:
- Check troubleshooting above
- Review [Python bindings documentation](../velox/experimental/cudf/benchmarks/python/)
- Open an issue on [Velox GitHub](https://github.com/facebookincubator/velox/issues)
