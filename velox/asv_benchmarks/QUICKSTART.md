# ASV Benchmarks - Quick Start Guide

## Prerequisites

1. **Build the ASV benchmark image** (one-time setup):
   ```bash
   cd /raid/avinash/projects/velox-testing/velox/scripts
   ./build_asv_image.sh
   ```

2. **Ensure TPC-H data is available** at a known path

## Run Benchmarks

### Generate Graphs (Recommended)

Run 3 times with auto-generated unique machine names to create graph data:

```bash
cd /raid/avinash/projects/velox-testing/velox/scripts

./run_asv_multi_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/ \
  --count 3
```

**Result**: View graphs at http://localhost:8080 showing 3 data points

### Single Run (No Graphs)

```bash
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/
```

### Specific Query

```bash
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --bench "tpch_benchmarks.TimeQuery06"
```

### Interactive Mode (Debugging)

```bash
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --interactive
```

Inside the container:
```bash
# Run all benchmarks
asv run --show-stderr

# Run specific benchmark
asv run --show-stderr --bench "tpch_benchmarks.TimeQuery06"

# Generate and view results
asv publish
asv preview --port 8080
```

## Common Scenarios

### 1. Compare Before/After Optimization

```bash
# Baseline run
ASV_MACHINE="baseline" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/ \
  --no-preview

# Make code changes and rebuild image
./build_asv_image.sh --rebuild

# Optimized run
ASV_MACHINE="optimized" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/
```

**Result**: Graph with 2 points comparing baseline vs optimized

### 2. Measure Performance Variance

```bash
./run_asv_multi_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --count 5
```

**Result**: Graph with 5 points showing variance

### 3. Fresh Start

Clear old results and start fresh:

```bash
ASV_CLEAR_RESULTS=true ./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --no-preview

# Then run multiple times
./run_asv_multi_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --count 3
```

## Environment Variables

Control behavior with these environment variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `ASV_MACHINE` | `docker-container` | Machine name for this run |
| `ASV_AUTO_MACHINE` | `false` | Auto-generate unique machine name |
| `ASV_RECORD_SAMPLES` | `false` | Record variance within run |
| `ASV_CLEAR_RESULTS` | `false` | Clear old results before run |
| `ASV_SKIP_SMOKE_TEST` | `false` | Skip initial data validation |
| `ASV_PREVIEW` | `true` | Start web server after benchmarks |

Example:
```bash
ASV_SKIP_SMOKE_TEST=true ASV_CLEAR_RESULTS=true \
./run_asv_benchmarks.sh --data-path /data/tpch
```

## Results Location

- **Host**: `../asv_benchmarks/results/` (or your `--results-path`)
- **Container**: `/asv_results`
- **HTML**: `../asv_benchmarks/results/html/`

## Troubleshooting

### No graphs appearing?
- **Problem**: Only 1 data point
- **Solution**: Run at least 2 times with different machine names
  ```bash
  ./run_asv_multi_benchmarks.sh --data-path /data/tpch --count 2
  ```

### Smoke test fails?
- **Problem**: Data format issues
- **Solution**: Verify TPC-H data is in Hive-style Parquet format
- **Skip it**: `ASV_SKIP_SMOKE_TEST=true ./run_asv_benchmarks.sh ...`

### Need to rebuild image?
- **When**: After code changes to C++ or Cython bindings
- **How**: `./build_asv_image.sh --rebuild`

### Want to test without running benchmarks?
- **Interactive mode**: `./run_asv_benchmarks.sh --data-path /data/tpch -i`

## File Structure

```
velox/
├── scripts/
│   ├── build_asv_image.sh          # Build ASV Docker image
│   ├── run_asv_benchmarks.sh       # Run single benchmark
│   └── run_asv_multi_benchmarks.sh # Run multiple (for graphs)
└── asv_benchmarks/
    ├── Dockerfile                   # ASV image definition
    ├── entrypoint.sh               # Container entrypoint
    ├── machine.json                # Machine metadata
    ├── asv.conf.json               # ASV configuration
    ├── benchmarks/                 # Benchmark definitions
    └── results/                    # Results directory (created on run)
        ├── machine.json            # Runtime machine config
        └── html/                   # Generated web reports
```

## Next Steps

For detailed information:
- **Graph Generation**: See [GRAPH_GENERATION.md](GRAPH_GENERATION.md)
- **Script Help**: Run with `--help` flag
- **ASV Documentation**: https://asv.readthedocs.io/

Happy benchmarking! 🚀




