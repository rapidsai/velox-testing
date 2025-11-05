# ASV Graph Generation Guide

## Overview

With `environment_type: "existing"`, ASV can only benchmark the current state (no git commit tracking). To generate graphs, we need multiple data points. This is achieved by running benchmarks multiple times with **unique machine names**.

## How It Works

Each unique machine name creates a separate data series in ASV:
- **Machine Name** = **Data Point** on the graph
- Running 3 times with 3 different machine names = 3 points on the graph
- `--record-samples` captures variance within each run

## Quick Start: Generate Graphs

### Option 1: Automated Multi-Run (Recommended)

Run multiple benchmarks automatically with unique machine names:

```bash
cd /raid/avinash/projects/velox-testing/velox/scripts

# Run 3 times (creates 3 data points for graphs)
./run_asv_multi_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/ \
  --count 3
```

**Result**: 3 data points with auto-generated names like:
- `docker-run-1730612345`
- `docker-run-1730612456` 
- `docker-run-1730612567`

### Option 2: Manual Runs with Custom Names

Run individually with meaningful names:

```bash
cd /raid/avinash/projects/velox-testing/velox/scripts

# Baseline run
ASV_MACHINE="baseline" \
ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/ \
  --no-preview

# After optimization changes
ASV_MACHINE="optimized" \
ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/ \
  --no-preview

# Final validation
ASV_MACHINE="final" \
ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh \
  --data-path ../../presto/testing/integration_tests/data/tpch/ \
  --results-path ../asv_benchmarks/results/
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASV_MACHINE` | `docker-container` | Machine name for this run |
| `ASV_AUTO_MACHINE` | `false` | Auto-generate unique machine name with timestamp |
| `ASV_RECORD_SAMPLES` | `false` | Record multiple samples for variance analysis |
| `ASV_SKIP_EXISTING` | `true` | Skip if already benchmarked (set false to re-run) |
| `ASV_PREVIEW` | `true` | Start preview server after benchmarks |

## Use Cases

### 1. Performance Variability Testing

Run same code multiple times to measure variance:

```bash
./run_asv_multi_benchmarks.sh \
  --data-path /data/tpch \
  --count 5
```

**Graph shows**: 5 points showing performance variability

### 2. Before/After Comparison

```bash
# Before optimization
ASV_MACHINE="before" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch --no-preview

# Apply optimization changes, rebuild image
./build_asv_image.sh --rebuild

# After optimization
ASV_MACHINE="after" ASV_RECORD_SAMPLES=true \
./run_asv_benchmarks.sh --data-path /data/tpch
```

**Graph shows**: 2 points comparing before/after performance

### 3. Multi-Configuration Testing

```bash
# Test different configurations
for config in default tuned aggressive; do
    ASV_MACHINE="config-$config" \
    ASV_RECORD_SAMPLES=true \
    ./run_asv_benchmarks.sh \
      --data-path /data/tpch \
      --no-preview
done

# View all results
./run_asv_benchmarks.sh --data-path /data/tpch --interactive
# Then in container: asv publish && asv preview --port 8080
```

**Graph shows**: 3 points comparing different configurations

## Viewing Results

After running benchmarks, view at **http://localhost:8080**

You'll see:
- **Summary Grid**: All benchmarks with color-coded performance
- **Individual Graphs**: Each query with multiple data points
- **Statistics**: Mean, std deviation, min/max for each series

## Troubleshooting

### No Graphs Appearing

**Problem**: Only one data point exists  
**Solution**: Run at least 2 times with different machine names

```bash
./run_asv_multi_benchmarks.sh --data-path /data/tpch --count 2
```

### Results Not Saving

**Problem**: Results directory not mounted correctly  
**Solution**: Check `--results-path` exists and is writable

```bash
mkdir -p ./asv_results
./run_asv_benchmarks.sh --data-path /data/tpch --results-path ./asv_results
```

### Machine Name Conflicts

**Problem**: Using same machine name overwrites previous results  
**Solution**: Use `ASV_AUTO_MACHINE=true` for unique names

```bash
ASV_AUTO_MACHINE=true ./run_asv_benchmarks.sh --data-path /data/tpch
```

## Advanced: Manual machine.json Updates

If you need full control over machine metadata:

```bash
# Create custom machine.json
cat > /tmp/custom-machine.json << 'EOF'
{
    "my-custom-run": {
        "machine": "my-custom-run",
        "os": "Linux",
        "arch": "x86_64",
        "cpu": "AMD EPYC 7742",
        "ram": "512GB",
        "gpu": "NVIDIA A100"
    }
}
EOF

# Copy to container and run
docker cp /tmp/custom-machine.json velox-asv-benchmark:/workspace/velox-testing/velox/asv_benchmarks/machine.json
docker exec velox-asv-benchmark bash -c "cd /workspace/velox-testing/velox/asv_benchmarks && asv run --machine my-custom-run --record-samples"
```

## Best Practices

1. **Use meaningful names** for manual runs: `baseline`, `optimized`, `gpu-v1`, etc.
2. **Use auto-generated names** for variability testing: `ASV_AUTO_MACHINE=true`
3. **Always use `--record-samples`** to capture variance
4. **Run at least 3 times** for meaningful statistical analysis
5. **Clear results** between major changes: `ASV_CLEAR_RESULTS=true`

## Example Workflow

```bash
# Start fresh
ASV_CLEAR_RESULTS=true ./run_asv_benchmarks.sh \
  --data-path /data/tpch --no-preview

# Run baseline (3 times for statistics)
./run_asv_multi_benchmarks.sh \
  --data-path /data/tpch \
  --count 3 \
  --skip-preview

# Make code changes...
# Rebuild image...
./build_asv_image.sh --rebuild

# Run optimized version (3 times)
./run_asv_multi_benchmarks.sh \
  --data-path /data/tpch \
  --count 3

# View graphs at http://localhost:8080
# You'll see 6 data points: 3 baseline + 3 optimized
```

## Summary

**Key Concept**: Each unique machine name = one data point on the graph

| Goal | Command |
|------|---------|
| Quick graphs (3 points) | `./run_asv_multi_benchmarks.sh --data-path /data/tpch` |
| Compare configs | `ASV_MACHINE="name" ./run_asv_benchmarks.sh ...` |
| Auto-unique names | `ASV_AUTO_MACHINE=true ./run_asv_benchmarks.sh ...` |
| Record variance | `ASV_RECORD_SAMPLES=true ./run_asv_benchmarks.sh ...` |

Happy benchmarking! 🚀




