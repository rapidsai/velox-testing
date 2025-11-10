# Benchmarking a Range of Velox Commits

This guide explains how to benchmark multiple Velox commits to track performance changes over time.

## Overview

The `run_asv_commit_range.sh` script automates the process of benchmarking multiple commits:

1. **For each commit in the range:**
   - Checks out the commit
   - Rebuilds Velox with sccache (using `--no-cache` for fresh builds)
   - Rebuilds ASV benchmark image (using `--no-cache`)
   - Runs ASV benchmarks with a unique machine name
   - Tags the Docker image with the commit hash

2. **After all commits are benchmarked:**
   - Publishes HTML reports for all results
   - Starts ASV preview server
   - Tags the most recent commit's image as `latest`
   - Cleans up intermediate tagged images

## Prerequisites

1. **Velox repository** with the commit range you want to benchmark
2. **TPC-H data** generated and available
3. **sccache authentication** set up (for faster builds)
4. **Docker** and Docker Compose installed

## Usage

### Basic Usage

```bash
cd /raid/avinash/projects/velox-testing/velox
./scripts/run_asv_commit_range.sh --commits HEAD~5..HEAD
```

This benchmarks the last 5 commits.

### Complete Options

```bash
./scripts/run_asv_commit_range.sh \
    --velox-repo /path/to/velox \
    --data-path /path/to/tpch_data \
    --results-path /path/to/asv_results \
    --sccache-auth-dir /path/to/.sccache-auth \
    --port 8080 \
    --commits HEAD~5..HEAD \
    --clear-results
```

### Command-Line Options

- `--velox-repo PATH` - Path to Velox repository (default: `/raid/avinash/projects/velox-testing/velox`)
- `--data-path PATH` - Path to TPC-H data directory (default: `/raid/avinash/projects/velox-testing/tpch_data`)
- `--results-path PATH` - Path to store ASV results (default: `/raid/avinash/projects/velox-testing/asv_results`)
- `--sccache-auth-dir PATH` - Path to sccache auth directory (default: `/raid/avinash/projects/velox-testing/.sccache-auth`)
- `--port PORT` - HTTP server port for preview (default: `8080`)
- `--commits RANGE` - Git commit range to benchmark (required)
- `--clear-results` - Clear previous results before starting
- `-h, --help` - Show help message

## Commit Range Syntax

The `--commits` argument accepts standard Git commit range syntax:

### Last N Commits

```bash
# Last 5 commits
--commits HEAD~5..HEAD

# Last 10 commits
--commits HEAD~10..HEAD
```

### Between Two Tags

```bash
--commits v1.0..v2.0
```

### Between Two Commits

```bash
--commits abc123..def456
```

### Specific Commits

```bash
# Single commit
--commits abc123^!

# Multiple specific commits (using `git rev-list`)
--commits abc123 def456 789ghi
```

## Examples

### Example 1: Benchmark Recent Development

Track performance changes in recent development:

```bash
./scripts/run_asv_commit_range.sh \
    --commits HEAD~10..HEAD \
    --clear-results
```

### Example 2: Benchmark Between Releases

Compare performance between two release tags:

```bash
./scripts/run_asv_commit_range.sh \
    --commits v0.0.1..v0.0.2
```

### Example 3: Benchmark Specific Feature Branch

Benchmark commits from a feature branch:

```bash
cd /raid/avinash/projects/velox-testing/velox
git checkout feature-branch
./scripts/run_asv_commit_range.sh \
    --commits main..feature-branch
```

### Example 4: Full Custom Configuration

```bash
./scripts/run_asv_commit_range.sh \
    --velox-repo /custom/path/to/velox \
    --data-path /custom/tpch/data \
    --results-path /custom/results \
    --sccache-auth-dir /custom/sccache-auth \
    --port 9090 \
    --commits HEAD~20..HEAD \
    --clear-results
```

## What Happens During Execution

### 1. Initialization

- Validates all paths and arguments
- Gets list of commits to benchmark (in chronological order)
- Displays commits and asks for confirmation
- Optionally clears previous results

### 2. For Each Commit

For each commit in the range (oldest to newest):

1. **Checkout**: Switches to the commit
2. **Build Velox**: Rebuilds `velox-adapters-build:latest` with sccache and `--no-cache`
3. **Run Benchmarks**: Builds ASV image with `--no-cache` and runs all TPC-H benchmarks
4. **Tag Image**: Tags the Docker image as `velox-adapters-build:<commit-hash>`

Each commit gets a unique machine name: `velox-commit-<hash>-<timestamp>`

### 3. Publishing and Preview

After all commits are benchmarked:

1. **Publish**: Generates HTML reports from all benchmark results
2. **Preview**: Starts ASV web server on specified port
3. **Cleanup**: Tags most recent commit as `latest`, removes intermediate images

### 4. Cleanup on Exit

When you stop the preview server (Ctrl+C) or if an error occurs:

1. Tags the most recent commit's image as `velox-adapters-build:latest`
2. Removes all other tagged commit images
3. Restores Git to original branch/commit

## Build Process Details

### Why `--no-cache`?

The script always uses `--no-cache` for both Velox and ASV image builds to ensure:

1. **Fresh Builds**: Each commit is built from scratch, eliminating cached layer issues
2. **Accurate Comparisons**: Performance measurements aren't affected by Docker cache artifacts
3. **Reproducibility**: Builds are deterministic and reproducible

### sccache Acceleration

While Docker cache is disabled, **sccache is still enabled** to speed up C++ compilation:

- sccache caches compiled object files (not Docker layers)
- Provides 2-10x speedup depending on code changes
- Shared across all commits being benchmarked

## Viewing Results

### During Benchmarking

Monitor progress in the terminal output:

```
============================================
  Commit 3/10: a1b2c3d
  Add feature X for better performance
============================================

Step 1: Building Velox-adapters-build:latest with sccache (--no-cache)...
✓ Velox image built successfully

Step 2: Running ASV benchmarks for commit a1b2c3d (--no-cache)...
✓ Benchmarks completed for commit a1b2c3d

Step 3: Tagging Docker image...
✓ Tagged as velox-adapters-build:a1b2c3d
```

### After Completion

Access the web interface at `http://localhost:8080` (or your specified port):

1. **Summary**: Overview of all benchmarks across commits
2. **Timeline**: Performance trends over time
3. **Regressions**: Automatic detection of performance regressions
4. **Graphs**: Interactive charts for each benchmark

## Troubleshooting

### Build Failures

If a build fails for a specific commit:

```
Error: Failed to build Velox image for commit a1b2c3d
```

The script will **exit** (not continue to next commit). Fix the issue and re-run.

### Benchmark Failures

If benchmarks fail for a specific commit:

```
Error: Benchmarks failed for commit a1b2c3d
Continuing with next commit...
```

The script will **continue** with the next commit. The failed commit's results won't be included.

### Out of Disk Space

Each commit build can use significant disk space. Monitor with:

```bash
# Check disk usage
df -h

# Check Docker disk usage
docker system df

# Clean up Docker (if needed)
docker system prune -a
```

### Memory Issues

If builds fail due to memory issues, consider:

1. Reducing parallel jobs in CMake
2. Closing other applications
3. Increasing swap space

## Performance Considerations

### Time Estimates

Benchmarking time depends on:

- **Number of commits**: ~30-60 minutes per commit
- **Velox build time**: ~15-30 minutes (with sccache)
- **Benchmark run time**: ~15-30 minutes (all 22 TPC-H queries)

Example: 10 commits ≈ 5-10 hours total

### Disk Space

Each commit requires:

- **Velox build**: ~5-10 GB
- **Docker images**: ~3-5 GB per tagged image
- **Benchmark results**: ~100-500 MB per commit

Example: 10 commits ≈ 50-100 GB total

### sccache Benefits

With sccache enabled:

- **First commit**: Full build (~30 minutes)
- **Subsequent commits**: Incremental builds (~5-15 minutes)
- **Total speedup**: 2-5x faster than without sccache

## Best Practices

### 1. Use Meaningful Commit Ranges

```bash
# Good: Track recent development
--commits HEAD~10..HEAD

# Good: Compare releases
--commits v1.0..v2.0

# Avoid: Too many commits
--commits HEAD~100..HEAD  # Will take days!
```

### 2. Clear Results for Fresh Comparison

```bash
# Start fresh for clean comparison
./scripts/run_asv_commit_range.sh \
    --commits HEAD~5..HEAD \
    --clear-results
```

### 3. Monitor System Resources

```bash
# In another terminal, monitor resources
watch -n 5 'docker stats --no-stream && echo && df -h | grep -E "Filesystem|/raid"'
```

### 4. Run During Off-Hours

Since benchmarking takes hours, consider:

- Running overnight
- Using `nohup` or `screen` for long-running sessions
- Monitoring remotely via SSH

### 5. Document Results

After benchmarking:

1. Take screenshots of interesting trends
2. Export CSV data for further analysis
3. Note any anomalies or unexpected results
4. Archive results for future reference

## Integration with CI/CD

This script can be integrated into CI/CD pipelines:

```bash
#!/bin/bash
# In your CI pipeline

# Benchmark last 5 commits on main branch
./scripts/run_asv_commit_range.sh \
    --commits HEAD~5..HEAD \
    --clear-results \
    --port 8080 &

# Get the PID
PREVIEW_PID=$!

# Wait for preview to start
sleep 30

# Take a snapshot of results (optional)
curl http://localhost:8080 > benchmark-snapshot.html

# Stop preview
kill $PREVIEW_PID

# Archive results
tar -czf asv-results-$(date +%Y%m%d).tar.gz $RESULTS_PATH
```

## Related Documentation

- [ASV Tuning Guide](TUNING.md) - Optimize benchmark parameters
- [Commit Range Examples](COMMIT_RANGE.md) - More commit range syntax examples
- [ASV Documentation](https://asv.readthedocs.io/) - Official ASV docs

## Summary

The `run_asv_commit_range.sh` script provides a complete solution for benchmarking multiple Velox commits:

✅ **Automated**: Handles checkout, build, benchmark, and cleanup
✅ **Reproducible**: Fresh builds with `--no-cache` for each commit
✅ **Fast**: Uses sccache for C++ compilation acceleration
✅ **Safe**: Always restores Git state and cleans up images
✅ **Visual**: Generates HTML reports with interactive charts
✅ **Robust**: Handles errors gracefully and continues when possible

Perfect for tracking performance changes over time and identifying regressions.

