# Benchmarking Commit Ranges with ASV

This guide explains how to benchmark multiple commits to see performance trends over time.

## Overview

ASV supports benchmarking commit ranges, allowing you to:
- Compare performance across different commits
- Track performance regressions/improvements over time
- Visualize performance trends with graphs
- Benchmark specific version ranges (e.g., between releases)

## Prerequisites

For commit range benchmarking, you **must** use `environment_type: virtualenv` in `asv.conf.json` (not `existing`).

This is because ASV needs to checkout different commits and rebuild the code for each commit. The `existing` environment type only benchmarks the current state without checking out commits.

Current configuration in `asv.conf.json`:
```json
{
    "environment_type": "virtualenv",
    "pythons": ["3.9"],
    "build_command": [...],
    "install_command": [...]
}
```

## Commit Range Syntax

ASV uses Git commit range syntax:

| Syntax | Description | Example |
|--------|-------------|---------|
| `HEAD^!` | Single commit (HEAD) | Default |
| `HEAD~N..HEAD` | Last N commits | `HEAD~5..HEAD` (last 5 commits) |
| `<hash>^!` | Single specific commit | `a1b2c3d^!` |
| `<hash1>..<hash2>` | Range between commits | `v1.0..v2.0` |
| `<branch>` | All commits on branch | `main`, `develop` |
| `ALL` | All commits (based on `branches` config) | Benchmarks all commits |

**Note:** The `^!` suffix specifies a single commit (not a range). Without it, ASV may interpret the commit as a range.

## Usage Examples

### 1. Single Script - Benchmark a Commit Range

```bash
# Benchmark last 5 commits
cd velox/scripts
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --commits "HEAD~5..HEAD"

# Benchmark between two tags
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --commits "v1.0..v2.0"

# Benchmark all commits on main branch
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --commits "main"
```

### 2. Multi-Run Script - Multiple Runs for Each Commit

For statistical robustness, you can run benchmarks multiple times for each commit:

```bash
# Run 3 times for each of the last 5 commits
cd velox/scripts
./run_asv_multi_benchmarks.sh \
    --data-path /data/tpch \
    --count 3 \
    --commits "HEAD~5..HEAD"
```

This creates a matrix of results:
- 5 commits × 3 runs each = 15 total benchmark runs
- Each commit gets multiple data points (for variance analysis)

### 3. Environment Variable Method

You can also set the commit range via environment variable:

```bash
# Single run
ASV_COMMIT_RANGE="HEAD~5..HEAD" ./run_asv_benchmarks.sh --data-path /data/tpch

# Multi-run
ASV_COMMIT_RANGE="HEAD~5..HEAD" ./run_asv_multi_benchmarks.sh --data-path /data/tpch --count 3
```

## How It Works

### 1. Environment Type: `virtualenv`

When using `virtualenv` mode, ASV:
1. Creates isolated virtual environments for each commit
2. Checks out each commit in the range
3. Runs `build_command` to compile the C++ code
4. Runs `install_command` to install Python bindings
5. Executes all benchmarks for that commit
6. Moves to the next commit and repeats

### 2. Build Process for Each Commit

For each commit, ASV automatically:

```bash
# 1. Checkout commit
git checkout <commit_hash>

# 2. Build C++ wrapper (from asv.conf.json build_command)
cd /workspace/velox/velox/experimental/cudf/benchmarks/python
./build.sh

# 3. Install Python bindings (from asv.conf.json install_command)
pip install --no-build-isolation -e /workspace/velox/velox/experimental/cudf/benchmarks/python

# 4. Run benchmarks
asv run <commit_hash>
```

### 3. Result Storage

Results are stored in `/asv_results/<machine>/<commit_hash>-<env>.json`:

```
asv_results/
├── docker-run-1234567890/
│   ├── a1b2c3d-py3.9-virtualenv.json  # Commit 1 results
│   ├── e4f5g6h-py3.9-virtualenv.json  # Commit 2 results
│   └── i7j8k9l-py3.9-virtualenv.json  # Commit 3 results
└── html/
    └── graphs/  # Generated graphs showing trends
```

## Viewing Results

After benchmarking, view results with:

```bash
# Results are automatically published and preview server starts
# Access at: http://localhost:8081 (or your specified port)
```

The ASV web interface will show:
- **Summary Table**: Performance metrics for each commit
- **Graphs**: Line charts showing performance trends over commits
- **Regressions**: Automatic detection of performance drops
- **Detailed Results**: Drill-down into individual benchmark results

## Best Practices

### 1. Limit the Range for Initial Testing

Don't benchmark too many commits at once initially:

```bash
# Good: Start with last 3-5 commits
./run_asv_benchmarks.sh --data-path /data/tpch --commits "HEAD~3..HEAD"

# Avoid: Too many commits on first run
# ./run_asv_benchmarks.sh --data-path /data/tpch --commits "ALL"
```

### 2. Use Multi-Run for Important Comparisons

When comparing critical changes:

```bash
# Run 5 times each for statistical significance
./run_asv_multi_benchmarks.sh \
    --data-path /data/tpch \
    --count 5 \
    --commits "HEAD~2..HEAD"
```

### 3. Skip Existing Results

Use `ASV_SKIP_EXISTING=true` (default) to avoid re-running benchmarks:

```bash
# Only benchmark new commits
ASV_SKIP_EXISTING=true ./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --commits "HEAD~10..HEAD"
```

### 4. Clear Results for Fresh Start

```bash
# Clear old results before starting
cd velox/scripts
./run_asv_multi_benchmarks.sh \
    --data-path /data/tpch \
    --count 3 \
    --commits "HEAD~5..HEAD" \
    --clear-results
```

## Troubleshooting

### Issue: "fatal: bad revision"

**Cause:** Commit or branch doesn't exist in the repository.

**Solution:** Verify the commit/branch exists:
```bash
git log --oneline -10  # Show last 10 commits
git branch -a          # Show all branches
```

### Issue: Build fails for older commits

**Cause:** Older commits may have different build requirements or incompatible code.

**Solution:** 
1. Check if `build.sh` existed in that commit
2. Update `build_command` in `asv.conf.json` to handle different versions
3. Or limit range to recent commits only

### Issue: "No results to publish"

**Cause:** Benchmarks failed or no commits were benchmarked.

**Solution:**
1. Check `asv.conf.json` `branches` setting matches your branch
2. Verify commits are on the configured branch
3. Check Docker logs for build/benchmark errors

### Issue: Slow performance

**Cause:** Building for each commit takes time (especially with C++ code).

**Solution:**
1. Use fewer commits initially
2. Enable `--skip-existing` to avoid re-runs
3. Use Docker layer caching for dependencies
4. Consider using `ccache` for C++ compilation

## Example Workflow

Here's a complete workflow for benchmarking the last 5 commits:

```bash
# 1. Ensure you're in the right directory
cd /raid/avinash/projects/velox-testing/velox/scripts

# 2. Clear previous results (optional)
rm -rf ../asv_benchmarks/results/*

# 3. Run benchmarks 3 times for each commit
./run_asv_multi_benchmarks.sh \
    --data-path /data/tpch \
    --count 3 \
    --commits "HEAD~5..HEAD" \
    --port 8081

# 4. View results at http://localhost:8081
# Graphs will show performance trends across all 5 commits
```

## Advanced: Comparing Branches

To compare performance between two branches:

```bash
# Benchmark main branch (last 5 commits)
ASV_MACHINE="main-branch" ./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --commits "main~5..main" \
    --no-preview

# Benchmark feature branch (last 5 commits)  
ASV_MACHINE="feature-branch" ./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --commits "feature~5..feature" \
    --no-preview

# Compare results in web interface
./run_asv_benchmarks.sh --data-path /data/tpch --interactive
# Then in container:
asv compare main-branch feature-branch
```

## Configuration Reference

Relevant `asv.conf.json` settings:

```json
{
    "environment_type": "virtualenv",  // REQUIRED for commit ranges
    "branches": ["python-bindings-for-velox-cudf-tpch-benchmarks"],  // Filter commits by branch
    "build_command": ["..."],          // Build C++ code for each commit
    "install_command": ["..."],        // Install Python bindings for each commit
    "pythons": ["3.9"]                 // Python version(s) to test
}
```

## Summary

**Key Points:**
- Use `--commits` or `ASV_COMMIT_RANGE` to specify commit ranges
- Requires `environment_type: virtualenv` in `asv.conf.json`
- ASV automatically checks out and builds each commit
- Results show performance trends over time in graphs
- Use `run_asv_multi_benchmarks.sh` with `--commits` for robust statistical analysis

**Quick Start:**
```bash
# Benchmark last 3 commits, 3 runs each
cd velox/scripts
./run_asv_multi_benchmarks.sh \
    --data-path /data/tpch \
    --count 3 \
    --commits "HEAD~3..HEAD"
```

For more information, see:
- `QUICKSTART.md` - Basic ASV usage
- `GRAPH_GENERATION.md` - Generating performance graphs
- [ASV Documentation](https://asv.readthedocs.io/)

