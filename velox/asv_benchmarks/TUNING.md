# ASV Benchmark Tuning Guide

This guide explains how to tune ASV benchmarks for more accurate and reliable timing measurements. Based on the [official ASV tuning documentation](https://asv.readthedocs.io/en/latest/tuning.html).

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

## ASV Command-Line Options

### For Long-Time Variation Averaging

```bash
# Run with interleaved rounds (recommended for production)
asv run --interleave-rounds

# Equivalent to running:
# Round 1: Query 1, Query 2, ..., Query 22
# Round 2: Query 1, Query 2, ..., Query 22
# Round 3: Query 1, Query 2, ..., Query 22
# Then average results across rounds
```

### For Appending New Samples

```bash
# Add more samples to existing results (same machine)
ASV_APPEND_SAMPLES=true ./run_asv_benchmarks.sh --data-path /data/tpch

# Or with ASV directly:
asv run --append-samples
```

### For CPU Affinity Pinning

```bash
# Pin to specific CPU core(s) - Linux only
asv run --cpu-affinity 0  # Pin to CPU 0
```

## Machine Tuning

For best results, especially on laptops with aggressive power management:

### 1. Basic Tuning with pyperf (Linux)

```bash
# Install pyperf
pip install pyperf

# Apply system tuning (requires root)
sudo python -m pyperf system tune

# This will:
# - Disable CPU frequency scaling
# - Disable turbo boost
# - Set CPU governor to 'performance'
# - Disable ASLR
# - More...
```

### 2. Manual CPU Frequency Locking

```bash
# Check current CPU frequency
cat /proc/cpuinfo | grep MHz

# Set CPU governor to performance (requires root)
sudo cpufreq-set -g performance

# Disable turbo boost (Intel)
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo

# Disable turbo boost (AMD)
echo 0 | sudo tee /sys/devices/system/cpu/cpufreq/boost
```

### 3. Isolate CPU Cores

For absolute best results, isolate CPU cores from the scheduler:

```bash
# Add to kernel boot parameters (requires reboot)
# Edit /etc/default/grub:
GRUB_CMDLINE_LINUX="isolcpus=0,1"

# Then update grub and reboot
sudo update-grub
sudo reboot

# Run benchmarks pinned to isolated core
asv run --cpu-affinity 0
```

## Multi-Run Strategy for Graphs

To generate graphs with statistical confidence intervals:

### Option 1: Multiple Machines (Recommended)
```bash
# Each run uses a unique machine name
./run_asv_multi_benchmarks.sh --data-path /data/tpch --count 5
```

### Option 2: Interleaved Rounds
```bash
# Modify tpch_benchmarks.py:
rounds = 5

# Run with interleaving
asv run --interleave-rounds
```

### Option 3: Repeated Appending
```bash
# First run
./run_asv_benchmarks.sh --data-path /data/tpch --no-preview
ASV_RECORD_SAMPLES=true

# Subsequent runs (same machine)
for i in {1..5}; do
    ASV_APPEND_SAMPLES=true ./run_asv_benchmarks.sh --data-path /data/tpch --no-preview
done

# View results
./run_asv_benchmarks.sh --data-path /data/tpch --no-publish
```

## Recommended Workflows

### Development Workflow
```bash
# Quick single run for testing
./run_asv_benchmarks.sh --data-path /data/tpch --bench "TimeQuery06"
```

### CI/CD Workflow
```bash
# 3 runs with balanced accuracy, no preview
./run_asv_multi_benchmarks.sh \
    --data-path /data/tpch \
    --count 3 \
    --skip-preview
```

### Production Benchmarking
1. Tune system with `pyperf system tune`
2. Update `tpch_benchmarks.py`: `repeat = 5, rounds = 3`
3. Run with interleaving:
```bash
./run_asv_benchmarks.sh \
    --data-path /data/tpch \
    --no-preview
```
4. Or use multi-run:
```bash
./run_asv_multi_benchmarks.sh \
    --data-path /data/tpch \
    --count 5
```

## Troubleshooting

### High Variability in Results
- **Symptom**: Large error bars, inconsistent timings
- **Solution**: 
  1. Increase `repeat` to 5-10
  2. Use `--interleave-rounds`
  3. Check for background processes: `top`, `htop`
  4. Apply system tuning: `sudo python -m pyperf system tune`

### Benchmarks Taking Too Long
- **Symptom**: Hours to complete
- **Solution**:
  1. Decrease `repeat` to 1-3
  2. Set `rounds = 1`
  3. Run specific queries: `--bench "TimeQuery06"`

### Thermal Throttling (Laptops)
- **Symptom**: Results slow down over time
- **Solution**:
  1. Ensure good cooling/ventilation
  2. Lock CPU frequency to lower value
  3. Use external cooling pad
  4. Use `--interleave-rounds` to spread heat over time

## References

- [ASV Tuning Documentation](https://asv.readthedocs.io/en/latest/tuning.html)
- [ASV Benchmark Attributes](https://asv.readthedocs.io/en/latest/benchmarks.html)
- [pyperf Documentation](https://pyperf.readthedocs.io/)

