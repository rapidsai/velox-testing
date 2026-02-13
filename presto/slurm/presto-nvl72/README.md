# Presto TPC-H Benchmark (NVL72)

This directory contains scripts for running Presto TPC-H benchmarks on CoreWeave NVL72 nodes.

## Directory Structure

```
presto-nvl72/
├── run-presto-benchmarks.slurm  # Main slurm job script with configuration
├── run-presto-benchmarks.sh     # Execution script
├── launch-run.sh                # Convenience launcher
├── functions.sh                 # Presto helper functions
├── echo_helpers.sh              # Logging helpers
├── logs/                        # Execution logs
└── result_dir/                  # Benchmark results
```

## Quick Start

### Running the Benchmark

```bash
cd /mnt/data/bzaitlen/presto-nvl72
./launch-run.sh
```

Or submit directly:

```bash
sbatch run-presto-benchmarks.slurm
```

## Configuration

**To change settings, edit the values directly in `run-presto-benchmarks.slurm`**

All configuration is at the top of the file in the "User Configuration" section.

### Configuration Variables

| Variable | Current Value | Description |
|----------|---------------|-------------|
| `SCALE_FACTOR` | 300 | TPC-H scale factor |
| `NUM_ITERATIONS` | 5 | Number of query iterations |
| `WORKER_IMAGE` | presto-native-worker-gpu | Worker container image |
| `NUM_NODES` | 4 | Number of nodes to allocate |
| `NUM_GPUS_PER_NODE` | 4 | GPUs per node |
| `DATA` | /mnt/data/tpch-rs/scale-300 | Data directory |
| `IMAGE_DIR` | /mnt/home/misiug/images | Container image directory |
| `LOGS` | /mnt/data/bzaitlen/presto-nvl72/logs | Log directory |

### SBATCH Directives

- **Time limit**: 1 hour (adjust `--time` if needed)
- **Node allocation**: Full node (144 CPUs, 4 GPUs, exclusive)
- **Memory**: All available (`--mem=0`)

## Monitoring

```bash
# Check job queue
squeue -u $USER

# Monitor job output
tail -f presto-tpch-run_<JOB_ID>.out

# Check logs during execution
tail -f logs/coord.log
tail -f logs/cli.log
tail -f logs/worker_0.log
```

## Results

Results are saved to:
- **Logs**: `logs/` directory
- **CSV Summary**: `result_dir/summary.csv`
- **Historical Results**: `${WORKSPACE}/benchmark-storage/YYYY/MM/DD/`

## Prerequisites

1. **Container images** must exist in `${IMAGE_DIR}`:
   - `presto-coordinator.sqsh`
   - `presto-native-worker-gpu.sqsh` or `presto-native-worker-cpu.sqsh`

2. **Data directory** must be accessible at `${DATA}` (will be mounted in containers)

3. **velox-testing repo** will be auto-cloned to `${WORKSPACE}/velox-testing` if not present

## Troubleshooting

### Coordinator fails to start
Check coordinator logs:
```bash
cat logs/coord.log
```

### Workers not registering
Check worker logs:
```bash
cat logs/worker_*.log
```

### Image not found
Verify images exist:
```bash
ls -lh /mnt/home/misiug/images/*.sqsh
```

### Data directory issues
Verify data path is accessible:
```bash
ls -la /mnt/data/tpch-presto
```
