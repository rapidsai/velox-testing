# Presto TPC-H Benchmark - Standalone Self-Contained Setup

This is a simplified, self-contained version of the Presto TPC-H benchmark launcher for Slurm.
Everything you need is in this single directory with minimal layers of indirection.

## Directory Structure

```
standalone/
├── launch.sh                    # Main entry point - submit job to Slurm
├── run.slurm                    # Slurm batch script with all logic
├── config-templates/            # Configuration templates
│   ├── common/
│   │   ├── jvm.config
│   │   ├── log.properties
│   │   └── hive.properties
│   ├── coordinator/
│   │   ├── config.properties
│   │   └── node.properties
│   └── worker/
│       ├── config.properties
│       └── node.properties
├── configs/                     # Generated at runtime (gitignored)
├── logs/                        # Runtime logs (gitignored)
├── result_dir/                  # Benchmark results (gitignored)
└── README.md                    # This file
```

## Usage

### Basic Usage

```bash
./launch.sh -n <nodes> -s <scale-factor> [-i <iterations>]
```

### Examples

```bash
# Run on 2 nodes with scale factor 1000, 3 iterations
./launch.sh -n 2 -s 1000 -i 3

# Run on 1 node with scale factor 100, 1 iteration (default)
./launch.sh -n 1 -s 100
```

### Options

- `-n, --nodes <count>` - Number of Slurm nodes (required)
- `-s, --scale-factor <sf>` - TPC-H scale factor (required)
- `-i, --iterations <n>` - Number of iterations (default: 1)

Any additional arguments are passed directly to `sbatch`.

## What Happens

1. **launch.sh** cleans up old files and submits the job to Slurm
2. **run.slurm** executes on the allocated nodes and:
   - Generates Presto configuration files from templates
   - Starts the coordinator on the first node
   - Starts workers (4 per node, one per GPU)
   - Sets up TPC-H schema and tables
   - Runs TPC-H queries
   - Saves results to `result_dir/`

## Configuration

### Environment Variables

You can customize behavior with environment variables before calling `launch.sh`:

```bash
export DATA_DIR=/path/to/tpch-data
export IMAGE_DIR=/path/to/images
export WORKER_IMAGE=presto-native-worker-gpu
export COORD_IMAGE=presto-coordinator
./launch.sh -n 2 -s 1000
```

### Config Templates

To modify Presto configuration:
1. Edit files in `config-templates/`
2. Use `__PLACEHOLDER__` syntax for runtime substitution
3. Available placeholders are defined in the `generate_configs()` function in `run.slurm`

### Memory Settings

Memory settings are hardcoded in `run.slurm` (search for "Memory calculations").
Adjust these based on your hardware:

```bash
RAM_GB=900              # Total system memory
HEAP_SIZE_GB=810        # JVM heap size
SYSTEM_MEM_GB=850       # Presto native worker system memory
QUERY_MEM_GB=807        # Presto native worker query memory
```

## Monitoring

After submitting, you'll see:

```bash
Job submitted with ID: 12345

Monitor with:
  squeue -j 12345
  tail -f presto-tpch_n2_sf1000_i3_12345.out
  tail -f logs/coordinator.log
  tail -f logs/worker_*.log
```

You can also access the Presto WebUI (instructions printed after job starts).

## Results

Results are saved to:
- `result_dir/` - Benchmark summary and query results
- `logs/` - Coordinator, worker, setup, and query logs
- `*.out` and `*.err` - Slurm stdout/stderr

## Differences from Original Setup

**Old Structure:**
- `launch-run.sh` → calls → `run-presto-benchmarks.slurm` → calls → `run-presto-benchmarks.sh`
- `run-presto-benchmarks.sh` sources `functions.sh` and `echo_helpers.sh`
- Config templates in `presto/docker/config/template/`
- Config generation via `presto/scripts/generate_presto_config.sh`
- Config modifications in `functions.sh:generate_configs()` and `functions.sh:duplicate_worker_configs()`

**New Structure:**
- `launch.sh` → submits → `run.slurm` (everything in one file)
- Config templates in `./config-templates/`
- Config generation inline in `run.slurm`
- No external script dependencies
- All logic self-contained in this directory

## Troubleshooting

**Job fails immediately:**
- Check `*.err` file for Slurm errors
- Verify image paths in environment variables
- Ensure data directory exists and is accessible

**Coordinator won't start:**
- Check `logs/coordinator.log`
- Verify port 9200 is not in use
- Check memory settings

**Workers won't register:**
- Check `logs/worker_*.log`
- Verify coordinator is accessible from workers
- Check GPU availability with `nvidia-smi`

**Queries fail:**
- Check `logs/queries.log`
- Verify data directory has TPC-H data in expected format
- Check schema name matches: `tpchsf<scale_factor>`
