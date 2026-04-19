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

### Running the benchmark via launcher (recommended)

```bash
cd presto/slurm/presto-nvl72
./launch-run.sh -n <nodes> -s <scale_factor> [-i <iterations>] [additional sbatch options]

# examples
./launch-run.sh -n 8 -s 3000
./launch-run.sh -n 4 -s 10000 -i 3 --partition gpu --account myacct
```

The launcher:
- requires node count (-n/--nodes) and scale factor (-s/--scale-factor)
- accepts optional iterations (-i/--iterations, default 1)
- embeds nodes/SF/iterations in .out/.err filenames
- prints the first node’s hostname/IP when allocated and a ready-to-run SSH port-forward command to access the Presto Web UI on your machine (http://localhost:9200)

### Submitting directly (advanced)

```bash
export SCALE_FACTOR=3000
export NUM_ITERATIONS=1
sbatch --nodes 8 \
  --output "presto-tpch-run_n8_sf3000_i1_%j.out" \
  --error  "presto-tpch-run_n8_sf3000_i1_%j.err" \
  --export "ALL,SCALE_FACTOR=${SCALE_FACTOR},NUM_ITERATIONS=${NUM_ITERATIONS}" \
  run-presto-benchmarks.slurm
```

## Configuration

Primary configuration is passed via the launcher flags and environment. The `.slurm` script validates that required variables are set.

Key variables:

- SCALE_FACTOR: required (provided via `-s/--scale-factor`)
- NUM_ITERATIONS: required by the job; launcher defaults to 1 (`-i/--iterations` to override)
- NUM_NODES: derived from Slurm allocation; provided via `-n/--nodes` to launcher
- REPO_ROOT: auto-detected from script location
- LOGS: `${SCRIPT_DIR}/logs` by default
- IMAGE_DIR, DATA, CONFIGS: see below or override via environment if needed

Other defaults:
- WORKER_IMAGE: `presto-native-worker-gpu`
- NUM_GPUS_PER_NODE: `4`
- DATA: `/mnt/data/tpch-rs`
- IMAGE_DIR: `/mnt/data/images/presto`
- CONFIGS: `${REPO_ROOT}/presto/docker/config/generated/gpu`

### SBATCH Directives

- **Time limit**: 1 hour (adjust `--time` if needed)
- **Node allocation**: Full node (144 CPUs, 4 GPUs, exclusive)
- **Memory**: All available (`--mem=0`)
- `--nodes`, `--output`, and `--error` are passed by the launcher instead of being embedded in the `.slurm` file.

## Monitoring

```bash
# Check job queue
squeue -u $USER

# Monitor job output
tail -f presto-tpch-run_n<NODES>_sf<SCALE_FACTOR>_i<ITER>_<JOB_ID>.out

# Check logs during execution
tail -f logs/coord.log
tail -f logs/cli.log
tail -f logs/worker_0.log
```

## Coordinator IP and Web UI

After submission, the launcher waits until nodes are allocated, then prints:
- the first node’s hostname/IP
- an SSH port-forward command you can run locally to access the Presto Web UI

Example output snippet:

```text
Run this command on a machine to get access to the webUI:
    ssh -N -L 9200:<COORDINATOR_IP>:9200 <jump-host>
The UI will be available at http://localhost:9200
```

## Results

Results are saved to:
- **Logs**: `logs/` directory
- **CSV Summary**: `result_dir/summary.csv`
- **Historical Results**: `${REPO_ROOT}/benchmark-storage/YYYY/MM/DD/`

## Prerequisites

1. **Container images** must exist in `${IMAGE_DIR}`:
   - `presto-coordinator.sqsh`
   - `presto-native-worker-gpu.sqsh` or `presto-native-worker-cpu.sqsh`

2. **Data directory** must be accessible at `${DATA}` (will be mounted in containers)

3. **velox-testing repo** will be auto-cloned to `${REPO_ROOT}/velox-testing` if not present

## Reusing an analyzed Hive metastore across runs

Running `ANALYZE TABLE` from scratch on every clone is expensive and wasteful when
the underlying parquet data is shared.  The launchers support publishing and
consuming a pre-analyzed metastore snapshot, keyed by a user-supplied version
string plus scale factor.

Two env vars control sharing (defined in `defaults.env`):

- `HIVE_METASTORE_SHARED_ROOT` (default `/scratch/$USER/shared_hive_metadata`) —
  directory on a filesystem reachable from compute nodes where snapshots live.
- `HIVE_METASTORE_VERSION` (default empty) — version tag chosen by the operator.
  **Leave unset to disable sharing.**  Bump it whenever the Presto/velox worker
  image or the parquet data format changes, so stale snapshots do not leak into
  runs against a newer image.

Layout: `$HIVE_METASTORE_SHARED_ROOT/$HIVE_METASTORE_VERSION/tpchsf<SF>/…`

### Publishing — run once per (version, SF)

```bash
export HIVE_METASTORE_VERSION=HIVE-METASTORE-20260419
./launch-analyze-tables.sh -s 1 -n 1
# On success, if .../$HIVE_METASTORE_VERSION/tpchsf1/ is empty it gets
# populated atomically.  Subsequent analyze runs with the same version leave
# the existing snapshot untouched.
```

### Consuming — every subsequent clone / run

```bash
export HIVE_METASTORE_VERSION=HIVE-METASTORE-20260419
rm -rf .hive_metastore      # optional; only strictly needed if stale
./launch-run.sh -n 2 -s 1 -i 1
```

`launch-run.sh` populates `.hive_metastore/tpchsf<SF>/` from the shared snapshot
only when the local copy is absent; existing local snapshots are preserved.  If
neither local nor shared is available the run fails fast with a message pointing
at `launch-analyze-tables.sh`.

### Disabling sharing

Leave `HIVE_METASTORE_VERSION` unset.  Analyze does not publish; benchmark runs
require a local `.hive_metastore/tpchsf<SF>/` populated by an earlier analyze
in the same clone.

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
ls -lh /mnt/data/images/presto/*.sqsh
```

### Data directory issues
Verify data path is accessible:
```bash
ls -la /mnt/data/tpch-presto
```
