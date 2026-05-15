# Presto TPC-H Benchmark (NVL72)

This directory contains scripts for running Presto TPC-H benchmarks on CoreWeave NVL72 nodes.

## Directory Structure

```
presto-nvl72/
├── defaults.env                     # Cluster-specific path defaults (override via env)
├── functions.sh                     # Presto helper functions
├── echo_helpers.sh                  # Logging helpers
├── enroot-decompress.sh             # Auto-detecting decompressor for enroot image pulls
│
├── pull_ghcr_image.sh               # Pull a GHCR image and save as .sqsh
│
├── launch-run.sh                    # Submit a benchmark run job
├── run-presto-benchmarks.slurm      # SLURM job script for benchmarks
├── run-presto-benchmarks.sh         # Benchmark execution logic
│
├── launch-analyze-tables.sh         # Submit an analyze-tables job
├── run-analyze-tables.slurm         # SLURM job script for ANALYZE TABLE
├── run-analyze-tables.sh            # Analyze-tables execution logic
│
├── launch-gen-data.sh               # Submit a TPC-H data generation job
├── gen-tpch-data.slurm              # SLURM job script for data generation
│
├── run-sweep.sh                     # Run benchmark + post results for a sweep of configs
├── run_interactive.sh               # Start an interactive Presto session
│
├── logs/                            # Execution logs
└── result_dir/                      # Benchmark results
```

## Quick Start

### 1. Pull container images

Images must be pre-pulled as `.sqsh` files before running benchmarks:

```bash
cd presto/slurm/presto-nvl72
./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:<coordinator-tag>
./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:<worker-tag>
```

Images are saved to `${IMAGE_DIR}` (default: `/scratch/${USER}/images/presto`).

### 2. Generate TPC-H data (one-time per scale factor)

```bash
# Pull the tpchgen-cli image first
./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:tpchgen-cli

./launch-gen-data.sh -s <scale_factor> -o <output_dir>
```

### 3. Analyze tables (one-time per scale factor / image version)

```bash
./launch-analyze-tables.sh -s <scale_factor> -n <nodes> \
    -w <worker-image-name> -c <coord-image-name>
```

### 4. Run benchmarks

```text
./launch-run.sh \
    -n, --nodes NODES \
    -s, --scale-factor SCALE_FACTOR \
    -w, --worker-image WORKER_IMAGE \
    -c, --coord-image COORD_IMAGE \
    [-i, --iterations ITERATIONS] \
    [--disable-gds] \
    [-m, --metrics] \
    [-p, --profile] \
    [--nsys-worker-id WORKER_ID] \
    [-q, --queries QUERIES] \
    [--worker-env-file PATH]
```

**Required:**

- `-n, --nodes NODES` — Number of SLURM nodes.
- `-s, --scale-factor SF` — TPC-H scale factor.
- `-w, --worker-image NAME` — Worker image name (without `.sqsh`), expected at `${IMAGE_DIR}/<NAME>.sqsh`.
- `-c, --coord-image NAME` — Coordinator image (without `.sqsh`), expected at `${IMAGE_DIR}/<NAME>.sqsh`.

**Optional:**

- `-i, --iterations N` — Iterations per query (default: `2`).
- `-g, --num-gpus-per-node N` — GPUs (and workers) per node (default: `4`).
- `--no-numa` — Disable NUMA pinning. Default: NUMA pinning performed.
- `--cpu` — CPU benchmark variant (forces `-g 1` and `--no-numa`).
- `-o, --output-path PATH` — Copy `result_dir/` to this path after the job completes.
- `--disable-gds` — Use POSIX I/O (`KVIKIO_COMPAT_MODE=ON`). Default: GDS enabled.
- `-m, --metrics` — Pull per-query stats from the coordinator REST API into `result_dir/metrics/`.
- `-p, --profile` — Capture an nsys report per query for one worker. Worker image must include the `nsys` CLI, which
  must be on `PATH` inside the worker container. The recommended approach is a symlink in the image build:

  ```bash
  ln -sf /opt/nvidia/nsight-systems-cli/<version>/bin/nsys /usr/local/bin/nsys
  ```

- `--nsys-worker-id ID` — Worker to profile (default: `0`). Requires `-p`.
- `-q, --queries LIST` — Comma-separated query numbers, e.g. `1,5,9` (default: all 22).
- `--worker-env-file PATH` — File sourced inside each worker before `presto_server` starts (default: `./worker.env`, sets `KVIKIO_TASK_SIZE=16MiB` and `KVIKIO_NTHREADS=16`).


```bash
# Examples
./launch-run.sh -n 8 -s 3000 \
    -w presto-native-worker-gpu-v1 -c presto-coordinator-v1

# Use POSIX I/O instead of GDS
./launch-run.sh -n 8 -s 3000 \
    -w presto-native-worker-gpu-v1 -c presto-coordinator-v1 \
    --disable-gds

# Use nsys to profile query 5 and 6 for worker 2
./launch-run.sh -n 8 -s 3000 \
    -w presto-native-worker-gpu-v1 -c presto-coordinator-v1 \
    -p --nsys-worker-id 2 -q 5,6

./launch-run.sh -n 4 -s 10000 -i 3 \
    -w presto-native-worker-gpu-v1 -c presto-coordinator-v1 \
    --partition gpu --account myacct
```

The launcher:
- requires node count (`-n/--nodes`), scale factor (`-s/--scale-factor`), worker image (`-w/--worker-image`), and coordinator image (`-c/--coord-image`)
- accepts optional iterations (`-i/--iterations`, default 2)
- embeds nodes/SF/iterations in `.out`/`.err` filenames
- prints a ready-to-run SSH port-forward command to access the Presto Web UI at http://localhost:9200

### 5. Run a sweep

```bash
./run-sweep.sh \
    --sku-name raplab-gb200-nvl72 \
    --storage-configuration-name <storage-config-name> \
    --velox-branch <branch> \
    --presto-branch <branch> \
    --velox-repo <url> \
    --presto-repo <url> \
    [-n "8 4"] \
    [-s "3000 10000"] \
    [-i <iterations>]
```

`--cache-state` is derived automatically: `lukewarm` for 1 iteration, `warm` for 2+. Pass `--cache-state` explicitly to override.

## Configuration

### Environment variables

Override any of these by exporting before running:

| Variable | Default | Description |
|---|---|---|
| `NODELIST` | All nodes | List of nodes to use |

### Path defaults (`defaults.env`)

Override any of these by exporting before running:

| Variable | Default | Description |
|---|---|---|
| `DATA` | `/scratch/${USER}/tpch-rs-float-no-delta` | TPC-H parquet dataset root |
| `IMAGE_DIR` | `/scratch/${USER}/images/presto` | Directory containing `.sqsh` image files |
| `RESULTS_BASE` | `${HOME}/${VT_WORKSPACE}/results` | Benchmark result output root |
| `HIVE_METASTORE_SHARED_ROOT` | `/scratch/${USER}/shared_hive_metadata` | Shared pre-analyzed metastore snapshots |
| `HIVE_METASTORE_VERSION` | `HIVE-METASTORE-20260419-no-delta` | Metastore snapshot version tag |

## Monitoring

```bash
# Check job queue
squeue -u $USER

# Monitor job output
tail -f presto-tpch-run_n<NODES>_sf<SF>_i<ITER>_<JOB_ID>.out

# Check logs during execution
tail -f logs/coord.log
tail -f logs/cli.log
tail -f logs/worker_0.log
```

## Coordinator Web UI

After submission, the launcher waits until nodes are allocated, then prints an SSH
port-forward command you can run locally:

```text
Run this command on a machine to get access to the webUI:
    ssh -N -L 9200:<COORDINATOR_IP>:9200 <jump-host>
The UI will be available at http://localhost:9200
```

## Reusing an analyzed Hive metastore across runs

Running `ANALYZE TABLE` from scratch on every clone is expensive. The launchers
publish and consume pre-analyzed metastore snapshots keyed by version string and
scale factor.

Two env vars control sharing (defined in `defaults.env`):

- `HIVE_METASTORE_SHARED_ROOT` — directory on a cluster-visible filesystem where
  snapshots live.
- `HIVE_METASTORE_VERSION` — version tag. Bump it when the worker image or parquet
  data format changes so stale snapshots don't leak into runs against a newer image.
  Unset to disable sharing entirely.

Layout: `$HIVE_METASTORE_SHARED_ROOT/$HIVE_METASTORE_VERSION/tpchsf<SF>/…`

### Consuming — the default path

With defaults, a benchmark run just works:

```bash
./launch-run.sh -n 2 -s 3000 -i 1 -w <worker-image> -c <coord-image>
```

`setup` in the SLURM job populates `.hive_metastore/tpchsf<SF>/` from the shared
snapshot when the local copy is absent. If neither local nor shared is available the
run fails fast with a message pointing at `launch-analyze-tables.sh`.

### Publishing — run once per (version, SF) to seed a new slot

```bash
export HIVE_METASTORE_VERSION=HIVE-METASTORE-20260419-no-delta   # or a new tag
./launch-analyze-tables.sh -s <SF> -n <nodes> -w <worker-image> -c <coord-image>
# On success, if the target slot is empty it gets populated atomically.
# Subsequent analyze runs with the same (version, SF) skip the publish.
```

### Disabling sharing (fall back to per-clone analyze)

```bash
unset HIVE_METASTORE_VERSION
./launch-analyze-tables.sh -s <SF> -n <nodes> -w <worker-image> -c <coord-image>
./launch-run.sh -n <nodes> -s <SF> -i <iters> -w <worker-image> -c <coord-image>
```

## Troubleshooting

### Coordinator fails to start
```bash
cat logs/coord.log
```

### Workers not registering
```bash
cat logs/worker_*.log
```

### Image not found
Pull the image first:
```bash
./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:<tag>
```
