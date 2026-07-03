# Presto TPC-H Benchmark (Slurm)

Scripts for running Presto TPC-H benchmarks on any Slurm cluster. Cluster-specific
values live outside the repo in `~/.cluster_config.env` — the repo itself is
cluster-agnostic.

## How the workflow is shaped

Three tasks, run in order. They have very different cadences:

| Step | When | Script |
|---|---|---|
| 1. Generate data | Once per cluster, per scale factor | `./launch-gen-data.sh` |
| 2. Analyze tables | When the worker image changes its stat tracking (rare) | `./launch-analyze-tables.sh` |
| 3. Run benchmarks | The main task — daily/per-experiment | `./launch-run.sh` |

Each launcher checks the prerequisites for its step and tells you exactly what to
run if something is missing. If you forget step 1 or 2, the next launcher will
print the command needed to satisfy it.

---

## First-time setup

1. **Cluster config.** Copy the template and fill in your cluster's values:

   ```bash
   cp cluster_config.env.example ~/.cluster_config.env
   $EDITOR ~/.cluster_config.env
   ```

   To use a different path: `export CLUSTER_CONFIG=/path/to/your/config.env`.

2. **Container images.** Pull the images referenced in your config. The
   `pull_ghcr_image.sh` helper fetches a GHCR image and converts it to `.sqsh`
   under `${IMAGE_DIR}`:

   ```bash
   ./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:tpchgen-cli
   ./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:<worker-tag>
   ./pull_ghcr_image.sh ghcr.io/rapidsai/velox-testing-images:<coord-tag>
   ```

   The exact worker/coord image tags are whatever you set as
   `CLUSTER_{GPU,CPU}_DEFAULT_{WORKER,COORD}_IMAGE` in your cluster config.

---

## Step 1 — Generate TPC-H data (once per scale factor)

```bash
./launch-gen-data.sh -s <scale_factor> -o <output_dir>

# example
./launch-gen-data.sh -s 100 -o $DATA/tpch-rs-100
```

Runs `tpchgen-rs` inside the `tpchgen-cli` image on a single CPU node. Default
parallelism is 100; tune with `-j` if your node has fewer cores. See
`./launch-gen-data.sh --help` for all flags.

**Prerequisites:** `tpchgen-cli` image on disk.

---

## Step 2 — Analyze tables (rarely, per image)

```bash
./launch-analyze-tables.sh -s <scale_factor>
```

Starts a coordinator + workers, registers the TPC-H tables in a local Hive
metastore, then runs `ANALYZE TABLE` to collect statistics. The resulting
metastore tree is written under `<repo>/.hive_metastore/tpchsf<SF>/` and reused
by subsequent benchmark runs.

`ANALYZE TABLE` disables cudf in the worker configs, so this step is always a
CPU-only workload — it pulls partition/account/images from the `CLUSTER_CPU_*`
section of your cluster config regardless of whether your benchmarks run on
GPU. Pass `-w`/`-c` to override the worker/coordinator images.

This step only needs to repeat when the worker image's stat tracking changes
(rare). For sharing the post-analyze snapshot across users, see
[Sharing analyzed metastores](#sharing-analyzed-metastores) below.

**Prerequisites:** worker + coord images on disk; data from step 1.

---

## Step 3 — Run benchmarks (frequent)

```bash
./launch-run.sh -n <nodes> -s <scale_factor> [-i <iterations>]

# examples
./launch-run.sh -n 8 -s 3000             # GPU, default 2 iterations
./launch-run.sh -n 4 -s 1000 -i 3        # GPU, 3 iterations
./launch-run.sh --cpu -n 2 -s 100        # CPU

# Use POSIX I/O instead of GDS
./launch-run.sh -n 8 -s 3000 --disable-gds

# Profile queries 5 and 6 on worker 2
./launch-run.sh -n 8 -s 3000 -p --nsys-worker-id 2 -q 5,6
```

Submits a benchmark sbatch job, polls until completion, and prints a summary.
Results land under `result_dir/`. See `./launch-run.sh --help` for the full
flag list (queries filter, output path, GDS toggle, profiling, metrics, …).

**Prerequisites:** worker + coord images on disk; data from step 1; analyzed
metastore from step 2 (either local or shared).

### Override images for a single run

```bash
./launch-run.sh -n 2 -s 100 -w my-worker-image -c my-coord-image
```

---

## Entry points (reference)

| Script | Purpose |
|---|---|
| `launch-gen-data.sh` | Step 1: submit a TPC-H data-generation job |
| `launch-analyze-tables.sh` | Step 2: submit an `ANALYZE TABLE` job |
| `launch-run.sh` | Step 3: submit a benchmark job |
| `run-sweep.sh` | Loop `launch-run.sh` over (nodes × scale factors) and post results |
| `run-interactive.sh` | `srun --pty bash` into a compute node with the worker image |
| `pull_ghcr_image.sh` | Fetch a GHCR image and convert it to `.sqsh` under `IMAGE_DIR` |

All launchers accept `-h/--help` for full flag listings.

---

## Configuration

All cluster-specific values come from `~/.cluster_config.env`. See
`cluster_config.env.example` for the full list. The most-edited variables:

| Variable group | Controls |
|---|---|
| `CLUSTER_GPU_PARTITION` / `CLUSTER_CPU_PARTITION` | Slurm partition |
| `CLUSTER_GPU_ACCOUNT` / `CLUSTER_CPU_ACCOUNT` | Slurm account (often required) |
| `CLUSTER_GPU_CPUS_PER_TASK` / `CLUSTER_CPU_CPUS_PER_TASK` | `--cpus-per-task` |
| `CLUSTER_GPU_TIME_BENCHMARK` / `CLUSTER_CPU_TIME_BENCHMARK` | `--time` for benchmark |
| `CLUSTER_GPU_TIME_ANALYZE` / `CLUSTER_CPU_TIME_ANALYZE` | `--time` for analyze |
| `CLUSTER_GPU_DEFAULT_PORT` / `CLUSTER_CPU_DEFAULT_PORT` | Presto HTTP port |
| `CLUSTER_GPU_NUM_WORKERS_PER_NODE` / `CLUSTER_CPU_NUM_WORKERS_PER_NODE` | Workers per node |
| `CLUSTER_GPU_DEFAULT_WORKER_IMAGE` / `CLUSTER_CPU_DEFAULT_WORKER_IMAGE` | Default worker image name |
| `CLUSTER_GPU_DEFAULT_COORD_IMAGE` / `CLUSTER_CPU_DEFAULT_COORD_IMAGE` | Default coordinator image name |
| `DATA` | TPC-H parquet data root (parent of `tpch-rs-<SF>/`) |
| `IMAGE_DIR` | Directory containing `.sqsh` container images |

Any variable can also be exported in your shell before invoking a launcher to
override the config for a single run.

`launch-run.sh` and `run-interactive.sh` accept `--cpu` / `--gpu` flags to
select between the `CLUSTER_GPU_*` and `CLUSTER_CPU_*` sets. The cluster-wide
default is taken from `CLUSTER_DEFAULT_VARIANT` (set in your cluster config,
e.g. `CLUSTER_DEFAULT_VARIANT=cpu` for CPU-only clusters); falls back to `gpu`
if unset. `launch-gen-data.sh`, `launch-analyze-tables.sh`, and
`pull_ghcr_image.sh` are always CPU-only.

---

## Sharing analyzed metastores

Running `ANALYZE TABLE` from scratch is expensive when the underlying parquet
data is shared. Set `HIVE_METASTORE_SHARED_ROOT` and `HIVE_METASTORE_VERSION`
in your config to opt in to a publish/consume workflow:

- **Analyze** publishes the post-ANALYZE metastore snapshot to
  `${HIVE_METASTORE_SHARED_ROOT}/${HIVE_METASTORE_VERSION}/tpchsf<SF>/` (once
  per slot; concurrent analyzers race harmlessly).
- **Benchmark** populates the local `.hive_metastore` from the shared snapshot
  instead of re-analyzing.

Bump `HIVE_METASTORE_VERSION` when the worker image or parquet encoding changes
so stale snapshots don't leak into new runs. To disable sharing, leave
`HIVE_METASTORE_VERSION` unset.

---

## Monitoring & UI

```bash
squeue -u $USER
tail -f logs/coord.log
tail -f logs/worker_*.log
tail -f logs/cli.log
```

After job submission, the launcher resolves the coordinator node's IP. If
`CLUSTER_SSH_TUNNEL_HOST` is set in your config, it prints a port-forward
command:

```text
ssh -N -L <PORT>:<COORD_IP>:<PORT> <CLUSTER_SSH_TUNNEL_HOST>
```

Otherwise it prints the coordinator address directly. Open
`http://localhost:<PORT>` in your browser to reach the Presto web UI.

---

## File layout

```
presto-nvl72/
├── cluster_config.env.example   # Template — copy to ~/.cluster_config.env
├── defaults.env                 # Sources cluster_config.env; computes workspace paths
├── worker.env                   # Worker container env (KVIKIO knobs etc.)
│
├── # User-facing entry points
├── launch-gen-data.sh           # Step 1: data generation
├── launch-analyze-tables.sh     # Step 2: analyze tables
├── launch-run.sh                # Step 3: run benchmarks
├── run-sweep.sh                 # Loop benchmarks over (nodes, scale-factor)
├── run-interactive.sh           # Interactive shell on a compute node
├── pull_ghcr_image.sh           # Fetch GHCR image → .sqsh
│
├── # Shared library code
├── launcher_common.sh           # Cluster-config resolution + preflight helpers
├── slurm_common.sh              # Shared env setup sourced by .slurm wrappers
├── functions.sh                 # Coordinator/worker/queries helpers
├── echo_helpers.sh              # Logging helpers
├── profiler_functions.sh        # nsys profiling helpers (bind-mounted into workers)
│
├── # Slurm wrappers + inner execution scripts
├── gen-tpch-data.slurm          # Step 1 sbatch entry
├── run-analyze-tables.slurm     # Step 2 sbatch entry
├── run-analyze-tables.sh        #   inner: orchestrates the analyze flow
├── run-presto-benchmarks.slurm  # Step 3 sbatch entry
├── run-presto-benchmarks.sh     #   inner: orchestrates the benchmark flow
│
├── # Runtime output
├── logs/                        # Coord/worker/cli logs (created per run)
└── result_dir/                  # Benchmark results (created per run)
```

---

## Migration note

If you were using `presto-nvl72/` with hardcoded values or a `presto-cpu1/`
fork: copy `cluster_config.env.example` to `~/.cluster_config.env`, fill in
your cluster's values, and the scripts will work without further edits.
