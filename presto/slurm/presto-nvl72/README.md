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
Results land under an immutable `result_dir_<jobid>/` directory. A later launch
does not delete or write into it, so it is safe to `scp` while another benchmark
runs. `latest_result_dir.txt` contains the newest job-specific directory name;
resolve that small pointer once before a copy rather than copying a stale legacy
`result_dir/` or following a mutable symlink. See `./launch-run.sh --help` for
the full flag list (queries filter, output path, GDS toggle, profiling,
metrics, …).

**Prerequisites:** worker + coord images on disk; data from step 1; analyzed
metastore from step 2 (either local or shared).

### CPU and GPU NUMA placement

`CLUSTER_{CPU,GPU}_USE_NUMA` is a Boolean switch; `0` means unbound and does
not mean NUMA node 0. CPU jobs discover CPU-bearing DRAM nodes on every
allocated host and ignore accelerator/HBM-only NUMA IDs. Local worker slots are
balanced in contiguous groups. On the two-socket, four-GPU GB200 topology this
gives:

| Workers per host (`-g`) | CPU worker mapping |
|---:|---|
| 1 | `0` |
| 2 | `0,1` |
| 4 | `0,0,1,1` |

Each CPU worker is bound with both `--cpunodebind` and `--membind`; its driver
and memory defaults are sized from its share of that CPU/DRAM node rather than
from host `MemTotal`, which also includes the four HBM proximity domains.
Every selected host must report the same CPU topology. Each worker log records
`numactl --show` plus `Cpus_allowed_list` and `Mems_allowed_list` after entering
the requested policy and before `exec`-ing `presto_server`.

GPU jobs retain their explicit `CLUSTER_GPU_NUMA_GPUS_PER_NODE` mapping. For
the topology above, setting it to `2` binds GPU workers 0–1 to CPU node 0 and
GPU workers 2–3 to CPU node 1. Values such as `2,10,18,26` in the
`nvidia-smi topo -m` **GPU NUMA ID** column are HBM nodes and are not valid CPU
binding targets.

For a one-worker historical comparison only, `--legacy-cpu-numa` reproduces
the old inconsistent behavior: it uses the deprecated
`numactl --cpubind=0 --membind=0` spelling while retaining configuration sized
for the complete host. On this topology the process still has only CPUs 0–71;
`task.max-drivers-per-task=144` oversubscribes those 72 cores rather than
granting 144 cores. The option requires `--cpu -g 1`, emits a warning, and
records the effective policy in the worker log. It is not a supported scaling
configuration.

Before startup, the job verifies that the coordinator, worker HTTP, and
exchange ports are free on each host. On exit it terminates only the exact
coordinator/worker `srun` clients it launched, waits for their steps, and
verifies port release. Listener ownership, command lines, and Slurm cgroups
are retained in the result logs when startup or cleanup fails.
Exchange ports are tested with an actual wildcard bind as well as `ss`, so a
closed UCX listener that is no longer in `LISTEN` but is still not reusable is
not mistaken for a free port. Preflight and teardown wait up to 90 seconds by
default; override `PRESTO_PORT_PREFLIGHT_TIMEOUT` or
`PRESTO_PORT_RELEASE_TIMEOUT` when needed.

For a CPU UCX validation run, `--verify-cpu-ucx` waits up to 60 seconds for
every generated HTTP+3 exchange port to be claimed before queries begin. For a
multi-worker run, it then requires the retained query details to contain a
`UcxCpuRowExchange` or `UcxCpuRowPartitionedOutput` operator. A one-worker run
checks listener readiness but skips that runtime operator assertion because it
cannot prove inter-worker transport. The readiness check does not depend on UCX
log-message spelling or timing. The option is CLI-only: an inherited
`VERIFY_CPU_UCX` environment variable cannot enable it. Use an exchange-heavy
query with at least two workers for end-to-end transport verification;
`PRESTO_CPU_UCX_READY_TIMEOUT` changes the readiness timeout.

### Override images for a single run

```bash
./launch-run.sh -n 2 -s 100 -w my-worker-image -c my-coord-image
```

### Query-scoped CPU flamegraph

`--perf` attaches the compute host's kernel-matched `perf` to worker 0. It does
not require `perf` inside the Enroot image. Keep this separate from `-p/--profile`
(nsys), and select one representative query and iteration:

```bash
./launch-run.sh --cpu -n 10 -g 1 -s 1000 -q 18 -i 2 \
  --perf --profile-iterations 1 \
  -w <cpu-worker-image> -c <coordinator-image>
```

The sidecar preserves or raises its inherited host `nofile` limit to the hard
limit granted to the Slurm job, then tests the requested event against the live
worker. For each capture, it snapshots every live worker TID and partitions
them among bounded `perf record` processes. Every recorder starts with events
disabled. Recorders are initialized serially and must answer a control `ping`
before the query-side enable barrier; a transient initialization failure is
retried up to three times. The query starts only after every recorder
acknowledges that every target event is open. Inheritance covers worker threads
created after that snapshot. An unrecoverable attach-time descriptor or setup
failure therefore aborts before the measured query starts. Missing host `perf`,
a low hard descriptor limit,
`perf_event_paranoid`, PMU, ptrace, or Slurm cgroup restrictions likewise fail
the run explicitly instead of producing an empty profile. Capture metadata
records the live worker thread/mapping counts, shard membership, and each
recorder's open descriptor count for capacity diagnosis. Every active raw
capture is retained on node-local
`${SLURM_TMPDIR:-/tmp}` (`PERF_CAPTURE_TMPDIR` overrides the staging base).
Stopping a query waits only until every recorder has stopped, closed its output,
and queued those node-local files. This prevents home/NFS publication and
symbolization from blocking pytest or perturbing the next measured query.

After the complete query suite, the sidecar atomically publishes all queued
`perf.data` shards into the result directory before worker teardown. It also
snapshots the worker's executable mappings into a portable `symfs/`; this keeps
symbolization independent of Enroot's private mount namespace and permits
reprocessing after the job. Publication progress and byte counts are written to
`logs/perf_worker_0.log`. By default, expensive derived-report generation is
deferred:

```text
result_dir_<jobid>/profiles/perf/worker_0/
├── symfs/              # exact mapped executables/shared libraries
├── symbol-manifest.tsv
└── Q18_iter1/
    ├── perf.data.part000
    ├── perf.data.part001
    ├── perf-shards.tsv
    ├── perf-startup-attempts.tsv
    ├── perf-target-tids.txt
    ├── perf-attached-tids.part000.txt
    ├── perf-dropped-tids.tsv
    └── capture-metadata.txt
```

One-shard captures retain the conventional `perf.data` name. For a multi-shard
capture, `process-perf-captures.sh` symbolizes each raw file and concatenates
the sample streams into one `perf.script.gz` and one aggregate flamegraph. The
text report labels each raw shard because its percentages are shard-local.
The generated `flamegraph.html` is self-contained: click a frame to zoom into
that subtree, use **Reset Zoom** to return, and use the search box to highlight
matching functions.

The recorder snapshots disjoint worker TID sets, but worker-pool threads can
exit while the shards are being initialized. Before every startup attempt, it
filters each shard against `/proc/<worker>/task`; an exited TID is recorded in
`perf-dropped-tids.tsv` instead of making every retry fail with `ESRCH`. The
per-shard `perf-attached-tids.*.txt` files preserve the exact final target set.
Inheritance remains enabled for threads created after attachment.

Generate `perf-report.txt`, `perf.script.gz`, `stacks.folded`, and
`flamegraph.html` later from the login/head node with:

```bash
./process-perf-captures.sh result_dir_<jobid>

# Or process one capture:
./process-perf-captures.sh result_dir_<jobid> --query Q18_iter1

# Omit inline-function expansion for a much faster, still symbolized graph:
./process-perf-captures.sh result_dir_<jobid> \
  --query Q18_iter1 --no-inline
```

An already generated aggregate `stacks.folded` is architecture-independent.
To upgrade or regenerate only its interactive HTML on the head node—without
rerunning `perf script` or requesting a compute allocation—run:

```bash
capture=result_dir_JOBID/profiles/perf/worker_0/Q18_iter1
python3 ../../scripts/perf_flamegraph.py \
  --input-folded "${capture}/stacks.folded" \
  --output "${capture}/flamegraph.html" \
  --title "Presto worker 0: Q18_iter1"
```

When called outside Slurm, the script uses the CPU partition/account from
`~/.cluster_config.env` and starts one blocking `srun` compute allocation. No
host name or rail is selected or hard-coded. It requests one CPU per raw
recorder shard, copies the capture and `symfs/` to
`${SLURM_TMPDIR:-/tmp}`, symbolizes the shards in parallel, renders the combined
stream, and atomically copies only the derived artifacts back. This ensures an
Arm64 Grace capture is unwound by an Arm64 `perf`, avoids repeated random I/O
against the shared result directory, and prevents copy permission warnings by
not preserving shared-filesystem metadata in the temporary tree. `--jobs N`
caps shard concurrency, `--time HH:MM:SS` changes the processing allocation,
and `--local` bypasses `srun` when the command is already running on a
compatible compute host.

`--no-inline` is passed to both `perf report` and `perf script`, including
across the automatic `srun` relaunch. It disables inline-function expansion,
not DWARF stack unwinding: physical call chains, symbols, raw data, and the
portable `symfs/` remain intact. The chosen mode, compute hostname,
architecture, shard count, and parallelism are recorded in
`postprocess-metadata.txt`. Re-running with a different inline mode regenerates
the derived files automatically; `--force` is only required to repeat the same
mode.

Pass `--perf-postprocess` to the original launch only when those derived files
must be generated inside the allocation. That work still waits until all
queries and raw-file publication have completed, so it cannot bias later query
captures. The postprocessor uses the bundled dependency-free Python renderer
over the combined `perf script` output, so it needs neither a network download
nor the old Perl FlameGraph scripts.

The bundled renderer rejects output when fewer than 5% of sampled call chains
contain a known symbol, rather than calling an all-`[unknown]` graph successful.
It also refuses any raw directory whose metadata lacks the complete
ready/enable/stop/publish sequence, whose shard manifest contains a failed
recorder, or whose published shard count is incomplete. Diagnostic partial
logs and metadata from a failed attach are retained, while its unusable
pre-query partial `perf.data` files are discarded instead of being copied to
shared storage. A failed attach therefore cannot silently become a folded stack
file or flamegraph.
If rendering fails, the command exits nonzero and publishes
`flamegraph-unavailable.txt`, `perf.script.gz`, the text report, and diagnostics;
the raw `perf.data` shards and matching `symfs/` are never removed or modified.

Profiled benchmark runs stop after the first failed query, preventing a broken
sampler setup from wasting the remainder of a full suite.

Use `--perf-frequency`, `--perf-event`, `--perf-call-graph`,
`--perf-user-regs`, `--perf-nofile-limit`, and
`--perf-threads-per-shard` to override the defaults (`19`, `cpu-clock:u`,
`dwarf,8192`, `auto`, a `65536` minimum, and `128` target TIDs per recorder).
The sidecar never lowers a larger inherited soft limit and raises it to the
job's hard limit when permitted. `perf record` gets 120 seconds to flush and
stop after each query; `--perf-record-stop-timeout` overrides that watchdog.
These conservative
sampling defaults are intentional on a 144-core worker: higher-frequency DWARF
capture can lose samples, create hundreds of megabytes per short query, and
materially perturb the query being measured. Profile-run timings should not be
posted as benchmark timings. If `-o/--output-path` is given, the profile is
included in the normal result-directory copy.

`cpu-clock:u` is intentionally a software, user-space, on-CPU sampling event.
It produces the CPU-time flamegraph needed for hotspot analysis without relying
on a particular hardware counter. On this Grace/NVIDIA Linux 6.14 stack, perf
can still expand an event across the PMU CPU map for every target thread. A
warmed Presto worker has over one thousand threads, so one recorder can exceed
the job's 131072-descriptor hard limit. The TID shards bound descriptors per
recorder without dropping threads or restricting the CPUs on which they may be
sampled. `--perf-event cycles:u` remains available for hardware-cycle sampling
and uses the same sharding mechanism.

On an Arm64 host that advertises SVE, perf's automatic DWARF register set also
contains the extended `vg` pseudo-register. Linux's software clock PMUs reject
that extended register even though the hardware PMU accepts it. The default
`--perf-user-regs auto` therefore supplies every base Arm64 GPR while omitting
`vg` for `cpu-clock`/`task-clock`; explicit register lists are passed through,
and `perf-default` restores perf's own selection. Preflight uses a short real
`perf record`, not `perf stat`, so unsupported sampling/register combinations
fail before the benchmark begins.

Artifact publication waits for completion by default and is ultimately bounded
by the Slurm job time limit, because prematurely killing a copy can lose the
only node-local raw capture. `--perf-finalize-timeout <seconds>` supplies an
explicit nonzero bound when desired.

For copy-back automation, resolve the pointer once:

```bash
run_dir="$(cat latest_result_dir.txt)"
rsync -aP "${run_dir}/" /destination/"${run_dir}/"

# For a quick diagnostic copy that intentionally omits large perf artifacts:
rsync -aP --exclude '/profiles/perf/' "${run_dir}/" /destination/"${run_dir}/"
```

By default, `--perf` also fails preflight when the worker executable contains
neither `.symtab` nor `.debug_info`, since an address-only flamegraph is not a
useful result. Set `PERF_REQUIRE_SYMBOLS=0` only when intentionally collecting
raw addresses for later symbolization with a matching external symbol build.

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
├── perf_profiler_functions.sh   # query-side perf token handshake
├── perf-worker-sampler.sh       # host-side worker-0 capture orchestration
├── perf-record-sharded.sh       # bounded TID-shard perf record processes
├── process-perf-captures.sh     # deferred perf report/flamegraph generation
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
└── result_dir_<jobid>/          # Immutable benchmark results per Slurm job
```

---

## Migration note

If you were using `presto-nvl72/` with hardcoded values or a `presto-cpu1/`
fork: copy `cluster_config.env.example` to `~/.cluster_config.env`, fill in
your cluster's values, and the scripts will work without further edits.
