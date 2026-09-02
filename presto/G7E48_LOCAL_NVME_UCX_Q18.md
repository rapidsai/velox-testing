# g7e.48xlarge local-NVMe Q18 over EFA/SRD

This recipe reproduces the compression-free SF3K Q18 experiment on the audited
AWS `g7e.48xlarge` topology. Table data is read from local NVMe; S3 is used only
to populate the disposable NVMe copy before the benchmark.

The tested software stack is:

- Presto `a2ef306cbfe7fe507f52c04ccfbe665df27d1f7b`, based on Daniel Bauer's
  August 19 UCX-exchange branch.
- Velox `a7f4ebca783b40773a125e6831234641c17be93c`, immediately before Matt
  Gara's adaptive exchange-compression commit.
- EFA-enabled UCX 1.21 based on `ae68ab20f59f00385ab30aee94b501056eb417bf`
  with OpenUCX commit `966d94d31` backported. That upstream fix caches CUDA
  allocation attributes while the allocating context is current.

No exchange-compression property is added by this branch. The run script fails
if one appears in the generated worker configuration.

## Stage SF3K on NVMe

After every EC2 stop/start, restore the disposable data directory:

```bash
export AWS_REGION=us-east-2 AWS_DEFAULT_REGION=us-east-2
export PRESTO_DATA_DIR=/opt/dlami/nvme/dmakkar/Presto/data/tpch
mkdir -p "$PRESTO_DATA_DIR/sf3k_v2_float"
aws s3 sync \
  s3://rapids-tpch/presto-gpu/sf3k_v2_float/ \
  "$PRESTO_DATA_DIR/sf3k_v2_float/" \
  --exclude 'hive_metastore/*' \
  --only-show-errors \
  --no-follow-symlinks
```

The expected table-data inventory is 264 files and 954,780,613,952 bytes.
Verify it before comparing results:

```bash
find "$PRESTO_DATA_DIR/sf3k_v2_float" -type f -printf '%s\n' |
  awk '{objects += 1; bytes += $1} END {print objects, bytes}'
```

## Create and analyze the local schema once

Use the CPU Velox worker for this one-time step. The Hive metastore persists on
EBS, so ordinary EC2 restarts require only the NVMe sync above.

```bash
export PRESTO_IMAGE_TAG=<image-tag>
export PRESTO_DATA_DIR=/opt/dlami/nvme/dmakkar/Presto/data/tpch
cd presto/scripts
./setup_benchmark_tables.sh \
  --benchmark-type tpch \
  --schema-name tianyu_nvme_sf3k_v2_float \
  --data-dir-name sf3k_v2_float
```

## Run Q18

The audited mapping pairs GPU workers 0/1, 2/3, 4/5, and 6/7 with EFA devices
`rdmap145s0`, `rdmap162s0`, `rdmap179s0`, and `rdmap196s0`, respectively. Verify
those names before using a different AMI or instance.

```bash
export PRESTO_IMAGE_TAG=<image-tag>
export PRESTO_DATA_DIR=/opt/dlami/nvme/dmakkar/Presto/data/tpch
export UCX_RUNTIME_ROOT=<efa-enabled-ucx-install-prefix>
export RESULTS_DIR=$HOME/Development/results
export BUILD_WORKER=true # First run only; builds with all host cores.
presto/scripts/run_g7e48_local_nvme_q18.sh
```

Unset `BUILD_WORKER` on later runs to reuse the built worker image.

The script enables the opt-in `--ucx-efa-runtime` launch path, sets each ENA to
16 combined queues, launches eight GPU workers in one host-network container,
and runs two Q18 iterations. The second iteration is the hot measurement. The
compression-free acceptance target is under 6 seconds.

The worker configuration intentionally uses:

- `ucxx.error_handling=false` and `ucxx.blocking_polling=false` for SRD progress;
- 32 MiB CUDA rendezvous fragments on one rail;
- `UCX_CUDA_COPY_ASYNC_MEM_TYPE=cuda-managed`, keeping application
  `cudaMallocAsync` buffers on UCX's fragment pipeline;
- the upstream allocation-cache fix rather than
  `UCX_CUDA_COPY_RETAIN_PRIMARY_CTX` or a requested-type override;
- 10 million partition-output rows to bound SF3K memory pressure; and
- benchmark-scoped `native_cudf_exchange_enabled=true`, leaving metadata and
  setup queries on the ordinary exchange path.
