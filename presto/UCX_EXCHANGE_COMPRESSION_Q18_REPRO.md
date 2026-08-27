# Running a matched TPC-H Q18 UCX compression comparison

This procedure validates the isolated UCX exchange compression branches with
a matched four-GPU TPC-H SF1000 Q18 comparison.

## Source revisions

Use these three repositories as sibling checkouts:

| Repository | Branch | Required revision |
| --- | --- | --- |
| https://github.com/mattgara/velox | ucx-exchange-compression | 7d4f8b38c326c84e222e6e115935a31e7ec0fc63 |
| https://github.com/mattgara/presto | ucx-exchange-compression | 73d1b5669fa455d18a5a7eb2ac32b780e91b9481 |
| https://github.com/rapidsai/velox-testing | mattgara/ucx-exchange-compression | Branch tip containing this document |

The directory layout must be:

    <base-directory>/
      presto/
      velox/
      velox-testing/

All workers must use the matching Presto and Velox revisions because the
compression descriptor extends the UCX wire format.

## Build

The normal velox-testing GPU build enables cuDF with
PRESTO_ENABLE_CUDF=ON. Velox downloads the pinned DietGPU archive, verifies
its SHA-256 checksum, and compiles only the required rANS sources.

From the velox-testing checkout:

    cd presto/scripts
    export PRESTO_DATA_DIR=<tpch-data-directory>
    export PRESTO_IMAGE_TAG=<unique-image-tag>

    ./start_native_gpu_presto.sh \
      --build all \
      --num-threads 4 \
      --num-workers 4 \
      --gpu-ids <id0,id1,id2,id3> \
      --num-drivers 2 \
      --overwrite-config \
      --logs-dir <server-log-directory>

Use four reserved, healthy GPUs. On a shared host, use private container and
network names or an otherwise isolated deployment. The compression branch
intentionally excludes unrelated shared-host namespace changes.

## Data and test setup

The original validation used:

- Four B200 workers.
- Two drivers per worker.
- Decimal TPC-H SF1000 Dataset B.
- lineitem sorted by l_shipdate and orders sorted by o_orderdate.
- Statistics for all eight tables.
- UCX_TLS=tcp,cuda_copy,cuda_ipc.
- Five iterations per campaign. The first is reported as lukewarm and the
  remaining four form the hot average.

Mount the selected dataset as PRESTO_DATA_DIR, create its Hive table
definitions, and run ANALYZE on all eight TPC-H tables. Do not benchmark until
the benchmark preflight reports statistics for all eight. A different TPC-H
layout can validate correctness and transport behavior, but its timing is not
directly comparable.

## Enable compression

UCX exchange must be enabled with a unique server port for each worker:

    cudf.enabled=true
    cudf.exchange=true
    cudf.exchange.server.port=<unique-worker-port>

The velox-testing config generator sets the last two properties for a
multi-worker configuration.

For the OFF control, leave this value in
presto/docker/config/template/overrides/gpu/etc_worker/config_native.properties:

    cudf.exchange_compression=none

For the ON treatment, change that value and retain these settings:

    cudf.exchange_compression=column-adaptive-freq-pfor-min128
    cudf.exchange_compression_pipeline=true
    cudf.exchange_compression_pipeline_threads=1
    cudf.exchange_compression_min_bytes=16777216
    cudf.exchange_compression_safety_margin=1.50

Stop the current cluster before changing the policy:

    cd velox-testing/presto/scripts
    ./stop_presto.sh

Then rerun start_native_gpu_presto.sh with --overwrite-config. Check every
generated worker configuration before benchmarking. Same-worker and CUDA IPC
transfers remain raw. Only eligible non-CUDA-IPC UCX transfers are compressed.

## Run the comparison

Use a fresh four-worker cluster for each campaign. Run OFF, ON, OFF, ON so
cache or machine drift cannot explain the difference.

From velox-testing/presto:

    ./scripts/run_benchmark.sh \
      --benchmark-type tpch \
      --queries 18 \
      --hostname <coordinator-host> \
      --port <coordinator-port> \
      --user <presto-user> \
      --schema-name <tpch-sf1000-schema> \
      --output-dir <output-directory> \
      --iterations 5 \
      --tag <unique-campaign-tag> \
      --skip-drop-cache

Use distinct tags such as q18_off_1, q18_on_1, q18_off_2, and q18_on_2.
Keep the data, query text, worker count, GPUs, driver count, UCX transports,
and cache policy unchanged.

## Required checks

Before accepting the result, verify:

1. The coordinator reports exactly the intended four workers.
2. Worker logs show UCX exchange for native-worker stages.
3. ON logs contain accepted UCX-CODEC-ATTEMPT records and matching
   UCX-CODEC-DECODE records.
4. Accepted transfers include wireBytes values below rawBytes.
5. The Q18 Parquet result has the same SHA-256 checksum in all campaigns.
6. Report all four hot averages, not only the fastest campaign.

## Interpreting the result

A native single-node deployment can use CUDA IPC for all inter-worker GPU
transfers. Those transfers intentionally remain raw, so seeing no accepted
codec attempts is valid. Measure the runtime effect on a real non-CUDA-IPC UCX
link, such as a multi-host TCP or RDMA deployment.

Do not treat host-staged TCP traffic between same-node workers as evidence for
the value of compression. That measures an avoidable routing path rather than
the normal local GPU transport.
