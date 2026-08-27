# Running optional UCX exchange compression

This branch adds only the configuration needed to exercise the matching
isolated Presto and Velox branches:

- `mattgara/presto:ucx-exchange-compression`
- `mattgara/velox:ucx-exchange-compression`
- `rapidsai/velox-testing:mattgara/ucx-exchange-compression`

The codec implementation and wire-format documentation are in
`velox/experimental/ucx-exchange/COMPRESSION.md` on the Velox branch.

The exact four-GPU TPC-H Q18 build, enablement, validation procedure, and
reference result are in
[`UCX_EXCHANGE_COMPRESSION_Q18_REPRO.md`](UCX_EXCHANGE_COMPRESSION_Q18_REPRO.md).

## Configuration

The GPU worker template keeps compression disabled:

```properties
cudf.exchange_compression=none
```

The remaining compression properties are present with a conservative starting
configuration. To evaluate adaptive compression on a remote link, change the
mode to:

```properties
cudf.exchange_compression=column-adaptive-freq-pfor-min128
cudf.exchange_compression_pipeline=true
cudf.exchange_compression_pipeline_threads=1
cudf.exchange_compression_min_bytes=16777216
cudf.exchange_compression_safety_margin=1.50
```

The existing config generator enables `cudf.exchange=true` when it creates
more than one worker and assigns each worker a unique UCX server port. Build
and run the benchmark through the normal velox-testing Presto workflow.

Compression is transport-aware:

- The in-process same-worker path is already zero-copy and remains raw.
- CUDA IPC endpoints remain raw.
- Known non-CUDA-IPC endpoints, including remote TCP or RDMA endpoints, are
  eligible.
- Failure to discover the endpoint transport leaves the transfer raw.

This means enabling the policy does not force needless encode and decode work
onto a local CUDA IPC path.

## What to compare

Use the same data, query order, worker count, GPU allocation, and cache state
for both configurations:

```properties
# Control
cudf.exchange_compression=none
```

```properties
# Treatment
cudf.exchange_compression=column-adaptive-freq-pfor-min128
```

Collect end-to-end runtime and the codec VLOG records. A useful result reports
both wall time and raw-versus-wire bytes. Traffic reduction can still be
valuable at larger scale even when runtime is near neutral.

## Checks

Before interpreting results, confirm:

1. Only the intended privately namespaced workers joined the coordinator.
2. Worker logs show that UCX, not HTTP exchange, carried the tested stage.
3. Every worker uses the matching Presto and Velox branches.
4. The endpoint transport is reported before any compressed send.
5. Raw and compressed runs return identical results.

Do not compare a compressed UCX run with an HTTP run or with a run that used
workers from another deployment.
