# Distributed Presto benchmarks on direct EC2

This directory manages an ephemeral Presto cluster with one coordinator-only
EC2 instance and one native CPU or GPU worker per worker EC2 instance. It uses
AWS Systems Manager (SSM) instead of SSH and keeps all Presto traffic on
private addresses.

The initial implementation targets TPC-H SF1000 at 1, 2, 8, 16, or 32 workers.
Worker counts exclude the coordinator.

## Source independence

The harness has no dependency on a particular benchmark or engine-development
branch. Every run supplies and archives an exact source repository, fetch ref,
commit, coordinator image, and worker image. Experimental S3 or GPU runs may
pin another branch, but that dependency belongs to the run manifest rather
than this harness.

## What the harness owns

- EC2 launch, inventory, and RunId-scoped termination.
- SSM bootstrap and command status.
- Docker image pulls and immutable source checkout.
- Per-role Presto config generation and a recorded config diff.
- Coordinator-first startup and exact worker-count registration.
- Benchmark execution and S3 artifact upload.
- Host/container telemetry and node artifact collection.

It does not create a VPC, subnet, security group, IAM role, S3 bucket, placement
group, or benchmark dataset. Those account-level resources are inputs.

## Prerequisites

The control machine needs Python 3.10+ and AWS CLI v2. The EC2 subnet needs
outbound access or VPC endpoints for SSM, S3, source checkout, and image pulls.

The control identity needs permission to launch, describe, price, tag, and
terminate EC2 resources; pass the configured instance profile; resolve the AMI
SSM parameter; and send/read SSM commands. Resolve permissions and prices
before EC2 launch so access failures do not leave a partial fleet.

The instance profile needs:

- SSM managed-instance permissions;
- read access to the benchmark data and metastore;
- write access to the configured results prefix;
- ECR permissions only when private ECR images are selected.

The security group needs no SSH ingress. Add self-referencing TCP rules for
port 8080 and any native exchange port selected by the generated worker config
(GPU currently uses 10003). Restrict all rules to the benchmark security group
and required egress paths.

Use a same-AZ subnet. A placement group is optional and must be recorded as a
separate experiment dimension.

## Configuration

Copy the example outside the repository and fill in real values:

```bash
mkdir -p ~/.config/velox-testing
cp aws_config.env.example ~/.config/velox-testing/aws-cpu.env
$EDITOR ~/.config/velox-testing/aws-cpu.env
```

Do not commit the populated file. Pin coordinator and worker images by digest
before qualification even if initial smoke tests use tags.

Set `VELOX_TESTING_REPOSITORY` and `VELOX_TESTING_FETCH_REF` to the fork and
branch containing this harness. Set `VELOX_TESTING_REF` to that branch's exact
40-character commit SHA. Bootstrap verifies that the fetched ref resolves to
the expected SHA before checkout.

For a pre-commit smoke test only, `HARNESS_BUNDLE_S3_URI` and
`HARNESS_BUNDLE_SHA256` may point to an immutable tar overlay containing
`presto/aws/ec2/`. Bootstrap verifies and extracts it after checking out the
pinned base commit. The run manifest records both values. Qualification runs
must use a committed source revision instead.

`m7a.4xlarge` has less memory than `r6i.4xlarge`; use a named worker-memory
overlay for each family. Keep the coordinator type and coordinator settings
fixed when comparing worker families.

Leave `AMI_ID` empty to resolve the configured Canonical Ubuntu SSM parameter
at launch. The resolved AMI and live on-demand Linux prices are written to the
run manifest.

Mixed-architecture fleets can instead set `COORDINATOR_AMI_SSM_PARAMETER` and
`WORKER_AMI_SSM_PARAMETER` (or their `_ID` equivalents). Bootstrap selects the
AWS CLI binary for each host architecture and pulls only the image used by that
role.

Set `ENGINE_VARIANT=cpu` for CPU workers or `ENGINE_VARIANT=gpu` for one GPU
worker per EC2 instance. GPU workers require an NVIDIA-driver AMI with NVIDIA
Container Toolkit and a GPU-enabled worker image. Bootstrap verifies both host
and container GPU visibility before startup. The example's Kvikio values
reproduce the initial G7e S3 configuration and should remain explicit in each
run manifest.

## Local validation and dry run

Validation and dry-run launch do not contact AWS:

```bash
python3 aws_cluster.py \
  --config ~/.config/velox-testing/aws-cpu.env \
  --run-id presto-ec2-dry-run \
  validate

python3 aws_cluster.py \
  --config ~/.config/velox-testing/aws-cpu.env \
  --run-id presto-ec2-dry-run \
  --workers 8 \
  --dry-run \
  launch

python3 -m unittest -v test_aws_cluster.py
```

Review both generated `run-instances` commands, especially tags, count,
metadata options, instance types, subnet, security group, role, and placement.

## Fleet workflow

Choose one unique RunId and use it for the full lifecycle:

```bash
CONFIG=~/.config/velox-testing/aws-cpu.env
RUN_ID=presto-m7a8-$(date -u +%Y%m%dT%H%M%SZ)

python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 launch
python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 status
python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 bootstrap
python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 start
python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 run
python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 collect
python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 stop
python3 aws_cluster.py --config "$CONFIG" --run-id "$RUN_ID" --workers 8 terminate
```

`launch` waits for EC2 health and SSM registration. `bootstrap` installs Docker
and AWS CLI v2, checks out the pinned source revision, pulls images, restores
the metastore, and probes IMDSv2 credentials. `start` generates configs on each
role, starts the coordinator first, then starts workers and waits for exactly N
workers.

All mutating commands select or tag resources with the exact RunId. `launch`
uses separate idempotency tokens for coordinator and worker requests.
Destructive inventory also includes the Owner tag to prevent a RunId collision
from crossing users.

`SSM_COMMAND_TIMEOUT_SECONDS` applies to both the remote document and the local
poller. Its default is six hours, which avoids SSM's one-hour default while
still bounding a hung benchmark.

Always run `collect` before `terminate`, including after failed benchmarks.
Collection accepts a partial fleet and gathers every still-running node.
Instances carry an expiry tag and an OS shutdown behavior, but neither replaces
explicit teardown and billing verification.

List expired benchmark instances without mutating them:

```bash
python3 aws_cluster.py \
  --config "$CONFIG" --run-id ignored-for-listing list-stale
```

## Correctness smoke

The first cloud sequence is one coordinator plus one `m7a.4xlarge` worker:

```bash
python3 aws_cluster.py \
  --config "$CONFIG" --run-id "$RUN_ID" --workers 1 \
  run --queries 6,18 --iterations 1 --tag c1_q6_q18
```

Then use two workers for one full Q1-Q22 pass before allocating eight workers.

The current benchmark runner collects query and task metrics. Provide expected
result files through the normal `velox-testing` benchmark configuration when
semantic validation artifacts are required for qualification.

## Baseline timing

For cache-off query-major timing, run five iterations. Treat
iteration 1 as warm-up. For every query, average iterations 2-5, then sum the 22
means. Provisioning, startup, registration, and warm-up are excluded from that
primary runtime and must be reported separately.

The harness archives raw output; top-line reduction should happen in the
experiment repository so rejected runs and the reduction rule remain visible.
Use `summarize_results.py --baseline <benchmark_result.json>` to apply the
one-warm-up/four-measured rule without relying on the runner's aggregate field.

For repeated data-cold suites with cache off, drop host page caches before each
single-iteration Q1-Q22 pass:

```bash
python3 aws_cluster.py \
  --config "$CONFIG" --run-id "$RUN_ID" --workers 8 \
  run-cold-series --repetitions 3 --tag-prefix sf1k
```

This preserves the Presto processes and worker membership while issuing
`sync` and dropping Linux page cache on every coordinator and worker before
each pass. Each repetition has a separate result tag.

## Controlled CPU tuning

The environment file exposes the generated CPU override as explicit controls:

- `TASK_MAX_DRIVERS_PER_TASK`;
- `HIVE_MAX_SPLIT_SIZE`;
- `HIVE_SPLIT_LOADER_CONCURRENCY`;
- `DYNAMIC_FILTERING_ENABLED`;
- `CPU_EXCHANGE_TUNING_ENABLED`.

The example values reproduce the normal generated CPU configuration. Change
one value at a time, rerun `start` to generate and archive the candidate
configuration, then use `run-cold-series` with a diagnostic query list. Setting
`CPU_EXCHANGE_TUNING_ENABLED=false` removes the generated CPU exchange
properties so the native worker uses its defaults.

## Async data cache

Do not compare cache-on and cache-off results as the same workload.

For cache-on qualification, use a fresh fleet and an explicit cache capacity.
Run one complete Q1-Q22 suite with one iteration per query as cold fill. Without
clearing, restarting, or changing the worker session, run four more complete
one-iteration suites. Average each query across those four warm suites and sum
the query means.

Do not use `--iterations 5` for this cache-on sequence because that produces
query-major order rather than suite-major order. Give each pass a unique tag,
and preserve one uninterrupted cluster session.

The harness encodes that sequence:

```bash
python3 aws_cluster.py \
  --config "$CONFIG" --run-id "$RUN_ID" --workers 8 \
  run-cache-series --tag-prefix sf1k_cache
```

It requires `ASYNC_DATA_CACHE_ENABLED=true`, runs one cold-fill suite followed
by four warm suites, and rejects a run if worker membership changes.
Pass the five resulting `benchmark_result.json` files to
`summarize_results.py --cache-series` in cold, warm-1, warm-2, warm-3, warm-4
order.

## Generated state and artifacts

Local mutable state defaults to:

```text
~/.cache/velox-testing/aws-ec2/<RunId>/
```

It contains the launch manifest and SSM command IDs. It is intentionally
outside the repository.

Remote state is under `/opt/presto-aws`. Collection uploads:

- generated base and final configs plus their diff and hashes;
- Presto and container logs;
- worker-registration response;
- host and container telemetry;
- CPU, memory, filesystem, network, Docker, kernel, and journal snapshots;
- benchmark raw output and detailed Presto/Velox metrics.

SSM command output also goes to the run-specific S3 prefix when
`RESULTS_S3_ROOT` is configured.

## Failure handling

- A benchmark never starts unless exactly N workers register.
- SSM failures identify the command and instance.
- Config patching requires exactly one occurrence of each expected property.
- A failed benchmark still attempts to sync partial results.
- `terminate` describes resources by the exact Project, Experiment, and RunId
  tags; it does not read a broad local instance list.

If coordinator sizing must change during 16/32-worker work, restart the scaling
series at eight workers. Only required-worker count and aggregate coordinator
query limits should vary mechanically with N.
