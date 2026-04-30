# Master Plan Build & Benchmark

The "master plan" build collapses Spark+Gluten's per-stage Substrait plans
into a single plan executed inside Velox. This directory contains the
self-contained Docker build (`docker/masterplan_build.dockerfile`) and the
benchmark runner (`scripts/run_masterplan_benchmark.sh`).

## Prerequisites

Sibling repos checked out at the expected branches:

```
../spark-gluten      branch: master-plan-v1-repro-20260317
../velox             branch: ferd-dev-v1-reconstructed
```

`build_masterplan.sh` checks branches and offers to switch automatically.

## Base image: `velox-dev:adapters-masterplan`

`masterplan_build.dockerfile` defaults to
`ghcr.io/facebookincubator/velox-dev:adapters-masterplan` rather than
`:adapters`. The reason is **dependency alignment**: this masterplan stack is
pinned to specific velox/cudf/rmm/arrow versions that match
`ferd-dev-v1-reconstructed`. If you have other velox checkouts in active
development (which periodically rebuild `:adapters` from their own velox
state), the generic `:adapters` tag will drift and the masterplan build will
break in subtle ways (rmm/CCCL version mismatch, arrow header changes, etc.).

The project-specific `:adapters-masterplan` tag isolates this build's base
from your other dev work.

### Building the base image without clobbering other tags

```bash
# 1. Save the current :adapters under a side tag (preserves other dev work).
docker tag ghcr.io/facebookincubator/velox-dev:adapters \
           ghcr.io/facebookincubator/velox-dev:adapters-otherdev

# 2. Make sure ../velox is on the masterplan branch:
git -C ../velox checkout ferd-dev-v1-reconstructed

# 3. Rebuild :adapters from ../velox source (this clobbers :adapters):
velox/scripts/build_centos_deps_image.sh

# 4. Capture the freshly-built image under the project-specific tag:
docker tag ghcr.io/facebookincubator/velox-dev:adapters \
           ghcr.io/facebookincubator/velox-dev:adapters-masterplan

# 5. Restore :adapters from the side tag so other work isn't disrupted:
docker tag ghcr.io/facebookincubator/velox-dev:adapters-otherdev \
           ghcr.io/facebookincubator/velox-dev:adapters
```

## Build the masterplan JAR + image

```bash
spark_gluten/scripts/build_masterplan.sh -j $(($(nproc)/2))
```

Outputs:
- `build_artifacts/masterplan/gluten-velox-bundle-spark3.4_2.12-linux_amd64-1.6.0-SNAPSHOT.jar`
- Docker image `apache/gluten:masterplan` (also `gluten-masterplan-build:latest`)

### Notes on the Dockerfile fixes

`masterplan_build.dockerfile` carries two non-obvious concessions that match
how `gluten_build.dockerfile` handles the same upstream constraints:

1. **`-include cassert` in `CXXFLAGS`** for both phase 1 (velox C++) and
   phase 2 (spark-gluten C++). Arrow 15's `arrow/c/helpers.h` uses `assert()`
   without `#include <assert.h>`. The flag pre-includes `<cassert>` so
   compilation succeeds. Harmless on Arrow 18+ (which fixes the upstream
   header).

2. **Conditional `cpp/build` wipe** at the start of phase 2. The cache mount
   may contain a `CMakeCache.txt` from a previous configure that was missing
   the cassert flag; CMake won't re-pick-up `CXXFLAGS` from env unless
   re-configured. The check `grep -q '\-include cassert' cpp/build/CMakeCache.txt`
   forces a fresh configure when needed.

## Generate TPC-H data

```bash
python3 -m venv /tmp/datagen_venv
source /tmp/datagen_venv/bin/activate
pip install -r benchmark_data_tools/requirements.txt
python benchmark_data_tools/generate_data_files.py -b tpch -d data/tpch_sf10 -s 10 -c
```

(The `-c` flag generates compressed parquet. SF10 produces ~3.2 GB.)

## Run benchmarks

```bash
# CPU master plan (collapsed single-stage)
spark_gluten/scripts/run_masterplan_benchmark.sh -d data/tpch_sf10 -q "1,3"

# GPU master plan (collapsed + cuDF)
spark_gluten/scripts/run_masterplan_benchmark.sh -d data/tpch_sf10 -q "1,3" --gpu

# CPU velox-spark baseline (normal Spark stages)
spark_gluten/scripts/run_masterplan_benchmark.sh -d data/tpch_sf10 -q "1,3" --baseline
```

### Profiling with `nsys`

Add `--nsys` to wrap the python invocation in `nsys profile`. Output lands at
`benchmark_output/masterplan/masterplan-<mode>-<timestamp>.nsys-rep`.

The runner (`scripts/masterplan_bench_runner.py`) emits NVTX ranges so the
profile timeline is navigable:

```
<mode>/Q<id>                          (cyan)    full per-query span
├─ <mode>/Q<id>/warmup                (yellow)  discarded run
└─ <mode>/Q<id>/iter1..N              (green)   timed iteration
   └─ <mode>/Q<id>/iter1..N/collect   (magenta) just the .collect() (the timed portion)
```

`--trace=cuda,nvtx,osrt` is set automatically so the ranges show up alongside
CUDA kernel activity and OS runtime calls.

## Reference results (SF10, RTX 5000 Ada, local[1])

| Mode             | Q1 avg / min  | Q3 avg / min  |
|------------------|---------------|---------------|
| baseline         | 2618 / 2588 ms | 7520 / 7401 ms |
| cpu-masterplan   | 2524 / 2476 ms | 4478 / 4379 ms |
| gpu-masterplan   |  534 /  501 ms |  459 /  448 ms |

Speedup vs baseline:
- Q1: cpu 1.04x, gpu **4.90x**
- Q3: cpu 1.68x, gpu **16.39x**
