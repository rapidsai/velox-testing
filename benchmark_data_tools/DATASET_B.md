# Reproducing TPC-H SF1000 Dataset B

This guide creates the decimal TPC-H SF1000 data used by the current benchmark
result. The accepted data lineage is:

```text
Dataset A
  -> Dataset B (standard Parquet V2 + Snappy, 53-column encoding policy)
  -> sorted Dataset B (lineitem by l_shipdate, orders by o_orderdate)
```

All three datasets contain the same logical rows. Dataset B is not a custom
file format and does not require a custom reader. The final benchmark input is
the **sorted Dataset B**.

The accepted result used the two rewrite passes below. The sorted writer can
technically apply both encoding and sorting in one pass, but that shortcut has
not been accepted as a performance-equivalent reproduction. Do not use it for
a claimable run yet.

## Requirements

- Run these commands from the root of this `velox-testing` checkout.
- Use Docker with the NVIDIA container runtime.
- Reserve four GPUs explicitly. The examples use GPUs 4, 5, 6, and 7 only as
  placeholders.
- Allow about 750 GB while Dataset A, Dataset B, and sorted Dataset B coexist.
- Build the rewriters against the same cuDF/Velox revision used by the
  benchmark worker.
- Always use new output directories. The generator replaces an existing
  destination, and the rewriters intentionally refuse ambiguous overwrites.

Set the paths and GPU allocation once:

```bash
export HOST_DATA_ROOT=/raid/mgara/complab/data/dataset-b-repro-YYYYMMDD
export DATASET_A_GENERATOR_IMAGE=dataset-b-a-generator:9f0071f
export PARQUET_REWRITERS_IMAGE=dataset-b-rewriters:current
export POLICY_FILE="$PWD/benchmark_data_tools/native/parquet_rewriters/b_all53_candidates.tsv"
GPUS=(4 5 6 7)

test ! -e "$HOST_DATA_ROOT"
mkdir -p "$HOST_DATA_ROOT"
```

Inside the containers, the three datasets will be:

```text
/data/dataset-a/sf1000
/data/dataset-b/sf1000
/data/dataset-b-sorted-dates/sf1000
```

## 1. Build the data tools

Dataset A uses `tpchgen-rs` commit
`9f0071fee879a90504a36e0188384b3dcf2e112e`. Build that source image, then
wrap it with the pinned Python generator in this repository. Use a fresh
BuildKit builder so another checkout's Rust target cache cannot leak into this
build:

```bash
export TPCHGEN_SOURCE="$(dirname "$PWD")/tpchgen-rs-dataset-b"
export TPCHGEN_BUILDER="dataset-b-tpchgen-$(date +%Y%m%d-%H%M%S)"

git clone https://github.com/TomAugspurger/tpchgen-rs.git "$TPCHGEN_SOURCE"
git -C "$TPCHGEN_SOURCE" checkout 9f0071fee879a90504a36e0188384b3dcf2e112e
test "$(git -C "$TPCHGEN_SOURCE" rev-parse HEAD)" = \
  9f0071fee879a90504a36e0188384b3dcf2e112e

docker buildx create --name "$TPCHGEN_BUILDER" --driver docker-container
docker buildx build --builder "$TPCHGEN_BUILDER" --no-cache --load \
  --tag dataset-b-tpchgen:9f0071f \
  "$TPCHGEN_SOURCE"

TPCHGEN_IMAGE=dataset-b-tpchgen:9f0071f \
DATASET_A_GENERATOR_IMAGE="$DATASET_A_GENERATOR_IMAGE" \
  ./benchmark_data_tools/scripts/build_dataset_a_generator_image.sh
```

Use an image containing `/usr/local/bin/parquet_b_rewriter` and
`/usr/local/bin/parquet_b_sort_rewriter` built with the benchmark worker's
cuDF revision. The clean-room reproduction used its matching native-worker
image directly.

To build a standalone image from a cuDF development image:

```bash
DEPS_IMAGE=replace-with-matching-cudf-development-image \
PARQUET_REWRITERS_IMAGE="$PARQUET_REWRITERS_IMAGE" \
CUDA_ARCHITECTURES=100 \
  ./benchmark_data_tools/scripts/build_parquet_rewriters_image.sh
```

`DEPS_IMAGE` must contain the cuDF headers, libraries, and `cudfConfig.cmake`.
The plain Presto dependency image does not contain cuDF and is not sufficient.

The tracked writer inputs should have these hashes:

```text
871c6f3e3f613106cca9c123a0ea36f5f433aac551a98c9914bcb4127b4485a4  b_all53_candidates.tsv
8a7cf71813dd6fb552fb950a7c4eba414e20bd87d8359648c23377da37c6e7b4  parquet_b/ParquetBRewriter.cpp
e7f5587c0f5c9a2d5bfdb969f10fc4ca4b0d8975b874184edb0b23ead3704e99  parquet_b_sort/ParquetBRewriter.cpp
```

Verify them with:

```bash
sha256sum \
  benchmark_data_tools/native/parquet_rewriters/b_all53_candidates.tsv \
  benchmark_data_tools/native/parquet_rewriters/parquet_b/ParquetBRewriter.cpp \
  benchmark_data_tools/native/parquet_rewriters/parquet_b_sort/ParquetBRewriter.cpp
```

## 2. Generate decimal Dataset A

Do not pass `-c`; that option converts decimals to floats. Do not pass
`--use-duckdb`.

```bash
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$HOST_DATA_ROOT,dst=/data" \
  "$DATASET_A_GENERATOR_IMAGE" \
  -b tpch \
  -d /data/dataset-a/sf1000 \
  -s 1000 \
  -j 4 \
  --max-rows-per-file 100000000 \
  --approx-row-group-bytes 134217728
```

Dataset A is complete when it contains 91 Parquet files:

```bash
find "$HOST_DATA_ROOT/dataset-a/sf1000" \
  -type f -name '*.parquet' | wc -l
```

## 3. Rewrite Dataset A into Dataset B

Run four shards concurrently, one per reserved GPU. Each output file is read
back and checked for unchanged schema, types, row count, and semantic xxhash64.
Do not use `--no-verify`.

```bash
pids=()
for shard in 0 1 2 3; do
  docker run --rm --gpus all \
    --user "$(id -u):$(id -g)" \
    --env CUDA_VISIBLE_DEVICES="${GPUS[$shard]}" \
    --env LD_LIBRARY_PATH=/usr/lib64/presto-native-libs:/usr/local/lib:/usr/local/cuda/lib64 \
    --mount "type=bind,src=$HOST_DATA_ROOT,dst=/data" \
    --mount "type=bind,src=$POLICY_FILE,dst=/tmp/b_all53_candidates.tsv,readonly" \
    "$PARQUET_REWRITERS_IMAGE" \
    /usr/local/bin/parquet_b_rewriter \
      --input-root /data/dataset-a/sf1000 \
      --output-root /data/dataset-b/sf1000 \
      --policy /tmp/b_all53_candidates.tsv \
      --shard-index "$shard" \
      --shard-count 4 \
    >"$HOST_DATA_ROOT/dataset-b-shard-$shard.log" 2>&1 &
  pids+=("$!")
done

status=0
for pid in "${pids[@]}"; do
  wait "$pid" || status=1
done
test "$status" -eq 0

cp "$HOST_DATA_ROOT/dataset-a/sf1000/metadata.json" \
  "$HOST_DATA_ROOT/dataset-b/sf1000/metadata.json"
```

Dataset B uses Parquet V2 headers, Snappy, page-level compression, 2,000,000
rows per row group, 1 MiB pages and dictionaries, and the tracked 53-column
encoding policy. Columns outside the policy use the standard cuDF writer
choice.

## 4. Create the accepted sorted Dataset B

The six unchanged tables are hardlinked from Dataset B. Only `lineitem` and
`orders` are rewritten.

```bash
mkdir -p "$HOST_DATA_ROOT/dataset-b-sorted-dates/sf1000"
for table in customer nation part partsupp region supplier; do
  cp -al \
    "$HOST_DATA_ROOT/dataset-b/sf1000/$table" \
    "$HOST_DATA_ROOT/dataset-b-sorted-dates/sf1000/"
done
cp "$HOST_DATA_ROOT/dataset-b/sf1000/metadata.json" \
  "$HOST_DATA_ROOT/dataset-b-sorted-dates/sf1000/metadata.json"

pids=()
for shard in 0 1 2 3; do
  docker run --rm --gpus all \
    --user "$(id -u):$(id -g)" \
    --env CUDA_VISIBLE_DEVICES="${GPUS[$shard]}" \
    --env LD_LIBRARY_PATH=/usr/lib64/presto-native-libs:/usr/local/lib:/usr/local/cuda/lib64 \
    --mount "type=bind,src=$HOST_DATA_ROOT,dst=/data" \
    --mount "type=bind,src=$POLICY_FILE,dst=/tmp/b_all53_candidates.tsv,readonly" \
    "$PARQUET_REWRITERS_IMAGE" \
    /usr/local/bin/parquet_b_sort_rewriter \
      --input-root /data/dataset-b/sf1000 \
      --output-root /data/dataset-b-sorted-dates/sf1000 \
      --policy /tmp/b_all53_candidates.tsv \
      --shard-index "$shard" \
      --shard-count 4 \
      --table lineitem \
      --table orders \
      --sort lineitem:l_shipdate \
      --sort orders:o_orderdate \
      --row-group-size-rows 2000000 \
    >"$HOST_DATA_ROOT/dataset-b-sorted-shard-$shard.log" 2>&1 &
  pids+=("$!")
done

status=0
for pid in "${pids[@]}"; do
  wait "$pid" || status=1
done
test "$status" -eq 0
```

## 5. Accept the result

The counts must be exactly as follows:

```bash
find "$HOST_DATA_ROOT/dataset-b/sf1000" \
  -type f -name '*.parquet' | wc -l
# 91

find "$HOST_DATA_ROOT/dataset-b/sf1000/_rewrite_state" \
  -type f -name '*.json' | wc -l
# 95: 91 file records and 4 shard summaries

find "$HOST_DATA_ROOT/dataset-b-sorted-dates/sf1000" \
  -type f -name '*.parquet' | wc -l
# 91

find "$HOST_DATA_ROOT/dataset-b-sorted-dates/sf1000/_rewrite_state" \
  -type f -name '*.json' | wc -l
# 80: 76 file records and 4 shard summaries

find "$HOST_DATA_ROOT" -type f -name '*.partial*' -print
# no output
```

With the accepted source and cuDF toolchain, the name-and-size fingerprints
are:

```bash
(cd "$HOST_DATA_ROOT/dataset-a/sf1000" && \
  find . -type f -name '*.parquet' -printf '%P\t%s\n' | sort | sha256sum)
# e6157e855187486dde3058df7691b7f03fffa333b190e4e6d67c877226394a04

(cd "$HOST_DATA_ROOT/dataset-b" && \
  find . -type f -name '*.parquet' -printf '%P\t%s\n' | sort | sha256sum)
# 3969692e8f28da77d5e56c0733b927e12941e365575ede3a31bc048907801e80

(cd "$HOST_DATA_ROOT/dataset-b-sorted-dates" && \
  find . -type f -name '*.parquet' -printf '%P\t%s\n' | sort | sha256sum)
# 2ffd523cd1fdfef42e5d741899b29a6af2ba68da941bc8d025762015a896d032
```

Before timing, point the Hive schema at
`$HOST_DATA_ROOT/dataset-b-sorted-dates`, analyze all eight tables, and run the
full correctness suite. The rewriter checks prove file-level semantic identity;
the benchmark validator proves query-level correctness.
