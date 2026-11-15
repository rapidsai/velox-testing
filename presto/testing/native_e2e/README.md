# Presto GPU conformance from upstream Prestissimo suites

Runs prestodb's own native end-to-end test classes (`TestPrestoNative*` in
`presto-native-execution`) **unmodified** against one GPU worker image in three
arms, and reports GPU coverage and parity from the results.

| Arm | Worker config | Answers |
| --- | --- | --- |
| `control` | cuDF off | Does Prestissimo itself pass? Must be green or nothing else is attributable. |
| `gpu` | cuDF on, CPU fallback on | Does it work for a user? **Parity** = gpu passes / control passes. |
| `gpu-strict` | cuDF on, fallback **off**, `Exchange,MergeExchange` allowlisted | Does it run fully on the GPU? |

A test's results across the arms classify it: **runs fully on GPU** (passes in
gpu-strict), **needs CPU fallback** (passes gpu, fails gpu-strict: an operator has
no GPU path), **GPU failure** (passes control, fails gpu even with fallback
allowed) or **fails in all arms** (a Prestissimo or test issue, not a GPU one). Java Presto is the
oracle in every arm, as in the upstream suites themselves.

## Requirements

- The presto and velox changes that make this possible (property-driven
  `PrestoNativeQueryRunnerUtils`, `cudf.allowed_cpu_operators` + fallback
  telemetry in velox-cudf); the GPU worker image must be built from them.
- A GPU host with Docker, JDK 17 and the presto checkout as `../presto`.

## Run

```bash
presto/scripts/run_native_e2e_3arm.sh                       # all 11 suites, smoke + 3 arms
WORKER_IMAGE=presto-native-worker-gpu:mytag OUT_DIR=/data/runs/2026-10-14 presto/scripts/run_native_e2e_3arm.sh
SUITES_OVERRIDE=TestPrestoNativeJoinQueries ARMS=gpu SKIP_SMOKE=1 presto/scripts/run_native_e2e_3arm.sh
```

Each suite leaves `<OUT_DIR>/<arm>/<Suite>.log`, `<Suite>.surefire.xml` and
`<Suite>.worker/` (gzipped worker logs). A smoke gate (TPC-H on GPU) aborts the
run early if the image cannot run on the GPU at all.

## Read the results

Text summary and optional JSON:

```bash
python3 presto/testing/native_e2e/compare_arms.py --root <OUT_DIR> [--json summary.json]
```

Viewer with cross-run diff: open `viewer.html` in a browser and drop one or
more result folders on it (a folder per run, `<run>/<arm>/<Suite>.xml`). The
*Compare runs* tab lists regressions and fixes test by test between any two
runs; *Trend* plots parity, runs-fully-on-GPU and GPU-failure counts across all
loaded runs. For a hosted copy, serve the folders next to the viewer and open
`viewer.html?manifest=manifest.json` with

```json
{"runs": [{"id": "2026-10-14", "files": ["2026-10-14/control/TestPrestoNativeJoinQueries.surefire.xml", "..."]}]}
```

The viewer reads Surefire XML only; it needs nothing from this repo at runtime.

## Published history (CI)

`.github/workflows/presto-native-e2e.yml` runs the three arms on a GPU runner
against the image built by `presto.yml`, uploads the run directory as an
artifact (`native-e2e-<date>-<cadence>`; nightlies expire after 60 days,
weeklies are kept 400), then gathers every live `native-e2e-*` artifact,
builds the site with `build_site.py` and deploys it to GitHub Pages. History
therefore lives in artifacts and the site is a Pages deployment: no results
branch, and clones of this repo never carry the data.

`build_site.py` writes a `summary.json` per run (`compare_arms.py --json`,
~80 KB) next to the verbatim XML. The viewer renders Run / Compare / Trend from
the summaries and fetches a run's XML only when a suite drill-down is opened.
Deep links: `?run=<id>`, `?base=<id>&target=<id>&tab=diff`, `&suite=<Suite>`.
