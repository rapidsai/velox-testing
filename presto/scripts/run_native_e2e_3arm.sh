#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Three-arm run of UPSTREAM Prestissimo e2e suites against the GPU worker image.
#
# The test classes are prestodb's own concrete classes from presto-native-execution,
# unmodified. Each arm differs only in worker config, injected through system
# properties that PrestoNativeQueryRunnerUtils now honours:
#
#   control     -DenableCudf=false                       Prestissimo baseline
#   gpu         cuDF on, CPU fallback on                  what a user gets
#   gpu-strict  cuDF on, CPU fallback OFF (+ allowlist)   what actually runs on GPU
#
# Exchange/MergeExchange are allowlisted in both GPU arms: this deployment runs
# them on the CPU on purpose (cudf.exchange=false), so strict mode must not
# count them and the worker reports them as cudfCpuAllowlisted, not fallback.
#
# Results: $OUT_DIR/<arm>/<Suite>.log, <Suite>.surefire.xml (what
# presto/testing/native_e2e/viewer.html reads) and <Suite>.worker/ (worker
# logs, for the per-operator fallback histogram). Attribute with:
#   python3 presto/testing/native_e2e/compare_arms.py --root $OUT_DIR
#
# Environment:
#   PRESTO_DIR       presto checkout (default: sibling ../presto, as elsewhere in this repo)
#   WORKER_IMAGE     GPU worker image (default: presto-native-worker-gpu:${PRESTO_IMAGE_TAG:-$USER})
#   OUT_DIR          where results go (default: presto/scripts/native_e2e_results/<UTC timestamp>)
#   ALLOWED_CPU_OPS  operators allowed on CPU in the GPU arms (default: Exchange,MergeExchange)
#   JAVA_HOME        a JDK 17 (used as-is when set)
#   SUITES_OVERRIDE / ARMS / SKIP_SMOKE   targeted re-runs, see below
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

PRESTO_DIR="${PRESTO_DIR:-${REPO_ROOT}/../presto}"
MODULE=presto-native-execution
if [ -n "${JAVA_HOME:-}" ]; then export PATH="${JAVA_HOME}/bin:${PATH}"; fi
IMG="${WORKER_IMAGE:-presto-native-worker-gpu:${PRESTO_IMAGE_TAG:-${USER:-latest}}}"
ALLOWED_CPU_OPS="${ALLOWED_CPU_OPS:-Exchange,MergeExchange}"
OUT="${OUT_DIR:-${SCRIPT_DIR}/native_e2e_results/$(date -u +%Y%m%dT%H%M%SZ)}"
# One data directory per run. The file-backed metastore lives inside it, so
# sharing it across runs lets a suite that aborted mid-CTAS leave "table
# already exists" residue for the next run.
DATA_DIR="${PRESTO_DIR}/${MODULE}/target/velox_data.$(basename "$OUT")"
WORKER_TMP=/tmp/PrestoNativeQueryRunnerUtils   # where the Java launcher puts worker dirs
mkdir -p "$OUT"
S="$OUT/STATUS.txt"; : > "$S"
say(){ echo "[$(date -u +%H:%M:%SZ)] $*" | tee -a "$S"; }

# Parquet variants only: the cuDF reader is Parquet-only, so any other format
# would measure the reader, not the operators.
SUITES=(
  TestPrestoNativeTpchQueriesParquetUsingJSON
  TestPrestoNativeGeneralQueriesThrift
  TestPrestoNativeAggregations
  TestPrestoNativeJoinQueries
  TestPrestoNativeWindowQueries
  TestPrestoNativeArrayFunctionQueries
  TestPrestoNativeCteExecutionParquetSingleNode
  TestPrestoNativeSystemQueries
  TestPrestoNativeBitwiseFunctionQueries
  TestPrestoNativeProbabilityFunctionQueries
  TestPrestoNativeCtasQueriesParquet
)
# Targeted re-runs (e.g. one suite after a harness fix, or a CI retry):
#   SUITES_OVERRIDE="TestPrestoNativeCtasQueriesParquet" ARMS="control" SKIP_SMOKE=1 ./run_native_e2e_3arm.sh
if [ -n "${SUITES_OVERRIDE:-}" ]; then read -ra SUITES <<< "$SUITES_OVERRIDE"; fi
ARMS="${ARMS:-control gpu gpu-strict}"

provenance(){
  local f="$OUT/PROVENANCE.txt"
  {
    echo "date: $(date -u +%FT%TZ)"
    echo "image: $IMG $(docker inspect --format '{{.Id}}' "$IMG" 2>/dev/null)"
    docker inspect "$IMG" --format '{{range $k,$v := .Config.Labels}}{{$k}}={{$v}}{{"\n"}}{{end}}' 2>/dev/null | grep -E 'velox-testing\.(velox|presto)\.sha'
    echo "presto: $(git -C "$PRESTO_DIR" rev-parse HEAD) (+ uncommitted: $(git -C "$PRESTO_DIR" diff --stat HEAD -- "$MODULE/src" | tail -1))"
    if [ -d "${REPO_ROOT}/../velox/.git" ]; then
      echo "velox:  $(git -C "${REPO_ROOT}/../velox" rev-parse HEAD) (+ uncommitted: $(git -C "${REPO_ROOT}/../velox" diff --stat HEAD | tail -1))"
    fi
    echo "allowed_cpu_ops: $ALLOWED_CPU_OPS"
  } > "$f"
}

# run_suite <arm-dir> <suite> <extra -D args...>
run_suite(){
  local dir=$1 suite=$2; shift 2
  mkdir -p "$dir"
  # Clear per-suite worker dirs but keep libcudf-kernel-cache: the launcher
  # points LIBCUDF_KERNEL_CACHE_PATH there so JIT kernels compile once per run.
  mkdir -p "$WORKER_TMP"; rm -rf "$WORKER_TMP"/worker*
  # Pre-create the data tree as the host user. The worker container bind-mounts
  # DATA_DIR/<format>; if that path does not exist yet Docker creates it as root
  # and the Java side then fails with "Mkdirs failed to create .../hive_data".
  mkdir -p "$DATA_DIR/PARQUET"
  (cd "$PRESTO_DIR" && timeout 3600 ./mvnw -B test -pl "$MODULE" -Dtest="$suite" \
      -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
      -DstorageFormat=PARQUET -DDATA_DIR="$DATA_DIR" \
      -Duser.timezone=America/Bahia_Banderas \
      -DPRESTO_SERVER=/usr/bin/presto_server -DworkerImage="$IMG" \
      "$@") > "$dir/$suite.log" 2>&1
  local rc=$?
  # Keep the worker logs: the fallback histogram is computed from them.
  if compgen -G "$WORKER_TMP/worker*/worker.*.out" >/dev/null; then
    mkdir -p "$dir/$suite.worker"
    for f in "$WORKER_TMP"/worker*/worker.*.out; do
      cp "$f" "$dir/$suite.worker/$(basename "$(dirname "$f")").$(basename "$f")"
    done
    gzip -f "$dir/$suite.worker"/*.out
  fi
  # Keep the Surefire XML too: it is the only artefact that names every test
  # method (the Maven log names only failures) and carries per-test durations.
  # Keep the Surefire XML verbatim: it is the only artefact that names every test
  # method (the Maven log names only failures) and carries per-test durations,
  # failure messages and traces.
  local xml
  for xml in "$PRESTO_DIR/$MODULE"/target/surefire-reports/TEST-*"$suite".xml; do
    [ -f "$xml" ] && cp "$xml" "$dir/$suite.surefire.xml"
  done
  local summary
  summary=$(grep -E 'Tests run:.*Failures' "$dir/$suite.log" | tail -1 | sed 's/\[ERROR\] //;s/\[INFO\] //')
  say "  [$(basename "$dir")] $suite: ${summary:-NO SUMMARY (mvn exit $rc)}"
}

run_arm(){ # arm name, then -D args
  local arm=$1; shift
  say "--- arm: $arm ($*) ---"
  for s in "${SUITES[@]}"; do run_suite "$OUT/$arm" "$s" "$@"; done
}

provenance
say "START 3-arm native e2e — ${#SUITES[@]} upstream suites, image $IMG, results $OUT"

# Smoke gate: if the freshly built image cannot run TPC-H on GPU there is no
# point burning an hour on the other arms.
if [ "${SKIP_SMOKE:-0}" != "1" ]; then
  run_suite "$OUT/smoke" TestPrestoNativeTpchQueriesParquetUsingJSON \
    -DenableCudf=true -DcudfAllowedCpuOperators="$ALLOWED_CPU_OPS"
  if ! grep -qE 'Tests run: [1-9][0-9]*, Failures: 0, Errors: 0' "$OUT/smoke/TestPrestoNativeTpchQueriesParquetUsingJSON.log"; then
    say "SMOKE FAILED — TPC-H on GPU did not pass cleanly; aborting before the arms. See $OUT/smoke/"
    say "DONE (aborted)"
    exit 1
  fi
  say "smoke OK"
fi

for arm in $ARMS; do
  case "$arm" in
    control)    run_arm control    -DenableCudf=false ;;
    gpu)        run_arm gpu        -DenableCudf=true -DcudfAllowedCpuOperators="$ALLOWED_CPU_OPS" ;;
    gpu-strict) run_arm gpu-strict -DenableCudf=true -DcudfAllowedCpuOperators="$ALLOWED_CPU_OPS" -DcudfAllowCpuFallback=false ;;
    *) say "unknown arm: $arm"; exit 2 ;;
  esac
done
say "DONE"
