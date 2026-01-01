#!/usr/bin/env bats

# Tests for presto/scripts/setup_benchmark_data_and_tables.sh

setup() {
  export TEST_ROOT="$BATS_TEST_TMPDIR/work"
  export LOG_DIR="$BATS_TEST_TMPDIR/logs"
  mkdir -p "$TEST_ROOT/velox-testing/scripts" "$TEST_ROOT/velox-testing/presto/scripts" \
           "$TEST_ROOT/velox-testing/benchmark_data_tools" "$LOG_DIR" "$BATS_TEST_TMPDIR/stubs"

  # Copy needed helpers verbatim
  cp -f "$BATS_TEST_DIRNAME/../../scripts/helper_function.sh" \
        "$TEST_ROOT/velox-testing/scripts/helper_function.sh"
  cp -f "$BATS_TEST_DIRNAME/../scripts/common_functions.sh" \
        "$TEST_ROOT/velox-testing/presto/scripts/common_functions.sh"
  cp -f "$BATS_TEST_DIRNAME/../scripts/setup_benchmark_helper_check_instance_and_parse_args.sh" \
        "$TEST_ROOT/velox-testing/presto/scripts/setup_benchmark_helper_check_instance_and_parse_args.sh"
  cp -f "$BATS_TEST_DIRNAME/../scripts/setup_benchmark_data_and_tables.sh" \
        "$TEST_ROOT/velox-testing/presto/scripts/setup_benchmark_data_and_tables.sh"

  # Stubs: curl and jq to satisfy wait_for_worker_node_registration()
  cat > "$BATS_TEST_TMPDIR/stubs/curl" <<'SH'
#!/usr/bin/env bash
# Simulate successful response and write a non-empty JSON array
for ((i=1; i<=$#; i++)); do
  if [[ "${!i}" == "-o" ]]; then
    outvar=$((i+1)); out=${!outvar}; echo '[1]' > "$out"; break
  fi
done
exit 0
SH
  chmod +x "$BATS_TEST_TMPDIR/stubs/curl"

  cat > "$BATS_TEST_TMPDIR/stubs/jq" <<'SH'
#!/usr/bin/env bash
# Return length 1 to indicate non-empty
echo 1
SH
  chmod +x "$BATS_TEST_TMPDIR/stubs/jq"

  # Stub: ../../scripts/run_py_script.sh (relative to presto/scripts)
  cat > "$TEST_ROOT/velox-testing/scripts/run_py_script.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
echo "[run_py_script] $*" >> "${RUN_LOG:?}"
exit 0
SH
  chmod +x "$TEST_ROOT/velox-testing/scripts/run_py_script.sh"

  # Stub: ./setup_benchmark_tables.sh (same dir as script)
  cat > "$TEST_ROOT/velox-testing/presto/scripts/setup_benchmark_tables.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
echo "[setup_benchmark_tables] $*" >> "${RUN_LOG:?}"
exit 0
SH
  chmod +x "$TEST_ROOT/velox-testing/presto/scripts/setup_benchmark_tables.sh"

  export PATH="$BATS_TEST_TMPDIR/stubs:$PATH"
  export PRESTO_DATA_DIR="$BATS_TEST_TMPDIR/presto_data"
  mkdir -p "$PRESTO_DATA_DIR"
  export RUN_LOG="$LOG_DIR/calls.log"
  : > "$RUN_LOG"
}

@test "prints help with --help" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run ./setup_benchmark_data_and_tables.sh --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "fails when PRESTO_DATA_DIR is unset" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run env -u PRESTO_DATA_DIR ./setup_benchmark_data_and_tables.sh -h
  [ "$status" -ne 0 ]
  [[ "$output" == *"PRESTO_DATA_DIR must be set"* ]]
}

@test "validates required args and invokes downstream scripts (tpch)" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run ./setup_benchmark_data_and_tables.sh -b tpch -s my_schema -d sf100 -f 100 -c
  [ "$status" -eq 0 ]

  # Check run_py_script was called with expected args (order-sensitive subset)
  run grep -F "[run_py_script] -p" "$RUN_LOG"
  [ "$status" -eq 0 ]
  [[ "$output" == *"--benchmark-type tpch"* ]]
  [[ "$output" == *"--data-dir-path ${PRESTO_DATA_DIR}/sf100"* ]]
  [[ "$output" == *"--scale-factor 100"* ]]
  [[ "$output" == *"--convert-decimals-to-floats"* ]]

  # Check setup_benchmark_tables invoked with expected args
  run grep -F "[setup_benchmark_tables]" "$RUN_LOG"
  [ "$status" -eq 0 ]
  [[ "$output" == *"-b tpch"* ]]
  [[ "$output" == *"-s my_schema"* ]]
  [[ "$output" == *"-d sf100"* ]]
}

@test "rejects invalid benchmark type" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run ./setup_benchmark_data_and_tables.sh -b foo -s s -d d -f 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"A valid benchmark type"* ]]
}

@test "requires schema and data dir" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run ./setup_benchmark_data_and_tables.sh -b tpch -d sf1 -f 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"Schema name is required"* ]]

  run ./setup_benchmark_data_and_tables.sh -b tpch -s myschema -f 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"Data directory name is required"* ]]
}


