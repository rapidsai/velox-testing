#!/usr/bin/env bats

# Tests for presto/scripts/setup_benchmark_tables.sh

setup() {
  export TEST_ROOT="$BATS_TEST_TMPDIR/work"
  export LOG_DIR="$BATS_TEST_TMPDIR/logs"
  mkdir -p "$TEST_ROOT/velox-testing/scripts" "$TEST_ROOT/velox-testing/presto/scripts" \
           "$TEST_ROOT/velox-testing/benchmark_data_tools" "$LOG_DIR" "$BATS_TEST_TMPDIR/stubs"

  # Copy needed helpers
  cp -f "$BATS_TEST_DIRNAME/../../scripts/helper_function.sh" \
        "$TEST_ROOT/velox-testing/scripts/helper_function.sh"
  cp -f "$BATS_TEST_DIRNAME/../scripts/common_functions.sh" \
        "$TEST_ROOT/velox-testing/presto/scripts/common_functions.sh"
  cp -f "$BATS_TEST_DIRNAME/../scripts/setup_benchmark_helper_check_instance_and_parse_args.sh" \
        "$TEST_ROOT/velox-testing/presto/scripts/setup_benchmark_helper_check_instance_and_parse_args.sh"
  cp -f "$BATS_TEST_DIRNAME/../scripts/setup_benchmark_tables.sh" \
        "$TEST_ROOT/velox-testing/presto/scripts/setup_benchmark_tables.sh"

  # Stubs for curl/jq
  cat > "$BATS_TEST_TMPDIR/stubs/curl" <<'SH'
#!/usr/bin/env bash
for ((i=1; i<=$#; i++)); do
  if [[ "${!i}" == "-o" ]]; then outvar=$((i+1)); out=${!outvar}; echo '[1]' > "$out"; break; fi
done
exit 0
SH
  chmod +x "$BATS_TEST_TMPDIR/stubs/curl"
  echo -e '#!/usr/bin/env bash\necho 1' > "$BATS_TEST_TMPDIR/stubs/jq"
  chmod +x "$BATS_TEST_TMPDIR/stubs/jq"

  # Stub run_py_script to log calls
  cat > "$TEST_ROOT/velox-testing/scripts/run_py_script.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
echo "[run_py_script] $*" >> "${RUN_LOG:?}"
exit 0
SH
  chmod +x "$TEST_ROOT/velox-testing/scripts/run_py_script.sh"

  # Create expected python scripts/requirements so readlink -f succeeds
  mkdir -p "$TEST_ROOT/velox-testing/benchmark_data_tools"
  mkdir -p "$TEST_ROOT/velox-testing/presto/testing/integration_tests"
  echo '#!/usr/bin/env python3' > "$TEST_ROOT/velox-testing/benchmark_data_tools/generate_table_schemas.py"
  echo '#!/usr/bin/env python3' > "$TEST_ROOT/velox-testing/presto/testing/integration_tests/create_hive_tables.py"
  echo '# requirements' > "$TEST_ROOT/velox-testing/presto/testing/requirements.txt"

  export PATH="$BATS_TEST_TMPDIR/stubs:$PATH"
  export PRESTO_DATA_DIR="$BATS_TEST_TMPDIR/presto_data"
  mkdir -p "$PRESTO_DATA_DIR/sf10"
  export RUN_LOG="$LOG_DIR/calls.log"
  : > "$RUN_LOG"
}

@test "prints help with --help" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run ./setup_benchmark_tables.sh --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "fails if data dir does not exist" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run ./setup_benchmark_tables.sh -b tpch -s myschema -d missing
  [ "$status" -ne 0 ]
  [[ "$output" == *"Benchmark data must already exist"* ]]
}

@test "invokes schema and create tables scripts with expected args" {
  cd "$TEST_ROOT/velox-testing/presto/scripts"
  run ./setup_benchmark_tables.sh -b tpch -s myschema -d sf10
  [ "$status" -eq 0 ]
  run grep -F "[run_py_script] -p" "$RUN_LOG"
  [ "$status" -eq 0 ]
  # Two invocations expected (schema gen, create tables)
  run grep -F "[run_py_script] -p" "$RUN_LOG"
  [ "$status" -eq 0 ]

  run grep -c "\[run_py_script] -p" "$RUN_LOG"
  [ "$status" -eq 0 ]
  [ "$output" -eq 2 ]

  # Check presence of key args in logs (no redirections inside `run`)
  run grep -F -- "--benchmark-type tpch" "$RUN_LOG"
  [ "$status" -eq 0 ]

  run grep -F -- "--schema-name myschema" "$RUN_LOG"
  [ "$status" -eq 0 ]

  run grep -F -- "--data-dir-name sf10" "$RUN_LOG"
}


