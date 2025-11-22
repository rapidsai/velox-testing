#!/usr/bin/env bats

# Tests for scripts/run_py_script.sh

setup() {
  export TEST_ROOT="$BATS_TEST_TMPDIR/work"
  mkdir -p "$TEST_ROOT/velox-testing/scripts" "$BATS_TEST_TMPDIR/stubs"
  cp -f "$BATS_TEST_DIRNAME/../run_py_script.sh" "$TEST_ROOT/velox-testing/scripts/run_py_script.sh"

  # Stub py_env_functions to avoid real venv work
  cat > "$TEST_ROOT/velox-testing/scripts/py_env_functions.sh" <<'SH'
#!/usr/bin/env bash
init_python_virtual_env(){ echo "init_python_virtual_env"; }
delete_python_virtual_env(){ echo "delete_python_virtual_env"; }
SH

  # Stub pip and python
  cat > "$BATS_TEST_TMPDIR/stubs/pip" <<'SH'
#!/usr/bin/env bash
echo "pip $*" >> "${RUN_LOG:?}"
exit 0
SH
  chmod +x "$BATS_TEST_TMPDIR/stubs/pip"

  cat > "$BATS_TEST_TMPDIR/stubs/python" <<'SH'
#!/usr/bin/env bash
echo "python $*" >> "${RUN_LOG:?}"
exit 0
SH
  chmod +x "$BATS_TEST_TMPDIR/stubs/python"

  export PATH="$BATS_TEST_TMPDIR/stubs:$PATH"
  export RUN_LOG="$BATS_TEST_TMPDIR/calls.log"
  : > "$RUN_LOG"
}

@test "prints help with --help" {
  cd "$TEST_ROOT/velox-testing/scripts"
  run ./run_py_script.sh --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "requires --python-script-path" {
  cd "$TEST_ROOT/velox-testing/scripts"
  run ./run_py_script.sh
  [ "$status" -ne 0 ]
  [[ "$output" == *"--python-script-path must be set"* ]]
}

@test "uses default requirements path and passes through args" {
  cd "$TEST_ROOT/velox-testing/scripts"
  # Create a dummy python script and requirements beside it
  mkdir -p "$BATS_TEST_TMPDIR/py"
  echo -e "print('ok')" > "$BATS_TEST_TMPDIR/py/tool.py"
  echo -e "# requirements" > "$BATS_TEST_TMPDIR/py/requirements.txt"
  run ./run_py_script.sh -p "$BATS_TEST_TMPDIR/py/tool.py" --foo bar --baz
  [ "$status" -eq 0 ]
  run grep -F "pip install -q -r $BATS_TEST_TMPDIR/py/requirements.txt" "$RUN_LOG"
  [ "$status" -eq 0 ]
  run grep -F "python $BATS_TEST_TMPDIR/py/tool.py --foo bar --baz" "$RUN_LOG"
  [ "$status" -eq 0 ]
}




