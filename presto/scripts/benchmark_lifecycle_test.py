# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def executable(path, body):
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(0o755)


@pytest.fixture
def launcher(tmp_path):
    commands = tmp_path / "bin"
    commands.mkdir()
    executable(commands / "ldconfig", "exit 0\n")
    executable(commands / "nvidia-smi", 'echo unexpected-gpu-probe >> "$TEST_TRACE"\nexit 1\n')
    executable(
        commands / "numactl",
        """if [[ "$1" == --hardware ]]; then
  echo 'node 0 cpus: 0 1'
  echo 'node 1 cpus: 2 3'
  exit 0
fi
echo "$*" >> "$TEST_TRACE"
while [[ "$1" == --* ]]; do shift; done
exec "$@"
""",
    )
    env = os.environ | {
        "PATH": f"{commands}:{os.environ['PATH']}",
        "LOGS_DIR": str(tmp_path / "logs"),
        "SERVER_START_TIMESTAMP": "test",
        "PRESTO_WORKER_VARIANT": "cpu",
        "TEST_TRACE": str(tmp_path / "trace"),
        "TEST_PIDS": str(tmp_path / "pids"),
    }
    return commands, env


def assert_children_gone(pid_path):
    for value in pid_path.read_text().splitlines():
        with pytest.raises(ProcessLookupError):
            os.kill(int(value), 0)


def test_failed_worker_stops_siblings(launcher):
    commands, env = launcher
    executable(
        commands / "presto_server",
        """echo $$ >> "$TEST_PIDS"
if [[ "$1" == *etc0 ]]; then sleep 0.05; exit 7; fi
exec sleep 30
""",
    )
    result = subprocess.run(
        ["bash", str(ROOT / "presto/docker/launch_presto_servers.sh"), "0", "1"],
        env=env,
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert result.returncode == 7, result.stderr
    assert_children_gone(Path(env["TEST_PIDS"]))


def test_cpu_worker_uses_identity_and_ignores_gpu_tool(launcher):
    commands, env = launcher
    executable(commands / "presto_server", 'echo $$ >> "$TEST_PIDS"\necho started-cpu\nexit 9\n')
    env["WORKER_ID"] = "7"
    result = subprocess.run(
        ["bash", str(ROOT / "presto/docker/launch_presto_servers.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert result.returncode != 0
    assert "started-cpu" in (Path(env["LOGS_DIR"]) / "worker_7_test.log").read_text()
    trace = Path(env["TEST_TRACE"]).read_text()
    assert "unexpected-gpu-probe" not in trace
    assert "--interleave=all" in trace


def test_shutdown_reaps_workers(launcher):
    commands, env = launcher
    executable(commands / "presto_server", 'echo $$ >> "$TEST_PIDS"\nexec sleep 30\n')
    process = subprocess.Popen(
        ["bash", str(ROOT / "presto/docker/launch_presto_servers.sh"), "0", "1"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            path = Path(env["TEST_PIDS"])
            if path.exists() and len(path.read_text().splitlines()) == 2:
                break
            time.sleep(0.01)
        else:
            pytest.fail("workers did not launch")
        process.terminate()
        process.communicate(timeout=3)
        assert process.returncode == 143
        assert_children_gone(path)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()


def test_stop_uses_active_project_and_known_service_names(tmp_path):
    scripts = tmp_path / "presto/scripts"
    scripts.mkdir(parents=True)
    shutil.copy(ROOT / "presto/scripts/stop_presto.sh", scripts)
    generated = tmp_path / "presto/docker/docker-compose/generated"
    generated.mkdir(parents=True)
    (generated / "docker-compose.native-cpu.rendered.yml").write_text("services: {}\n")
    commands = tmp_path / "bin"
    commands.mkdir()
    docker = commands / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        + """import json, os, sys
with open(os.environ['TEST_TRACE'], 'a') as trace:
    trace.write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[1] == 'ps':
    print('presto-native-worker-cpu-0\\npresto-coordinator\\npresto-unrelated-job')
elif sys.argv[1] == 'inspect':
    print('original-project')
"""
    )
    docker.chmod(0o755)
    trace = tmp_path / "trace"
    result = subprocess.run(
        ["bash", str(scripts / "stop_presto.sh")],
        env=os.environ | {"PATH": f"{commands}:{os.environ['PATH']}", "TEST_TRACE": str(trace)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in trace.read_text().splitlines()]
    down = [call for call in calls if call[0] == "compose"]
    assert len(down) == 1
    assert down[0][1:3] == ["-p", "original-project"]
    removed = [call for call in calls if call[0] == "rm"]
    assert removed == [["rm", "-f", "presto-native-worker-cpu-0", "presto-coordinator"]]


@pytest.mark.parametrize("sccache", [False, True])
def test_cpu_multiworker_build_targets_rendered_service(tmp_path, sccache):
    scripts = tmp_path / "presto/scripts"
    scripts.mkdir(parents=True)
    for filename in ("start_presto_helper.sh", "start_presto_helper_parse_args.sh"):
        shutil.copy(ROOT / "presto/scripts" / filename, scripts)
    (scripts / "common_functions.sh").write_text("capture_build_provenance() { :; }\n")
    for filename in ("stop_presto.sh", "generate_presto_config.sh"):
        executable(scripts / filename, "exit 0\n")
    shutil.copytree(ROOT / "presto/docker/docker-compose/template", tmp_path / "presto/docker/docker-compose/template")
    shutil.copytree(
        ROOT / "template_rendering", tmp_path / "template_rendering", ignore=shutil.ignore_patterns("__pycache__")
    )
    common_scripts = tmp_path / "scripts"
    common_scripts.mkdir()
    executable(common_scripts / "validate_directories_exist.sh", "exit 0\n")
    executable(common_scripts / "run_py_script.sh", f'script=$2\nshift 2\nexec "{sys.executable}" "$script" "$@"\n')
    auth = tmp_path / "auth"
    auth.mkdir()
    for name in ("github_token", "aws_credentials"):
        (auth / name).write_text("fixture\n")
    commands = tmp_path / "bin"
    commands.mkdir()
    executable(commands / "git", 'echo "$TEST_REPO_ROOT"\n')
    docker = commands / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        + """import json, os, sys, yaml
args = sys.argv[1:]
with open(os.environ['TEST_TRACE'], 'a') as trace:
    trace.write(json.dumps(args) + '\\n')
if args[0] == 'images':
    print('existing-image')
elif args[0] == 'compose':
    with open(args[args.index('-f') + 1]) as source:
        services = yaml.safe_load(source)['services']
    assert set(services) == {'presto-coordinator', 'presto-native-worker-cpu-0', 'presto-native-worker-cpu-1'}
    if 'build' in args:
        assert args[-1] == 'presto-native-worker-cpu-0'
        assert bool(services[args[-1]]['build'].get('secrets')) == (os.environ['TEST_SCCACHE'] == 'true')
else:
    raise AssertionError(args)
"""
    )
    docker.chmod(0o755)
    trace = tmp_path / "trace"
    env = os.environ | {
        "PATH": f"{commands}:{os.environ['PATH']}",
        "TEST_TRACE": str(trace),
        "TEST_REPO_ROOT": str(tmp_path),
        "TEST_SCCACHE": str(sccache).lower(),
        "VARIANT_TYPE": "cpu",
        "SCRIPT_NAME": "start_native_cpu_presto.sh",
        "SCCACHE_AUTH_DIR": str(auth),
    }
    args = ["bash", str(scripts / "start_presto_helper.sh"), "-w", "2", "-b", "worker"]
    if sccache:
        args.append("--sccache")
    result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in trace.read_text().splitlines()]
    builds = [call for call in calls if call[0] == "compose" and "build" in call]
    assert len(builds) == 1
    assert builds[0][-1] == "presto-native-worker-cpu-0"


@pytest.mark.parametrize("use_numa,expected_binding", [("0", "false"), ("1", "true")])
def test_slurm_exports_local_worker_layout(tmp_path, use_numa, expected_binding):
    scripts = tmp_path / "presto/slurm/presto-nvl72"
    scripts.mkdir(parents=True)
    shutil.copy(ROOT / "presto/slurm/presto-nvl72/slurm_common.sh", scripts)
    (scripts / "defaults.env").write_text("")
    commands = tmp_path / "bin"
    commands.mkdir()
    executable(commands / "scontrol", "echo node0\n")
    env = os.environ | {
        "PATH": f"{commands}:{os.environ['PATH']}",
        "SCRIPT_DIR": str(scripts),
        "VARIANT_TYPE": "cpu",
        "SCALE_FACTOR": "100",
        "WORKER_IMAGE": "worker",
        "COORD_IMAGE": "coord",
        "NUM_GPUS_PER_NODE": "2",
        "SLURM_JOB_NUM_NODES": "4",
        "SLURM_JOB_NODELIST": "node[0-3]",
        "SLURM_JOB_ID": "test",
        "CLUSTER_DEFAULT_PORT": "8080",
        "USE_NUMA": use_numa,
        "DATA": str(tmp_path),
        "IMAGE_DIR": str(tmp_path),
        "EXPECTED_BINDING": expected_binding,
    }
    result = subprocess.run(
        [
            "bash",
            "-eu",
            "-c",
            """source "$SCRIPT_DIR/slurm_common.sh"
[[ "$NUM_WORKERS" == 8 ]]
[[ "$CPU_WORKERS_PER_NODE" == 2 ]]
[[ "$CPU_BIND_NUMA" == "$EXPECTED_BINDING" ]]
""",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "args",
    [
        ["-w", "0"],
        ["--wait-timeout", "bad"],
        ["--num-drivers", "0"],
        ["--single-container"],
        ["--gpu-ids", "0"],
    ],
)
def test_invalid_cpu_launch_arguments_fail(args):
    env = os.environ | {"VARIANT_TYPE": "cpu", "SCRIPT_NAME": "start_native_cpu_presto.sh"}
    result = subprocess.run(
        ["bash", str(ROOT / "presto/scripts/start_presto_helper_parse_args.sh"), *args],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "Error:" in result.stderr
