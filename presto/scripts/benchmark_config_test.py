# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def write_executable(path, contents):
    path.write_text("#!/bin/bash\n" + contents)
    path.chmod(0o755)


@pytest.fixture
def generator(tmp_path):
    scripts = tmp_path / "presto/scripts"
    scripts.mkdir(parents=True)
    shutil.copy(ROOT / "presto/scripts/generate_presto_config.sh", scripts)
    config = tmp_path / "presto/docker/config"
    shutil.copytree(ROOT / "presto/docker/config/template", config / "template")
    shutil.copy(ROOT / "presto/docker/config/params.json", config)
    (tmp_path / "presto/pbench").symlink_to(ROOT / "presto/pbench", target_is_directory=True)
    topology = tmp_path / "template_rendering"
    topology.mkdir()
    shutil.copy(ROOT / "template_rendering/cpu_topology.py", topology)
    commands = tmp_path / "bin"
    commands.mkdir()
    write_executable(commands / "nproc", "echo 128\n")
    write_executable(commands / "lsmem", 'echo "Total online memory: ${TEST_RAM_BYTES:-549755813888}"\n')
    # Missing lscpu data must not abort generation under pipefail.
    write_executable(commands / "lscpu", "exit 1\n")

    def run(variant="cpu", workers=1, **overrides):
        env = {
            key: value for key, value in os.environ.items() if not key.startswith(("CPU_", "SLURM_", "GPU_", "VCPU_"))
        }
        env.update(
            PATH=f"{commands}:{env['PATH']}",
            VARIANT_TYPE=variant,
            NUM_WORKERS=str(workers),
            OVERWRITE_CONFIG="false",
            CPU_BIND_NUMA="false",
        )
        env.update({key: str(value) for key, value in overrides.items()})
        result = subprocess.run(
            ["bash", str(scripts / "generate_presto_config.sh")], env=env, capture_output=True, text=True
        )
        return result, config / "generated" / variant

    return run


def properties(path):
    return dict(
        line.split("=", 1) for line in path.read_text().splitlines() if "=" in line and not line.startswith("#")
    )


def worker(config, name="etc_worker_0"):
    return properties(config / name / "config_native.properties")


def test_cpu_defaults_and_explicit_overrides(generator):
    result, config = generator(workers=2)
    assert result.returncode == 0, result.stdout + result.stderr
    props = worker(config)
    assert props["system-memory-gb"] == "221"
    assert props["task.max-drivers-per-task"] == "64"
    assert props["exchange.max-buffer-size"] == "1GB"
    assert props["sink.max-buffer-size"] == "1GB"
    result, config = generator(
        workers=2,
        CPU_SYSTEM_MEM_GB=128,
        CPU_QUERY_MEM_GB=64,
        CPU_DRIVERS=12,
        CPU_ASYNC_DATA_CACHE="false",
        CPU_EXCHANGE_BUFFER="512MB",
        CPU_SINK_BUFFER="256MB",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    for name in ("etc_worker", "etc_worker_0", "etc_worker_1"):
        props = worker(config, name)
        assert props["system-memory-gb"] == "128"
        assert props["query-memory-gb"] == "64"
        assert props["task.max-drivers-per-task"] == "12"
        assert props["async-data-cache-enabled"] == "false"
        assert props["exchange.max-buffer-size"] == "512MB"
        assert props["sink.max-buffer-size"] == "256MB"
    coordinator = properties(config / "etc_coordinator/config_native.properties")
    assert coordinator["exchange.max-buffer-size"] == "512MB"
    assert coordinator["sink.max-buffer-size"] == "256MB"


def test_slurm_uses_workers_per_host(generator):
    for count in (2, 8, 16, 101):
        result, config = generator(workers=count, CPU_WORKERS_PER_NODE=2)
        assert result.returncode == 0, result.stdout + result.stderr
        props = worker(config)
        assert props["system-memory-gb"] == "221"
        assert props["task.max-drivers-per-task"] == "64"
        assert props["single-node-execution-enabled"] == "false"
        assert worker(config, f"etc_worker_{count - 1}")["http-server.http.port"] == str(10000 + 10 * (count - 1))


def test_slurm_one_worker_per_host_and_allocator(generator):
    result, config = generator(workers=8, CPU_WORKERS_PER_NODE=1, USE_MMAP_ALLOCATOR="false")
    assert result.returncode == 0, result.stdout + result.stderr
    assert worker(config)["task.max-drivers-per-task"] == "128"
    assert worker(config)["system-memory-gb"] == "512"
    assert worker(config)["use-mmap-allocator"] == "false"
    assert worker(config)["single-node-execution-enabled"] == "false"


def test_small_memory_and_slurm_memory_cap(generator):
    result, config = generator(workers=2, SLURM_MEM_PER_NODE=32768)
    assert result.returncode == 0, result.stdout + result.stderr
    props = worker(config)
    assert props["system-memory-gb"] == "12"
    assert props["system-mem-limit-gb"] == "16"
    assert props["system-mem-shrink-gb"] == "1"
    assert props["query-memory-gb"] == "8"


@pytest.mark.parametrize("variant,extra", [("cpu", {}), ("gpu", {"GPU_IDS": "2,3"})])
def test_worker_count_changes_without_regeneration(generator, variant, extra):
    result, config = generator(variant, 2, **extra)
    assert result.returncode == 0, result.stdout + result.stderr
    single_extra = {"GPU_IDS": "3"} if variant == "gpu" else {}
    result, config = generator(variant, 1, **single_extra)
    assert result.returncode == 0, result.stdout + result.stderr
    name = "etc_worker_3" if variant == "gpu" else "etc_worker_0"
    assert worker(config, name)["single-node-execution-enabled"] == "true"
    assert properties(config / "etc_coordinator/config_native.properties")["single-node-execution-enabled"] == "true"
    if variant == "gpu":
        assert worker(config, name)["cudf.exchange"] == "false"
    else:
        assert worker(config, "etc_worker")["single-node-execution-enabled"] == "true"
        assert worker(config, name)["task.max-drivers-per-task"] == "128"


def test_explicit_driver_argument_survives_reuse(generator):
    for count in (2, 1):
        result, config = generator(workers=count, VCPU_PER_WORKER=7)
        assert result.returncode == 0, result.stdout + result.stderr
        assert worker(config)["task.max-drivers-per-task"] == "7"


def test_reject_invalid_cpu_settings(generator):
    result, _ = generator(workers=2, CPU_DRIVERS=256)
    assert result.returncode != 0
    assert "at most 255" in result.stdout


def test_pbench_failure_is_reported(generator, tmp_path):
    pbench = tmp_path / "presto/pbench"
    pbench.unlink()
    pbench.mkdir()
    write_executable(pbench / "pbench", "echo 'generator failure' >&2\nexit 7\n")
    result, _ = generator()
    assert result.returncode != 0
    assert "pbench genconfig failed" in result.stdout
    assert "generator failure" in result.stderr


@pytest.mark.parametrize("count", [1, 2])
@pytest.mark.parametrize("sccache", ["true", "false"])
def test_render_cpu_layout_and_build_secrets(tmp_path, count, sccache):
    output = tmp_path / "compose.yml"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "template_rendering/render_docker_compose_template.py"),
            "--template-path",
            str(ROOT / "presto/docker/docker-compose/template/docker-compose.native-cpu.yml.jinja"),
            "--output-path",
            str(output),
            "--num-workers",
            str(count),
            "--variant",
            "cpu",
            "--single-container",
            "false",
            "--sccache",
            sccache,
        ],
        check=True,
    )
    compose = yaml.safe_load(output.read_text())
    workers = {name: spec for name, spec in compose["services"].items() if name != "presto-coordinator"}
    assert len(workers) == count
    for name, spec in workers.items():
        assert name == "presto-native-worker-cpu" or name in {
            "presto-native-worker-cpu-0",
            "presto-native-worker-cpu-1",
        }
        assert bool(spec["build"].get("secrets")) == (sccache == "true")
        assert spec["environment"]["PRESTO_WORKER_VARIANT"] == "cpu"
        if count == 2:
            assert "NUMA_NODE" in spec["environment"]
    assert ("secrets" in compose) == (sccache == "true")
