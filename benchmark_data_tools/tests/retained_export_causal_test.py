# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# BEGIN LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
# This module tests only local retained-export causal instrumentation.

import importlib.util
import sys
import threading
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


runner = _load(
    "retained_export_causal_test_runner",
    REPO
    / "rg_analysis"
    / "current"
    / "tools"
    / "retained-export"
    / "retained_database_export_runner_dev_only.py",
)
telemetry = _load(
    "retained_export_causal_test_telemetry",
    REPO
    / "rg_analysis"
    / "current"
    / "studies"
    / "copy-parallelism-causal"
    / "tools"
    / "process_telemetry.py",
)
publisher = _load(
    "retained_export_causal_test_publisher",
    REPO
    / "rg_analysis"
    / "current"
    / "studies"
    / "copy-parallelism-causal"
    / "tools"
    / "publish.py",
)
fanout_probe = _load(
    "retained_export_causal_test_fanout_probe",
    REPO
    / "rg_analysis"
    / "current"
    / "tools"
    / "cursor-fanout-probe.py",
)


def test_experiment_settings_accept_only_scoped_allowlist():
    assert runner._experiment_settings(
        ["pin_threads=off", "external_threads=32", "scheduler_process_partial=true"]
    ) == {
        "pin_threads": "off",
        "external_threads": "32",
        "scheduler_process_partial": "true",
    }

    with pytest.raises(ValueError, match="unsupported"):
        runner._experiment_settings(["memory_limit=1GiB"])
    with pytest.raises(ValueError, match="duplicate"):
        runner._experiment_settings(["pin_threads=off", "pin_threads=on"])


def test_export_settings_validate_requested_intervention():
    settings = {
        "threads": "256",
        "memory_limit": "1.0 TiB",
        "preserve_insertion_order": "false",
        "pin_threads": "off",
    }
    runner._validate_export_settings(settings, 256, 1024, "test", {"pin_threads": "off"})

    with pytest.raises(runner.SettingsMismatch, match="pin_threads"):
        runner._validate_export_settings(settings, 256, 1024, "test", {"pin_threads": "on"})


def test_cpu_list_expansion():
    assert telemetry._expand_cpu_list("0-2,7,10-11") == {0, 1, 2, 7, 10, 11}


def test_factorial_interaction_reports_host_gap_attenuation():
    rows = []
    values = {
        ("gen", False): (200.0, 100.0),
        ("gen", True): (120.0, 100.0),
        ("tur", False): (100.0, 90.0),
        ("tur", True): (100.0, 90.0),
    }
    for (host, enabled), (same, reopen) in values.items():
        for lifecycle, copy_seconds in (
            ("same_connection", same),
            ("reopen_read_write", reopen),
        ):
            rows.append(
                {
                    "host": host,
                    "scale_factor": 2000,
                    "lifecycle": lifecycle,
                    "allocator_background": enabled,
                    "median_copy_seconds": copy_seconds,
                    "median_e2e_seconds": copy_seconds,
                }
            )

    result = publisher._factorial_interactions(rows)[0]

    assert result["gen_minus_turin_copy_interaction_control_points"] == pytest.approx(40)
    assert result["gen_minus_turin_copy_interaction_allocator_points"] == pytest.approx(
        6.6666667
    )
    assert result["interaction_attenuation_percent"] == pytest.approx(83.3333333)


def test_cursor_fanout_probe_exposes_serialized_admission():
    class Cursor:
        def execute(self, _query):
            return self

        def fetchone(self):
            return (1,)

        def close(self):
            return None

    class SerializedRoot:
        def __init__(self):
            self.lock = threading.Lock()

        def cursor(self):
            with self.lock:
                time.sleep(0.01)
                return Cursor()

    result = fanout_probe._fanout("serialized", SerializedRoot(), 4, 5)

    assert result["writers"] == 4
    assert len(result["details"]) == 4
    assert result["admission_offset_seconds"]["maximum"] >= 0.035
    assert result["cursor_open_seconds"]["maximum"] >= 0.035


# END LOCAL E2E PROFILING (REMOVE BEFORE UPSTREAM)
