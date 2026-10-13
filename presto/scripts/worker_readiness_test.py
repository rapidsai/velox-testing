# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
import wait_for_presto_workers as readiness


@pytest.fixture
def coordinator(monkeypatch):
    state = {"activeWorkers": 1, "worker_state": "ACTIVE"}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/v1/cluster"):
                body = {"activeWorkers": state["activeWorkers"]}
            elif self.path == "/v1/node":
                body = state["nodes"]
            elif self.path == "/v1/info/state":
                body = state["worker_state"]
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"
    state["nodes"] = [{"properties": {"http": url}}]
    monkeypatch.setattr(readiness, "inspect_endpoints", lambda *_: [(url, "http://worker:8080")])
    try:
        yield url, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_healthy_workers_ignore_proxy_settings(coordinator, monkeypatch):
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("no_proxy", "")
    url, _ = coordinator
    readiness.check_ready([("worker", 8080)], url, time.monotonic() + 2)


@pytest.mark.parametrize("count", [0, 2, True, None])
def test_retained_registrations_do_not_satisfy_readiness(coordinator, count):
    url, state = coordinator
    state["activeWorkers"] = count
    with pytest.raises(ValueError, match="active workers"):
        readiness.check_ready([("worker", 8080)], url, time.monotonic() + 2)


def test_starting_worker_not_ready(coordinator):
    url, state = coordinator
    state["worker_state"] = "STARTING"
    with pytest.raises(ValueError, match="STARTING"):
        readiness.check_ready([("worker", 8080)], url, time.monotonic() + 2)


def test_stale_worker_address_not_ready(coordinator):
    url, state = coordinator
    state["nodes"] = [{"properties": {"http": "http://old-worker:8080"}}]
    with pytest.raises(ValueError, match="current worker"):
        readiness.check_ready([("worker", 8080)], url, time.monotonic() + 2)


def test_extra_registration_not_ready(coordinator):
    url, state = coordinator
    state["nodes"].append({"properties": {"http": "http://old-worker:8080"}})
    with pytest.raises(ValueError, match="registration count"):
        readiness.check_ready([("worker", 8080)], url, time.monotonic() + 2)


def test_ready_streak_resets_after_failed_poll(monkeypatch):
    observations = iter([True, False, True, True])
    calls = []

    def check(*_):
        calls.append(next(observations))
        if not calls[-1]:
            raise ValueError("recovering")

    monkeypatch.setattr(readiness, "check_ready", check)
    readiness.wait_for_workers([("worker", 8080)], 1, poll_interval=0)
    assert calls == [True, False, True, True]


def test_timeout_is_failure(monkeypatch):
    def check(*_):
        raise ValueError("stale registration")

    monkeypatch.setattr(readiness, "check_ready", check)
    with pytest.raises(TimeoutError, match="stale registration"):
        readiness.wait_for_workers([("worker", 8080)], 0.02, poll_interval=0.005)


def test_worker_exit_is_immediate_failure(monkeypatch):
    containers = [
        {"Name": "/presto-coordinator", "State": {"Running": True}},
        {"Name": "/worker", "State": {"Status": "exited", "ExitCode": 7}},
    ]
    monkeypatch.setattr(
        readiness.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(stdout=json.dumps(containers))
    )
    with pytest.raises(readiness.WorkerExited, match="status 7"):
        readiness.inspect_endpoints([("worker", 8080)], time.monotonic() + 1)


@pytest.mark.parametrize(
    "variant,count,ids,combined,configs,expected",
    [
        ("cpu", 1, None, False, [("etc_worker", 8080)], [("presto-native-worker-cpu", 8080)]),
        (
            "cpu",
            2,
            None,
            False,
            [("etc_worker_0", 10000), ("etc_worker_1", 10010)],
            [("presto-native-worker-cpu-0", 10000), ("presto-native-worker-cpu-1", 10010)],
        ),
        (
            "gpu",
            2,
            [3, 5],
            True,
            [("etc_worker_3", 10030), ("etc_worker_5", 10050)],
            [("presto-native-worker-gpu", 10030), ("presto-native-worker-gpu", 10050)],
        ),
    ],
)
def test_read_ports_from_active_config_layout(tmp_path, variant, count, ids, combined, configs, expected):
    for name, port in configs:
        directory = tmp_path / name
        directory.mkdir()
        (directory / "config_native.properties").write_text(f"http-server.http.port={port}\n")
    assert readiness.expected_workers(tmp_path, variant, count, ids, combined) == expected
