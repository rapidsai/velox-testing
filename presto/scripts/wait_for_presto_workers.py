#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Wait for the requested Docker workers to be healthy and coordinator-active."""

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


class WorkerExited(RuntimeError):
    pass


def expected_workers(config_dir, variant, count, gpu_ids=None, single_container=False):
    workers = []
    ids = gpu_ids if variant == "gpu" and gpu_ids is not None else range(count)
    for worker_id in ids:
        container = f"presto-native-worker-{variant}" if variant != "java" else "presto-java-worker"
        if count > 1 and not single_container:
            container += f"-{worker_id}"
        config_name = f"etc_worker_{worker_id}" if variant == "gpu" or count > 1 else "etc_worker"
        config_type = "java" if variant == "java" else "native"
        config = Path(config_dir) / config_name / f"config_{config_type}.properties"
        properties = dict(
            line.strip().split("=", 1)
            for line in config.read_text().splitlines()
            if "=" in line and not line.lstrip().startswith("#")
        )
        port = int(properties.get("http-server.http.port", "0"))
        if not 1 <= port <= 65535:
            raise ValueError(f"Missing or invalid http-server.http.port in {config}")
        workers.append((container, port))
    return workers


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("readiness deadline reached")
    return seconds


def inspect_endpoints(workers, deadline):
    names = list(dict.fromkeys(["presto-coordinator", *(name for name, _ in workers)]))
    result = subprocess.run(
        ["docker", "inspect", *names], check=True, capture_output=True, text=True, timeout=min(5, remaining(deadline))
    )
    containers = {item["Name"].lstrip("/"): item for item in json.loads(result.stdout)}
    for name in names:
        state = containers[name]["State"]
        if state.get("Status") in ("exited", "dead"):
            raise WorkerExited(f"{name} exited (status {state.get('ExitCode')})")
        if not state.get("Running"):
            raise ValueError(f"{name} is not running")
    coordinator_networks = containers["presto-coordinator"]["NetworkSettings"]["Networks"]
    endpoints = []
    for name, port in workers:
        networks = containers[name]["NetworkSettings"]["Networks"]
        addresses = [network.get("IPAddress") for key, network in networks.items() if key in coordinator_networks]
        ip = next((address for address in addresses if address), None)
        if not ip:
            raise ValueError(f"{name} has no address on the coordinator's network")
        endpoints.append((f"http://{ip}:{port}", f"http://{name}:{port}"))
    return endpoints


def registered_urls(value):
    if isinstance(value, dict):
        return set().union(*(registered_urls(item) for item in value.values()))
    if isinstance(value, list):
        return set().union(*(registered_urls(item) for item in value))
    if isinstance(value, str) and value.startswith(("http://", "https://")):
        return {value.rstrip("/")}
    return set()


def check_ready(workers, coordinator, deadline):
    endpoints = inspect_endpoints(workers, deadline)
    # Local control-plane requests must not follow HTTP proxy environment settings.
    http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def fetch(url):
        with http.open(url, timeout=min(1, remaining(deadline))) as response:
            return json.load(response)

    count = fetch(coordinator + "/v1/cluster?includeLocalInfoOnly=true").get("activeWorkers")
    if type(count) is not int or count != len(workers):
        raise ValueError(f"coordinator reports {count!r}/{len(workers)} active workers")
    nodes = fetch(coordinator + "/v1/node")
    if not isinstance(nodes, list) or len(nodes) != len(workers):
        raise ValueError("coordinator registration count does not match the requested workers")
    urls = registered_urls(nodes)
    for address, hostname in endpoints:
        if not {address, hostname} & urls:
            raise ValueError(f"coordinator has not registered the current worker at {address}")
        state = fetch(address + "/v1/info/state")
        if state != "ACTIVE":
            raise ValueError(f"worker at {address} is {state!r}")


def wait_for_workers(workers, timeout, coordinator="http://localhost:8080", poll_interval=5):
    deadline = time.monotonic() + timeout
    consecutive = 0
    last_error = "workers have not completed two healthy polls"
    while time.monotonic() < deadline:
        try:
            check_ready(workers, coordinator, deadline)
            consecutive += 1
            if consecutive >= 2 and time.monotonic() < deadline:
                return
            print("All workers are active; confirming on the next poll.", flush=True)
        except WorkerExited:
            raise
        except (OSError, ValueError, KeyError, subprocess.SubprocessError, urllib.error.URLError) as error:
            consecutive = 0
            last_error = str(error)
            print(f"Waiting for workers: {last_error}", flush=True)
        time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"Workers did not stay ready within {timeout}s: {last_error}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("cpu", "gpu", "java"), required=True)
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("--num-workers", type=int, required=True)
    parser.add_argument("--gpu-ids")
    parser.add_argument("--single-container", choices=("true", "false"), default="false")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    if args.num_workers < 1 or args.timeout < 1:
        parser.error("worker count and timeout must be positive")
    try:
        ids = [int(value) for value in args.gpu_ids.split(",")] if args.gpu_ids else None
        workers = expected_workers(
            args.config_dir, args.variant, args.num_workers, ids, args.single_container == "true"
        )
        wait_for_workers(workers, args.timeout)
    except (OSError, ValueError, RuntimeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
