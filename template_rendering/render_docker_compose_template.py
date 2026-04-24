#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os
import re
import sys


def detect_numa_nodes():
    """Return list of NUMA node IDs visible on this host.

    Reads /sys/devices/system/node/node<N> entries. Falls back to [0] when
    the sysfs layout is unavailable (non-Linux, minimal container, etc.).
    """
    node_dir = "/sys/devices/system/node"
    if not os.path.isdir(node_dir):
        return [0]
    nodes = []
    for entry in sorted(os.listdir(node_dir)):
        m = re.match(r"^node(\d+)$", entry)
        if m:
            nodes.append(int(m.group(1)))
    return nodes or [0]


def parse_args():
    parser = argparse.ArgumentParser(description="Render a Docker Compose template")

    # Helper to parse boolean-like strings passed from shell (e.g., "true"/"false")
    def str_to_bool(value: str) -> bool:
        truthy = {"1", "true", "t", "yes", "y", "on"}
        falsy = {"0", "false", "f", "no", "n", "off"}
        val = value.strip().lower()
        if val in truthy:
            return True
        if val in falsy:
            return False
        raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")

    parser.add_argument(
        "--template-path", type=str, required=True, dest="template_path", help="Path to the template file"
    )
    parser.add_argument("--output-path", type=str, required=True, dest="output_path", help="Path to the output file")
    parser.add_argument("--num-workers", type=int, required=True, dest="num_workers", help="Number of workers")
    parser.add_argument(
        "--single-container",
        type=str_to_bool,
        required=True,
        dest="single_container",
        help="Whether to run in a single container",
    )
    parser.add_argument(
        "--gpu-ids", type=str, default=None, dest="gpu_ids", required=False, help="Comma-delimited list of GPU IDs"
    )
    parser.add_argument(
        "--kvikio-threads",
        type=int,
        default=None,
        dest="kvikio_threads",
        required=False,
        help="Number of KvikIO threads (optional).",
    )
    parser.add_argument(
        "--sccache",
        type=str_to_bool,
        default=False,
        dest="sccache",
        required=False,
        help="Enable sccache build secrets in the rendered compose file.",
    )
    parser.add_argument(
        "--variant",
        type=str,
        default="gpu",
        choices=["gpu", "cpu"],
        dest="variant",
        required=False,
        help="Which variant this template describes. 'cpu' uses NUMA-node assignment; "
        "'gpu' uses per-GPU assignment via --gpu-ids.",
    )
    return parser.parse_args()


def main() -> int:
    parsed_args = parse_args()

    # Parse GPU IDs if provided
    gpu_ids = None
    if parsed_args.gpu_ids:
        gpu_ids = [int(gpu_id.strip()) for gpu_id in parsed_args.gpu_ids.split(",")]
        if len(gpu_ids) != parsed_args.num_workers:
            print(
                f"ERROR: Number of GPU IDs ({len(gpu_ids)}) must match num_workers ({parsed_args.num_workers})",
                file=sys.stderr,
            )
            return 2

    try:
        from jinja2 import Environment, FileSystemLoader
    except Exception:
        print("ERROR: Jinja2 is required. Install it via requirements.txt using run_py_script.sh.", file=sys.stderr)
        return 1

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(parsed_args.template_path)),
        autoescape=False,
        keep_trailing_newline=True,
    )
    template = env.get_template(os.path.basename(parsed_args.template_path))

    # Build the worker list.
    # - GPU variant (default): plain list of GPU IDs. Preserves the existing
    #   contract the GPU template expects (worker loop variable is the GPU id).
    # - CPU variant: list of dicts with {id, numa_node}. NUMA assignment is
    #   round-robin across the NUMA nodes detected on the host so that with
    #   --num-workers equal to the node count each worker lands on its own
    #   socket. With fewer workers than nodes, leading nodes are used.
    if parsed_args.variant == "gpu":
        if gpu_ids:
            workers = gpu_ids
        else:
            workers = list(range(max(0, parsed_args.num_workers)))
    else:
        numa_nodes = detect_numa_nodes()
        workers = [
            {"id": i, "numa_node": numa_nodes[i % len(numa_nodes)]}
            for i in range(max(0, parsed_args.num_workers))
        ]

    rendered = template.render(
        num_workers=parsed_args.num_workers,
        workers=workers,
        single_container=parsed_args.single_container,
        kvikio_threads=parsed_args.kvikio_threads,
        sccache=parsed_args.sccache,
        variant=parsed_args.variant,
    )

    os.makedirs(os.path.dirname(parsed_args.output_path), exist_ok=True)
    with open(parsed_args.output_path, "w") as f:
        f.write(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main())
