#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""General-purpose Jinja template renderer.

Renders any Jinja template with arbitrary variables passed via ``--var key=value``.
Values are auto-coerced: booleans (true/false), integers, and comma-separated
lists of integers (e.g. ``0,1,2``) are converted to their Python equivalents.
"""

import argparse
import os
import sys


def _coerce_value(raw: str):
    """Convert a string value to bool, int, int-list, or leave as str."""
    lower = raw.strip().lower()
    if lower in {"true", "yes", "on", "1"}:
        return True
    if lower in {"false", "no", "off", "0"}:
        return False

    if "," in raw:
        parts = [p.strip() for p in raw.split(",")]
        try:
            return [int(p) for p in parts]
        except ValueError:
            return parts

    try:
        return int(raw)
    except ValueError:
        return raw


def parse_args():
    parser = argparse.ArgumentParser(description="Render a Jinja template with arbitrary variables")
    parser.add_argument(
        "--template-path", type=str, required=True, dest="template_path", help="Path to the Jinja template file"
    )
    parser.add_argument("--output-path", type=str, required=True, dest="output_path", help="Path to the output file")
    parser.add_argument(
        "--var",
        action="append",
        default=[],
        dest="vars",
        metavar="KEY=VALUE",
        help="Template variable in KEY=VALUE format (repeatable)",
    )
    return parser.parse_args()


def main() -> int:
    parsed_args = parse_args()

    template_vars: dict = {}
    for item in parsed_args.vars:
        if "=" not in item:
            print(f"ERROR: --var value must be KEY=VALUE, got: {item}", file=sys.stderr)
            return 2
        key, _, value = item.partition("=")
        template_vars[key.strip()] = _coerce_value(value)

    if "num_executors" in template_vars and "executors" not in template_vars:
        n = template_vars["num_executors"]
        if "gpu_ids" in template_vars:
            gpu_ids = template_vars.pop("gpu_ids")
            if isinstance(gpu_ids, list):
                template_vars["executors"] = gpu_ids
            else:
                template_vars["executors"] = [int(g.strip()) for g in str(gpu_ids).split(",")]
        else:
            template_vars["executors"] = list(range(n))

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

    rendered = template.render(**template_vars)

    os.makedirs(os.path.dirname(parsed_args.output_path), exist_ok=True)
    with open(parsed_args.output_path, "w") as f:
        f.write(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main())
