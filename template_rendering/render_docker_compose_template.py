#!/usr/bin/env python3

import os
import sys
import argparse

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

    parser.add_argument("--template-path", type=str, required=True, dest="template_path",
                        help="Path to the template file")
    parser.add_argument("--output-path", type=str, required=True, dest="output_path",
                        help="Path to the output file")
    parser.add_argument("--num-workers", type=int, required=True, dest="num_workers",
                        help="Number of workers")
    parser.add_argument("--single-container", type=str_to_bool, required=True, dest="single_container",
                        help="Whether to run in a single container")
    parser.add_argument("--gpu-ids", type=str, default=None, dest="gpu_ids", required=False,
                        help="Comma-delimited list of GPU IDs")
    parser.add_argument("--kvikio-threads", type=int, default=None, dest="kvikio_threads", required=False,
                        help="Number of KvikIO threads (optional).")
    parser.add_argument("--embedded-coordinator", type=str_to_bool, required=False, default=True,
                        dest="embedded_coordinator",
                        help="If true, first worker container will also run the Java coordinator and the separate coordinator service is omitted.")
    return parser.parse_args()

def main() -> int:
    parsed_args = parse_args()

    # Parse GPU IDs if provided
    gpu_ids = None
    if parsed_args.gpu_ids:
        gpu_ids = [int(gpu_id.strip()) for gpu_id in parsed_args.gpu_ids.split(',')]
        if len(gpu_ids) != parsed_args.num_workers:
            print(f"ERROR: Number of GPU IDs ({len(gpu_ids)}) must match num_workers ({parsed_args.num_workers})", file=sys.stderr)
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
    
    # If GPU IDs are provided, use them; otherwise default to range
    if gpu_ids:
        workers = gpu_ids
    else:
        workers = list(range(max(0, parsed_args.num_workers)))
    
    rendered = template.render(
        num_workers=parsed_args.num_workers,
        workers=workers,
        single_container=parsed_args.single_container,
        kvikio_threads=parsed_args.kvikio_threads,
        embedded_coordinator=parsed_args.embedded_coordinator,
    )

    os.makedirs(os.path.dirname(parsed_args.output_path), exist_ok=True)
    with open(parsed_args.output_path, "w") as f:
        f.write(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main())


