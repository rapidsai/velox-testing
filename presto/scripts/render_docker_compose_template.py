#!/usr/bin/env python3

import os
import sys


def main() -> int:
    if len(sys.argv) < 4:
        print("Usage: render_docker_compose_template.py <template_path> <output_path> <num_workers>", file=sys.stderr)
        return 2

    template_path = sys.argv[1]
    output_path = sys.argv[2]
    num_workers_arg = sys.argv[3]

    try:
        num_workers = int(num_workers_arg)
    except ValueError:
        print("ERROR: <num_workers> must be an integer", file=sys.stderr)
        return 2

    try:
        from jinja2 import Environment, FileSystemLoader
    except Exception:
        print("ERROR: Jinja2 is required. Install it via requirements.txt using run_py_script.sh.", file=sys.stderr)
        return 1

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=False,
        keep_trailing_newline=True,
    )
    template = env.get_template(os.path.basename(template_path))
    workers = list(range(max(0, num_workers)))
    rendered = template.render(num_workers=num_workers, workers=workers)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main())


