#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Record cuDF provenance and retry two AST compilations without compiler caches."""

import argparse
import hashlib
import json
import re
import shlex
import subprocess
import tempfile
from pathlib import Path

TRANSLATION_UNITS = ("src/ast/expressions.cpp", "src/ast/jit/expressions.cpp")
HEADERS = (
    "src/jit/row_ir.hpp",
    "src/ast/jit/expressions.hpp",
    "include/cudf/transform.hpp",
    "include/cudf/ast/expressions.hpp",
    "include/cudf/ast/jit/udf.hpp",
)


def provenance(build, velox):
    module = velox / "CMake/resolve_dependency_modules/cudf.cmake"
    print(f"cuDF pin declarations: {module}")
    if module.is_file():
        for match in re.finditer(r"set\(\s*VELOX_cudf_\w+\b[^)]*\)", module.read_text()):
            print("  " + " ".join(match.group().split()))
    else:
        print("  missing dependency module")
    cache = build / "CMakeCache.txt"
    overrides = (
        [] if not cache.is_file() else re.findall(r"^FETCHCONTENT_SOURCE_DIR_CUDF:[^\n]*", cache.read_text(), re.M)
    )
    print("cuDF source override: " + ("; ".join(overrides) or "not recorded"))
    stamps = sorted(build.glob("CMakeFiles/fc-stamp/cudf/*urlinfo.txt"))
    stamps += sorted(build.glob("_deps/cudf-subbuild/cudf-populate-prefix/src/cudf-populate-stamp/*urlinfo.txt"))
    for stamp in stamps:
        print(f"cuDF fetch metadata: {stamp}")
        for line in stamp.read_text().splitlines():
            if line.startswith(("url(s)=", "hash=")):
                print("  " + line)
    if not stamps:
        print("cuDF fetch metadata: not recorded")


def select_entries(build):
    database = json.loads((build / "compile_commands.json").read_text())
    selected = []
    for relative in TRANSLATION_UNITS:
        matches = []
        for entry in database:
            directory = (build / entry["directory"]).resolve()
            source = (directory / entry["file"]).resolve()
            if source.as_posix().endswith("/cpp/" + relative):
                matches.append((entry, directory, source))
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one compile entry for {relative}; found {len(matches)}")
        selected.append(matches[0])
    return selected


def direct_command(entry, output):
    argv = list(entry["arguments"]) if "arguments" in entry else shlex.split(entry["command"])
    while argv and Path(argv[0]).name in {"sccache", "ccache"}:
        argv.pop(0)
    if not argv or not re.fullmatch(r"(?:g\+\+|c\+\+)(?:-\d+(?:\.\d+)*)?", Path(argv[0]).name):
        raise ValueError("Expected a direct GCC C++ compiler after removing cache launchers")
    if "-c" not in argv:
        raise ValueError("Expected a compile-only command containing -c")
    # Reject indirect arguments and auxiliary outputs that cannot be isolated here.
    forbidden = (
        "@",
        "-save-temps",
        "-gsplit-dwarf",
        "-fprofile",
        "-ftest-coverage",
        "--coverage",
        "-fdump",
        "-Wp,-M",
        "-MJ",
    )
    if any(arg.startswith(forbidden) for arg in argv[1:]):
        raise ValueError("Unsupported response file or auxiliary-output flag")
    outputs = {
        "-o": output.with_suffix(".o"),
        "-MF": output.with_suffix(".d"),
        "-MT": output.with_suffix(".o"),
        "-MQ": output.with_suffix(".o"),
    }
    result = [argv[0]]
    dependencies = any(arg in {"-MD", "-MMD", "-M", "-MM"} for arg in argv)
    index = 1
    while index < len(argv):
        arg = argv[index]
        option = next((flag for flag in outputs if arg.startswith(flag)), None)
        if option:
            dependencies |= option != "-o"
            if arg == option:
                index += 1
                if index == len(argv):
                    raise ValueError(f"Missing argument for {option}")
        else:
            result.append(arg)
        index += 1
    result += ["-o", str(outputs["-o"])]
    if dependencies:
        result += ["-MF", str(outputs["-MF"]), "-MT", str(outputs["-MT"])]
    return result


def diagnose(build, velox):
    provenance(build, velox)
    selected = select_entries(build)
    roots = {
        source.parents[len(Path(relative).parts) - 1] for (_, _, source), relative in zip(selected, TRANSLATION_UNITS)
    }
    if len(roots) != 1:
        raise ValueError("The two AST compilation entries use different cuDF source roots")
    source_root = roots.pop()
    for relative in (*TRANSLATION_UNITS, *HEADERS):
        source = source_root / relative
        if source.is_file():
            data = source.read_bytes()
            print(f"SHA256 {hashlib.sha256(data).hexdigest()} bytes={len(data)} {source}")
        else:
            print(f"MISSING {source}")
    output_dir = Path(tempfile.mkdtemp(prefix="cudf-direct-diagnostics-", dir=build))
    print(f"Isolated compiler outputs: {output_dir}", flush=True)
    failed = False
    for number, (entry, directory, source) in enumerate(selected, 1):
        log = output_dir / f"ast-{number}.log"
        try:
            if not source.is_file():
                raise ValueError(f"Missing translation unit: {source}")
            argv = direct_command(entry, output_dir / f"ast-{number}")
            print(f"Direct compile: {source}; compiler={argv[0]}; cwd={directory}", flush=True)
            with log.open("w") as stream:
                result = subprocess.run(
                    argv, cwd=directory, stdout=stream, stderr=subprocess.STDOUT, timeout=300, check=False
                )
            code = result.returncode
        except subprocess.TimeoutExpired:
            code = 124
            print("Direct compilation timed out after 300 seconds")
        except (OSError, ValueError, KeyError) as error:
            code = 2
            print(f"Cannot run direct compilation: {error}")
        print(f"Direct compile exit code: {code}; source={source}; log={log}", flush=True)
        if log.is_file():
            with log.open(errors="replace") as stream:
                excerpt = stream.read(12001)
            print(excerpt[:12000], end="" if excerpt.endswith("\n") else "\n")
            if len(excerpt) > 12000:
                print(f"Compiler output truncated; full output retained in {log}")
        failed |= code != 0
    return int(failed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--velox-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        return diagnose(args.build_dir.resolve(), args.velox_dir.resolve())
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"cuDF diagnostics unavailable: {error}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
