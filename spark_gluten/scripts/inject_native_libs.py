#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Inject native shared libraries (.so) into a Gluten bundle JAR.

Writes to /tmp first then copies back to avoid filesystem corruption
on certain Docker mount types.

Usage:
    python3 inject_native_libs.py <jar_path> <releases_dir> [--jni-lib <path>]

Example:
    python3 inject_native_libs.py \
        package/target/gluten-velox-bundle-*.jar \
        cpp/build/releases \
        --jni-lib /tmp/libarrow_cdata_jni.so
"""

import argparse
import hashlib
import os
import sys
import zipfile


def inject(jar_path, releases_dir, jni_libs=None):
    tmp_path = "/tmp/_inject_native.jar"

    so_map = {
        "linux/amd64/libvelox.so": os.path.join(releases_dir, "libvelox.so"),
        "linux/amd64/libgluten.so": os.path.join(releases_dir, "libgluten.so"),
    }

    if jni_libs:
        for lib_path in jni_libs:
            name = os.path.basename(lib_path)
            so_map[f"x86_64/{name}"] = lib_path

    # Verify all source files exist
    for jar_entry, src_path in so_map.items():
        if not os.path.exists(src_path):
            print(f"ERROR: {src_path} not found", file=sys.stderr)
            sys.exit(1)

    with zipfile.ZipFile(jar_path, "r") as zi:
        with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zo:
            for item in zi.infolist():
                if item.filename in so_map:
                    continue
                zo.writestr(item, zi.read(item.filename))

            for jar_entry, src_path in so_map.items():
                info = zipfile.ZipInfo(jar_entry)
                info.compress_type = zipfile.ZIP_STORED
                with open(src_path, "rb") as f:
                    zo.writestr(info, f.read())
                size_mb = os.path.getsize(src_path) / 1024 / 1024
                print(f"  Added {jar_entry} ({size_mb:.1f} MB)")

    # Safe copy back (avoids mount corruption)
    with open(tmp_path, "rb") as s:
        data = s.read()
    h1 = hashlib.md5(data).hexdigest()
    with open(jar_path, "wb") as d:
        d.write(data)
    with open(jar_path, "rb") as f:
        h2 = hashlib.md5(f.read()).hexdigest()

    if h1 != h2:
        print("ERROR: JAR copy corrupted", file=sys.stderr)
        sys.exit(1)

    os.remove(tmp_path)
    size_mb = os.path.getsize(jar_path) / 1024 / 1024
    print(f"Bundle JAR: {size_mb:.1f} MB")


def main():
    parser = argparse.ArgumentParser(description="Inject native libs into Gluten JAR")
    parser.add_argument("jar_path", help="Path to the bundle JAR")
    parser.add_argument("releases_dir", help="Directory containing libvelox.so and libgluten.so")
    parser.add_argument("--jni-lib", action="append", help="Additional JNI .so to inject under x86_64/")
    args = parser.parse_args()
    inject(args.jar_path, args.releases_dir, args.jni_lib)


if __name__ == "__main__":
    main()
