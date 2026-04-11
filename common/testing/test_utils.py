# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import os


def get_queries(benchmark_type, queries_file=None):
    if queries_file:
        path = queries_file if os.path.isabs(queries_file) else os.path.abspath(queries_file)
    else:
        path = get_abs_file_path(__file__, f"./queries/{benchmark_type}/queries.json")
    with open(path, "r") as file:
        return json.load(file)


def get_scale_factor_from_file(file):
    with open(get_abs_file_path(__file__, file), "r") as file:
        metadata = json.load(file)
        if "scale_factor" in metadata:
            return metadata["scale_factor"]
        # Support richer metadata format where scale_factor is nested under "options"
        if "options" in metadata and "scale_factor" in metadata["options"]:
            return metadata["options"]["scale_factor"]
        raise KeyError(f"scale_factor not found in metadata file: {file.name}")


def get_abs_file_path(file_path, relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(file_path), relative_path))
