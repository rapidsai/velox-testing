# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import os


def get_queries(benchmark_type):
    with open(get_abs_file_path(__file__, f"./queries/{benchmark_type}/queries.json"), "r") as file:
        return json.load(file)


def get_scale_factor_from_file(file):
    with open(get_abs_file_path(__file__, file), "r") as file:
        metadata = json.load(file)
        return metadata["scale_factor"]


def get_abs_file_path(file_path, relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(file_path), relative_path))
