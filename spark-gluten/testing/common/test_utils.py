# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import os

from common.testing.query_utils import load_queries_from_file


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


def get_queries(benchmark_type):
    return load_queries_from_file(get_abs_file_path(f"./queries/{benchmark_type}/queries.json"))


def get_scale_factor_from_file(file_path):
    with open(file_path, "r") as file:
        metadata = json.load(file)
        return metadata["scale_factor"]
