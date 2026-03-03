# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os

import pytest

from common.testing.test_utils import get_abs_file_path, get_scale_factor_from_file


def get_scale_factor(request, benchmark_type):
    dataset_name = request.config.getoption("--dataset-name")

    dataset_dir = get_dataset_dir(benchmark_type, dataset_name)
    meta_file = f"{dataset_dir}/metadata.json"
    if not os.path.exists(meta_file):
        raise pytest.UsageError(
            f"Could not find metadata file in dataset directory '{dataset_dir}'.\n"
            "Metadata file must be called 'metadata.json' and have the following format:\n"
            "{\n"
            '  "scale_factor": <scale_factor>\n'
            "}\n"
            "where <scale_factor> is a floating point number."
        )
    return get_scale_factor_from_file(meta_file)


def get_dataset_dir(benchmark_type, dataset_name):
    if dataset_name:
        dataset_dir = f"{os.environ['SPARK_DATA_DIR']}/{dataset_name}"
    else:
        # Use the common testing data directory
        dataset_dir = get_abs_file_path(__file__, f"../../../common/testing/integration_tests/data/{benchmark_type}")
    if not os.path.isdir(dataset_dir):
        raise Exception(f"Dataset directory path '{dataset_dir}' does not exist")
    return dataset_dir
