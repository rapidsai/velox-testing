# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest

from common.testing.integration_tests import test_utils

from ..common.fixtures import tables_from_dataset_dir
from ..common.test_utils import get_dataset_dir


@pytest.fixture(scope="module")
def setup_and_teardown(request, base_setup_and_teardown):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    dataset_name = request.config.getoption("--dataset-name")

    dataset_dir = get_dataset_dir(benchmark_type, dataset_name)
    tables = tables_from_dataset_dir(dataset_dir)

    use_reference_results = bool(request.config.getoption("--reference-results-dir"))

    for table in tables:
        table_data_dir = f"{dataset_dir}/{table}"

        if not use_reference_results:
            test_utils.create_duckdb_table(table, table_data_dir)

    test_utils.initialize_output_dir(request.config, "spark")

    yield
