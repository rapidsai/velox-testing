# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest

from . import test_utils


@pytest.fixture(scope="module")
def tpch_queries(request):
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    queries = test_utils.get_queries(benchmark_type)

    # Get scale factor from config option or default to 0.01
    scale_factor = request.config.getoption("--scale-factor")
    if scale_factor is None:
        scale_factor = 0.01
    else:
        scale_factor = float(scale_factor)

    # The "fraction" portion of Q11 is a value that depends on scale factor
    # (it should be 0.0001 / scale_factor), whereas our query is currently hard-coded as 0.0001.
    value_ratio = 0.0001 / scale_factor
    queries["Q11"] = queries["Q11"].format(SF_FRACTION=f"{value_ratio:.12f}")

    return queries
