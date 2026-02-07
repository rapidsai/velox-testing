# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from .common_fixtures import *  # noqa: F403

BENCHMARK_TYPE = "tpcds"


def test_query(benchmark_query, tpcds_query_id):
    benchmark_query(tpcds_query_id)
