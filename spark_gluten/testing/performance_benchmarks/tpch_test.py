# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

BENCHMARK_TYPE = "tpch"


def test_query(benchmark_query, tpch_query_id):
    benchmark_query(tpch_query_id)
