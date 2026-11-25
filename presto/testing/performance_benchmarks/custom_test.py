# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

BENCHMARK_TYPE = "custom"


def test_query(benchmark_query, custom_query_id):
    benchmark_query(custom_query_id)
