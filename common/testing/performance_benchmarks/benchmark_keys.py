# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum


class BenchmarkKeys(str, Enum):
    RAW_TIMES_KEY = "raw_times_ms"
    FAILED_QUERIES_KEY = "failed_queries"
    FORMAT_WIDTH_KEY = "format_width"
    AGGREGATE_TIMES_KEY = "agg_times_ms"
    AGGREGATE_TIMES_SUM_KEY = "agg_times_sum_ms"
    AGGREGATE_TIMES_FIELDS_KEY = "agg_times_fields"
    AVG_KEY = "avg"
    MIN_KEY = "min"
    MAX_KEY = "max"
    MEDIAN_KEY = "median"
    GMEAN_KEY = "geometric_mean"
    LUKEWARM_KEY = "lukewarm"
    TAG_KEY = "tag"
    CONTEXT_KEY = "context"
    ITERATIONS_COUNT_KEY = "iterations_count"
    SCHEMA_NAME_KEY = "schema_name"
    # Run configuration (from run context; written to context in benchmark_result.json)
    TIMESTAMP_KEY = "timestamp"
    NUM_WORKERS_KEY = "n_workers"
    GPU_NAME_KEY = "gpu_name"

    ENGINE_KEY = "engine"
    KIND_KEY = "kind"
    GPU_COUNT_KEY = "gpu_count"
    DATASET_NAME_KEY = "dataset_name"
