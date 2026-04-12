#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

function start_profiler() {
    local -r profile_output_file_path=$1
    local -r query_id=$(basename ${profile_output_file_path})
    touch "/workspace/presto/slurm/presto-nvl72/logs/.nsys_start_token_${query_id}"
}

function stop_profiler() {
    local -r profile_output_file_path=$1
    local -r query_id=$(basename ${profile_output_file_path})
    touch "/workspace/presto/slurm/presto-nvl72/logs/.nsys_stop_token_${query_id}"
}
