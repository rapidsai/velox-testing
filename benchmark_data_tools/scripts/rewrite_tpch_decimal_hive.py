#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""CLI entry point for the local TPC-H decimal Hive rewrite."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tpch_hive_rewrite import parse_args, run  # noqa: I001


if __name__ == "__main__":
    run(parse_args())
