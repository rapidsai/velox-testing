# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest
from generate_data_files import generate_data_files

pytestmark = pytest.mark.parametrize("setup_and_teardown", ["tpch"], indirect=True)


def test_memory_limit_rejected_for_tpch(setup_and_teardown):
    """--memory-limit is only supported for TPC-DS generation."""
    _, args = setup_and_teardown
    args.memory_limit = 1024 * 1024 * 1024
    with pytest.raises(ValueError, match="only supported for TPC-DS"):
        generate_data_files(args)
