# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from .common_fixtures import ensure_tpchgen_cli_available


def pytest_sessionstart(session):
    ensure_tpchgen_cli_available()
