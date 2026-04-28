# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import subprocess
from pathlib import Path

import pytest

from .common_fixtures import setup_and_teardown  # noqa: F401


@pytest.fixture(scope="session", autouse=True)
def install_tpchgen_cli():
    benchmark_data_tools_dir = Path(__file__).resolve().parent.parent
    install_script = benchmark_data_tools_dir / "scripts" / "install_tpchgen_cli.sh"
    tpchgen_cli_bin = benchmark_data_tools_dir / ".local_installs" / "bin" / "tpchgen-cli"

    if not tpchgen_cli_bin.exists():
        subprocess.run([str(install_script)], check=True)
