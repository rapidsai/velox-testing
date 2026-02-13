#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

pip install pre-commit

# Run pre-commit checks
pre-commit run --all-files --show-diff-on-failure
