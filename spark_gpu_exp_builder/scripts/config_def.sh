#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Shell bridge for config definitions.
# Reads config_def.json via parse-config.py and emits CONFIG_DEF_TABLE +
# shell helper functions (apply_env_aliases, apply_defaults, validate_required).
#
# Usage: source this file (same API as before).

_CFG_DEF_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
eval "$(python3 "${_CFG_DEF_DIR}/parse-config.py" shell-helpers)"
unset _CFG_DEF_DIR
