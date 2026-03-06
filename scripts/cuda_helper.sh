#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# CUDA utility functions shared across projects.

# Detect the native CUDA architecture from the host GPU using nvidia-smi.
detect_cuda_architecture() {
  if ! command -v nvidia-smi >/dev/null 2>&1; then
    return 1
  fi

  local compute_cap
  if compute_cap=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader,nounits 2>/dev/null | head -1); then
    if [[ -n "$compute_cap" && "$compute_cap" =~ ^[0-9]+\.[0-9]+$ ]]; then
      echo "$compute_cap" | tr -d '.'
      return 0
    fi
  fi
  return 1
}
