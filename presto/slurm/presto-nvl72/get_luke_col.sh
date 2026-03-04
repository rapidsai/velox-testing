#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

awk -F'|' '
  /^[[:space:]]*Q[0-9]+/ {
    v = $NF
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
    up = toupper(v)
    if (up == "NULL" || v == "") { print "NULL"; next }
    if (v ~ /^-?[0-9]+(\.[0-9]+)?$/) { printf "%.3f\n", v/1000 }
  }
' "$@"
