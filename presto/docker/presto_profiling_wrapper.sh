#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

if [[ "$PROFILE" == "ON" ]]; then
  mkdir -p /presto_profiles

  if [[ -z $PROFILE_ARGS ]]; then
    PROFILE_ARGS="-t nvtx,cuda"
  fi
  PROFILE_CMD="nsys launch $PROFILE_ARGS"
fi

ldconfig

$PROFILE_CMD bash /opt/launch_presto_servers.sh "$@"
