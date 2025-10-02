#!/bin/bash

set -e

if [[ "$PROFILE" == "ON" ]]; then
  mkdir /presto_profiles

  if [[ -z $PROFILE_ARGS ]]; then
    PROFILE_ARGS="-t nvtx,cuda,osrt 
                  --cuda-memory-usage=true 
                  --cuda-um-cpu-page-faults=true 
                  --cuda-um-gpu-page-faults=true 
                  --cudabacktrace=true"
  fi
  PROFILE_CMD="nsys launch $PROFILE_ARGS"
fi

ldconfig

$PROFILE_CMD presto_server --etc-dir=/opt/presto-server/etc
