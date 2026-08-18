#!/usr/bin/env bash
set -uo pipefail

eval "$(aws configure export-credentials --format env)"
