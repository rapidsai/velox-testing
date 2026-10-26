#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

Downloads the SQLStorm StackOverflow "DBA" data set (the 1GB scale, from the
dba.stackexchange.com dump) and, optionally, a checkout of the SQLStorm repository
that contains the queries and schema.

OPTIONS:
    -h, --help           Show this help message.
    -d, --data-dir       Directory to extract the CSV files into. The files end up in
                         <data-dir>/stackoverflow_dba/.
    -s, --sqlstorm-dir   If set and missing, shallow-clone github.com/SQL-Storm/SQLStorm here.

EXAMPLES:
    $0 -d /data/sqlstorm -s /data/sqlstorm/SQLStorm

EOF
}

DATA_URL="https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz"
SQLSTORM_REPO="https://github.com/SQL-Storm/SQLStorm.git"
DATA_DIR=""
SQLSTORM_DIR=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      print_help
      exit 0
      ;;
    -d|--data-dir)
      DATA_DIR=$2
      shift 2
      ;;
    -s|--sqlstorm-dir)
      SQLSTORM_DIR=$2
      shift 2
      ;;
    *)
      echo "Error: Unknown argument $1"
      print_help
      exit 1
      ;;
  esac
done

if [[ -z ${DATA_DIR} ]]; then
  echo "Error: --data-dir is required"
  print_help
  exit 1
fi

mkdir -p "${DATA_DIR}"
if [[ -d "${DATA_DIR}/stackoverflow_dba" ]]; then
  echo "Data already present in ${DATA_DIR}/stackoverflow_dba"
else
  echo "Downloading ${DATA_URL}"
  curl -fsSL "${DATA_URL}" | tar -xz -C "${DATA_DIR}"
fi

if [[ -n ${SQLSTORM_DIR} && ! -d ${SQLSTORM_DIR} ]]; then
  echo "Cloning ${SQLSTORM_REPO} into ${SQLSTORM_DIR}"
  git clone --depth 1 "${SQLSTORM_REPO}" "${SQLSTORM_DIR}"
fi
