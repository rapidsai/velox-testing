#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Deletes old images on GHCR ("GitHub Packages").
#
# This reduces this project's impact on GitHub's services and reduces the risk
# of hitting quota issues or incurring unnecessary bills.
#
# Positional Arguments:
#
#   * [PACKAGE_NAME] name of the package without organization identifier
#                    (e.g. 'velox-testing-images', not 'rapidsai/velox-testing-images')
#
#   * [DAYS_TO_RETAIN_VERSIONS] Positive integer. All versions more than this many days old will be deleted.
#
# Usage:
#
#  # delete anything more than 7 days old
#  delete-old-images.sh 'rapidsai/velox-testing-images' '7'
#

set -e -u -o pipefail

PACKAGE_NAME="${1}"

DAYS_TO_RETAIN_VERSIONS="${2}"
export DAYS_TO_RETAIN_VERSIONS

echo "searching for '${PACKAGE_NAME}' packages created more than '${DAYS_TO_RETAIN_VERSIONS}' ago"

# 'shellcheck' thinks the $ENV stuff in the 'jq' expression is accidentally-ignored shell interpolation.
# It isn't, it's meant for 'jq' to handle.
#
# shellcheck disable=SC2016
PACKAGE_VERSIONS_TO_DELETE=$(
  SECONDS_IN_A_DAY=86400 \
    gh api \
      --paginate \
      "orgs/rapidsai/packages/container/${PACKAGE_NAME}/versions" \
      --jq '
        map(
          select(
            (now - ((.created_at | fromdateiso8601))) > (($ENV.DAYS_TO_RETAIN_VERSIONS | tonumber) * ($ENV.SECONDS_IN_A_DAY | tonumber))
          )
        )
        | map({id, created_at, metadata})
      '
)

num_package_versions=$(echo "${PACKAGE_VERSIONS_TO_DELETE}" | jq 'length')
echo "found ${num_package_versions} packages to delete:"
echo "${PACKAGE_VERSIONS_TO_DELETE}" | jq .
echo ""

echo "${PACKAGE_VERSIONS_TO_DELETE}" | jq -c '.[]' | while read -r item; do
  package_id=$(echo "${item}" | jq -r '.id')
  created_at=$(echo "${item}" | jq -r '.created_at')

  echo "Deleting package '${package_id}' created at '${created_at}'..."
  gh api \
    --method DELETE \
    "orgs/rapidsai/packages/container/${PACKAGE_NAME}/versions/${package_id}"
done

echo "done deleting old package versions"
