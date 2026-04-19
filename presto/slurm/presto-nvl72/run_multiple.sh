#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

NUM_ITERATIONS=2
while [[ $# -gt 0 ]]; do
    case "$1" in
	 -n|--nodes)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                NODES_COUNT="$2"
                shift 2
            else
		echo "Error: -n|--nodes requires a set of comma separated values.  E.g. (2,4,8)"
                echo "Usage: $0 -n|--nodes <count1,count2> -s|--scale-factor <sf1,sf2> -w <image_name> -c <image_name> [additional sbatch options]"
                exit 1
            fi
            ;;
        -s|--scale-factor)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                SCALE_FACTOR="$2"
                shift 2
            else
		echo "Error: -s|--scale-factor requires a set of comma separated values. E.g. (1000,3000)"
		echo "Usage: $0 -n|--nodes <count1,count2> -s|--scale-factor <sf1,sf2> -w <image_name> -c <image_name> [additional sbatch options]"
                exit 1
            fi
            ;;
        -i|--iterations)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                NUM_ITERATIONS="$2"
                shift 2
            else
                echo "Error: -i|--iterations requires a value"
		echo "Usage: $0 -n|--nodes <count1,count2> -s|--scale-factor <sf1,sf2> -w <image_name> -c <image_name> [additional sbatch options]"
                exit 1
            fi
            ;;
	-w|--worker-image)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                WORKER_IMAGE="$2"
                shift 2
            else
                echo "Error: -w|--worker-image requires a value"
		echo "Usage: $0 -n|--nodes <count1,count2> -s|--scale-factor <sf1,sf2> -w <image_name> -c <image_name> [additional sbatch options]"
                exit 1
            fi
            ;;
	-c|--coord-image)
            if [[ -n "${2:-}" && "${2:0:1}" != "-" ]]; then
                COORD_IMAGE="$2"
                shift 2
            else
                echo "Error: -c|--coord-image requires a value"
		echo "Usage: $0 -n|--nodes <count1,count2> -s|--scale-factor <sf1,sf2> -w <image_name> -c <image_name> [additional sbatch options]"
                exit 1
            fi
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -z "${NODES_COUNT}" ]]; then
    echo "Error: -n|--nodes is required"
    exit 1
fi
if [[ -z "${SCALE_FACTOR}" ]]; then
    echo "Error: -s|--scale-factor is required"
    exit 1
fi
if [[ -z "${WORKER_IMAGE}" ]]; then
    echo "Error: -w|--worker-image is required"
    exit 1
fi
if [[ -z "${COORD_IMAGE}" ]]; then
    echo "Error: -c|--coord-image is required"
    exit 1
fi


mkdir -p kept_results

IFS=',' read -ra NODES_ARRAY <<< "$NODES_COUNT"
IFS=',' read -ra SF_ARRAY <<< "$SCALE_FACTOR"
for s in "${SF_ARRAY[@]}"; do
    for n in "${NODES_ARRAY[@]}"; do
        ./launch-run.sh -s $s -n $n -i $NUM_ITERATIONS -w $WORKER_IMAGE -c $COORD_IMAGE
        cp logs/cli.log kept_results/${n}N-${s}SF-summary.txt
    done
done
