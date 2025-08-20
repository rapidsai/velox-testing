#!/bin/bash

function install_tpchgen() {
    local TPCHGEN_PACKAGE="tpchgen-cli"
    pip show $TPCHGEN_PACKAGE &>/dev/null || pip install $TPCHGEN_PACKAGE
}

function parse_args() { 
    while [[ $# -gt 0 ]]; do
	case $1 in
	    -h|--help)
		echo "Generates a directory of TPCH data in parquet form.  Usage:"    
		echo "$0 -s <scale_factor> -p <num_partitions> -o <output_dir> -t <temp_dir> -n <num_threads> -v (verbose?) -k (keep raw data?)"
		exit 0
		;;
	    -s|--scale-factor)
		if [[ -n $2 ]]; then
		    SCALE_FACTOR=$2
		    shift 2
		else
		    echo "Error: --scale-factor requires a value"
		    exit 1
		fi
		;;
	    -p|--partitions)
		if [[ -n $2 ]]; then
		    NUM_PARTITIONS=$2
		    shift 2
		else
		    echo "Error: --num-partitions requires a value"
		    exit 1
		fi
		;;
	    -o|--output-dir)
		if [[ -n $2 ]]; then
		    OUTPUT_DIR=$2
		    shift 2
		else
		    echo "Error: --output-dir requires a value"
		    exit 1
		fi
		;;
	    -t|--temp-dir)
		if [[ -n $2 ]]; then
		    TEMP_DIR=$2
		    shift 2
		else
		    echo "Error: --temp-dir requires a value"
		    exit 1
		fi
		;;
	    -n|--num-threads)
		if [[ -n $2 ]]; then
		    NUM_THREADS=$2
		    shift 2
		else
		    echo "Error: --num-threads requires a value"
		    exit 1
		fi
		;;
	    -v|--verbose)
		VERBOSE=" -v"
		shift 1
		;;
	    -k|--keep-raw)
		KEEP_RAW=1
		shift 1
		;;
	    *)
		echo "Error: Unknown argument $1"
		exit 1
		;;
	esac
    done
}

function create_data() {
    mkdir $TEMP_DIR
    pushd $TEMP_DIR > /dev/null

    # Create partitioned data (could parallelize).
    for PART in `seq 1 $NUM_PARTITIONS`; do
	[ ! -z $VERBOSE ] && echo "Creating partition $PART"
        mkdir part-$PART
	tpchgen-cli -s $SCALE_FACTOR --output-dir part-$PART --parts $NUM_PARTITIONS --part $PART --format=parquet &
	# Limit concurrent jobs
	if [[ $(jobs -r -p | wc -l) -ge $NUM_THREADS ]]; then
	    wait -n # Wait for any one background job to finish
	fi
    done
    wait

    # Create output directory organized by directory for each table
    for TABLE in customer lineitem nation orders part partsupp region supplier; do
	mkdir $TABLE
    done

    # Move the partitioned data into the new directory structure.
    for PART in `seq 1 $NUM_PARTITIONS`; do
	for TABLE in customer lineitem nation orders part partsupp region supplier; do
	    mv part-$PART/$TABLE.parquet $TABLE/$TABLE-$PART.parquet
	done
	rmdir part-$PART
    done
    popd > /dev/null
    [ ! -z $VERBOSE ] && echo "Data created sucessfully"
}

# defaults
SCALE_FACTOR=1
NUM_PARTITIONS=1
OUTPUT_DIR="data"
TEMP_DIR="data_raw"
VERBOSE=""

parse_args "$@"

install_tpchgen

[ -e $OUTPUT_DIR ] && echo "$OUTPUT_DIR already exists" && exit 1
[ -e $TEMP_DIR ] && echo "$TEMP_DIR already exists" && exit 1
[ ! -z $VERBOSE ] && echo "Starting data creation"
( [ ! -z $VERBOSE ] && time create_data ) || create_data

# Rewrite the files including:
#   decimal -> double conversion
#   page size adjustment
#   dictionary encoding changes
[ ! -z $VERBOSE ] && echo "Starting parquet file rewriting."
REWRITE_ARGS="-i $TEMP_DIR -o $OUTPUT_DIR -n $NUM_THREADS $VERBOSE"
( [ ! -z $VERBOSE ] && time ./rewrite_parquet.py $REWRITE_ARGS ) || ./rewrite_parquet.py $REWRITE_ARGS
[ ! -z $VERBOSE ] && echo "Parquet files rewritten"

# TODO: Add verification step (optional) that lists schema?

[ -z $KEEP_RAW ] && echo "Removing raw data" && rm -rf $TEMP_DIR
