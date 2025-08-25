#!/bin/bash

# Parse cmd args
function parse_args() { 
    while [[ $# -gt 0 ]]; do
	case $1 in
	    -h|--help)
		echo "Generates a directory of TPCH data in parquet form.  Usage:"
		echo "  <scale-factor>: size of data == 1GB * scale-factor"
		echo "  <num-partitions>: number of files to split each table into"
		echo "  <output-dir>: directory for the final (optimized and rewritten) parquet data"
		echo "  <temp-dir>: directory for the temporary (unoptimized) data"
		echo "  <num-threads>: number of threads to run while generating/optimizing data"
		echo "  <verbose>: run in verbose mode"
		echo "  <keep raw data>: keep the temp-dir with unoptmized data"
		echo "$0 -s <scale-factor> -p <num-partitions> -o <output-dir> -t <temp-dir> -n <num-threads> -v (verbose?) -k (keep-raw-data?)"
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
	    -k|--keep-raw-data)
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

# Create raw parquet files organized in hive format.
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

# Install Miniforge if not present
function ensure_miniforge() {
  if [[ -x "$MINIFORGE_DIR/bin/conda" ]]; then
    return 0
  fi
  echo "Installing Miniforge into $MINIFORGE_DIR ..."
  local url installer
  url=$(get_installer_url)
  installer=${TMPDIR:-/tmp}/miniforge_installer.sh
  curl -fsSL "$url" -o "$installer"
  bash "$installer" -b -p "$MINIFORGE_DIR"
  rm -f "$installer"
}

# Initialize conda for this shell session
function init_conda() {
  if [[ -f "$MINIFORGE_DIR/etc/profile.d/conda.sh" ]]; then
    . "$MINIFORGE_DIR/etc/profile.d/conda.sh"
  elif [[ -x "$MINIFORGE_DIR/bin/conda" ]]; then
    eval "$("$MINIFORGE_DIR/bin/conda" shell.bash hook)"
  else
    echo "Could not locate conda in $MINIFORGE_DIR" >&2
    exit 1
  fi
}

# Create env if missing
function ensure_env() {
  if conda env list | awk '{print $1}' | grep -qx "$CONDA_ENV_NAME"; then
    return 0
  fi
  echo "Creating conda env $CONDA_ENV_NAME with python=$PYTHON_VERSION ..."
  conda create -y -n "$CONDA_ENV_NAME" "python=$PYTHON_VERSION"
}

# Install dependencies if manifest files are present
function install_dependencies() {
  if [[ -f "$ENV_FILE" ]]; then
    echo "Applying $ENV_FILE to env $CONDA_ENV_NAME ..."
    conda env update -n "$CONDA_ENV_NAME" -f "$ENV_FILE"
  elif [[ -f "$REQUIREMENTS_FILE" ]]; then
    echo "Installing pip requirements from $REQUIREMENTS_FILE ..."
    python -m pip install --upgrade pip
    python -m pip install -r "$REQUIREMENTS_FILE"
  else
    echo "No $ENV_FILE or $REQUIREMENTS_FILE found. Skipping dependency install."
  fi
}

# Conda Configuration via environment variables (override as needed)
MINIFORGE_DIR=${MINIFORGE_DIR:-"$HOME/miniforge3"}
CONDA_ENV_NAME=${CONDA_ENV_NAME:-"velox-testing-env"}
PYTHON_VERSION=${PYTHON_VERSION:-"3.10"}
REQUIREMENTS_FILE=${REQUIREMENTS_FILE:-"requirements.txt"}
ENV_FILE=${ENV_FILE:-"environment.yml"}

ensure_miniforge
init_conda
ensure_env
conda activate "$CONDA_ENV_NAME"
install_dependencies

# defaults
SCALE_FACTOR=1
NUM_PARTITIONS=1
NUM_THREADS=1
OUTPUT_DIR="data"
TEMP_DIR="data_raw"
VERBOSE=""

parse_args "$@"

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

conda deactivate
