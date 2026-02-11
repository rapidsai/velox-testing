#!/bin/bash

# Defaults
KVIKIO_ARRAY=(8)
DRIVERS_ARRAY=(2)
WORKERS_ARRAY=(1)
SCHEMA_ARRAY=()
parse_args() {
  while [[ $# -gt 0 ]]; do
      case $1 in
        -k|--kvikio-threads)
            if [[ -n $2 ]]; then
                IFS=',' read -ra KVIKIO_ARRAY <<< "$2"
                shift 2
            else
                echo "Error: --kvikio-threads requires a value"
                exit 1
            fi
            ;;
        -d|--num-drivers)
            if [[ -n $2 ]]; then
                IFS=',' read -ra DRIVERS_ARRAY <<< "$2"
                shift 2
            else
                echo "Error: --kvikio-threads requires a value"
                exit 1
            fi
            ;;
        -w|--num-workers)
            if [[ -n $2 ]]; then
                IFS=',' read -ra WORKERS_ARRAY <<< "$2"
                shift 2
            else
                echo "Error: --num-workers requires a value"
                exit 1
            fi
            ;;
        -s|--schemas)
            if [[ -n $2 ]]; then
                IFS=',' read -ra SCHEMA_ARRAY <<< "$2"
                shift 2
            else
                echo "Error: --schemas requires a value"
                exit 1
            fi
            ;;
        --data-dir)
            if [[ -n $2 ]]; then
                PRESTO_DATA_DIR="$2"
                shift 2
            else
                echo "Error: --data-dir requires a value"
                exit 1
            fi
            ;;
        *)
            echo "Error: Unknown argument $1"
            print_help
            exit 1
            ;;
    esac
  done
}

parse_args "$@"

if [[ ${#SCHEMA_ARRAY[@]} -eq 0 ]]; then
    echo "Error: --schemas is required. Provide a comma-separated list of schema names."
    exit 1
fi

export PRESTO_DATA_DIR=${PRESTO_DATA_DIR:-/raid/ocs_benchmark_data/tpch/experimental}
for schema in "${SCHEMA_ARRAY[@]}"; do
    for kvikio in "${KVIKIO_ARRAY[@]}"; do
        for drivers in "${DRIVERS_ARRAY[@]}"; do
            for workers in "${WORKERS_ARRAY[@]}"; do
                    echo "Running combo: num_workers = $workers, kvikio_threads = $kvikio, num_drivers = $drivers, schema = $schema"
                    ./start_native_gpu_presto.sh -w $workers --kvikio-threads $kvikio --num-drivers $drivers
                    ./run_benchmark.sh -b tpch -s ${schema} --tag "${schema}_${workers}wk_${drivers}dr_${kvikio}kv"
                    ./stop_presto.sh
            done
        done
    done
done
