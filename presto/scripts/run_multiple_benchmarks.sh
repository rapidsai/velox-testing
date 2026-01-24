#!/bin/bash

# Defaults
MEMORY_ARRAY=(0)
KVIKIO_ARRAY=(8)
DRIVERS_ARRAY=(2)
WORKERS_ARRAY=(1)
SCHEMA_ARRAY=(sf1k_64mb)
parse_args() {
  while [[ $# -gt 0 ]]; do
      case $1 in
	  -m|--memory-percents)
            if [[ -n $2 ]]; then
		IFS=',' read -ra MEMORY_ARRAY <<< "$2"
		shift 2
            else
		echo "Error: --memory-percents requires a value"
		exit 1
            fi
            ;;
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
	-s|--schema)
	    if [[ -n $2 ]]; then
		IFS=',' read -ra SCHEMA_ARRAY <<< "$2"
		shift 2
            else
		echo "Error: --schema requires a value"
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

export PRESTO_DATA_DIR=/raid/ocs_benchmark_data/tpch/experimental
parse_args "$@"
for schema in "${SCHEMA_ARRAY[@]}"; do
    for kvikio in "${KVIKIO_ARRAY[@]}"; do
	for drivers in "${DRIVERS_ARRAY[@]}"; do
	    for workers in "${WORKERS_ARRAY[@]}"; do
		for memory in "${MEMORY_ARRAY[@]}"; do
		    echo "Running combo: num_workers = $workers, kvikio_threads = $kvikio, num_drivers = $drivers, schema = $schema, memory = $memory"
		    ./start_native_gpu_presto.sh -w $workers --kvikio-threads $kvikio --num-drivers $drivers --memory-percent $memory
		    ./run_benchmark.sh -b tpch -s ${schema} --tag "${schema}_${workers}w_${drivers}d_${kvikio}k_${memory}m_dropcache"
		    ./stop_presto.sh
		done
	    done
	done
    done
done
