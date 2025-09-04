#!/bin/bash

BASE_DIR="$(dirname $(realpath $0))/../.."
CREATE_TABLES=""
CREATE_PROFILES=""
QUERIES="Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22"

function print_help() {
    cat << EOF

Usage: $0 [OPTIONS]

This script runs tpch benchmarks

OPTIONS:
    -h, --help              Show this help message.
    -c, --create-tables	    Create the tpch tables.
    -p, --profile	    Profile queries with nsys
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.

EXAMPLES:
    $0 -c -q "Q1, Q2" -p
    $0 -h

EOF
}

function parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
	-h|--help)
            print_help
            exit 0
            ;;
	-c|--create-tables)
	    CREATE_TABLES=true
	    shift 1
	    ;;
	-p|--profile)
	    CREATE_PROFILES=true
	    shift 1
	    ;;
	-q|--queries)
	    if [[ -n $2 ]]; then
		QUERIES=$2
		shift 2
            else
		echo "Error: --queries requires a value"
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

function create_tables() {
    pattern="\/([^\/]*)\.sql"
    for sql_file in $(ls $BASE_DIR/presto/testing/integration_tests/schemas/tpch/*.sql); do
	if [[ "$sql_file" =~ $pattern ]]; then
	    table_name="${BASH_REMATCH[1]}"
	    drop_table="DROP TABLE IF EXISTS $table_name"
	else
	    echo "failed to parse schema files"
	    exit 1
	fi
	docker compose -f $BASE_DIR/presto/docker/docker-compose.native-gpu.yml exec presto-cli presto-cli --server presto-coordinator:8080 --catalog hive --schema tpch_test --execute "$drop_table"
	table_dir="/var/lib/presto/data/hive/data/integration_test/tpch/$table_name"
	create_table=$(cat $sql_file | sed "s+{file_path}+$table_dir+g")
	docker compose -f $BASE_DIR/presto/docker/docker-compose.native-gpu.yml exec presto-cli presto-cli --server presto-coordinator:8080 --catalog hive --schema tpch_test --execute "$create_table"
    done
}

# TODO: Q5 uses a constant that needs to be modified based on the SF of the data (it currently is not).
function run_queries() {
    worker_exec="docker compose -f $BASE_DIR/presto/docker/docker-compose.native-gpu.yml exec presto-native-worker-gpu"
    cli_exec="docker compose -f $BASE_DIR/presto/docker/docker-compose.native-gpu.yml exec presto-cli"
    for query in $QUERIES; do
	sql=$(cat $BASE_DIR/presto/testing/integration_tests/queries/tpch/queries.json | jq ".$query")
	sql="${sql:1:-1}" # remove quotes wrapping query.
	sql=$(echo "$sql" | sed "s/LIMIT .*//g") # removing limits
	out="$BASE_DIR/benchmark_output/tpch/$query.out"
	echo "running $query"
	[ -z "$CREATE_PROFILES" ] || $worker_exec bash -c "nsys start -o /benchmark_output/$query.nsys-rep --force-overwrite true"
	$cli_exec presto-cli --server presto-coordinator:8080 --catalog hive --schema tpch_test --execute "$sql" > $out
	[ -z "$CREATE_PROFILES" ] || $worker_exec bash -c "nsys stop"
    done
}

parse_args "$@"
[ -z "$CREATE_TABLES" ] || create_tables
run_queries
