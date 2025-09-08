#!/bin/bash

BASE_DIR="$(dirname $(realpath $0))/../.."
CREATE_TABLES=""
CREATE_PROFILES=""
#QUERIES="Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22"
QUERIES="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"

# --- Print error messages in red ---
echo_error() {
    echo -e "\033[0;31m$*\033[0m" >&2
}

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
    $0 -c -q "1 2" -p
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

# TODO: Q11 uses a constant that needs to be modified based on the SF of the data (it currently is not).
function run_queries() {
    worker_exec="docker compose -f $BASE_DIR/presto/docker/docker-compose.native-gpu.yml exec presto-native-worker-gpu"
    cli_exec="docker compose -f $BASE_DIR/presto/docker/docker-compose.native-gpu.yml exec presto-cli"
    for query in $QUERIES; do
	sql=$(cat $BASE_DIR/presto/testing/integration_tests/queries/tpch/queries.json | jq ".Q$query")
	sql="${sql:1:-1}" # remove quotes wrapping query.
	sql=$(echo "$sql" | sed "s/LIMIT .*//g") # removing limits
	out="$BASE_DIR/benchmark_output/tpch/$query.out"
	echo "running $query"
	if [ -f "$BASE_DIR/benchmark_output/$query.nsys-rep" ]; then
	    FORCE_OVERWRITE="--force-overwrite true"
	else
	    FORCE_OVERWRITE=""
	fi
	[ -z "$CREATE_PROFILES" ] || $worker_exec bash -c "nsys start -o /benchmark_output/$query.nsys-rep $FORCE_OVERWRITE"

	local timeout_seconds=${3:-300}  # Default 5 minute timeout
	local start_time=$(date +%s.%N)
	local response=$(curl -sS -X POST "http://localhost:8080/v1/statement" \
			      -H "X-Presto-Catalog: hive" \
			      -H "X-Presto-Schema: tpch_test" \
			      -H "X-Presto-User: tpch-benchmark" \
			      --data "$sql")
	local next_uri=$(echo "$response" | jq -r '.nextUri // empty')
	if [[ -z "$next_uri" ]]; then
            echo_error "Failed to submit query $query"
            return 1
	fi
	local state=""
	local final_response=""
	local elapsed_seconds=0
	local current_uri="$next_uri"
	while [[ "$state" != "FINISHED" && "$state" != "FAILED" ]]; do
            sleep 1
            elapsed_seconds=$((elapsed_seconds + 1))
            if [[ $elapsed_seconds -ge $timeout_seconds ]]; then
		echo_error "Query $query timed out after ${timeout_seconds}s"
		return 1
            fi
            final_response=$(curl -sS "$current_uri")
            state=$(echo "$final_response" | jq -r '.stats.state // empty')
            if [[ "$state" == "FAILED" ]]; then
		local error=$(echo "$final_response" | jq -r '.error.message // "Unknown error"')
		echo_error "Query $query failed: $error"
		return 1
            fi
            if [[ "$state" == "FINISHED" ]]; then
		break
            fi
            local next_uri_response=$(echo "$final_response" | jq -r '.nextUri // empty')
            if [[ -n "$next_uri_response" ]]; then
		current_uri="$next_uri_response"
            fi
	done
	local end_time=$(date +%s.%N)

	#$cli_exec presto-cli --server presto-coordinator:8080 --catalog hive --schema tpch_test --execute "$sql" > $out
	[ -z "$CREATE_PROFILES" ] || $worker_exec bash -c "nsys stop"

	# write final_response to file
	echo "$final_response" > "out.json"
	local execution_time=$(echo "$end_time - $start_time" | bc -l)
	local stats=$(echo "$final_response" | jq '.stats // {}')
	local processed_rows=$(echo "$stats" | jq -r '.processedRows // 0')
	local processed_bytes=$(echo "$stats" | jq -r '.processedBytes // 0')
	local cpu_time_ms=$(echo "$stats" | jq -r '.cpuTimeMillis // 0')
	local wall_time_ms=$(echo "$stats" | jq -r '.wallTimeMillis // 0')
	local elapsed_time_ms=$(echo "$stats" | jq -r '.elapsedTimeMillis // 0')
	jq -n \
           --arg query "$query" \
           --arg execution_time "$execution_time" \
           --arg processed_rows "$processed_rows" \
           --arg processed_bytes "$processed_bytes" \
           --arg cpu_time_ms "$cpu_time_ms" \
           --arg wall_time_ms "$wall_time_ms" \
           --arg elapsed_time_ms "$elapsed_time_ms" \
           '{
		query_number: $query,
		curl_execution_time_seconds: ($execution_time | tonumber),
		processed_rows: ($processed_rows | tonumber),
		processed_bytes: ($processed_bytes | tonumber),
		cpu_time_ms: ($cpu_time_ms | tonumber),
		wall_time_ms: ($wall_time_ms | tonumber),
		elapsed_time_ms: ($elapsed_time_ms | tonumber)
            }'
    done
}

parse_args "$@"
mkdir -p "$BASE_DIR/benchmark_output/tpch"
[ -z "$CREATE_TABLES" ] || create_tables
run_queries
