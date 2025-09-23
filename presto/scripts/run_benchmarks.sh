#!/bin/bash

BASE_DIR="$(dirname $(realpath $0))/../.."
OUTPUT_DIR="$BASE_DIR/benchmark_output/tpch"
PROFILES_DIR="benchmark_output/tpch/profiles"
CREATE_TABLES=""
CREATE_PROFILES=""
QUERIES="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"
COMPOSE_FILE=""
WORKER=""
QUERY_VIA_CURL="true"
SCHEMA="tpch_test"
COORD="localhost:8080"
CATALOG="hive"
DATA_DIR="/var/lib/presto/data/hive/data/integration_test/tpch" # local to container.
SKIP_WARMUP=""

# --- Print error messages in red ---
echo_error() {
    echo -e "\033[0;31m$*\033[0m" >&2
}

echo_fail() {
    echo_error $1
    exit 1
}

echo_warning() {
    echo -e "\033[0;33mWARNING: $*\033[0m: " >&2
}

function print_help() {
    cat << EOF

Usage: $0 [OPTIONS]

This script runs tpch benchmarks

OPTIONS:
    -h, --help              Show this help message.
    -c, --create-tables	    Create the tpch tables.
    -p, --profile	    Profile queries with nsys.
    -q, --queries           Set of benchmark queries to run. This should be a comma separate list of query numbers.
                            By default, all benchmark queries are run.
    -l, --command-line      Run queries via presto-cli instead of curl.
    -d, --data-dir          Location (in docker image) where benchmark data reside.
                            This location is mapped in the containers to ${PRESTO_DATA_DIR}
    -s, --schema            Schema name for benchmark (default tpch_test).
    -C, --coordinator       Coordinator URL (default localhost:8080 - only used for curl runs).
    -S, --skip-warmup       Skip warmup queries.

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
                    echo_fail "Error: --queries requires a value"
                fi
                ;;
            -s|--schema)
                if [[ -n $2 ]]; then
                    SCHEMA=$2
                    shift 2
                else
                    echo_fail "Error: --schema requires a value"
                fi
                ;;
            -l|--command-line)
                QUERY_VIA_CURL=""
                shift 1
                ;;
            -d|--data-dir)
                [ -z "$PRESTO_DATA_DIR" ] && echo_fail "PRESTO_DATA_DIR needs to be set to use --data-dir"
                [ -z "$CREATE_TABLES" ] && echo_warning "--data-dir won't do anything unless --create-tables is specified"
                if [[ -n $2 ]]; then
                    # 'user_data' in the container is mapped to PRESTO_DATA_DIR environment variable externally.
                    DATA_DIR="/var/lib/presto/data/hive/data/user_data/$2"
                    shift 2
                else
                    echo_fail "Error: --data-dirs requires a value"
                fi
                ;;
            -C|--coordinator)
                if [[ -n $2 ]]; then
                    COORD=$2
                    shift 2
                else
                    echo_fail "Error: --coordinator requires a value"
                fi
                ;;
            -S|--skip-warmup)
                SKIP_WARMUP=true
                shift 1
                ;;
            *)
                echo_error "Error: Unknown argument $1"
                print_help
                exit 1
                ;;
        esac
    done
}

function detect_containers() {
    local images=$(docker ps)
    if echo "$images" | grep -q "presto-native-worker-gpu"; then
        COMPOSE_FILE="$BASE_DIR/presto/docker/docker-compose.native-gpu.yml"
        WORKER="presto-native-worker-gpu"
    fi
    if echo "$images" | grep -q "presto-native-worker-cpu"; then
        [[ -n $WORKER ]] && echo_fail "mismatch in worker types"
        COMPOSE_FILE="$BASE_DIR/presto/docker/docker-compose.native-cpu.yml"
        WORKER="presto-native-worker-cpu"
    fi
    if echo "$images" | grep -q "presto-java-worker"; then
        [[ -n $WORKER ]] && echo_fail "mismatch in worker types"
        COMPOSE_FILE="$BASE_DIR/presto/docker/docker-compose.java.yml"
        WORKER="presto-java-worker"
    fi
    [ -z $WORKER ] && echo_fail "No worker container running"
}

function start_profile() {
    local query=$1
    local options=""
    [ -f "${BASE_DIR}/${PROFILES_DIR}/Q${query}.nsys-rep" ] && options="--force-overwrite true" || options=""
    docker compose -f $COMPOSE_FILE exec $WORKER bash -c \
           "nsys start -o /${PROFILES_DIR}/Q${query}.nsys-rep ${options}"
}

function stop_profile() {
    docker compose -f $COMPOSE_FILE exec $WORKER bash -c "nsys stop"
}

function presto_cli() {
    docker compose -f $COMPOSE_FILE exec \
           presto-cli presto-cli --server presto-coordinator:8080 --catalog $CATALOG \
           --schema $SCHEMA --execute "$1"
}

function get_query() {
    local query=$1
    local sql=$(cat $BASE_DIR/presto/testing/integration_tests/queries/tpch/queries.json \
                    | jq ".Q${query}")
    sql="${sql:1:-1}" # remove quotes wrapping query.
    #sql=$(echo "$sql" | sed "s/LIMIT .*//g") # removing limits
    # Q11 uses a constant that needs to be modified based on the SF of the data.
    # The value is calculated as (0.0001 / scale_factor)
    if [[ "$query" == "11" ]]; then
        local sf=$(cat $BASE_DIR/presto/testing/integration_tests/data/tpch/metadata.json \
                       | jq ".scale_factor")
        sf=$(awk "BEGIN {printf \"%10f\\n\", 0.0001 / $sf}")
        sql="${sql/0.0001000000/$sf}"
    fi
    # Referencing the CTE defined "supplier_no" alias in the parent query causes issues on presto.
    if [[ "$query" == "15" ]]; then
        sql=$(echo "$sql" | sed "s/ AS supplier_no//g")
        sql=$(echo "$sql" | sed "s/supplier_no/l_suppkey/g")
    fi
    echo "$sql"
}

function process_response() {
    local rc=0
    local timeout_seconds=${3:-300}  # Default 5 minute timeout
    local current_uri=$(echo "$1" | jq -r '.nextUri // empty')
    [[ -z "$current_uri" ]] && echo_error "Failed to submit query $query" && return 1
    local state=""
    local final_response=""
    local elapsed_seconds=0
    while [[ "$state" != "FINISHED" && "$state" != "FAILED" ]]; do
        sleep 1
        elapsed_seconds=$((elapsed_seconds + 1))
        if [[ $elapsed_seconds -ge $timeout_seconds ]]; then
	    echo_error "Query $query timed out after ${timeout_seconds}s"
            rc=1
            break
        fi
        final_response=$(curl -sS "$current_uri")
        state=$(echo "$final_response" | jq -r '.stats.state // empty')
        if [[ "$state" == "FAILED" ]]; then
	    local error=$(echo "$final_response" | jq -r '.error.message // "Unknown error"')
	    echo_error "Query $query failed: $error"
            rc=1
	    break
        fi
        [[ "$state" == "FINISHED" ]] && break
        local next_uri_response=$(echo "$final_response" | jq -r '.nextUri // empty')
        [[ -n "$next_uri_response" ]] && current_uri="$next_uri_response"
    done
    echo "$final_response"
    return $rc
}

function filter_output() {
    local query=$1
    local execution_time=$2
    local final_response=$3
    if [[ -n $final_response ]]; then
        local stats=$(echo "$final_response" | jq '.stats // {}')
        local processed_rows=$(echo "$stats" | jq -r '.processedRows // 0')
        local processed_bytes=$(echo "$stats" | jq -r '.processedBytes // 0')
        local cpu_time_ms=$(echo "$stats" | jq -r '.cpuTimeMillis // 0')
        local wall_time_ms=$(echo "$stats" | jq -r '.wallTimeMillis // 0')
        local elapsed_time_ms=$(echo "$stats" | jq -r '.elapsedTimeMillis // 0')
        jq -C -n \
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
    else
        jq -C -n \
           --arg query "$query" \
           --arg execution_time "$execution_time" \
           '{
                query_number: $query,
		execution_time_seconds: ($execution_time | tonumber)
            }'
    fi
}

function create_tables() {
    presto_cli "CREATE SCHEMA IF NOT EXISTS $CATALOG.$SCHEMA"
    local pattern="\/([^\/]*)\.sql"
    for sql_file in $(ls $BASE_DIR/presto/testing/integration_tests/schemas/tpch/*.sql); do
        local drop_table=""
	if [[ "$sql_file" =~ $pattern ]]; then
	    local table_name="${BASH_REMATCH[1]}"
	    drop_table="DROP TABLE IF EXISTS $table_name"
	else
	    echo_fail "failed to parse schema files"
	fi
        presto_cli "$drop_table"
        local table_dir="$DATA_DIR/$table_name"
        local create_table=$(cat $sql_file | sed "s+{file_path}+$table_dir+g" | sed "s+tpch_test+$SCHEMA+g")
        presto_cli "$create_table"
    done
}

function run_query() {
    local sql=$1
    local query=$2
    if [[ -n $QUERY_VIA_CURL ]]; then
        local response=$(curl -sS -X POST "http://${COORD}/v1/statement" \
			      -H "X-Presto-Catalog: $CATALOG" \
			      -H "X-Presto-Schema: $SCHEMA" \
			      -H "X-Presto-User: tpch-benchmark" \
			      --data "$sql")
        FINAL_RESPONSE="$(process_response $response)"
    else
        docker compose -f $COMPOSE_FILE exec presto-cli presto-cli \
               --server presto-coordinator:8080 --catalog $CATALOG \
               --schema $SCHEMA --execute "$sql" > "$OUTPUT_DIR/$query.out"
    fi
}

function run_queries() {
    for query in $QUERIES; do
        local sql=$(get_query $query)

        echo "running query: $query"
        [ -z "$SKIP_WARMUP" ] && echo "running warmup query" && run_query "$sql" "$query"
        [ -z "$CREATE_PROFILES" ] || start_profile "$query"
        FINAL_RESPONSE=""
        echo "executing sql: ($sql)"

        local end_time=""
        local start_time=$(date +%s.%N)
        run_query "$sql" "$query"
        end_time=$(date +%s.%N)
        [ -n "$FINAL_RESPONSE" ] && echo "$FINAL_RESPONSE" > "$OUTPUT_DIR/$query.out.json"
	local execution_time=$(echo "$end_time - $start_time" | bc -l)
        local output_json=$(filter_output "$query" "$execution_time" "$final_response")
        echo "$output_json"
        echo "$output_json" > "$OUTPUT_DIR/$query.summary.json"

        [ -z "$CREATE_PROFILES" ] || stop_profile
    done
}

parse_args "$@"
detect_containers
mkdir -p "$OUTPUT_DIR"
[ -z "$CREATE_PROFILES" ] || mkdir -p "$BASE_DIR/$PROFILES_DIR"
[ -z "$CREATE_TABLES" ] || create_tables
run_queries
