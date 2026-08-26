#!/usr/bin/env bash
set -Eeuo pipefail

# g7e.48xlarge launch profile for s3_ktls_cuda_ref.cu.
#
# This file intentionally contains one measured topology and one production
# data path. It is executable documentation for database-engine ports, not a
# universal autotuner. Retune only the INSTANCE PROFILE block on another host.
#
# Performance prerequisites:
#   * a current ENA driver (the measured host used 2.17.2g);
#   * MTU 9001 on every selected ENA interface and a jumbo-capable path;
#   * a regional S3 gateway endpoint is strongly recommended. When the
#     instance role permits ec2:DescribeRouteTables and
#     ec2:DescribeVpcEndpoints, the runner reports the association; otherwise
#     it marks the endpoint unverified and continues;
#   * CATALOG_S3_URI export additionally requires s3:ListBucket on its bucket;
#   * Linux TLS 1.3 RX kTLS, CUDA 13, sufficient memlock/NOFILE limits;
#   * NUMA-local NIC/GPU/CPU lanes and isolated ENA receive IRQs.
#
# Experimental, not a correctness prerequisite:
#   ena large_rx_page=1 from the order-2 RX-page patch collapses a jumbo frame
#   from roughly three 4 KiB RX descriptors to one 16 KiB compound-page
#   descriptor. It reduces receive-side descriptor/page work, but is not an
#   upstream ENA interface, requires 4 KiB base pages, and excludes XDP and
#   AF_XDP. Both the runner and binary report its live state.
#
# Input is a catalog snapshot, analogous to a database metastore. This runner
# adapts an existing JSON metadata export into a tiny line protocol; the timed
# query never performs LIST or HEAD. To create a new saved ListObjectsV2 export
# with the instance role before running, provide CATALOG_S3_URI:
#
#   size=BYTES<TAB>s3://bucket/key
#   size=BYTES<TAB>etag=VALUE<TAB>s3://bucket/key
#
#   CATALOG_S3_URI=s3://bucket/prefix/ ./run_s3_ktls_cuda_ref.sh
#
# Default: rebuild only when the binary is absent or this .cu file is newer,
# establish the complete TCP/TLS/kTLS pool without sending an HTTP request or
# transferring payload, then measure two complete S3 -> final GPU allocation
# queries. PRECONNECT_RESULT enforces the zero-payload setup contract; the two
# measured values expose variance instead of presenting a lucky single
# iteration.
#
#   ./run_s3_ktls_cuda_ref.sh
#   PAYLOAD_SINK=receive-only ./run_s3_ktls_cuda_ref.sh

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

# ---------------------------------------------------------------------------
# INSTANCE PROFILE: measured g7e.48xlarge values. Retune together.
# ---------------------------------------------------------------------------

CONNECTIONS_PER_LANE=${CONNECTIONS_PER_LANE:-192}
H2D_BATCH=${H2D_BATCH:-32}
SLOT_KIB=${SLOT_KIB:-256}
RANGE_MIB=${RANGE_MIB:-64}
PINNED_HWM_MIB=${PINNED_HWM_MIB:-512}
GPU_RESERVE_MIB=${GPU_RESERVE_MIB:-2048}
MAX_RETRIES=${MAX_RETRIES:-3}
ENA_QUEUE_COUNT=${ENA_QUEUE_COUNT:-12}

# Each CPU in a lane is one reactor. NIC and GPU must share its NUMA node.
LANE0_CPUS=${LANE0_CPUS:-12-19,108-115}
LANE1_CPUS=${LANE1_CPUS:-21-28,117-124}
LANE2_CPUS=${LANE2_CPUS:-30-37,126-133}
LANE3_CPUS=${LANE3_CPUS:-39-46,135-142}
LANE4_CPUS=${LANE4_CPUS:-60-67,156-163}
LANE5_CPUS=${LANE5_CPUS:-69-76,165-172}
LANE6_CPUS=${LANE6_CPUS:-78-85,174-181}
LANE7_CPUS=${LANE7_CPUS:-87-94,183-190}

APP_CPUS=${APP_CPUS:-12-47,60-95,108-143,156-191}
IRQ_ENP135S0_CPUS=${IRQ_ENP135S0_CPUS:-0-11}
IRQ_ENP153S0_CPUS=${IRQ_ENP153S0_CPUS:-0-11}
IRQ_ENP170S0_CPUS=${IRQ_ENP170S0_CPUS:-48-59}
IRQ_ENP187S0_CPUS=${IRQ_ENP187S0_CPUS:-48-59}

NICS=(enp135s0 enp153s0 enp170s0 enp187s0)
LANES=(
    "enp135s0:0:$LANE0_CPUS"
    "enp135s0:1:$LANE1_CPUS"
    "enp153s0:2:$LANE2_CPUS"
    "enp153s0:3:$LANE3_CPUS"
    "enp170s0:4:$LANE4_CPUS"
    "enp170s0:5:$LANE5_CPUS"
    "enp187s0:6:$LANE6_CPUS"
    "enp187s0:7:$LANE7_CPUS"
)

# ---------------------------------------------------------------------------
# Query, catalog, and build inputs
# ---------------------------------------------------------------------------

WORK_DIR=${WORK_DIR:-$SCRIPT_DIR}
SOURCE_FILE=${SOURCE_FILE:-$WORK_DIR/s3_ktls_cuda_ref.cu}
BIN=${BIN:-$WORK_DIR/s3_ktls_cuda_ref}
NVCC=${NVCC:-/usr/local/cuda/bin/nvcc}

RUN_USER=${RUN_USER:-ubuntu}
REGION=${REGION:-us-east-2}
BUCKET=${BUCKET:-rapids-tpch}
CATALOG_S3_URI=${CATALOG_S3_URI:-}
CATALOG_SUFFIX=${CATALOG_SUFFIX:-.parquet}
if [[ -n $CATALOG_S3_URI ]]; then
    CATALOG_JSON=${CATALOG_JSON:-$WORK_DIR/catalog-list-objects-v2.json}
else
    CATALOG_JSON=${CATALOG_JSON:-/home/ubuntu/aws-crt-s3-results/official-crt-c-20260819T021145Z/rapids-tpch-scale-1000.run.json}
fi
CATALOG_SNAPSHOT=${CATALOG_SNAPSHOT:-$WORK_DIR/catalog-snapshot.tsv}
ENDPOINT_IP_FILE=${ENDPOINT_IP_FILE:-$WORK_DIR/jumbo-us-east-2.txt}
RESULT_DIR=${RESULT_DIR:-$WORK_DIR/s3-ktls-cuda-results}

MEASURED_ITERATIONS=${MEASURED_ITERATIONS:-2}
PAYLOAD_SINK=${PAYLOAD_SINK:-h2d}

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

if [[ -n ${WARMUP_ITERATIONS+x} ]]; then
    fail "WARMUP_ITERATIONS was removed: pool setup is always transport-only and moves zero payload bytes"
fi

# Ignore workstation/SSO configuration deliberately. Both catalog export and
# advisory gateway-endpoint discovery exercise the same EC2 instance role
# model as the benchmark binary.
aws_instance_role() {
    env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
        -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
        -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN \
        -u AWS_ROLE_ARN -u AWS_WEB_IDENTITY_TOKEN_FILE \
        -u AWS_CONTAINER_CREDENTIALS_FULL_URI \
        -u AWS_CONTAINER_CREDENTIALS_RELATIVE_URI \
        AWS_CONFIG_FILE=/dev/null \
        AWS_SHARED_CREDENTIALS_FILE=/dev/null \
        AWS_EC2_METADATA_DISABLED=false \
        AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" AWS_PAGER= \
        aws "$@"
}

parse_catalog_s3_uri() {
    local uri=$1
    if [[ ! $uri =~ ^s3://([^/]+)(/(.*))?$ ]]; then
        fail "CATALOG_S3_URI must be s3://bucket/prefix/: $uri"
    fi
    BUCKET=${BASH_REMATCH[1]}
    CATALOG_PREFIX=${BASH_REMATCH[3]:-}
}

imds_get() {
    local token=$1 path=$2
    curl --fail --silent --show-error --connect-timeout 2 --max-time 5 \
        -H "X-aws-ec2-metadata-token: $token" \
        "http://169.254.169.254/latest/meta-data/$path"
}

report_s3_gateway_endpoint() {
    local token nic mac subnet vpc route_json route_table endpoint_json endpoint
    local -a route_tables=() endpoints=()
    local -A checked_subnets=()

    if ! token=$(curl --fail --silent --show-error \
            --connect-timeout 2 --max-time 5 -X PUT \
            -H 'X-aws-ec2-metadata-token-ttl-seconds: 300' \
            http://169.254.169.254/latest/api/token 2>/dev/null); then
        echo "S3_GATEWAY_ENDPOINT status=unverified reason=imds_unavailable" \
             "recommendation=associate_regional_gateway_endpoint"
        return 0
    fi

    for nic in "${NICS[@]}"; do
        mac=$(<"/sys/class/net/$nic/address")
        if ! subnet=$(imds_get "$token" \
                "network/interfaces/macs/$mac/subnet-id" 2>/dev/null); then
            echo "S3_GATEWAY_ENDPOINT status=unverified nic=$nic" \
                 "reason=subnet_metadata_unavailable" \
                 "recommendation=associate_regional_gateway_endpoint"
            return 0
        fi
        if [[ -n ${checked_subnets[$subnet]+present} ]]; then
            continue
        fi
        if ! vpc=$(imds_get "$token" \
                "network/interfaces/macs/$mac/vpc-id" 2>/dev/null); then
            echo "S3_GATEWAY_ENDPOINT status=unverified nic=$nic subnet=$subnet" \
                 "reason=vpc_metadata_unavailable" \
                 "recommendation=associate_regional_gateway_endpoint"
            return 0
        fi

        if ! route_json=$(aws_instance_role ec2 describe-route-tables \
                --filters "Name=association.subnet-id,Values=$subnet" \
                --output json 2>&1); then
            echo "S3_GATEWAY_ENDPOINT status=unverified region=$REGION" \
                 "vpc=$vpc subnet=$subnet reason=describe_route_tables_denied_or_failed" \
                 "recommendation=associate_regional_gateway_endpoint"
            return 0
        fi
        mapfile -t route_tables < <(
            jq -r '.RouteTables[].RouteTableId' <<< "$route_json")
        if ((${#route_tables[@]} == 0)); then
            if ! route_json=$(aws_instance_role ec2 describe-route-tables \
                    --filters "Name=vpc-id,Values=$vpc" \
                              'Name=association.main,Values=true' \
                    --output json 2>&1); then
                echo "S3_GATEWAY_ENDPOINT status=unverified region=$REGION" \
                     "vpc=$vpc subnet=$subnet" \
                     "reason=describe_main_route_table_denied_or_failed" \
                     "recommendation=associate_regional_gateway_endpoint"
                return 0
            fi
            mapfile -t route_tables < <(
                jq -r '.RouteTables[].RouteTableId' <<< "$route_json")
        fi
        if ((${#route_tables[@]} != 1)); then
            echo "S3_GATEWAY_ENDPOINT status=unverified region=$REGION" \
                 "vpc=$vpc subnet=$subnet reason=effective_route_table_ambiguous" \
                 "recommendation=associate_regional_gateway_endpoint"
            continue
        fi
        route_table=${route_tables[0]}

        if ! endpoint_json=$(aws_instance_role ec2 describe-vpc-endpoints \
                --filters "Name=vpc-id,Values=$vpc" \
                          "Name=service-name,Values=com.amazonaws.$REGION.s3" \
                          'Name=vpc-endpoint-state,Values=available' \
                          'Name=vpc-endpoint-type,Values=Gateway' \
                --output json 2>&1); then
            echo "S3_GATEWAY_ENDPOINT status=unverified region=$REGION" \
                 "vpc=$vpc subnet=$subnet route_table=$route_table" \
                 "reason=describe_vpc_endpoints_denied_or_failed" \
                 "recommendation=associate_regional_gateway_endpoint"
            return 0
        fi
        mapfile -t endpoints < <(
            jq -r --arg route_table "$route_table" '
                .VpcEndpoints[]
                | select((.RouteTableIds // []) | index($route_table))
                | .VpcEndpointId
            ' <<< "$endpoint_json")
        if ((${#endpoints[@]} == 0)); then
            echo "S3_GATEWAY_ENDPOINT status=not_detected region=$REGION" \
                 "vpc=$vpc subnet=$subnet route_table=$route_table" \
                 "recommendation=associate_regional_gateway_endpoint"
            checked_subnets[$subnet]=1
            continue
        fi
        endpoint=${endpoints[0]}
        if ! jq -e --arg endpoint "$endpoint" '
                any(.RouteTables[].Routes[];
                    .GatewayId == $endpoint and
                    .State == "active" and
                    (.DestinationPrefixListId | type) == "string")
            ' >/dev/null <<< "$route_json"; then
            echo "S3_GATEWAY_ENDPOINT status=not_active region=$REGION" \
                 "vpc=$vpc subnet=$subnet route_table=$route_table" \
                 "endpoint=$endpoint" \
                 "recommendation=repair_gateway_endpoint_route"
            checked_subnets[$subnet]=1
            continue
        fi
        checked_subnets[$subnet]=1
        echo "S3_GATEWAY_ENDPOINT region=$REGION vpc=$vpc subnet=$subnet" \
             "route_table=$route_table endpoint=$endpoint" \
             "status=verified association_verified=yes"
    done
}

parse_cpu_list() {
    local text=$1 destination=$2 token first last cpu
    local -n output=$destination
    local -a tokens=()
    output=()
    IFS=',' read -r -a tokens <<< "$text"
    ((${#tokens[@]} > 0)) || fail "CPU list is empty"
    for token in "${tokens[@]}"; do
        if [[ $token =~ ^([0-9]+)-([0-9]+)$ ]]; then
            first=${BASH_REMATCH[1]}
            last=${BASH_REMATCH[2]}
            ((last >= first)) || fail "descending CPU range: $token"
            for ((cpu = first; cpu <= last; ++cpu)); do output+=("$cpu"); done
        elif [[ $token =~ ^[0-9]+$ ]]; then
            output+=("$token")
        else
            fail "invalid CPU-list token: $token"
        fi
    done
}

compact_console_filter() {
    local line word
    local -a words=() compact_words=()
    while IFS= read -r line; do
        case $line in
            T\ *)
                compact_words=(T)
                read -r -a words <<< "$line"
                for word in "${words[@]}"; do
                    case $word in
                        elapsed_s=*|s3_interval_agg_gbps=*|h2d_interval_agg_gbps=*|\
                        h2d_copy_avg_KiB=*|total_body_GB=*|active_connections=*|\
                        completed_ranges=*|retry_count=*|pinned_slots_used=*|\
                        pinned_slots_total=*|pinned_peak=*|ring_full_percent=*|\
                        ssl_want_read=*|ssl_want_write=*)
                            compact_words+=("$word")
                            ;;
                    esac
                done
                printf '%s' "${compact_words[0]}"
                printf ' %s' "${compact_words[@]:1}"
                printf '\n'
                ;;
            CATALOG_SNAPSHOT\ *|TLS_SAMPLE\ *|NIC\ name=*|\
            PRECONNECT_START\ *|PRECONNECT_RESULT\ *|\
            ITERATION_START\ *|ITERATION_RESULT\ *|ITERATION\ *|\
            =====\ FINAL\ SUMMARY\ =====|RATE_SCOPE\ *|PRETRANSFER_TIMING\ *|\
            GET_RAMP\ *|RAMP_100MS\ *|TIMING\ *|THROUGHPUT\ *|\
            NIC_FINAL\ *|LANE_FINAL\ *|GPU_FINAL\ *|ranges_completed=*|\
            RETRY_WORK\ *|\
            RECEIVE_ONLY_FINAL\ *|h2d_submitted_copies=*|pinned_hwm_bytes=*|\
            ring_stall_connection_seconds=*|REACTORS_FINAL\ *|\
            getrusage_transfer_user_s=*|tls_connections_established=*|\
            OBJECTS_FINAL\ *|OVERALL\ *|WARNING\ *|ERROR\ *|FATAL:*|\
            RETRY\ *|HTTP_ERROR\ *|STARTUP_FAIL:*|Failed\ *|sudo:\ *|systemd-run:*)
                printf '%s\n' "$line"
                ;;
        esac
    done
}

emit_compact_result() {
    local log_path=$1 json compact
    json=$(awk '
        index($0, "RESULT_JSON=") == 1 {
            result = substr($0, length("RESULT_JSON=") + 1)
        }
        END { print result }
    ' "$log_path")
    [[ -n $json ]] || return 0
    if compact=$(jq -c '
        {
            pass, iteration_role, measured_iteration, connection_pool_mode,
            scan_warmup_iterations,
            preconnect_mode, preconnect_s, preconnect_connections,
            preconnect_ktls_connections, preconnect_tls_ready,
            preconnect_first_tls_ready_s, preconnect_tls_50_s,
            preconnect_tls_90_s, preconnect_tls_100_s,
            preconnect_retries, preconnect_reconnects,
            preconnect_http_requests, preconnect_s3_body_bytes,
            preconnect_h2d_bytes, preconnect_tls_rx_sw_delta,
            pool_connections_at_start, pool_connections_live_at_end,
            new_tls_connections, wall_s, first_body_s, network_complete_s,
            device_complete_s, network_to_device_drain_s,
            network_complete_gbps, device_complete_gbps,
            first_to_last_body_gbps, useful_object_gbps,
            catalog_load_s, catalog_objects, catalog_sizes, catalog_etags,
            head_requests, expected_bytes, payload_sink, h2d_mode, slot_bytes,
            s3_body_bytes, retry_s3_body_bytes, h2d_completed_bytes,
            retry_h2d_bytes,
            h2d_completed_copies, h2d_copy_avg_bytes, h2d_inline_batches,
            h2d_inline_batch_avg_copies, pinned_hwm_bytes, pinned_peak_slots,
            ring_full_percent, ranges_completed, retries, reconnects,
            http_errors, tls_errors, cuda_errors, ktls_rx_connections,
            no_pad_ok, tls_rx_sw_validation_ok, epoll_ctl_calls, tls_stat_delta
        }
        | with_entries(select(.value != null))
    ' <<< "$json"); then
        printf 'RESULT_COMPACT_JSON=%s\n' "$compact"
    else
        echo "WARNING compact RESULT_JSON extraction failed; see $log_path" >&2
    fi
}

# ---------------------------------------------------------------------------
# Build and materialize the metastore/catalog snapshot
# ---------------------------------------------------------------------------

[[ -f $SOURCE_FILE ]] || fail "source missing: $SOURCE_FILE"
[[ -f $ENDPOINT_IP_FILE ]] || fail "endpoint IP file missing: $ENDPOINT_IP_FILE"
command -v jq >/dev/null || fail "jq is required for the catalog adapter"
if [[ -n $CATALOG_S3_URI ]]; then
    command -v aws >/dev/null || \
        fail "AWS CLI is required to export CATALOG_S3_URI"
fi

BUILD_ACTION=reused
BUILD_REASON=up_to_date
BUILD_WARNINGS=not_rechecked
if [[ ! -x $BIN ]]; then
    BUILD_REASON=binary_missing
elif [[ $SOURCE_FILE -nt $BIN ]]; then
    BUILD_REASON=source_newer
fi

if [[ $BUILD_REASON != up_to_date ]]; then
    [[ -x $NVCC ]] || fail "nvcc missing: $NVCC"
    "$NVCC" -O3 -std=c++17 \
        -Xcompiler=-Wall,-Wextra,-Wconversion,-Wshadow \
        "$SOURCE_FILE" -o "$BIN" -lssl -lcrypto -lpthread
    BUILD_ACTION=compiled
    BUILD_WARNINGS=none
fi

[[ -x $BIN ]] || fail "binary missing: $BIN"
BINARY_HELP=$("$BIN" --help)
grep -q -- '--receive-only' <<< "$BINARY_HELP" || \
    fail "binary is stale despite its timestamp; remove it and rerun"
grep -q -- '--catalog-snapshot' <<< "$BINARY_HELP" || \
    fail "binary lacks the strict catalog interface; remove it and rerun"
grep -q -- 'Transport-only pool preconnect is mandatory' <<< "$BINARY_HELP" || \
    fail "binary lacks zero-payload pool preconnect; remove it and rerun"
if grep -q -- '--warmup-iterations' <<< "$BINARY_HELP"; then
    fail "binary still exposes scan warmup; remove it and rerun"
fi
if grep -Eq -- '--(manifest|h2d-worker|verify-h2d|lane-reactors|inline-batch-probe)' \
        <<< "$BINARY_HELP"; then
    fail "binary still exposes retired experiment modes; remove it and rerun"
fi
"$BIN" --self-test

SOURCE_SHA=$(sha256sum "$SOURCE_FILE" | awk '{print $1}')
BINARY_SHA=$(sha256sum "$BIN" | awk '{print $1}')
echo "BUILD source_sha=$SOURCE_SHA binary_sha=$BINARY_SHA action=$BUILD_ACTION reason=$BUILD_REASON warnings=$BUILD_WARNINGS"

CATALOG_SOURCE=saved_json
if [[ -n $CATALOG_S3_URI ]]; then
    parse_catalog_s3_uri "$CATALOG_S3_URI"
    catalog_json_tmp=$(mktemp "$WORK_DIR/.catalog-list-objects-v2.XXXXXX")
    trap 'rm -f -- "$catalog_json_tmp"' ERR
    if ! aws_instance_role s3api list-objects-v2 \
            --bucket "$BUCKET" --prefix "$CATALOG_PREFIX" \
            --page-size 1000 --output json > "$catalog_json_tmp"; then
        fail "ListObjectsV2 catalog export failed for $CATALOG_S3_URI"
    fi
    mv -f -- "$catalog_json_tmp" "$CATALOG_JSON"
    trap - ERR
    CATALOG_SOURCE=s3_list_objects_v2
    echo "CATALOG_EXPORT source=$CATALOG_SOURCE uri=$CATALOG_S3_URI" \
         "suffix=$CATALOG_SUFFIX json=$CATALOG_JSON pagination=automatic"
else
    [[ -f $CATALOG_JSON ]] || fail "catalog JSON missing: $CATALOG_JSON"
fi

# This is the only schema adapter. CRT .tasks and raw ListObjectsV2 .Contents
# are both filtered by CATALOG_SUFFIX so sidecars such as metadata.json remain
# catalog inputs rather than timed data payload. Database ports should replace
# this jq step with their metastore scan and retain the size/ETag contract.
CATALOG_STATS=$(jq -r '
    if (.tasks? | type) == "array" then
        ["crt_tasks",
         ([.tasks[] | select(.action == "download")] | length)]
    elif (.Contents? | type) == "array" then
        ["list_objects_v2", (.Contents | length)]
    else
        error("expected CRT .tasks or ListObjectsV2 .Contents")
    end
    | @tsv
' "$CATALOG_JSON") || fail "could not inspect catalog JSON: $CATALOG_JSON"
read -r CATALOG_SCHEMA CATALOG_SOURCE_ROWS <<< "$CATALOG_STATS"
[[ $CATALOG_SOURCE_ROWS =~ ^[0-9]+$ ]] || \
    fail "invalid source-row count in catalog JSON: $CATALOG_JSON"

catalog_tmp=$(mktemp "$WORK_DIR/.catalog-snapshot.XXXXXX")
trap 'rm -f -- "$catalog_tmp"' ERR
if ! jq -r --arg bucket "$BUCKET" --arg suffix "$CATALOG_SUFFIX" '
    def rows:
        if (.tasks? | type) == "array" then
            .tasks[]
            | select(.action == "download")
            | {key: .key, size: (.size | tonumber),
               etag: (.etag // .ETag // "")}
        elif (.Contents? | type) == "array" then
            .Contents[]
            | {key: .Key, size: (.Size | tonumber), etag: (.ETag // "")}
        else
            error("expected CRT .tasks or ListObjectsV2 .Contents")
        end;
    rows
    | select($suffix == "" or (.key | endswith($suffix)))
    | if .size <= 0 then
          error("catalog object must have positive size: \(.key)")
      else . end
    | "size=\(.size)\t"
      + (if (.etag | tostring | length) > 0
         then "etag=\(.etag)\t" else "" end)
      + "s3://\($bucket)/\(.key | @uri)"
' "$CATALOG_JSON" > "$catalog_tmp"; then
    fail "could not adapt catalog JSON: $CATALOG_JSON"
fi
[[ -s $catalog_tmp ]] || fail "catalog snapshot has no nonempty objects"
mv -f -- "$catalog_tmp" "$CATALOG_SNAPSHOT"
trap - ERR

read -r OBJECT_COUNT OBJECT_BYTES MAX_OBJECT_BYTES <<< "$(awk -F '\t' '
    {
        size = substr($1, 6) + 0
        count += 1
        bytes += size
        if (size > maximum) maximum = size
    }
    END { printf "%d %.0f %.0f\n", count, bytes, maximum }
' "$CATALOG_SNAPSHOT")"
CATALOG_SUFFIX_EXCLUDED=$((CATALOG_SOURCE_ROWS - OBJECT_COUNT))
((CATALOG_SUFFIX_EXCLUDED >= 0)) || \
    fail "catalog adapter produced more objects than its source"
ENDPOINT_IP_COUNT=$(awk 'NF && $1 !~ /^#/ { count++ } END { print count + 0 }' \
    "$ENDPOINT_IP_FILE")
((ENDPOINT_IP_COUNT > 0)) || fail "endpoint IP file is empty"

case $PAYLOAD_SINK in
    h2d|receive-only) ;;
    *) fail "PAYLOAD_SINK must be h2d or receive-only" ;;
esac
[[ $MEASURED_ITERATIONS =~ ^[0-9]+$ && $MEASURED_ITERATIONS -gt 0 ]] || \
    fail "MEASURED_ITERATIONS must be positive"
[[ $ENA_QUEUE_COUNT =~ ^[0-9]+$ && $ENA_QUEUE_COUNT -ge 1 ]] || \
    fail "ENA_QUEUE_COUNT must be positive"
parse_cpu_list "$LANE0_CPUS" LANE0_CPU_VALUES
parse_cpu_list "$LANE1_CPUS" LANE1_CPU_VALUES
parse_cpu_list "$LANE2_CPUS" LANE2_CPU_VALUES
parse_cpu_list "$LANE3_CPUS" LANE3_CPU_VALUES
parse_cpu_list "$LANE4_CPUS" LANE4_CPU_VALUES
parse_cpu_list "$LANE5_CPUS" LANE5_CPU_VALUES
parse_cpu_list "$LANE6_CPUS" LANE6_CPU_VALUES
parse_cpu_list "$LANE7_CPUS" LANE7_CPU_VALUES
LANE_REACTOR_COUNTS=(
    "${#LANE0_CPU_VALUES[@]}" "${#LANE1_CPU_VALUES[@]}"
    "${#LANE2_CPU_VALUES[@]}" "${#LANE3_CPU_VALUES[@]}"
    "${#LANE4_CPU_VALUES[@]}" "${#LANE5_CPU_VALUES[@]}"
    "${#LANE6_CPU_VALUES[@]}" "${#LANE7_CPU_VALUES[@]}"
)
LANE_REACTOR_COUNTS_TEXT=$(IFS=,; echo "${LANE_REACTOR_COUNTS[*]}")

echo "PROFILE name=g7e.48xlarge source=measured retune_for_other_instances=yes"
echo "RUN_CONFIG connections_per_lane=$CONNECTIONS_PER_LANE" \
     "reactor_counts=$LANE_REACTOR_COUNTS_TEXT" \
     "slot_kib=$SLOT_KIB range_mib=$RANGE_MIB" \
     "pinned_hwm_mib=$PINNED_HWM_MIB payload_sink=$PAYLOAD_SINK" \
     "h2d_mode=$([[ $PAYLOAD_SINK == h2d ]] && echo same-reactor-batch || echo disabled)" \
     "h2d_batch=$H2D_BATCH queue_count=$ENA_QUEUE_COUNT" \
     "connection_pool_preconnect=transport_only" \
     "preconnect_http_requests=0 preconnect_payload_bytes=0" \
     "scan_warmup_iterations=0" \
     "measured_iterations=$MEASURED_ITERATIONS"
echo "CATALOG_INPUT format=metastore_snapshot source=$CATALOG_SOURCE" \
     "schema=$CATALOG_SCHEMA json=$CATALOG_JSON list_suffix=$CATALOG_SUFFIX" \
     "suffix_scope=all_catalog_rows" \
     "snapshot=$CATALOG_SNAPSHOT" \
     "source_rows=$CATALOG_SOURCE_ROWS suffix_excluded=$CATALOG_SUFFIX_EXCLUDED" \
     "objects=$OBJECT_COUNT bytes=$OBJECT_BYTES max_object_bytes=$MAX_OBJECT_BYTES" \
     "head_requests=0 endpoint_ips=$ENDPOINT_IP_COUNT"

# ---------------------------------------------------------------------------
# Verify prerequisites; report the optional order-2 RX-page experiment
# ---------------------------------------------------------------------------

ENA_VERSION=$(modinfo -F version ena 2>/dev/null || true)
[[ -n $ENA_VERSION ]] || ENA_VERSION=unavailable
for nic in "${NICS[@]}"; do
    [[ -d /sys/class/net/$nic ]] || fail "profile NIC is missing: $nic"
    driver=$(basename "$(readlink -f "/sys/class/net/$nic/device/driver")")
    [[ $driver == ena ]] || fail "$nic uses $driver, expected ena"
    mtu=$(<"/sys/class/net/$nic/mtu")
    ((mtu >= 9001)) || fail "$nic MTU is $mtu; jumbo MTU 9001 is required"
done
if [[ -r /sys/module/ena/parameters/large_rx_page ]]; then
    LARGE_RX_PAGE=$(< /sys/module/ena/parameters/large_rx_page)
    case ${LARGE_RX_PAGE,,} in
        1|y|yes|true|on) LARGE_RX_PAGE=enabled ;;
        0|n|no|false|off) LARGE_RX_PAGE=disabled ;;
        *) LARGE_RX_PAGE="unknown($LARGE_RX_PAGE)" ;;
    esac
else
    LARGE_RX_PAGE=unavailable
fi
if command -v aws >/dev/null && command -v curl >/dev/null; then
    report_s3_gateway_endpoint
else
    echo "S3_GATEWAY_ENDPOINT status=unverified reason=aws_cli_or_curl_unavailable" \
         "recommendation=associate_regional_gateway_endpoint"
fi
echo "PREREQUISITES ena_driver_version=$ENA_VERSION ena_driver_current_required=yes" \
     "local_mtu_9001=yes jumbo_path_mtu_required=yes" \
     "s3_gateway_endpoint=recommended_advisory no_so_rcvbuf=yes"
echo "ENA_RX_PAGE_EXPERIMENT large_rx_page=$LARGE_RX_PAGE" \
     "order2_descriptor_collapse=optional_non_upstream" \
     "requires_4k_base_pages=yes xdp_af_xdp=incompatible"

# ---------------------------------------------------------------------------
# Reversible queue and IRQ setup
# ---------------------------------------------------------------------------

parse_cpu_list "$IRQ_ENP135S0_CPUS" IRQ_CPUS_NIC0
parse_cpu_list "$IRQ_ENP153S0_CPUS" IRQ_CPUS_NIC1
parse_cpu_list "$IRQ_ENP170S0_CPUS" IRQ_CPUS_NIC2
parse_cpu_list "$IRQ_ENP187S0_CPUS" IRQ_CPUS_NIC3

IRQ_STATE=$(mktemp -d /tmp/s3-ktls-irq.XXXXXX)
IRQBALANCE_WAS_ACTIVE=0
declare -A IRQ_EXPECTED_CPU=()
declare -A IRQ_DEVICE=()
declare -A IRQ_LABEL=()
declare -A ORIGINAL_QUEUE_COUNT=()

cleanup() {
    local rc=$? saved irq dev
    trap - EXIT INT TERM
    set +e
    if [[ -d $IRQ_STATE ]]; then
        echo "Restoring IRQ affinity..."
        for saved in "$IRQ_STATE"/*.aff; do
            [[ -e $saved ]] || continue
            irq=${saved##*/}
            irq=${irq%.aff}
            sudo tee "/proc/irq/$irq/smp_affinity_list" \
                < "$saved" >/dev/null || true
        done
        rm -rf -- "$IRQ_STATE"
    fi
    if ((${#ORIGINAL_QUEUE_COUNT[@]} != 0)); then
        echo "Restoring NIC queue counts..."
        for dev in "${!ORIGINAL_QUEUE_COUNT[@]}"; do
            sudo ethtool -L "$dev" combined \
                "${ORIGINAL_QUEUE_COUNT[$dev]}" || true
        done
    fi
    if ((IRQBALANCE_WAS_ACTIVE)); then sudo systemctl start irqbalance || true; fi
    exit "$rc"
}
trap cleanup EXIT INT TERM

pin_device_irqs() {
    local dev=$1
    shift
    local -a cpus=("$@")
    local path irq label queue_index cpu requested attempt pinned=0
    for path in /sys/class/net/"$dev"/device/msi_irqs/*; do
        [[ -e $path ]] || continue
        irq=${path##*/}
        [[ -r /proc/irq/$irq/smp_affinity_list ]] || continue
        label=$(awk -v needle="$irq:" '$1 == needle { print $NF; exit }' \
            /proc/interrupts)
        [[ $label =~ Tx-Rx-([0-9]+)$ ]] || continue
        queue_index=${BASH_REMATCH[1]}
        ((queue_index < ENA_QUEUE_COUNT)) || continue
        if [[ ! -e $IRQ_STATE/$irq.aff ]]; then
            cp "/proc/irq/$irq/smp_affinity_list" "$IRQ_STATE/$irq.aff"
        fi
        cpu=${cpus[$((queue_index % ${#cpus[@]}))]}
        requested=
        for attempt in 1 2 3 4 5; do
            printf '%s\n' "$cpu" |
                sudo tee "/proc/irq/$irq/smp_affinity_list" >/dev/null
            requested=$(<"/proc/irq/$irq/smp_affinity_list")
            [[ $requested == "$cpu" ]] && break
        done
        [[ $requested == "$cpu" ]] || fail "IRQ $irq rejected CPU $cpu"
        IRQ_EXPECTED_CPU[$irq]=$cpu
        IRQ_DEVICE[$irq]=$dev
        IRQ_LABEL[$irq]=$label
        ((pinned += 1))
    done
    ((pinned == ENA_QUEUE_COUNT)) || \
        fail "found $pinned active RX IRQs for $dev, expected $ENA_QUEUE_COUNT"
    echo "IRQ_PIN dev=$dev rx_vectors=$pinned cpus=$(IFS=,; echo "${cpus[*]}")"
}

report_effective_irq_affinity() {
    local irq expected effective mismatches=0
    for irq in "${!IRQ_EXPECTED_CPU[@]}"; do
        expected=${IRQ_EXPECTED_CPU[$irq]}
        effective=$(<"/proc/irq/$irq/effective_affinity_list")
        if [[ $effective != "$expected" ]]; then
            echo "IRQ_EFFECTIVE_WARNING dev=${IRQ_DEVICE[$irq]} irq=$irq" \
                 "label=${IRQ_LABEL[$irq]} requested=$expected effective=$effective"
            ((mismatches += 1))
        fi
    done
    echo "IRQ_EFFECTIVE_SUMMARY vectors=${#IRQ_EXPECTED_CPU[@]} mismatches=$mismatches"
}

sudo modprobe tls
if systemctl is-active --quiet irqbalance; then
    IRQBALANCE_WAS_ACTIVE=1
    sudo systemctl stop irqbalance
fi
systemctl is-active --quiet irqbalance && fail "irqbalance remained active"
echo "IRQBALANCE stopped=yes restore_on_exit=$IRQBALANCE_WAS_ACTIVE"

for nic in "${NICS[@]}"; do
    current=$(ethtool -l "$nic" | awk '
        /^Current hardware settings:/ { current = 1; next }
        current && $1 == "Combined:" { print $2; exit }
    ')
    [[ $current =~ ^[0-9]+$ ]] || fail "cannot read queue count for $nic"
    if ((current != ENA_QUEUE_COUNT)); then
        ORIGINAL_QUEUE_COUNT[$nic]=$current
        sudo ethtool -L "$nic" combined "$ENA_QUEUE_COUNT"
    fi
done

pin_device_irqs enp135s0 "${IRQ_CPUS_NIC0[@]}"
pin_device_irqs enp153s0 "${IRQ_CPUS_NIC1[@]}"
pin_device_irqs enp170s0 "${IRQ_CPUS_NIC2[@]}"
pin_device_irqs enp187s0 "${IRQ_CPUS_NIC3[@]}"

# Avoid an expired developer SSO profile; the binary and transient unit use
# the instance role through IMDSv2.
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN AWS_SECURITY_TOKEN AWS_CONFIG_FILE
unset AWS_SHARED_CREDENTIALS_FILE

PROGRAM=(
    "$BIN"
    --catalog-snapshot "$CATALOG_SNAPSHOT"
    --region "$REGION"
    --connections-per-lane "$CONNECTIONS_PER_LANE"
    --h2d-batch "$H2D_BATCH"
    --slot-kib "$SLOT_KIB"
    --range-mib "$RANGE_MIB"
    --pinned-hwm-mib "$PINNED_HWM_MIB"
    --gpu-reserve-mib "$GPU_RESERVE_MIB"
    --max-retries "$MAX_RETRIES"
    --iterations "$MEASURED_ITERATIONS"
    --endpoint-ip-file "$ENDPOINT_IP_FILE"
)
for lane in "${LANES[@]}"; do PROGRAM+=(--lane "$lane"); done
[[ $PAYLOAD_SINK == receive-only ]] && PROGRAM+=(--receive-only)

mkdir -p "$RESULT_DIR"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
mode_label=$([[ $PAYLOAD_SINK == h2d ]] && echo h2d || echo receive-only)
log="$RESULT_DIR/g7e48xl-${mode_label}-preconnect-measure${MEASURED_ITERATIONS}-${stamp}.log"
echo "RUN_START log=$log"

set +e
sudo systemd-run \
    --quiet --wait --pipe --collect \
    -p User="$RUN_USER" \
    -p WorkingDirectory="$WORK_DIR" \
    -p AllowedCPUs="$APP_CPUS" \
    -p LimitMEMLOCK=infinity \
    -p LimitNOFILE=65536 \
    -p AmbientCapabilities=CAP_NET_RAW \
    -p CapabilityBoundingSet=CAP_NET_RAW \
    --setenv=HOME=/home/ubuntu \
    --setenv=AWS_REGION="$REGION" \
    --setenv=AWS_DEFAULT_REGION="$REGION" \
    --setenv=AWS_EC2_METADATA_DISABLED=false \
    "${PROGRAM[@]}" \
    2>&1 | tee "$log" | compact_console_filter
pipeline_status=("${PIPESTATUS[@]}")
run_rc=${pipeline_status[0]}
if ((run_rc == 0 && pipeline_status[1] != 0)); then run_rc=${pipeline_status[1]}; fi
if ((run_rc == 0 && pipeline_status[2] != 0)); then run_rc=${pipeline_status[2]}; fi
set -e

emit_compact_result "$log"
echo "RUN_END rc=$run_rc log=$log"
report_effective_irq_affinity
exit "$run_rc"
