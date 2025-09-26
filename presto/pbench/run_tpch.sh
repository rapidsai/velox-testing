
#!/bin/bash

BASE_DIR="$(dirname $(realpath $0))/../.."
OUTPUT_DIR="$BASE_DIR/pbench_output/tpch"
PBENCH_DIR="$BASE_DIR/presto/pbench/"
COORD="localhost:8080"

mkdir -p $OUTPUT_DIR
$PBENCH_DIR/pbench run -s http://$COORD/ -o $OUTPUT_DIR $PBENCH_DIR/benchmarks/tpch/sf100.json 
