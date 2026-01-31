---
layout: default
title: Velox Testing
---

# Presto Profile Analysis Tools

Tools for Presto performance analysis.

The benchmark_output/metrics/*.presto_metrics.json files generated from below command can be used for these tools.
[`presto/scripts/run_benchmark.sh ... --metrics`](https://github.com/rapidsai/velox-testing/blob/main/presto/scripts/run_benchmark.sh)

Eg. 
`presto/scripts/run_benchmark.sh -b tpch -s sf_100_date --metrics -i 1 -q 2`

---

### [Query Comparison](query_comparison.html)
Compare query execution across different configurations.

### [Query Profiler](query_profiler.html)
Analyze and profile query performance metrics.
