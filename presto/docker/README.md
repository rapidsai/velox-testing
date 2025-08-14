# Presto with TPC-H Benchmarking Setup

This directory contains a custom Docker setup for Presto that includes TPC-H benchmarking capabilities.

## What's Included

### Custom Dockerfile
- **Base Image**: `prestodb/presto:latest`
- **Additional Packages**: 
  - `jq` - JSON processor for parsing Presto API responses
  - `bc` - Basic calculator for mathematical operations in benchmark scripts

### Docker Compose Configuration
- **File**: `docker-compose.java.yml`
- **Services**:
  - `presto-coordinator` - Presto coordinator node
  - `presto-java-worker` - Presto worker node
- **Volumes**: 
  - Configuration files from `./config/`
  - TPC-H data from `./data/tpch/`
  - Hive metastore from `./data/hive-metastore/`

## Building and Running

### 1. Build Custom Images
```bash
cd /workspace/velox-testing/presto/docker
docker-compose -f docker-compose.java.yml build
```

### 2. Start Presto Cluster
```bash
cd /workspace/velox-testing/presto/scripts/deployment
./start_java_presto.sh
```

### 3. Install Host Dependencies (if needed)
```bash
cd /workspace/velox-testing/presto/scripts
./setup_dependencies.sh
```

### 4. Run TPC-H Benchmark
```bash
cd /workspace/velox-testing/presto/scripts
# Run single query
./tpch_benchmark.sh benchmark -s 1 -q 1

# Run all queries
./tpch_benchmark.sh benchmark -s 1

# Full workflow (generate data + register tables + benchmark)
./tpch_benchmark.sh full -s 1
```

## Key Features

- **JMX Fix**: Custom JVM configuration includes `-Djdk.attach.allowAttachSelf=true` to resolve JMX attachment issues
- **Docker Host Fix**: Scripts automatically unset `DOCKER_HOST` to use local Docker daemon
- **TPC-H Ready**: Pre-configured with all dependencies needed for TPC-H benchmarking
- **Scale Factor Support**: Supports TPC-H scale factors 1, 10, and 100

## Ports

- **Presto Coordinator**: `8080` (HTTP API and Web UI)
- **Presto Worker**: Internal communication only

## Volumes

- `./config/etc_common` → `/opt/presto-server/etc` (shared configuration)
- `./config/etc_coordinator/` → coordinator-specific config
- `./config/etc_worker/` → worker-specific config  
- `./data/tpch/` → TPC-H Parquet data (read-only)
- `./data/hive-metastore/` → Hive metastore data

## Troubleshooting

### Docker Daemon Issues
If you see "Cannot connect to the Docker daemon", run:
```bash
unset DOCKER_HOST
```

### Missing Dependencies
If benchmark scripts fail with "command not found", run:
```bash
./setup_dependencies.sh
```

### JMX Issues
The custom Dockerfile includes the JMX fix. If you see JMX errors, ensure you're using the custom-built images, not the stock prestodb images.
