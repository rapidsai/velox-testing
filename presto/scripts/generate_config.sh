#!/bin/bash
set -e
# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
# Configuration directory
CONFIG_DIR="$(cd "$(dirname "$0")"/../docker/config && pwd)"
# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}
print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}
print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}
print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}
# Function to create directory if it doesn't exist
ensure_dir() {
    if [[ ! -d "$1" ]]; then
        mkdir -p "$1"
        print_status "Created directory: $1"
    fi
}
# Function to generate hive.properties
generate_hive_properties() {
    local file="$CONFIG_DIR/etc_common/catalog/hive.properties"
    cat > "$file" << 'EOF'
connector.name=hive-hadoop2
# Use a local file-based metastore so we can register external Parquet tables
hive.metastore=file
hive.metastore.catalog.dir=file:/data/hive-metastore
# Helpful defaults for working with external Parquet data
hive.allow-drop-table=true
hive.non-managed-table-writes-enabled=true
hive.recursive-directories=true
hive.parquet.use-column-names=true
EOF
    print_success "Generated: $file"
}
# Function to generate tpch.properties
generate_tpch_properties() {
    local file="$CONFIG_DIR/etc_common/catalog/tpch.properties"
    cat > "$file" << 'EOF'
connector.name=tpch
tpch.column-naming=STANDARD
EOF
    print_success "Generated: $file"
}
# Function to generate common config.properties
generate_common_config() {
    local file="$CONFIG_DIR/etc_common/config.properties"
    cat > "$file" << 'EOF'
# Common Presto configuration
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF
    print_success "Generated: $file"
}
# Function to generate common node.properties
generate_common_node() {
    local file="$CONFIG_DIR/etc_common/node.properties"
    cat > "$file" << 'EOF'
# Node configuration
node.environment=testing
node.id=presto-node
node.data-dir=/var/presto/data
EOF
    print_success "Generated: $file"
}
# Function to generate common log.properties
generate_common_log() {
    local file="$CONFIG_DIR/etc_common/log.properties"
    cat > "$file" << 'EOF'
# Logging configuration
com.facebook.presto=INFO
EOF
    print_success "Generated: $file"
}
# Function to generate coordinator config
generate_coordinator_config() {
    local file="$CONFIG_DIR/etc_coordinator/config_java.properties"
    cat > "$file" << 'EOF'
# Coordinator configuration for Java Presto
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF
    print_success "Generated: $file"
}
# Function to generate worker config
generate_worker_config() {
    local file="$CONFIG_DIR/etc_worker/config_java.properties"
    cat > "$file" << 'EOF'
# Worker configuration for Java Presto
coordinator=false
http-server.http.port=8081
discovery.uri=http://localhost:8080
EOF
    print_success "Generated: $file"
}
# Function to generate native configs
generate_native_configs() {
    # Native CPU coordinator
    cat > "$CONFIG_DIR/etc_coordinator/config_native.properties" << 'EOF'
# Coordinator configuration for Native CPU Presto
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
native-execution-enabled=true
EOF
    # Native CPU worker
    cat > "$CONFIG_DIR/etc_worker/config_native.properties" << 'EOF'
# Worker configuration for Native CPU Presto
coordinator=false
http-server.http.port=8081
discovery.uri=http://localhost:8080
native-execution-enabled=true
EOF
    print_success "Generated native configuration files"
}
# Function to generate node properties
generate_node_properties() {
    # Coordinator node
    cat > "$CONFIG_DIR/etc_coordinator/node.properties" << 'EOF'
# Coordinator node configuration
node.environment=testing
node.id=presto-coordinator
node.data-dir=/var/presto/data
EOF
    # Worker node
    cat > "$CONFIG_DIR/etc_worker/node.properties" << 'EOF'
# Worker node configuration
node.environment=testing
node.id=presto-worker
node.data-dir=/var/presto/data
EOF
    print_success "Generated node property files"
}
# Function to generate JVM config
generate_jvm_config() {
    local file="$CONFIG_DIR/etc_common/jvm.config"
    cat > "$file" << 'EOF'
-server
-Xmx16G
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:+UseGCOverheadLimit
EOF
    print_success "Generated: $file"
}
# Function to show usage
show_usage() {
    cat << 'EOF'
Usage: $0 [OPTIONS]
Options:
  --all                    Generate all configuration files (default)
  --catalog-only           Generate only catalog configuration files
  --coordinator-only       Generate only coordinator configuration files
  --worker-only            Generate only worker configuration files
  --native-only            Generate only native execution configuration files
  --java-only              Generate only Java execution configuration files
  --force                  Overwrite existing files
  --dry-run                Show what would be generated without creating files
  -h, --help               Show this help message
Examples:
  $0                       # Generate all configuration files
  $0 --catalog-only        # Generate only hive.properties and tpch.properties
  $0 --force               # Overwrite existing files
  $0 --dry-run             # Preview what would be generated
EOF
}
# Main function
main() {
    local generate_all=true
    local catalog_only=false
    local coordinator_only=false
    local worker_only=false
    local native_only=false
    local java_only=false
    local force=false
    local dry_run=false
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --all)
                generate_all=true
                shift
                ;;
            --catalog-only)
                generate_all=false
                catalog_only=true
                shift
                ;;
            --coordinator-only)
                generate_all=false
                coordinator_only=true
                shift
                ;;
            --worker-only)
                generate_all=false
                worker_only=true
                shift
                ;;
            --native-only)
                generate_all=false
                native_only=true
                shift
                ;;
            --java-only)
                generate_all=false
                java_only=true
                shift
                ;;
            --force)
                force=true
                shift
                ;;
            --dry-run)
                dry_run=true
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    print_status "Starting Presto configuration generation..."
    print_status "Configuration directory: $CONFIG_DIR"
    if [[ "$dry_run" == "true" ]]; then
        print_warning "DRY RUN MODE - No files will be created"
    fi
    # Create necessary directories
    if [[ "$dry_run" == "false" ]]; then
        ensure_dir "$CONFIG_DIR/etc_common/catalog"
        ensure_dir "$CONFIG_DIR/etc_coordinator"
        ensure_dir "$CONFIG_DIR/etc_worker"
    fi
    # Generate catalog configurations
    if [[ "$generate_all" == "true" || "$catalog_only" == "true" ]]; then
        print_status "Generating catalog configurations..."
        if [[ "$dry_run" == "false" ]]; then
            generate_hive_properties
            generate_tpch_properties
        else
            print_status "Would generate: $CONFIG_DIR/etc_common/catalog/hive.properties"
            print_status "Would generate: $CONFIG_DIR/etc_common/catalog/tpch.properties"
        fi
    fi
    # Generate common configurations
    if [[ "$generate_all" == "true" ]]; then
        print_status "Generating common configurations..."
        if [[ "$dry_run" == "false" ]]; then
            generate_common_config
            generate_common_node
            generate_common_log
            generate_jvm_config
        else
            print_status "Would generate common configuration files"
        fi
    fi
    # Generate coordinator configurations
    if [[ "$generate_all" == "true" || "$coordinator_only" == "true" || "$java_only" == "true" ]]; then
        print_status "Generating coordinator configurations..."
        if [[ "$dry_run" == "false" ]]; then
            generate_coordinator_config
            generate_node_properties
        else
            print_status "Would generate coordinator configuration files"
        fi
    fi
    # Generate worker configurations
    if [[ "$generate_all" == "true" || "$worker_only" == "true" || "$java_only" == "true" ]]; then
        print_status "Generating worker configurations..."
        if [[ "$dry_run" == "false" ]]; then
            generate_worker_config
        else
            print_status "Would generate worker configuration files"
        fi
    fi
    # Generate native configurations
    if [[ "$generate_all" == "true" || "$native_only" == "true" ]]; then
        print_status "Generating native execution configurations..."
        if [[ "$dry_run" == "false" ]]; then
            generate_native_configs
        else
            print_status "Would generate native execution configuration files"
        fi
    fi
    if [[ "$dry_run" == "false" ]]; then
        print_success "Configuration generation completed!"
        print_status "Files generated in: $CONFIG_DIR"
    else
        print_success "Dry run completed!"
    fi
}
# Run main function
main "$@"





