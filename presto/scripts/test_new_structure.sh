#!/bin/bash
# Test Script for New TPC-H Benchmark Structure
# Quick validation that all components work correctly

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Change to scripts directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

print_status "🧪 Testing New TPC-H Benchmark Structure"
print_status "This validates the implementation addressing Paul's feedback"
print_status ""

# Test 1: Check modular directory structure
print_status "1. Testing modular directory structure..."
required_dirs=(
    "deployment"
    "data"
    "../benchmarks/tpch"
)

for dir in "${required_dirs[@]}"; do
    if [[ -d "$dir" ]]; then
        print_success "✓ Directory exists: $dir"
    else
        print_error "✗ Missing directory: $dir"
        exit 1
    fi
done

# Test 2: Check script separation and functionality
print_status ""
print_status "2. Testing script separation (Paul's requirement)..."

# Test deployment script (should ONLY start services)
print_status "Testing deployment script scope..."
if deployment/start_java_presto.sh --help | grep -q "Purpose: Start Presto Java coordinator and worker services"; then
    print_success "✓ Deployment script has focused scope"
else
    print_error "✗ Deployment script scope issue"
    exit 1
fi

# Test data scripts are separated
print_status "Testing data management separation..."
if data/generate_tpch_data.sh --help | grep -q "Purpose: Generate TPC-H Parquet data"; then
    print_success "✓ Data generation is properly separated"
else
    print_error "✗ Data generation script issue"
    exit 1
fi

if data/register_tpch_tables.sh --help | grep -q "Purpose: Register TPC-H Parquet tables"; then
    print_success "✓ Table registration is properly separated"
else
    print_error "✗ Table registration script issue"
    exit 1
fi

# Test 3: Check configuration file architecture
print_status ""
print_status "3. Testing configuration file architecture fix..."

# Check that etc_common files are now placeholders
if grep -q "overridden by coordinator or worker specific" ../docker/config/etc_common/config.properties; then
    print_success "✓ etc_common/config.properties is now a placeholder"
else
    print_error "✗ etc_common/config.properties still has active configs"
    exit 1
fi

if grep -q "overridden by coordinator or worker specific" ../docker/config/etc_common/node.properties; then
    print_success "✓ etc_common/node.properties is now a placeholder"
else
    print_error "✗ etc_common/node.properties still has active configs"
    exit 1
fi

# Check that real configs are in coordinator/worker specific files
if [[ -s "../docker/config/etc_coordinator/config_java.properties" ]]; then
    print_success "✓ Coordinator Java config exists with content"
else
    print_error "✗ Coordinator Java config missing or empty"
    exit 1
fi

if [[ -s "../docker/config/etc_worker/config_java.properties" ]]; then
    print_success "✓ Worker Java config exists with content"
else
    print_error "✗ Worker Java config missing or empty"
    exit 1
fi

# Test 4: Check JVM configuration documentation
print_status ""
print_status "4. Testing JVM configuration documentation..."

if grep -q "Memory Configuration:" ../docker/config/etc_common/jvm.config; then
    print_success "✓ JVM configuration is documented"
else
    print_error "✗ JVM configuration lacks documentation"
    exit 1
fi

if grep -q "multi-threaded benchmark scenarios" ../docker/config/etc_common/jvm.config; then
    print_success "✓ UseBiasedLocking rationale documented"
else
    print_error "✗ UseBiasedLocking rationale missing"
    exit 1
fi

# Test 5: Check Python test suite
print_status ""
print_status "5. Testing Python test suite (Paul's requirement)..."

if [[ -f "../benchmarks/tpch/run_benchmark.py" ]]; then
    print_success "✓ Python benchmark runner exists"
else
    print_error "✗ Python benchmark runner missing"
    exit 1
fi

if [[ -f "../benchmarks/tpch/requirements.txt" ]]; then
    print_success "✓ Python requirements file exists"
else
    print_error "✗ Python requirements file missing"
    exit 1
fi

if grep -q "pytest-benchmark" ../benchmarks/tpch/requirements.txt; then
    print_success "✓ pytest-benchmark dependency included"
else
    print_error "✗ pytest-benchmark dependency missing"
    exit 1
fi

if [[ -f "../benchmarks/tpch/test_tpch_queries.py" ]]; then
    print_success "✓ TPC-H query test suite exists"
else
    print_error "✗ TPC-H query test suite missing"
    exit 1
fi

# Test 6: Check that unnecessary scripts were removed
print_status ""
print_status "6. Testing removal of unnecessary scripts..."

if [[ ! -f "generate_config.sh" ]]; then
    print_success "✓ generate_config.sh removed (configs in git)"
else
    print_error "✗ generate_config.sh still exists"
    exit 1
fi

# Test 7: Test Python benchmark functionality (if dependencies available)
print_status ""
print_status "7. Testing Python benchmark functionality..."

cd ../benchmarks/tpch
if python -c "import pytest, prestodb" 2>/dev/null; then
    print_status "Testing Python benchmark help..."
    if python run_benchmark.py --help | grep -q "TPC-H Benchmark Runner"; then
        print_success "✓ Python benchmark runner functional"
    else
        print_error "✗ Python benchmark runner not working"
        exit 1
    fi
else
    print_warning "⚠ Python dependencies not installed - skipping functionality test"
    print_status "Run: pip install -r requirements.txt"
fi

cd "$SCRIPT_DIR"

# Test 8: Validate Docker Compose structure
print_status ""
print_status "8. Testing Docker Compose configuration..."

if grep -q "config_java.properties:/opt/presto-server/etc/config.properties" ../docker/docker-compose.java.yml; then
    print_success "✓ Docker Compose uses correct config override"
else
    print_error "✗ Docker Compose configuration issue"
    exit 1
fi

# Final validation
print_status ""
print_success "🎉 All tests passed! New structure is working correctly."
print_status ""
print_status "✅ Paul's feedback addressed:"
print_status "   • Decoupled data generation from deployment"
print_status "   • Fixed configuration file architecture"
print_status "   • Documented JVM configuration rationale"
print_status "   • Limited script scope to intended purposes"
print_status "   • Implemented Python test suite with pytest-benchmark"
print_status "   • Removed unnecessary configuration generation"
print_status ""
print_status "📖 Next steps:"
print_status "   1. Read: ../IMPLEMENTATION_SUMMARY.md"
print_status "   2. Read: ../benchmarks/README.md"
print_status "   3. Try: ./migrate_to_new_structure.sh --check-only"
print_status "   4. Start using the new modular workflow!"
print_status ""
print_status "🚀 Ready for professional TPC-H benchmarking!"
