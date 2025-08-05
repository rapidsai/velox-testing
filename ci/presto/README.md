# Presto CI Scripts

This directory contains CI scripts for building and testing Presto with Velox connector.

## Scripts

### `build-and-test.sh`

A script for building and testing Presto using the Docker Compose infrastructure as described in the main README.md. This script handles the deployment of different Presto variants and includes ccache support for native builds.

#### Usage

```bash
# Build native GPU Presto with ccache support and run tests
./build-and-test.sh --build-target native-gpu --run-tests true --ccache-dir /path/to/ccache

# Build native CPU Presto without tests
./build-and-test.sh --build-target native-cpu --run-tests false

# Build Java-only Presto
./build-and-test.sh --build-target java-only

# Show help
./build-and-test.sh --help
```

#### Options

- `--build-target TARGET`: Presto deployment variant (native-gpu, native-cpu, java-only) (default: native-gpu)
- `--run-tests BOOLEAN`: Run tests after deployment (true/false) (default: true)
- `--ccache-dir PATH`: Path to ccache directory for native builds (optional)
- `--help`: Show help message

#### Features

- ✅ **Docker Compose Infrastructure** - Uses the official Docker Compose testing infrastructure
- ✅ **Multiple Deployment Variants** - Supports Java-only, native CPU, and native GPU workers
- ✅ **ccache Integration** - Automatic ccache support for native builds when cache directory is provided
- ✅ **Service Health Checks** - Verifies Presto server accessibility after deployment
- ✅ **Automated Testing** - Configurable test execution after deployment
- ✅ **Comprehensive Logging** - Detailed output for debugging and monitoring

#### When to Use

- **For CI/CD workflows** - Use `build-and-test.sh` for GitHub Actions and automated testing
- **For Docker Compose environments** - Use `build-and-test.sh` for complete service deployment
- **For production-like testing** - Use `build-and-test.sh` for full stack testing with coordinator + workers
- **For local development** - Use `build-and-test.sh` for consistent builds across environments

#### Requirements

- Docker and Docker Compose
- Presto source code checked out
- Velox source code checked out (when using native builds)
- ccache directory (optional, for build acceleration)

## Docker Infrastructure Integration

The Presto native build infrastructure now includes comprehensive ccache support and enhanced Docker integration.

### Enhanced Start Scripts

The native start scripts (`presto/scripts/start_native_gpu_presto.sh` and `presto/scripts/start_native_cpu_presto.sh`) have been enhanced with ccache support:

#### Usage
```bash
# Build with ccache support
./start_native_gpu_presto.sh --ccache-dir /path/to/ccache
./start_native_cpu_presto.sh --ccache-dir /path/to/ccache

# Build without ccache (backward compatible)
./start_native_gpu_presto.sh
./start_native_cpu_presto.sh
```

#### Features
- ✅ **ccache Integration** - Automatic Docker BuildKit usage when cache directory provided
- ✅ **Backward Compatibility** - Works without ccache for existing workflows
- ✅ **Enhanced Performance** - Significantly faster native builds with compiler caching
- ✅ **Docker BuildKit** - Uses advanced container build features for optimization

### Docker Build Infrastructure

The `native_build.dockerfile` has been enhanced with comprehensive ccache support:

- **ccache Configuration** - Optimized settings for C++ compilation performance
- **Cache Mount Support** - Docker cache mounts for persistent compiler cache
- **Build Statistics** - ccache statistics reporting before/after builds
- **Environment Variables** - Configurable cache size and behavior

## GitHub Actions Integration

The scripts are integrated with the `presto-test.yml` GitHub Actions workflow using Docker Compose infrastructure.

### Workflow Inputs

When manually triggering the Presto test workflow, you can specify:

- **Velox commit**: SHA or branch to build (required, default: 'main')
- **Presto commit**: SHA or branch to build (required, default: 'main')  
- **Velox build type**: CMake build type for Velox (choice, default: Release)
- **Velox CUDA architecture**: CUDA arch specification for Velox (string, default: 'native')
- **Velox build directory**: Build directory name for Velox (string, default: 'build')
- **Build target**: Presto deployment variant - native-gpu/native-cpu/java-only (choice, default: 'native-gpu')
- **Run Presto tests**: Whether to run Presto integration tests (boolean, default: true)

### Example Usage in GitHub Actions

```yaml
# Manual trigger with custom options
- Velox commit: main
- Presto commit: feature-branch
- Velox build type: Debug
- Build target: native-gpu
- Run Presto tests: ✅ (checked)

# Results in script call:
./build-and-test.sh --build-target native-gpu --run-tests true --ccache-dir /workspace/ccache
```

### Workflow Process

1. **Build Velox** - Uses composite action `.github/actions/build-velox` with cuDF support and ccache
2. **Checkout Presto** - Downloads Presto source at specified commit  
3. **Replace Velox Submodule** - Links the pre-built Velox to avoid rebuilding
4. **Restore Presto Compiler Cache** - Restores ccache for Presto native builds
5. **Build and Test Presto** - Uses `build-and-test.sh` with Docker Compose infrastructure:
   - **Java-only**: Presto Java Coordinator + Java Workers
   - **Native CPU**: Presto Java Coordinator + Native CPU Workers (with ccache)
   - **Native GPU**: Presto Java Coordinator + Native GPU Workers (with ccache)
6. **Health Check** - Verifies Presto server accessibility at http://localhost:8080
7. **Run Tests** - Executes Presto integration tests (if enabled)
8. **Stash Presto Compiler Cache** - Saves ccache for future runs
9. **Cleanup** - Stops all Presto services

### Performance Optimizations

- ✅ **Dual ccache Strategy** - Separate cache keys for Velox (`ccache-linux-adapters-gcc`) and Presto (`ccache-linux-presto-native-gcc`)
- ✅ **Velox Build Reuse** - Presto workflow reuses pre-built Velox from step 1 (no rebuilding)
- ✅ **Docker BuildKit** - Advanced container build caching for native components
- ✅ **Persistent Caching** - Compiler caches persist across workflow runs

The workflow automatically constructs the appropriate command line arguments based on your inputs and leverages the official Docker Compose testing infrastructure for reliable deployment.