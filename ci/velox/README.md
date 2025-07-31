# Velox CI Scripts

This directory contains CI scripts for building and testing Velox.

## Scripts

### `build-and-test.sh`

A comprehensive script for building Velox with cuDF support and optionally running tests.

#### Usage

```bash
# Basic build (no tests)
./build-and-test.sh

# Build and run tests
./build-and-test.sh --run-tests

# Custom build type
./build-and-test.sh --build-type Debug

# Custom CUDA architecture
./build-and-test.sh --cuda-arch "75;80"

# Custom build directory
./build-and-test.sh --build-dir velox-build-debug

# All options
./build-and-test.sh --run-tests --build-type Debug --cuda-arch "75;80" --build-dir my-build
```

#### Options

- `--run-tests`: Build and run the test suite (default: false)
- `--build-type`: CMake build type - Release, Debug, etc. (default: Release)
- `--cuda-arch`: CUDA architecture specification (default: native)
- `--build-dir`: Build directory name (default: build)
- `--help, -h`: Show help message

#### Features

- ✅ Builds Velox with cuDF, Parquet, and S3 support
- ✅ Configurable build options
- ✅ Optional test execution
- ✅ Colored output for better readability
- ✅ Error handling with early exit on failure

#### Requirements

- CMake
- Ninja build system
- CUDA toolkit
- Git configured with safe directory access

## GitHub Actions Integration

The script is integrated with multiple GitHub Actions workflows:
- **`velox-test.yml`** - Dedicated Velox testing with full configuration options
- **`presto-test.yml`** - Presto integration testing with Velox build configuration

You can pass arguments through the workflow inputs:

### Workflow Inputs

#### `velox-test.yml` Workflow
When manually triggering the Velox test workflow, you can specify:

- **Velox commit**: SHA or branch to build (required, default: 'main')
- **Run tests**: Whether to run tests after build (boolean, default: false)
- **Build type**: CMake build type - Release/Debug/RelWithDebInfo (choice, default: Release)
- **CUDA architecture**: CUDA arch specification (string, default: 'native')
- **Build directory**: Build directory name (string, default: 'build')

#### `presto-test.yml` Workflow  
When manually triggering the Presto test workflow, you can specify:

- **Velox commit**: SHA or branch to build (required, default: 'main')
- **Presto commit**: SHA or branch to build (required, default: 'main')  
- **Velox build type**: CMake build type for Velox (choice, default: Release)
- **Velox CUDA architecture**: CUDA arch specification for Velox (string, default: 'native')
- **Velox build directory**: Build directory name for Velox (string, default: 'build')

*Note: The Presto workflow only builds Velox (no Velox tests), then focuses on Presto integration testing.*

### Example Usage in GitHub Actions

```yaml
# Manual trigger with custom options
- Run tests: ✅ (checked)
- Build type: Debug
- CUDA architecture: "75;80"
- Build directory: "velox-debug-build"

# Results in script call (copied as velox-build.sh in workflow):
./velox-build.sh --run-tests --build-type Debug --cuda-arch "75;80" --build-dir "velox-debug-build"
```

The workflow automatically constructs the appropriate command line arguments based on your inputs.