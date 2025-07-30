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

# All options
./build-and-test.sh --run-tests --build-type Debug --cuda-arch "75;80"
```

#### Options

- `--run-tests`: Build and run the test suite (default: false)
- `--build-type`: CMake build type - Release, Debug, etc. (default: Release)
- `--cuda-arch`: CUDA architecture specification (default: native)
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

The script is integrated with the `velox-test.yml` GitHub Actions workflow. You can pass arguments through the workflow inputs:

### Workflow Inputs

When manually triggering the workflow (workflow_dispatch), you can specify:

- **Velox commit**: SHA or branch to build (required, default: 'main')
- **Run tests**: Whether to run tests after build (boolean, default: false)
- **Build type**: CMake build type - Release/Debug/RelWithDebInfo (choice, default: Release)
- **CUDA architecture**: CUDA arch specification (string, default: 'native')

### Example Usage in GitHub Actions

```yaml
# Manual trigger with custom options
- Run tests: ✅ (checked)
- Build type: Debug
- CUDA architecture: "75;80"

# Results in script call:
./build-and-test.sh --run-tests --build-type Debug --cuda-arch "75;80"
```

The workflow automatically constructs the appropriate command line arguments based on your inputs.