# Presto CI Scripts

This directory contains CI scripts for building and testing Presto with Velox connector.

## Scripts

### `build-and-test.sh`

A comprehensive script for building Presto Coordinator (Java) and Prestissimo C++ GPU worker with Velox connector, and optionally running integration tests.

#### Usage

```bash
# Basic build - both coordinator and prestissimo (no tests)
./build-and-test.sh

# Build and run integration tests
./build-and-test.sh --run-tests

# Build only Presto coordinator (Java)
./build-and-test.sh --coordinator-only

# Build only Prestissimo C++ worker
./build-and-test.sh --prestissimo-only

# Custom Maven profile and Prestissimo directory
./build-and-test.sh --profile myprofile --prestissimo-dir /path/to/prestissimo --run-tests

# Use existing Velox build for Prestissimo (avoids rebuilding Velox)
./build-and-test.sh --prestissimo-only --velox-build-dir /path/to/velox/build
```

#### Options

- `--run-tests`: Run integration tests after build (default: false)
- `--profile`: Maven profile to use (default: velox)
- `--coordinator-only`: Build only Presto coordinator (Java)
- `--prestissimo-only`: Build only Prestissimo C++ worker
- `--prestissimo-dir`: Directory containing Prestissimo source (default: presto-native-execution)
- `--velox-build-dir`: Path to existing Velox build directory (reuses Velox build for Prestissimo)
- `--help, -h`: Show help message

#### Features

- ✅ Builds both Presto Coordinator (Java) and Prestissimo C++ GPU worker
- ✅ Configurable build targets (coordinator-only, prestissimo-only, or both)
- ✅ **Velox build reuse** - Can reuse existing Velox build for Prestissimo (avoids rebuilding)
- ✅ Configurable Maven profiles for Java builds
- ✅ CMake-based build for Prestissimo C++ worker with Velox integration
- ✅ Skips tests during build by default for faster compilation
- ✅ Optional integration test execution
- ✅ Colored output for better readability
- ✅ Error handling with early exit on failure
- ✅ Validates directory structure for both Java and C++ components

#### Requirements

**For Presto Coordinator (Java):**
- Java and Maven (via ./mvnw)
- Git configured with safe directory access
- Presto source code checked out to `./presto/` directory

**For Prestissimo C++ Worker:**
- CMake and Make/Ninja build system
- C++ compiler with C++17 support
- Velox dependencies (automatically handled if built with Velox)
- Prestissimo source code (usually in `presto-native-execution/` directory)

**Common:**
- Git configured with safe directory access

## GitHub Actions Integration

The script is integrated with the `presto-test.yml` GitHub Actions workflow.

### Workflow Inputs

When manually triggering the Presto test workflow, you can specify:

- **Velox commit**: SHA or branch to build (required, default: 'main')
- **Presto commit**: SHA or branch to build (required, default: 'main')  
- **Velox build type**: CMake build type for Velox (choice, default: Release)
- **Velox CUDA architecture**: CUDA arch specification for Velox (string, default: 'native')
- **Velox build directory**: Build directory name for Velox (string, default: 'build')
- **Build target**: What to build - both/coordinator-only/prestissimo-only (choice, default: 'both')
- **Run Presto tests**: Whether to run Presto integration tests (boolean, default: true)

### Example Usage in GitHub Actions

```yaml
# Manual trigger with custom options
- Velox commit: main
- Presto commit: feature-branch
- Velox build type: Debug
- Build target: coordinator-only
- Run Presto tests: ✅ (checked)

# Results in script call:
./build-and-test.sh --coordinator-only --run-tests
```

### Workflow Process

1. **Build Velox** - Uses standardized Velox build script (`velox-build.sh`) with cuDF support
2. **Checkout Presto** - Downloads Presto source at specified commit  
3. **Build Presto Components** - Builds selected components (`presto-build.sh`):
   - **Coordinator** (Java): Compiles Presto coordinator with Velox connector using Maven
   - **Prestissimo** (C++): Compiles Prestissimo C++ GPU worker using CMake, **reusing the Velox build from step 1**
4. **Run Tests** - Executes Presto integration tests (if enabled)

**Note:** The workflow uses unique script names (`velox-build.sh` and `presto-build.sh`) to avoid filename collisions between the two build steps.

The workflow automatically constructs the appropriate command line arguments based on your inputs.