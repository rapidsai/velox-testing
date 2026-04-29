# Velox + Presto Devcontainer

A GPU-ready development environment for building [Velox](https://github.com/facebookincubator/velox) with cuDF acceleration and [Presto Native Execution](https://github.com/prestodb/presto/tree/master/presto-native-execution). Based on the RAPIDS devcontainer image with scripts for building cuDF, RMM, UCXX, and KvikIO from source.

## Quick Start

```bash
# 1. Clone sibling repos under the same parent directory
mkdir ~/code && cd ~/code
git clone https://github.com/<org>/velox-testing.git
git clone https://github.com/facebookincubator/velox.git
git clone https://github.com/prestodb/presto.git
git clone https://github.com/rapidsai/rmm.git
git clone https://github.com/rapidsai/cudf.git
git clone https://github.com/rapidsai/ucxx.git
git clone https://github.com/rapidsai/kvikio.git

# 2. Open in VS Code (or any devcontainer-compatible editor)
code velox-testing

# 3. Reopen in container (pick container based on CUDA version)

# 4. Build
build-all       # Build RAPIDS libraries, standalone Velox, then Presto
build-velox     # Build standalone Velox
build-presto    # Build Presto; syncs ~/velox and builds it inside the Presto tree

# 5. Test
test-velox      # Run Velox test suite
test-presto     # Run Presto test suite
```

## Directory Layout

The devcontainer expects this layout on the host:

```
~/code/
├── velox-testing/     # this repo (devcontainer workspace root)
├── velox/             # facebookincubator/velox
├── presto/            # prestodb/presto
├── rmm/               # rapidsai/rmm
├── cudf/              # rapidsai/cudf
├── ucxx/              # rapidsai/ucxx
└── kvikio/            # rapidsai/kvikio
```

All repos are bind-mounted into the container under `/home/coder/`.

## Build Architecture

`build-velox` builds `~/velox` as a standalone Velox tree:

```
source: ~/velox
build:  /opt/velox-build/<release|debug>
mode:   VELOX_MONO_LIBRARY=ON, VELOX_BUILD_SHARED=ON
```

`build-presto` builds Presto Native Execution and Velox together:

```
source: ~/presto/presto-native-execution
build:  /opt/presto-build
deps:   /opt/fb-deps for folly, fbthrift, proxygen, and related libraries
velox:  rsync ~/velox/ into ~/presto/presto-native-execution/velox/
```

Both builds consume **cudf**, **rmm**, **ucxx**, and **kvikio** built from source in the RAPIDS devcontainer.

**Standalone velox** bundles all its dependencies (folly, xsimd, Arrow, etc.) — no prerequisite steps.

**Presto** requires Facebook's OSS stack (folly, fbthrift, proxygen, etc.) for its thrift RPC layer. `build-presto` builds these automatically on first run and caches them at `/opt/fb-deps`. Subsequent runs skip this step unless `--rebuild-deps` is passed.

### When Velox Builds Twice

`build-presto` does not require `build-velox` to run first. It copies `~/velox` into `~/presto/presto-native-execution/velox` with `rsync`, then Presto's CMake build compiles Velox through `add_subdirectory(velox)`.

Velox builds twice only when you run `build-all` or `build-all-cpp`: once as the standalone `/opt/velox-build` build, then again inside the Presto build tree under `/opt/presto-build`.

## Commands

| Command | Description |
|---------|-------------|
| `build-velox` | Build standalone velox with cuDF (fully self-contained) |
| `build-presto` | Build presto-native-execution (auto-builds FB deps on first run) |
| `configure-velox` | CMake configure only (for IDE integration) |
| `test-velox` | Run velox tests via ctest |
| `test-presto` | Run presto tests via ctest |
| `clean-velox` | Delete velox build artifacts |
| `clean-presto` | Delete presto build artifacts |

All commands accept `--help`. Common options:

```bash
build-velox --debug            # debug build
build-velox -j 16              # limit parallelism
build-presto --release         # release build (default)
build-presto --rebuild-deps    # force rebuild of FB deps
```

## CUDA Variants

Two devcontainer configurations are provided:

| Path | CUDA | Base Image |
|------|------|------------|
| `.devcontainer/cuda13.1/` | 13.1 | `rapidsai/devcontainers:latest-cpp-cuda13.1-*` |
| `.devcontainer/cuda12.9/` | 12.9 | `rapidsai/devcontainers:latest-cpp-cuda12.9-*` |

VS Code will prompt you to choose when opening the workspace. The `CUDAARCHS` environment variable defaults to `RAPIDS`, which expands to all RAPIDS-supported architectures.

## Build Outputs

| Build | Location | Contents |
|-------|----------|----------|
| FB deps | `/opt/fb-deps/` | folly, fbthrift, proxygen (auto-built by `build-presto`) |
| Velox | `/opt/velox-build/release/` | Mono shared library, tests |
| Presto | `/opt/presto-build/` | `presto_server` binary, tests |

Build directories are under `/opt/` so they don't pollute mounted source trees and persist across container sessions (unless the container is recreated).

## Dependency Resolution

Velox has many dependencies. Each can be `SYSTEM` (pre-installed) or `BUNDLED` (built from source via FetchContent). The build scripts set these automatically:

| Dependency | Velox Build | Presto Build | Source |
|------------|------------|--------------|--------|
| folly | BUNDLED | SYSTEM | Built from source / `/opt/fb-deps` |
| xsimd | BUNDLED | SYSTEM | Built from source / `/opt/fb-deps` |
| cudf | SYSTEM | SYSTEM | RAPIDS build |
| rmm | SYSTEM | SYSTEM | RAPIDS build |
| ucxx | SYSTEM | SYSTEM | RAPIDS build |
| kvikio | SYSTEM | SYSTEM | RAPIDS build |
| Arrow | BUNDLED | BUNDLED | Built from source |
| DuckDB | BUNDLED | BUNDLED | Built from source |
| GTest | BUNDLED | BUNDLED | Built from source |
| simdjson | BUNDLED | BUNDLED | Built from source |
| geos | BUNDLED | BUNDLED | Built from source |

## Workarounds

The build scripts include several workarounds for toolchain issues:

- **GCC 14** instead of GCC 13: avoids false-positive `-Wstringop-overflow` in system `fmt` v9.
- **`-DCMAKE_CXX_SCAN_FOR_MODULES=OFF`**: CMake 4.x + GCC 14 + Ninja triggers `-fmodules-ts` which causes GCC 14 ICE (segfault).
- **`-no-pie` linker flag** (presto only): fbthrift static archives have construction vtables with hidden visibility from virtual inheritance in `apache::thrift` exception classes. The linker cannot resolve `R_X86_64_PC32` relocations against hidden symbols in PIE executables.
- **`-Wno-error=nonnull`** (presto only): presto's `SystemConnector.cpp` triggers a false-positive `this` null check warning.

## Scripts

All scripts live in `scripts/devcontainer/` and are installed to `/usr/local/bin/` in the container image. They share common functions via `_common.sh`.

```
scripts/devcontainer/
├── _common.sh         # Shared constants (CUDA archs, RAPIDS detection)
├── build-velox        # Standalone velox build (all deps bundled)
├── build-presto       # Presto + velox build (includes FB deps)
├── configure-velox    # CMake configure only
├── test-velox         # Run velox tests
├── test-presto        # Run presto tests
├── clean-velox        # Clean velox build dir
├── clean-presto       # Clean presto build dir
└── post-create        # Devcontainer post-create hook
```
