# Velox + Presto Devcontainer

A GPU-ready development environment for building [Velox](https://github.com/facebookincubator/velox) with cuDF acceleration and [Presto Native Execution](https://github.com/prestodb/presto/tree/master/presto-native-execution). Based on the RAPIDS devcontainer image with pre-built cuDF, RMM, and KvikIO.

## Quick Start

```bash
# 1. Clone sibling repos under the same parent directory
mkdir ~/code && cd ~/code
git clone https://github.com/<org>/velox-testing.git
git clone https://github.com/facebookincubator/velox.git
git clone https://github.com/prestodb/presto.git
git clone https://github.com/rapidsai/rmm.git
git clone https://github.com/rapidsai/cudf.git
git clone https://github.com/rapidsai/kvikio.git

# 2. Open in VS Code (or any devcontainer-compatible editor)
code velox-testing

# 3. Reopen in container (pick CUDA 13.1 or 12.9 variant)

# 4. Build
build-velox            # ~25 min — standalone velox with cuDF
build-presto           # ~40 min first run (builds FB deps), ~25 min after

# 5. Test
test-velox             # run velox test suite
test-presto            # run presto test suite
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
└── kvikio/            # rapidsai/kvikio
```

All repos are bind-mounted into the container under `/home/coder/`.

## Build Architecture

```
  ┌───────────────┐           ┌───────────────────────────────┐
  │  build-velox  │           │         build-presto          │
  │               │           │                               │
  │ ~/velox src   │           │  1. FB deps (auto, once)      │
  │ all BUNDLED   │           │     folly, fbthrift, proxygen │
  │ MONO_LIB=ON  │           │     → /opt/fb-deps            │
  │ SHARED=ON    │           │                               │
  │               │           │  2. rsync ~/velox → submodule │
  │               │           │     add_subdirectory(velox)   │
  │               │           │     MONO_LIB=OFF              │
  └───────┬───────┘           └──────────────┬────────────────┘
          │                                  │
          ▼                                  ▼
  /opt/velox-build/              /opt/presto-build/
     release/                       presto_server
     3879 targets                   1978 targets
```

Both builds consume pre-built **cudf**, **rmm**, and **kvikio** from the RAPIDS devcontainer.

**Standalone velox** bundles all its dependencies (folly, xsimd, Arrow, etc.) — no prerequisite steps.

**Presto** requires Facebook's OSS stack (folly, fbthrift, proxygen, etc.) for its thrift RPC layer. `build-presto` builds these automatically on first run and caches them at `/opt/fb-deps`. Subsequent runs skip this step unless `--rebuild-deps` is passed.

### Why velox builds twice

Presto integrates velox via `add_subdirectory()` with different options (`VELOX_MONO_LIBRARY=OFF`, no testing, etc.). The rsync in `build-presto` copies `~/velox` into presto's git submodule directory so both repos share the same source while keeping presto's git state clean.

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
| cudf | SYSTEM | SYSTEM | RAPIDS pre-built |
| rmm | SYSTEM | SYSTEM | RAPIDS pre-built |
| kvikio | SYSTEM | SYSTEM | RAPIDS pre-built |
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
