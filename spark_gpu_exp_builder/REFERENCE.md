# Reference

Detailed documentation for the Spark GPU Experimental Builder.
For a quick start, see [README.md](README.md).

---

## Architecture

```text
                          ┌────────────────────────────────┐
                          │       ggbuild build            │
                          │      (docker-build.sh)         │
                          │                                │
                          │  Config → Target → Exec/Print  │
                          └──┬─────────────────────────┬───┘
                             │                         │
                 prebuild /  │                         │  runtime / libs
                 all target  │                         │
                             ▼                         ▼
                      docker build            ┌────────────────────┐
                      (prebuild image)        │     builder.sh     │
                                              │ (7-step pipeline)  │
                                              │ --mode=docker|direct│
                                              └┬──┬──┬──┬──┬──┬──┬┘
                                               │  │  │  │  │  │  │
                             ┌─────────────────┘  │  │  │  │  │  └──────────┐
                             ▼                    ▼  ▼  ▼  ▼  ▼             ▼
                        [1] Container       [3] Arrow [4] Velox [5] Gluten  [7] Collect
                        or env setup        C++ + Java  (cmake)  C++ + JVM  deploy libs
```

### Design principles

1. **Self-contained** — Build scripts call cmake/Maven directly. `gluten/dev/`
   is never invoked. Experimental Velox forks can be built without matching
   upstream Gluten tooling.

2. **Pre-built cuDF with commit verification** — The prebuild image ships
   pre-compiled cuDF to `/usr/local` (saves 1-2h). At build time, the cudf
   commit in the image is verified against Velox's CMake pin to prevent ABI
   mismatches.

3. **Portable deployment** — All ~35 native .so files are collected into a flat
   `libs/` directory with RPATH patched to `$ORIGIN`. Copy the directory to any
   Linux with a compatible GPU driver.

4. **Same builder for workstation and CI** — Docker mode (workstation) or Direct
   mode (k8s pod) with identical logic. No Docker-in-Docker required for CI.

5. **Incremental builds** — Per-step `--skip_*` and `--rebuild_*` flags. Arrow
   auto-detected.

6. **Resolved config on every run** — A `resolved_config.xml` is written to the
   output directory before each build for auditing and reuse.

### Image layering

```text
apache/gluten:centos-9-jdk8-cudf          ← upstream base (CUDA, GCC 14, JDK 8)
  └─► prebuild image                      ← Arrow + Maven cache + cuDF + protobuf + patchelf
        ├─► builder.sh (docker mode)      ← developer workstation path
        └─► runtime image                 ← CI / deployment path
              └── Spark + Python + ENV vars
```

---

## Configuration

### Three config input modes

#### 1. Interactive mode (default when TTY is available)

Environment variables are detected and shown for confirmation. Missing required
values are prompted for. CUDA architecture offers a selection menu.

```bash
export GLUTEN_DIR=/path/to/gluten
export VELOX_DIR=/path/to/velox
export BUILD_IMG=gluten-builder:prebuild

./ggbuild build
#   GLUTEN_DIR from env: /path/to/gluten
#   Confirm? [Y/n]: ...
```

#### 2. CLI flags (non-interactive)

All config values passed explicitly. No prompts.

```bash
./ggbuild build \
  --gluten_dir=/path/to/gluten --velox_dir=/path/to/velox \
  --prebuild_image=gluten:prebuild --cuda_arch=80
```

Environment variables (`GLUTEN_DIR`, `VELOX_DIR`, `BUILD_IMG`, `MVN_SET`) are
still read as fallbacks when CLI flags are not provided.

#### 3. Config file mode (sole source of truth)

When `--config=PATH` is set, the XML file is the **only** config source.
Environment aliases are **not** read — use `${VAR}` syntax in the XML to
reference environment variables (expanded at read time).

```bash
./ggbuild config --output=my-config.xml   # generate template
# edit my-config.xml
./ggbuild build --config=my-config.xml    # use it
```

Example XML with `${VAR}` references:

```xml
<builder>
  <sources>
    <gluten_dir>${GLUTEN_DIR}</gluten_dir>
    <velox_dir>${VELOX_DIR}</velox_dir>
  </sources>
  <docker>
    <prebuild_image>${BUILD_IMG}</prebuild_image>
    <build_output>/tmp/my-libs</build_output>
  </docker>
</builder>
```

If a `${VAR}` reference cannot be resolved, a warning is printed to stderr.

### Config priority (when not in config file mode)

1. CLI flags (highest)
2. Environment variables (via aliases: `BUILD_IMG`, `MVN_SET`, etc.)
3. Script defaults from `config_def.json` (lowest)

### Config definition (`config_def.json`)

All config entries are defined in `scripts/config_def.json` — the single source
of truth. Each entry specifies: shell variable name, default, required flag,
environment alias, description, and env-ref template default.

Consumed by:
- `parse-config.py` — reads JSON natively for XML read/write
- `config_def.sh` — thin shell bridge that emits `CONFIG_DEF_TABLE` + helper
  functions via `python3 parse-config.py shell-helpers`

### Resolved config output

Every `ggbuild build` run writes a `resolved_config.xml` to the output directory
with all resolved values. This file can be reused with `--config` for
reproducible builds.

---

## `ggbuild build` options

### Execution modes

| Mode | Flag | Description |
| ---- | ---- | ----------- |
| **run** (default) | `--mode=run` | Execute the build |
| **print** | `--mode=print` | Print docker/build commands to stdout without executing |

### Build targets

| Target | Flag | Description |
| ------ | ---- | ----------- |
| **runtime** (default) | `--target=runtime` | Build libs or runtime image (depends on `--build_output`) |
| **prebuild** | `--target=prebuild` | Build prebuild Docker image only |
| **all** | `--target=all` | Build prebuild + runtime/libs |

### Build output detection

The `--build_output` flag determines what gets built for the `runtime` and `all`
targets:

| Value | Behavior |
| ----- | -------- |
| Docker tag (e.g. `gluten:runtime`) | Build a runtime Docker image |
| Filesystem path (starts with `/`, `./`, `../`) | Build libs to that directory |
| *(unset)* | Build libs to `target/libs_<epoch>` |

### Libs run mode

When building libs, `--run_mode` controls how `builder.sh` is invoked:

| Mode | Flag | Description |
| ---- | ---- | ----------- |
| **direct** (default) | `--run_mode=direct` | `docker-build.sh` creates `docker run/exec`; builder.sh runs `--mode=direct` inside |
| **docker** | `--run_mode=docker` | `builder.sh --mode=docker` manages its own container |

### Full option reference

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--config=PATH` | *(none)* | XML config file (sole source when set) [env: `CFG_FILE`] |
| `--prebuild_image=TAG` | *(required for builds)* | Prebuild image tag [env: `BUILD_IMG`] |
| `--build_output=TAG\|PATH` | *(auto libs dir)* | Runtime image tag or libs output path |
| `--target=prebuild\|runtime\|all` | `runtime` | What to build |
| `--gluten_dir=PATH` | *(required)* | Gluten source tree [env: `GLUTEN_DIR`] |
| `--velox_dir=PATH` | *(required)* | Velox source tree [env: `VELOX_DIR`] |
| `--base_image=IMAGE` | `apache/gluten:centos-9-jdk8-cudf` | Base image for prebuild |
| `--spark_version=VER` | `3.5` | Spark major.minor |
| `--spark_full_version=VER` | `3.5.5` | Full Spark version |
| `--arrow_version=VER` | `15.0.0` | Arrow version |
| `--cuda_arch=ARCH` | *(prompted or auto-detected)* | CUDA architectures (semicolon-separated) |
| `--enable_hdfs=ON\|OFF` | `ON` | Build HDFS connector |
| `--enable_s3=ON\|OFF` | `OFF` | Build S3 connector |
| `--maven_settings=PATH` | *(none)* | Custom Maven settings.xml [env: `MVN_SET`] |
| `--container=NAME` | *(none)* | Reuse existing named container |
| `--run_mode=direct\|docker` | `direct` | Libs execution mode |
| `--mode=run\|print` | `run` | Execute or print commands |
| `--log_file=PATH` | *(none)* | Build log file (implies `--mode=run`) |
| `--no_cache` | `false` | Docker `--no-cache` |
| `--extra=ARG` | *(none)* | Extra docker build arg (repeatable) |

---

## `builder.sh` (low-level)

The 7-step build pipeline. Normally invoked by `ggbuild build`, but can be
called directly inside a prebuild container for debugging.

### Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--gluten_dir=PATH` | *required* | Gluten source tree |
| `--velox_dir=PATH` | *required* | Velox source tree |
| `--mode=docker\|direct` | `direct` | `direct` = current shell; `docker` = manage container |
| `--image=IMAGE` | *(none)* | Prebuild image (docker mode only) |
| `--container=NAME` | *(none)* | Reuse running container (docker mode) |
| `--cuda_arch=ARCH` | *(auto-detect)* | CUDA architectures |
| `--spark_version=VER` | `3.5` | Spark version |
| `--enable_hdfs=ON\|OFF` | `ON` | HDFS connector |
| `--enable_s3=ON\|OFF` | `OFF` | S3 connector |
| `--build_cudf` | off | Build cuDF from source (BUNDLED) |
| `--build_arrow` | off | Force Arrow build |
| `--skip_build_native` | off | Skip Velox + Gluten C++ |
| `--skip_velox` | off | Skip Velox only |
| `--rebuild_velox` | off | Clean + rebuild Velox |
| `--rebuild_gluten_cpp` | off | Clean + rebuild Gluten C++ |
| `--ignore_version_check` | off | Skip cudf commit check |
| `--output_dir=PATH` | `target/build_<epoch>` | Output directory |

### Build pipeline (7 steps)

```text
Step 1:   Start container (docker) / verify environment (direct)
Step 2:   Verify GPU access (nvidia-smi)
Step 2.5: Verify cudf commit consistency (SYSTEM mode only)
Step 3:   Build Arrow C++ + Java JNI           [auto-detect / --build_arrow]
Step 4:   Build Velox (GPU-only, cmake)        [--skip_velox]
Step 5:   Build Gluten C++ (direct cmake)      [--skip_build_native]
Step 6:   Build Gluten Maven (bundle JAR)
Step 7:   Collect deploy libraries → output_dir
```

### Build stage durations

| Stage | SYSTEM (default) | BUNDLED (`--build_cudf`) | Skip flag |
| ----- | ---------------- | ----------------------- | --------- |
| Arrow C++ + Java JNI | ~20-30 min | ~20-30 min | auto-skip if pre-installed |
| Velox | ~30-60 min | ~90-150 min (includes cuDF) | `--skip_velox` |
| Gluten C++ | ~10 min | ~10 min | `--skip_build_native` |
| Gluten Maven | ~5 min | ~5 min | — |
| Collect deploy libs | ~1 min | ~1 min | — |

---

## Incremental builds

Pass step-control flags via `ggbuild build` — they are forwarded to `builder.sh`:

```bash
# Skip Velox, rebuild Gluten C++ only
./ggbuild build --config=my-config.xml --skip_velox --rebuild_gluten_cpp

# Skip all C++ (Gluten Java only)
./ggbuild build --config=my-config.xml --skip_build_native
```

Or edit the `<steps>` section in the config XML:

```xml
<steps>
  <skip_velox>true</skip_velox>
  <rebuild_gluten_cpp>true</rebuild_gluten_cpp>
</steps>
```

### cudf commit consistency check (step 2.5)

When using `cudf_SOURCE=SYSTEM` (default), the builder verifies the cudf commit
in the Docker image (`/home/cudf-version-info`) against Velox's
`CMake/resolve_dependency_modules/cudf.cmake`. On mismatch:

1. Rebuild the prebuild image with the current Velox checkout
2. Use `--build_cudf` to build from source (slower)
3. Check out the matching Velox revision
4. Use `--ignore_version_check` to skip (at your own risk)

---

## Docker prebuild image

All build dependencies pre-compiled into a single image:

```text
base image (centos-9-jdk8-cudf)
  └─► prebuild image
        ├── Arrow C++ (static) + Arrow Java + Maven cache
        ├── libcudf + rmm + kvikio (pre-compiled)
        ├── protobuf 3.21.8 (shared lib)
        └── patchelf
```

### Building

```bash
./ggbuild build --target=prebuild \
  --prebuild_image=gluten:prebuild \
  --gluten_dir=$GLUTEN_DIR --velox_dir=$VELOX_DIR
```

### `build-cudf-standalone.cmake`

A thin CMake project that builds only libcudf + RAPIDS stack (rapids-cmake, rmm,
kvikio, cudf) **without configuring the full Velox project**. Re-uses
`ResolveDependency.cmake` and `cudf.cmake` from Velox's `CMake/` directory.
Saves ~1-2 hours per downstream build via `cudf_SOURCE=SYSTEM`.

---

## Runtime image

Self-contained image with everything needed to run Spark + Gluten GPU:

```text
prebuild image
  └─► runtime image
        ├── Gluten bundle JAR + native libs (built by builder.sh)
        ├── Apache Spark 3.5.x
        ├── Python 3 + pyspark
        ├── JDK 8 (from base)
        └── spark-defaults.conf pre-configured for Gluten GPU
```

### Building

```bash
# Full pipeline (prebuild + runtime)
./ggbuild build --target=all \
  --prebuild_image=gluten:prebuild --build_output=gluten:runtime \
  --gluten_dir=$GLUTEN_DIR --velox_dir=$VELOX_DIR

# Runtime only (prebuild image must already exist)
./ggbuild build --target=runtime \
  --prebuild_image=gluten:prebuild --build_output=gluten:runtime \
  --gluten_dir=$GLUTEN_DIR --velox_dir=$VELOX_DIR
```

### Pre-configured environment

| Variable | Value |
| -------- | ----- |
| `SPARK_HOME` | `/opt/spark` |
| `JAVA_HOME` | `/usr/lib/jvm/java-*-openjdk*` |
| `GLUTEN_DEPLOY_DIR` | `/opt/gluten-deploy` |
| `GPU_LIBS` | `/opt/gluten-deploy/libs` |
| `LD_LIBRARY_PATH` | `/opt/gluten-deploy/libs:...` |
| `PATH` | `$SPARK_HOME/bin:...` |

### Default Spark configuration

```properties
spark.plugins                                  org.apache.gluten.GlutenPlugin
spark.jars                                     /opt/gluten-deploy/*.jar
spark.driver.extraLibraryPath                  /opt/gluten-deploy/libs
spark.executor.extraLibraryPath                /opt/gluten-deploy/libs
spark.memory.offHeap.enabled                   true
spark.memory.offHeap.size                      20g
spark.gluten.sql.columnar.cudf                 true
spark.shuffle.manager                          org.apache.spark.shuffle.sort.ColumnarShuffleManager
spark.gluten.sql.columnar.forceShuffledHashJoin true
```

Override via `--conf` at `spark-submit` time or mount a custom
`spark-defaults.conf`.

---

## Other tools

### `ggbuild check-deploy`

Validate deploy artifacts (bundle JAR + shared libraries).

```bash
./ggbuild check-deploy --output-dir=target/libs_XXXX
./ggbuild check-deploy --image=gluten:runtime
```

Checks: bundle JAR exists, required `.so` files present, RPATH set correctly,
all `ldd` dependencies resolved.

### `ggbuild clean`

Clean build caches while preserving clangd/intellisense data.

```bash
./ggbuild clean --gluten_dir=$GLUTEN_DIR --velox_dir=$VELOX_DIR
./ggbuild clean --gluten_dir=$GLUTEN_DIR --dry-run
```

### `ggbuild config`

Generate a configuration file template.

```bash
./ggbuild config --output=my-config.xml
./ggbuild config   # print to stdout
```

### `ggbuild extract-libs`

Extract deploy libraries from a runtime Docker image.

```bash
./ggbuild extract-libs --image=gluten:runtime --output_dir=./deploy
```

---

## Builder scripts

Each build step is a standalone script in `scripts/`. Run manually for
debugging inside a prebuild container:

| Script | Purpose | Key env vars |
| ------ | ------- | ------------ |
| `build-helper-functions.sh` | Shared utilities | — |
| `build-arrow.sh` | Arrow C++ (static) + Java JNI | `GLUTEN_DIR` |
| `build-velox.sh` | Velox with GPU/cuDF | `VELOX_HOME`, `CUDA_ARCH`, `CUDF_SOURCE` |
| `build-gluten-cpp.sh` | Gluten C++ (direct cmake) | `GLUTEN_DIR`, `VELOX_HOME`, `ENABLE_HDFS`, `ENABLE_S3` |
| `build-gluten-jvm.sh` | Gluten Maven bundle JAR | `GLUTEN_DIR`, `SPARK_VERSION` |
| `collect-deploy-libs.sh` | Library collection + RPATH | `GLUTEN_DIR`, `DEPLOY_DIR` |

---

## Hardcoded build settings

| Setting | Value | Rationale |
| ------- | ----- | --------- |
| `ENABLE_GPU` | `ON` | GPU-only builder |
| `ENABLE_GCS` | `OFF` | Not needed |
| `ENABLE_ABFS` | `OFF` | Not needed |
| `ENABLE_QAT` | `OFF` | Intel-only |
| `ENABLE_VCPKG` | `OFF` | Unnecessary complexity |
| `BUILD_TESTS` | `OFF` | Faster builds |
| `BUILD_BENCHMARKS` | `OFF` | Faster builds |
| `CMAKE_BUILD_TYPE` | `Release` | Production |
| `VELOX_MONO_LIBRARY` | `ON` | Single libvelox.so |

---

## Troubleshooting

### `nvidia-smi failed inside container`

`nvidia-container-toolkit` not configured or driver incompatible.
Verify: `docker run --rm --gpus all nvidia/cuda:13.1.1-base-ubuntu24.04 nvidia-smi`

### Arrow build fails on first run

Arrow must be built before Velox. Auto-detected and built if missing.
Force with `--build_arrow`.

### ABI mismatch after partial rebuild

Skip Velox but rebuild Gluten C++ → linker errors. Fix: drop `--skip_velox`.

### cudf commit mismatch

Step 2.5 halts. Options: rebuild prebuild image, `--build_cudf`, pin Velox, or
`--ignore_version_check`.

### maven-shade-plugin ASM error

`build-gluten-jvm.sh` patches shade plugin 3.6.x → 3.2.4 automatically.

### `ldd` shows "not found" in deploy output

Missing .so in collection script. Add to the appropriate array in
`collect-deploy-libs.sh`.

### `${VAR}` not resolved in config file

The environment variable is not set. Set it before running `ggbuild build`, or
replace the `${VAR}` reference with a literal value in the XML.
