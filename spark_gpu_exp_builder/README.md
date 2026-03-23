# Spark GPU Experimental Builder
f
Self-contained build pipeline: direct cmake/Maven calls, no dependency on
`gluten/dev/` scripts. Build output is a relocatable directory (bundle JAR +
RPATH-patched native libs) — compile once, run on any Linux.

See [REFERENCE.md](REFERENCE.md) for architecture, all options, and detailed docs.

---

## Prerequisites

- Docker with `nvidia-container-toolkit`
- NVIDIA driver compatible with CUDA 13.1+
- Velox and Gluten repos checked out locally
- Python 3 (standard library only — no pip packages)

---

## Quick start

```bash
cd velox-testing/spark_gpu_exp_builder
```

### Generate a config template

```bash
# Print to stdout
./ggbuild config

# Write to file (edit, then use with --config)
./ggbuild config --output=my-config.xml
```

The generated template uses `${VAR}` references (e.g. `${GLUTEN_DIR}`,
`${BUILD_IMG}`) that are expanded from the environment at read time.

### Build libs (interactive mode)

```bash
# Set env vars — ggbuild detects them and asks for confirmation
export GLUTEN_DIR=/path/to/gluten
export VELOX_DIR=/path/to/velox
export BUILD_IMG=gluten-builder:prebuild

./ggbuild build
```

### Build libs (config file mode)

```bash
./ggbuild build --config=my-config.xml
```

### Build Docker images

```bash
# Prebuild image only
./ggbuild build --target=prebuild \
  --prebuild_image=gluten:prebuild

# Full pipeline (prebuild + runtime image)
./ggbuild build --target=all \
  --prebuild_image=gluten:prebuild --build_output=gluten:runtime

# Build libs to a directory
./ggbuild build --target=runtime \
  --prebuild_image=gluten:prebuild --build_output=/tmp/my-libs
```

### Print commands without executing

```bash
./ggbuild build --mode=print --prebuild_image=gluten:prebuild
```

---

## Output

```
target/libs_<timestamp>/
  resolved_config.xml           # auto-generated resolved config
  gluten-velox-bundle-spark3.5_2.12-*.jar
  libs/
    libgluten.so, libvelox.so, libcudf.so, librmm.so,
    libcudart.so.*, libprotobuf.so.32, ...  (~35 files)
```

### Deploy

```bash
export GPU_LIBS=/path/to/target/libs_XXXX/libs
export LD_LIBRARY_PATH=$GPU_LIBS:${LD_LIBRARY_PATH:-}

spark-submit \
  --jars /path/to/target/libs_XXXX/gluten-velox-bundle-*.jar \
  --conf spark.executor.extraLibraryPath=$GPU_LIBS \
  --conf spark.driver.extraLibraryPath=$GPU_LIBS \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.gluten.sql.columnar.cudf=true \
  your-app.py
```

### Deploy via runtime image

```bash
docker run --rm -it --gpus all gluten:runtime spark-shell

# With custom build output (replace entire deploy dir)
docker run --rm --gpus all \
  -v /my/build:/opt/gluten-deploy \
  gluten:runtime spark-submit /data/app.py
```

---

### Other tools

```bash
# Validate deploy artifacts
./ggbuild check-deploy --output-dir=target/libs_XXXX

# Extract deploy libs from a runtime Docker image
./ggbuild extract-libs --image=gluten:runtime --output_dir=./deploy

# Clean build caches
./ggbuild clean --gluten_dir=$GLUTEN_DIR --velox_dir=$VELOX_DIR
```
