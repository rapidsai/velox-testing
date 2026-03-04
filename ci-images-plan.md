# CI Image Publishing Pipeline Plan

## Goal

Replace the trivial `ci-images.yml` workflow with a comprehensive image-building pipeline that publishes pre-built Velox and Presto container images to `ghcr.io/rapidsai/velox-testing-images`. These images will be multi-arch (amd64/arm64), support CUDA 12.9 and CUDA 13.1, and include both CPU-only and GPU (cuDF-enabled) build variants. All compilation uses sccache for distributed build caching.

## Background

### Current State

- `ci-images.yml` builds a trivial Dockerfile (`docker/presto-deps/Dockerfile`) that is just `nvidia/cuda:...-base-...` with no actual build content.
- Velox builds happen in `velox-test.yml` using `build_velox.sh`, which runs `docker compose build` with bind mounts to a local Velox checkout. These produce local images only — nothing is published.
- Presto native builds happen in `presto-test-composite.yml`, which builds a `presto/prestissimo-dependency:centos9` deps image locally, then builds the native worker on top. Again, nothing is published.
- The upstream Velox project publishes `ghcr.io/facebookincubator/velox-dev:adapters` (CentOS 9 + all Velox build dependencies), but only for the default CUDA version (12.9).

### Why This Matters

Every CI run currently rebuilds everything from scratch. Pre-built images eliminate redundant compilation, speed up CI, and provide reproducible build artifacts. Supporting CUDA 12 and CUDA 13 requires building our own dependency images (upstream only publishes one CUDA version).

### Upstream PRs Enabling CUDA Version Configuration

- **Velox:** [facebookincubator/velox#16234](https://github.com/facebookincubator/velox/pull/16234) — Added `CUDA_VERSION` build arg to the `adapters` stage in `centos-multi.dockerfile`. Defaults to `12.9`. Merged.
- **Presto:** [prestodb/presto#27074](https://github.com/prestodb/presto/pull/27074) — Added `CUDA_VERSION` build arg to `centos-dependency.dockerfile`. Defaults to `12.9`.

Both Dockerfiles accept `CUDA_VERSION` as a build arg, allowing us to build dependency images for any CUDA version.

## Image Dependency Chain

```
Layer 0 (deps, changes infrequently):
  centos-multi.dockerfile "adapters" stage (from upstream Velox)
    → "Velox deps image" (CentOS 9 + CUDA + all build libraries)

  centos-dependency.dockerfile (from upstream Presto)
    → "Presto deps image" (CentOS 9 + CUDA + Presto build libraries)

Layer 1 (built, changes with every commit):
  Velox deps image
    ├─ + Velox source + compile → "Velox built image (GPU)"
    └─ + Velox source + compile (no cuDF) → "Velox built image (CPU)"

  Presto deps image
    ├─ + Presto/Velox source + compile → "Presto native worker (GPU)"
    └─ + Presto/Velox source + compile (no cuDF) → "Presto native worker (CPU)"
```

## Image Matrix

All images are published under the single GitHub package `ghcr.io/rapidsai/velox-testing-images` using tags to differentiate.

| Image | CUDA Versions | Architectures | Tag Format |
|---|---|---|---|
| Velox deps | cu12, cu13 | amd64, arm64 | `velox-deps-{vsha}-cu{12,13}-{date}-{arch}` |
| Velox built GPU | cu12, cu13 | amd64, arm64 | `velox-{vsha}-gpu-cu{12,13}-{date}-{arch}` |
| Velox built CPU | (default) | amd64, arm64 | `velox-{vsha}-cpu-{date}-{arch}` |
| Presto deps | cu12, cu13 | amd64, arm64 | `presto-deps-{psha}-velox-{vsha}-cu{12,13}-{date}-{arch}` |
| Presto native GPU | cu12, cu13 | amd64, arm64 | `presto-{psha}-velox-{vsha}-gpu-cu{12,13}-{date}-{arch}` |
| Presto native CPU | (default) | amd64, arm64 | `presto-{psha}-velox-{vsha}-cpu-{date}-{arch}` |

After per-arch builds complete, multi-arch manifests are created (without the `-{arch}` suffix) that point to both the amd64 and arm64 images.

**Total: 20 per-arch image builds + 10 multi-arch manifest merges.**

### Tag Details

- `{vsha}` is the first 7 characters of the resolved Velox git commit SHA (never `main` or `master`).
- `{psha}` is the first 7 characters of the resolved Presto git commit SHA (never `master`).
- `{date}` is the build date in `YYYYMMDD` format.
- `{arch}` is `amd64` or `arm64` (only in per-arch tags; the multi-arch manifest tags omit this).
- CPU images do not include a CUDA version in the tag.
- GPU images always include the CUDA version (`cu12` or `cu13`).

### CUDA Version Strings

| Short Name | `CUDA_VERSION` Build Arg | Notes |
|---|---|---|
| cu12 | `12.9` | Matches upstream Velox/Presto default |
| cu13 | `13.1` | CUDA 13.1 (current trivial Dockerfile uses `13.1.1` for the nvidia/cuda base image) |

## Workflow Inputs

```yaml
workflow_dispatch:
  inputs:
    velox_repository:
      description: 'Velox repository'
      type: string
      default: 'facebookincubator/velox'
    velox_commit:
      description: 'Velox commit SHA or branch'
      type: string
      default: 'main'
    presto_repository:
      description: 'Presto repository'
      type: string
      default: 'prestodb/presto'
    presto_commit:
      description: 'Presto commit SHA or branch'
      type: string
      default: 'master'
```

On scheduled (cron) runs, inputs default to `main`/`master`. The actual commit SHA is resolved via `git rev-parse HEAD` after checkout and used in image tags (the tag never says `main` or `master`).

**Required permissions** (for OIDC-based sccache, ghcr.io push, and attestations):
```yaml
permissions:
  id-token: write        # OIDC for sccache AWS credentials
  contents: read
  packages: write        # push to ghcr.io
  attestations: write    # build provenance
  artifact-metadata: write
```

## Workflow Jobs

### Job 1: `velox-deps`

**Purpose:** Build the Velox dependency base image (equivalent to `ghcr.io/facebookincubator/velox-dev:adapters`) for each CUDA version and architecture.

**Matrix:** `cuda_version: [12, 13]` × `arch: [amd64, arm64]` = 4 builds

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout Velox at specified commit (needed for `centos-multi.dockerfile` and setup scripts)
2. Checkout velox-testing (for workflow scripts)
3. Resolve Velox commit SHA
4. Compute content hash of Dockerfile + setup scripts + CUDA version (for cache key)
5. Log in to ghcr.io
6. Build using `docker/build-push-action` with:
   - `file: velox/scripts/docker/centos-multi.dockerfile` (the upstream Dockerfile)
   - `target: adapters`
   - `build-args: CUDA_VERSION={version}` (e.g., `12.9` or `13.1`)
   - `context: velox/` (the Velox checkout)
   - Registry-based cache (`cache-from`/`cache-to` using content hash)
7. Push to `ghcr.io/rapidsai/velox-testing-images:velox-deps-{vsha}-cu{12,13}-{date}-{arch}`

**Caching:** These images change infrequently (only when dependency versions or setup scripts change). Content-hash based caching means rebuilds are skipped when nothing changed. The cache key is derived from a hash of the Dockerfile, setup scripts, and CUDA version argument.

### Job 2: `velox-build`

**Purpose:** Build Velox from source and publish the image.

**Depends on:** `velox-deps`

**Matrix:**
- GPU builds: `cuda_version: [12, 13]` × `arch: [amd64, arm64]` = 4 builds
- CPU builds: `arch: [amd64, arm64]` = 2 builds
- Total: 6 builds

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout Velox + velox-testing
2. Resolve Velox commit SHA
3. Log in to ghcr.io
4. Setup sccache via `./velox-testing/.github/actions/sccache-setup` (OIDC → AWS credentials)
5. Build using `docker/build-push-action` with a new Dockerfile (`docker/velox-build/Dockerfile`) that:
   - `FROM` our velox-deps image (from Job 1)
   - `COPY`s the Velox source tree into the image
   - Runs the cmake + make build (adapted from `adapters_build.dockerfile`)
   - Receives sccache secrets via `--mount=type=secret` (see [sccache section](#sccache-integration))
   - For GPU: `BUILD_WITH_VELOX_ENABLE_CUDF=ON`
   - For CPU: `BUILD_WITH_VELOX_ENABLE_CUDF=OFF`
6. Push with appropriate tag

**Tags:**
- GPU: `velox-{vsha}-gpu-cu{12,13}-{date}-{arch}`
- CPU: `velox-{vsha}-cpu-{date}-{arch}`

### Job 3: `presto-deps`

**Purpose:** Build the Presto dependency base image (equivalent to `presto/prestissimo-dependency:centos9`) for each CUDA version and architecture.

**Matrix:** `cuda_version: [12, 13]` × `arch: [amd64, arm64]` = 4 builds

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout Presto, Velox, and velox-testing
2. Resolve Presto and Velox commit SHAs
3. The Presto deps image build requires Velox scripts to be overlaid into the Presto source tree (the existing `build_centos_deps_image.sh` copies `velox/scripts` and `velox/CMake` into `presto/presto-native-execution/velox/`). The new Dockerfile must handle this in its build context or with a multi-stage approach.
4. Build using `docker/build-push-action` with:
   - A new Dockerfile (`docker/presto-deps/Dockerfile`, replacing the current trivial one) based on upstream `centos-dependency.dockerfile`
   - `build-args: CUDA_VERSION={version}` (e.g., `12.9` or `13.1`)
   - Build context that includes both Presto and Velox sources
   - Registry-based caching (content hash of Dockerfile + setup scripts + CUDA version)
5. Push to `ghcr.io/rapidsai/velox-testing-images:presto-deps-{psha}-velox-{vsha}-cu{12,13}-{date}-{arch}`

**Caching:** Same content-hash strategy as velox-deps.

### Job 4: `presto-build`

**Purpose:** Build the Presto native worker binary image.

**Depends on:** `presto-deps`

**Matrix:**
- GPU builds: `cuda_version: [12, 13]` × `arch: [amd64, arm64]` = 4 builds
- CPU builds: `arch: [amd64, arm64]` = 2 builds
- Total: 6 builds

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout Presto, Velox, and velox-testing
2. Resolve Presto + Velox commit SHAs
3. Log in to ghcr.io
4. Setup sccache via `./velox-testing/.github/actions/sccache-setup` (OIDC → AWS credentials)
5. Build using `docker/build-push-action` with a new Dockerfile (`docker/presto-build/Dockerfile`) that:
   - `FROM` our presto-deps image (from Job 3)
   - `COPY`s Presto + Velox sources
   - Compiles Presto native worker (adapted from `native_build.dockerfile`)
   - Receives sccache secrets via `--mount=type=secret` (see [sccache section](#sccache-integration))
   - For GPU: `-DPRESTO_ENABLE_CUDF=ON`
   - For CPU: `-DPRESTO_ENABLE_CUDF=OFF`
6. Push with appropriate tag

**Tags:**
- GPU: `presto-{psha}-velox-{vsha}-gpu-cu{12,13}-{date}-{arch}`
- CPU: `presto-{psha}-velox-{vsha}-cpu-{date}-{arch}`

### Job 5: `merge-manifests`

**Purpose:** Create multi-arch manifests that combine the amd64 and arm64 images under a single tag.

**Depends on:** `velox-deps`, `velox-build`, `presto-deps`, `presto-build`

**Runner:** `linux-amd64-cpu4` (lightweight, just runs docker manifest commands)

**Steps:**
For each logical image (e.g., `velox-{vsha}-gpu-cu13-{date}`):
1. `docker manifest create ghcr.io/.../velox-testing-images:{tag} --amend ghcr.io/.../velox-testing-images:{tag}-amd64 --amend ghcr.io/.../velox-testing-images:{tag}-arm64`
2. `docker manifest push ghcr.io/.../velox-testing-images:{tag}`

This covers all 10 logical image tags (2 velox-deps + 4 velox-build + 2 presto-deps + 4 presto-build... wait, let me recount):
- Velox deps: 2 (cu12, cu13)
- Velox built: 4 GPU (cu12, cu13) + 2 CPU = 6
- Presto deps: 2 (cu12, cu13)
- Presto built: 4 GPU (cu12, cu13) + 2 CPU = 6
- **Total: 16 multi-arch manifests**

### Job 6: `test` (optional)

**Purpose:** Smoke test the published images.

**Depends on:** `merge-manifests`

**Runner:** GPU runners for GPU images, CPU runners for CPU images

**Steps:**
1. Pull the multi-arch image
2. Verify `nvidia-smi` works (GPU images)
3. Run a trivial Velox or Presto command to verify the build artifacts are present

## sccache Integration

All compilation jobs (velox-build, presto-build) must use sccache for build caching. This is essential for performance.

### How sccache Works

The RAPIDS sccache fork (`rapidsai/sccache`) provides S3-backed compilation caching and optional distributed compilation. Key components:

1. **sccache setup script:** `scripts/sccache/sccache_setup.sh` installs the RAPIDS sccache fork from GitHub releases, configures it, and starts the server.

2. **CMake integration:** When sccache is enabled, builds add `-DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache` to the CMake flags.

3. **S3 cache bucket:** `rapids-sccache-devs` in `us-east-2`.

4. **Docker build secrets:** Two secrets are mounted during Docker builds:
   - `github_token` → mounted as env var `SCCACHE_DIST_AUTH_TOKEN` (for distributed compilation auth)
   - `aws_credentials` → mounted at `/root/.aws/credentials` (for S3 cache bucket access)

5. **Distributed compilation:** The sccache fork supports distributing compilation to a build cluster at `https://{arch}.linux.sccache.rapids.nvidia.com`. This can be disabled with `SCCACHE_NO_DIST_COMPILE=1`.

### CI Authentication: OIDC-Based AWS Credentials

The existing `.github/actions/sccache-setup/action.yml` (added in [#228](https://github.com/rapidsai/velox-testing/pull/228)) provides the CI authentication pattern. It does **not** use stored secrets. Instead, it uses GitHub Actions OIDC to assume an AWS IAM role:

```yaml
- name: Setup sccache
  id: sccache
  uses: ./velox-testing/.github/actions/sccache-setup
  with:
    role-to-assume: ${{ vars.AWS_ROLE_ARN }}
    aws-region: ${{ vars.AWS_REGION }}
```

The action:
1. Uses `aws-actions/configure-aws-credentials@v4` with OIDC to assume `${{ vars.AWS_ROLE_ARN }}` (repository variable, not a secret)
2. Creates `~/.sccache-auth/aws_credentials` with the temporary session credentials
3. Creates `~/.sccache-auth/github_token` as a placeholder (distributed compilation is disabled in CI; `SCCACHE_NO_DIST_COMPILE=1`)
4. Exports `SCCACHE_AUTH_DIR` for downstream steps

**Required permissions:** The workflow must declare `id-token: write` and `contents: read` for OIDC to work.

**Required repository variables:** `AWS_ROLE_ARN` and `AWS_REGION` (default `us-east-2`) must be set as repository variables.

### sccache in `docker/build-push-action`

The `docker/build-push-action` supports Docker build secrets via its `secrets` input. After the sccache-setup action creates the auth files, they are passed to the Docker build:

```yaml
- name: Setup sccache
  id: sccache
  uses: ./velox-testing/.github/actions/sccache-setup
  with:
    role-to-assume: ${{ vars.AWS_ROLE_ARN }}
    aws-region: ${{ vars.AWS_REGION }}

- uses: docker/build-push-action@...
  with:
    secrets: |
      github_token=${{ env.SCCACHE_AUTH_DIR }}/github_token
      aws_credentials=${{ env.SCCACHE_AUTH_DIR }}/aws_credentials
    build-args: |
      ENABLE_SCCACHE=ON
      SCCACHE_NO_DIST_COMPILE=1
```

The Dockerfiles reference these secrets via `--mount=type=secret`:
```dockerfile
RUN --mount=type=secret,id=github_token,env=SCCACHE_DIST_AUTH_TOKEN \
    --mount=type=secret,id=aws_credentials,target=/root/.aws/credentials \
    ...build commands...
```

This is the same pattern already used by `adapters_build.dockerfile` and `native_build.dockerfile`. The only difference is that in CI, the auth files come from OIDC (via the sccache-setup action) rather than from interactive `setup_sccache_auth.sh`.

### Reference: velox-test.yml Usage

The `velox-test.yml` workflow (as of [#228](https://github.com/rapidsai/velox-testing/pull/228)) demonstrates the pattern:
1. Runs the `sccache-setup` action to obtain credentials
2. Passes `--sccache` flag to `build_velox.sh`
3. `build_velox.sh` selects the sccache compose file which includes the secret mounts
4. Shows sccache stats after the build

For `ci-images.yml`, we follow the same credential setup but pass the secrets directly to `docker/build-push-action` instead of through `build_velox.sh`.

## New Files

### Dockerfiles

| File | Purpose | Based on |
|---|---|---|
| `docker/velox-build/Dockerfile` | Build Velox from source on top of deps image | `velox/docker/adapters_build.dockerfile` |
| `docker/presto-deps/Dockerfile` | Build Presto deps image (replaces current trivial file) | upstream `presto-native-execution/scripts/dockerfiles/centos-dependency.dockerfile` |
| `docker/presto-build/Dockerfile` | Build Presto native worker on top of deps image | `presto/docker/native_build.dockerfile` |

The Velox deps image does **not** need a new Dockerfile — it uses the upstream `centos-multi.dockerfile` directly from the Velox checkout, targeting the `adapters` stage with `--target adapters`.

### Key Differences from Existing Dockerfiles

The existing `adapters_build.dockerfile` and `native_build.dockerfile` use `--mount=type=bind,source=velox,...` to bind-mount the source tree from the host. This only works with local `docker compose build`. For `docker/build-push-action`, we must use `COPY` to bring sources into the build context. This means:

1. The build context must include the source trees (Velox, Presto, velox-testing as needed)
2. A `.dockerignore` should be added to exclude `.git/`, build artifacts, and other unnecessary files to keep context size reasonable
3. The Dockerfiles will `COPY` the source instead of using bind mounts

## Build Triggers

- **Scheduled:** Weekly on Tuesdays at 4:00 UTC (existing cron), builds against latest `main`/`master`
- **Manual:** `workflow_dispatch` with configurable repository/commit inputs
- **Push:** Remove the `push` trigger before merging (it's marked as TODO in the current file)

## Runner Configuration

| Job | Runner Label |
|---|---|
| velox-deps | `linux-{arch}-cpu16` |
| velox-build | `linux-{arch}-cpu16` |
| presto-deps | `linux-{arch}-cpu16` |
| presto-build | `linux-{arch}-cpu16` |
| merge-manifests | `linux-amd64-cpu4` (lightweight) |
| test (GPU) | `linux-{arch}-gpu-l4-latest-1` |
| test (CPU) | `linux-{arch}-cpu4` |

## Estimated Build Times

| Job | Estimated Time (sccache warm) | Estimated Time (sccache cold) |
|---|---|---|
| velox-deps | ~1 min (Docker layer cached) | ~45 min |
| velox-build | ~10-15 min | ~30 min |
| presto-deps | ~1 min (Docker layer cached) | ~45 min |
| presto-build | ~10-15 min | ~30 min |
| merge-manifests | ~2 min | ~2 min |
| test | ~5 min | ~5 min |

sccache "warm" means prior compilations of similar code are cached in S3. Even on the first run of this workflow, the sccache S3 bucket may already contain cache entries from `velox-test.yml` runs, so builds should benefit immediately.

**Wall clock (warm caches):** ~30 min (deps cached + sccache-accelerated build + merge + test)
**Wall clock (cold, first run):** ~110 min (deps build + cold compile + merge + test)

## Open Items and Risks

1. **Build context size:** `COPY`ing the full Velox source tree (~2 GB with submodules) into the Docker build context may be slow. A `.dockerignore` is essential. Alternatively, we may explore multi-stage builds that `git clone` at a specific SHA to avoid large contexts.

2. **Presto source layout:** The Presto deps build requires Velox scripts overlaid into the Presto directory structure (`presto-native-execution/velox/scripts/`). The Dockerfile or build context must replicate this layout. This can be handled with a multi-stage Dockerfile or by structuring the `docker/build-push-action` context appropriately.

3. **Upstream Velox deps image compatibility:** The `centos-multi.dockerfile` expects to be built from within the Velox repo root as context (it `COPY`s `scripts/setup-centos-adapters.sh`). When building from `ci-images.yml`, the context must be the Velox checkout directory.

4. **sccache OIDC prerequisites:** The `AWS_ROLE_ARN` and `AWS_REGION` repository variables must be configured. The workflow must declare `id-token: write` permission. The sccache-setup action gracefully degrades (builds without sccache) if OIDC fails, but builds will be significantly slower.

5. **Registry cache quotas:** GitHub Container Registry has storage limits. Aggressive caching of deps images across multiple CUDA versions and architectures may consume significant space. Monitor usage after deployment.

6. **CUDA 13.1 compatibility:** CUDA 13.1 is newer than the upstream default (12.9). Verify that the Velox and Presto build systems compile cleanly against CUDA 13.1 with all dependencies.
