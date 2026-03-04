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

Every CI run currently rebuilds everything from scratch. Pre-built images eliminate redundant compilation, speed up CI, and provide reproducible build artifacts. Supporting CUDA 12.9 and CUDA 13.1 requires building our own dependency images (upstream only publishes one CUDA version).

### Upstream PRs Enabling CUDA Version Configuration

- **Velox:** [facebookincubator/velox#16234](https://github.com/facebookincubator/velox/pull/16234) — Added `CUDA_VERSION` build arg to the `adapters` stage in `centos-multi.dockerfile`. Defaults to `12.9`. Merged.
- **Presto:** [prestodb/presto#27074](https://github.com/prestodb/presto/pull/27074) — Added `CUDA_VERSION` build arg to `centos-dependency.dockerfile`. Defaults to `12.9`.

Both Dockerfiles accept `CUDA_VERSION` as a build arg, allowing us to build dependency images for any CUDA version.

## Key Design Decisions

### Use upstream Dockerfiles directly — no forked Dockerfiles in the repo

All four image types use upstream Dockerfiles without modification:

- **velox-deps:** `velox/scripts/docker/centos-multi.dockerfile` with `--target adapters`
- **velox-build:** `velox/docker/adapters_build.dockerfile` with `build-contexts` to remap the `FROM` image
- **presto-deps:** `presto/presto-native-execution/scripts/dockerfiles/centos-dependency.dockerfile` with a staged build context
- **presto-build:** `velox-testing/presto/docker/native_build.dockerfile` with `build-contexts` to remap the `FROM` image

The `docker/build-push-action` `build-contexts` input remaps `FROM` image references. For example, setting `ghcr.io/facebookincubator/velox-dev:adapters=docker-image://our-deps-image` makes the upstream `adapters_build.dockerfile` use our custom deps image instead of the upstream one. This avoids maintaining forked Dockerfiles.

Upstream Dockerfiles that use `--mount=type=bind,source=X` work because `source` resolves relative to the Docker build context. When the build context (`.`) is the workspace root containing `velox/` and `presto/`, the bind mounts resolve correctly.

### presto-deps requires a staged build context

The upstream `centos-dependency.dockerfile` expects a build context laid out like `presto-native-execution/` with `scripts/`, `velox/scripts/`, `velox/CMake/`, and `CMake/`. A staging step copies `presto/presto-native-execution` as `presto-deps-context` and overlays `velox/scripts` and `velox/CMake` on top. The entire `presto-native-execution` tree is copied (rather than cherry-picking specific files) to avoid breakage when upstream adds new `COPY` lines.

### sccache is required, not optional

If sccache setup fails, the build job fails. There is no graceful degradation — building without sccache would be too slow for CI. Distributed sccache is enabled (no `SCCACHE_NO_DIST_COMPILE`).

Sccache credentials are passed to Docker builds via the `secret-files` input of `docker/build-push-action` (not the `secrets` input), since the sccache-setup action writes credential files to disk.

### NUM_THREADS=16

Overrides the upstream defaults (8 for Velox, 12 for Presto) to match the cpu16 runners.

### TREAT_WARNINGS_AS_ERRORS uses upstream default (not overridden)

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
| Velox deps | cuda12.9, cuda13.1 | amd64, arm64 | `velox-deps-{vsha}-cuda{12.9,13.1}-{date}-{arch}` |
| Velox built GPU | cuda12.9, cuda13.1 | amd64, arm64 | `velox-{vsha}-gpu-cuda{12.9,13.1}-{date}-{arch}` |
| Velox built CPU | (default cu12.9) | amd64, arm64 | `velox-{vsha}-cpu-{date}-{arch}` |
| Presto deps | cuda12.9, cuda13.1 | amd64, arm64 | `presto-deps-{psha}-velox-{vsha}-cuda{12.9,13.1}-{date}-{arch}` |
| Presto native GPU | cuda12.9, cuda13.1 | amd64, arm64 | `presto-{psha}-velox-{vsha}-gpu-cuda{12.9,13.1}-{date}-{arch}` |
| Presto native CPU | (default cu12.9) | amd64, arm64 | `presto-{psha}-velox-{vsha}-cpu-{date}-{arch}` |

After per-arch builds complete, multi-arch manifests are created (without the `-{arch}` suffix) that point to both the amd64 and arm64 images.

**Total: 20 per-arch image builds + 10 multi-arch manifest merges.**

### Tag Details

- `{vsha}` is the first 7 characters of the resolved Velox git commit SHA (never `main` or `master`).
- `{psha}` is the first 7 characters of the resolved Presto git commit SHA (never `master`).
- `{date}` is the build date in `YYYYMMDD` format.
- `{arch}` is `amd64` or `arm64` (only in per-arch tags; the multi-arch manifest tags omit this).
- CPU images do not include a CUDA version in the tag. CPU builds default to the cuda12.9 deps image.
- GPU images always include the CUDA version (e.g., `cuda12.9` or `cuda13.1`).

### CUDA Version Strings

Tags use the full `cuda_version` string (e.g., `cuda12.9`, `cuda13.1`) — not short names like `cu12`/`cu13`.

| `cuda_version` | `CUDA_VERSION` Build Arg | Notes |
|---|---|---|
| `12.9` | `12.9` | Matches upstream Velox/Presto default |
| `13.1` | `13.1` | CUDA 13.1 |

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

On scheduled (cron) runs, inputs default to `main`/`master`. The actual commit SHA is resolved via `git ls-remote` in the `resolve-commits` job and used in image tags (the tag never says `main` or `master`).

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

The workflow has 10 jobs:

```
resolve-commits ─┬─→ velox-deps  ─┬─→ velox-build  ──→ merge-velox-build  ──→ test-velox
                 │                 └─→ merge-velox-deps
                 └─→ presto-deps ─┬─→ presto-build ──→ merge-presto-build ──→ test-presto
                                  └─→ merge-presto-deps
```

Each image type gets its own merge-manifests job that runs as soon as its per-arch builds complete — not blocked by other image types. Each image type gets its own test job that runs as soon as its merge-manifests job completes.

### Job 0: `resolve-commits`

**Purpose:** Resolve branch names to concrete commit SHAs via `git ls-remote`, and compute the build date.

**Runner:** `ubuntu-latest`

**Outputs:** `velox_sha`, `velox_short_sha`, `presto_sha`, `presto_short_sha`, `date`

### Job 1: `velox-deps`

**Purpose:** Build the Velox dependency base image (equivalent to `ghcr.io/facebookincubator/velox-dev:adapters`) for each CUDA version and architecture.

**Matrix:** `cuda_version: [12.9, 13.1]` × `arch: [amd64, arm64]` = 4 builds (explicit `include` entries)

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout Velox at resolved commit SHA
2. Setup proxy cache (`nv-gha-runners/setup-proxy-cache@main`)
3. Log in to ghcr.io
4. Build using `docker/build-push-action` with:
   - `file: velox/scripts/docker/centos-multi.dockerfile` (upstream Dockerfile)
   - `context: velox` (the Velox checkout)
   - `target: adapters`
   - `build-args: CUDA_VERSION={version}` (e.g., `12.9` or `13.1`)
   - `outputs: type=registry,compression=zstd,...,compression-level=15`
5. Push to `ghcr.io/rapidsai/velox-testing-images:velox-deps-{vsha}-cuda{version}-{date}-{arch}`
6. Generate artifact attestation via `actions/attest-build-provenance`

### Job 2: `velox-build`

**Purpose:** Build Velox from source and publish the image.

**Depends on:** `resolve-commits`, `velox-deps`

**Matrix:** `build_type` × `arch` (explicit `include` entries):
- GPU builds: `build_type: [gpu-cuda12.9, gpu-cuda13.1]` × `arch: [amd64, arm64]` = 4 builds (with `cudf: ON`)
- CPU builds: `build_type: cpu` × `arch: [amd64, arm64]` = 2 builds (with `cudf: OFF`, no `cuda_version`)
- Total: 6 builds

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout velox-testing + Velox at resolved commit SHA
2. Setup proxy cache
3. Log in to ghcr.io
4. Setup sccache via `./velox-testing/.github/actions/sccache-setup` (OIDC → AWS credentials)
5. Fail if sccache setup did not succeed
6. Create `.dockerignore` (excludes `.git`, `build`, `__pycache__`, etc.)
7. Determine deps image tag (CPU builds default to `cuda12.9`)
8. Build using `docker/build-push-action` with:
   - `context: .` (workspace root — bind mounts in the upstream Dockerfile resolve from here)
   - `file: velox/docker/adapters_build.dockerfile` (upstream Dockerfile)
   - `build-contexts: ghcr.io/facebookincubator/velox-dev:adapters=docker-image://{deps-image}` (remaps `FROM` to our deps image)
   - `build-args: BUILD_WITH_VELOX_ENABLE_CUDF={ON|OFF}, CUDA_VERSION={version}, ENABLE_SCCACHE=ON, NUM_THREADS=16`
   - `secret-files: github_token=..., aws_credentials=...` (sccache credentials)
9. Push with appropriate tag
10. Generate artifact attestation

**Tags:**
- GPU: `velox-{vsha}-gpu-cuda{version}-{date}-{arch}`
- CPU: `velox-{vsha}-cpu-{date}-{arch}`

### Job 3: `presto-deps`

**Purpose:** Build the Presto dependency base image (equivalent to `presto/prestissimo-dependency:centos9`) for each CUDA version and architecture.

**Matrix:** `cuda_version: [12.9, 13.1]` × `arch: [amd64, arm64]` = 4 builds (explicit `include` entries)

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout velox-testing, Velox, and Presto at resolved commit SHAs
2. Setup proxy cache
3. Log in to ghcr.io
4. Stage presto-deps build context:
   ```bash
   cp -r presto/presto-native-execution presto-deps-context
   cp -r velox/scripts presto-deps-context/velox/scripts
   cp -r velox/CMake presto-deps-context/velox/CMake
   ```
5. Build using `docker/build-push-action` with:
   - `context: presto-deps-context` (the staged directory)
   - `file: presto/presto-native-execution/scripts/dockerfiles/centos-dependency.dockerfile` (upstream Dockerfile)
   - `build-args: CUDA_VERSION={version}`
   - `outputs: type=registry,compression=zstd,...,compression-level=15`
6. Push to `ghcr.io/rapidsai/velox-testing-images:presto-deps-{psha}-velox-{vsha}-cuda{version}-{date}-{arch}`
7. Generate artifact attestation

### Job 4: `presto-build`

**Purpose:** Build the Presto native worker binary image.

**Depends on:** `resolve-commits`, `presto-deps`

**Matrix:** `build_type` × `arch` (explicit `include` entries):
- GPU builds: `build_type: [gpu-cuda12.9, gpu-cuda13.1]` × `arch: [amd64, arm64]` = 4 builds (with `gpu_flag: ON`)
- CPU builds: `build_type: cpu` × `arch: [amd64, arm64]` = 2 builds (with `gpu_flag: OFF`, no `cuda_version`)
- Total: 6 builds

**Runner:** `linux-{arch}-cpu16`

**Steps:**
1. Checkout velox-testing, Velox, and Presto at resolved commit SHAs
2. Setup proxy cache
3. Log in to ghcr.io
4. Setup sccache (OIDC → AWS credentials), fail if it doesn't succeed
5. Create `.dockerignore`
6. Determine deps image tag (CPU builds default to `cuda12.9`)
7. Build using `docker/build-push-action` with:
   - `context: .` (workspace root)
   - `file: velox-testing/presto/docker/native_build.dockerfile` (from velox-testing repo)
   - `build-contexts: presto/prestissimo-dependency:centos9=docker-image://{deps-image}` (remaps `FROM`)
   - `build-args: GPU={ON|OFF}, ENABLE_SCCACHE=ON, NUM_THREADS=16, CUDA_ARCHITECTURES=...`
   - `secret-files: github_token=..., aws_credentials=...`
   - GPU builds: `CUDA_ARCHITECTURES="75;80;86;90;100;120"`
   - CPU builds: `CUDA_ARCHITECTURES="70"`
8. Push with appropriate tag
9. Generate artifact attestation

**Tags:**
- GPU: `presto-{psha}-velox-{vsha}-gpu-cuda{version}-{date}-{arch}`
- CPU: `presto-{psha}-velox-{vsha}-cpu-{date}-{arch}`

### Jobs 5–8: `merge-velox-deps`, `merge-velox-build`, `merge-presto-deps`, `merge-presto-build`

**Purpose:** Create multi-arch manifests that combine the amd64 and arm64 images under a single tag.

**Runner:** `ubuntu-latest` (lightweight — just runs `docker manifest` commands)

Each merge job depends only on its corresponding build job and `resolve-commits`, so image types are not blocked by each other.

**Steps:**
1. Log in to ghcr.io
2. For each tag: `docker manifest create {tag} --amend {tag}-amd64 --amend {tag}-arm64`
3. `docker manifest push {tag}`

Manifest counts:
- `merge-velox-deps`: 2 (cuda12.9, cuda13.1)
- `merge-velox-build`: 3 (gpu-cuda12.9, gpu-cuda13.1, cpu)
- `merge-presto-deps`: 2 (cuda12.9, cuda13.1)
- `merge-presto-build`: 3 (gpu-cuda12.9, gpu-cuda13.1, cpu)
- **Total: 10 multi-arch manifests**

### Jobs 9–10: `test-velox`, `test-presto`

**Purpose:** Smoke test the published GPU images.

**Depends on:** `merge-velox-build` / `merge-presto-build` respectively

**Runner:** `linux-{arch}-gpu-{gpu}-latest-1` (currently amd64 + L4 only)

**Steps:**
1. Log in to ghcr.io
2. Pull the `cuda13.1` GPU multi-arch image
3. Verify `arch` reports correctly
4. Verify `nvidia-smi` works under `--gpus all`

## sccache Integration

All compilation jobs (`velox-build`, `presto-build`) use sccache for build caching. This is required — if sccache setup fails, the job fails.

### How sccache Works

The RAPIDS sccache fork (`rapidsai/sccache`) provides S3-backed compilation caching and distributed compilation. Key components:

1. **sccache setup script:** `scripts/sccache/sccache_setup.sh` installs the RAPIDS sccache fork from GitHub releases, configures it, and starts the server.

2. **CMake integration:** When sccache is enabled, builds add `-DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache` to the CMake flags.

3. **S3 cache bucket:** `rapids-sccache-devs` in `us-east-2`.

4. **Docker build secrets:** Two secrets are passed via `secret-files` during Docker builds:
   - `github_token` → mounted as env var `SCCACHE_DIST_AUTH_TOKEN` (for distributed compilation auth)
   - `aws_credentials` → mounted at `/root/.aws/credentials` (for S3 cache bucket access)

5. **Distributed compilation:** The sccache fork distributes compilation to a build cluster at `https://{arch}.linux.sccache.rapids.nvidia.com`. Distributed compilation is enabled (no `SCCACHE_NO_DIST_COMPILE`).

### CI Authentication: OIDC-Based AWS Credentials

The `.github/actions/sccache-setup/action.yml` (added in [#228](https://github.com/rapidsai/velox-testing/pull/228)) provides CI authentication using GitHub Actions OIDC to assume an AWS IAM role:

```yaml
- name: Setup sccache
  id: sccache
  uses: ./velox-testing/.github/actions/sccache-setup
  with:
    role-to-assume: ${{ vars.AWS_ROLE_ARN }}
    aws-region: ${{ vars.AWS_REGION }}

- name: Ensure sccache auth dir exists
  run: |
    if [[ "${{ steps.sccache.outputs.enabled }}" != "true" ]]; then
      echo "::error::sccache setup failed. sccache is required for ci-images builds."
      exit 1
    fi
```

The action:
1. Uses `aws-actions/configure-aws-credentials@v4` with OIDC to assume `${{ vars.AWS_ROLE_ARN }}`
2. Creates `~/.sccache-auth/aws_credentials` with temporary session credentials
3. Creates `~/.sccache-auth/github_token`
4. Exports `SCCACHE_AUTH_DIR` for downstream steps

**Required permissions:** The workflow must declare `id-token: write` and `contents: read` for OIDC to work.

**Required repository variables:** `AWS_ROLE_ARN` and `AWS_REGION` (default `us-east-2`).

### sccache in `docker/build-push-action`

After the sccache-setup action creates the auth files, they are passed to the Docker build via `secret-files`:

```yaml
- uses: docker/build-push-action@...
  with:
    secret-files: |
      github_token=${{ env.SCCACHE_AUTH_DIR }}/github_token
      aws_credentials=${{ env.SCCACHE_AUTH_DIR }}/aws_credentials
    build-args: |
      ENABLE_SCCACHE=ON
      NUM_THREADS=16
```

The Dockerfiles reference these secrets via `--mount=type=secret`:
```dockerfile
RUN --mount=type=secret,id=github_token,env=SCCACHE_DIST_AUTH_TOKEN \
    --mount=type=secret,id=aws_credentials,target=/root/.aws/credentials \
    ...build commands...
```

This is the same pattern already used by `adapters_build.dockerfile` and `native_build.dockerfile`. The only difference is that in CI, the auth files come from OIDC (via the sccache-setup action) rather than from interactive `setup_sccache_auth.sh`.

## Build Triggers

- **Scheduled:** Daily at 5:00 UTC / 1:00 AM EST (`0 5 * * *`), builds against latest `main`/`master`
- **Manual:** `workflow_dispatch` with configurable repository/commit inputs
- **Push:** Kept for testing on the `ci-images` branch (TODO: remove before merging to `main`)

## Runner Configuration

| Job | Runner Label |
|---|---|
| resolve-commits | `ubuntu-latest` |
| velox-deps | `linux-{arch}-cpu16` |
| velox-build | `linux-{arch}-cpu16` |
| presto-deps | `linux-{arch}-cpu16` |
| presto-build | `linux-{arch}-cpu16` |
| merge-* | `ubuntu-latest` |
| test-velox | `linux-{arch}-gpu-l4-latest-1` |
| test-presto | `linux-{arch}-gpu-l4-latest-1` |

## Estimated Build Times

| Job | Estimated Time (sccache warm) | Estimated Time (sccache cold) |
|---|---|---|
| velox-deps | ~1 min (Docker layer cached) | ~45 min |
| velox-build | ~10-15 min | ~30 min |
| presto-deps | ~1 min (Docker layer cached) | ~45 min |
| presto-build | ~10-15 min | ~30 min |
| merge-* | ~2 min | ~2 min |
| test-* | ~5 min | ~5 min |

sccache "warm" means prior compilations of similar code are cached in S3. Even on the first run of this workflow, the sccache S3 bucket may already contain cache entries from `velox-test.yml` runs, so builds should benefit immediately.

**Wall clock (warm caches):** ~30 min (deps cached + sccache-accelerated build + merge + test)
**Wall clock (cold, first run):** ~110 min (deps build + cold compile + merge + test)

## Discoveries During Implementation

- `--mount=type=bind,source=X` in Dockerfiles resolves `source` relative to the Docker build context, so upstream Dockerfiles that use bind mounts work with `docker/build-push-action` when `context: .` matches the expected layout. This eliminated the need for forked Dockerfiles.
- `docker/build-push-action` `build-contexts` input can remap `FROM` image references (e.g., `ghcr.io/facebookincubator/velox-dev:adapters=docker-image://our-image`).
- GitHub Actions `include`-only matrices skip entries with empty string values. CPU builds originally had `cuda_short: ''` which caused them to be silently skipped. Fixed by giving all matrix entries concrete values and using `build_type` as the unique key.
- `docker/build-push-action` has both `secrets` (inline values) and `secret-files` (file paths) inputs. We use `secret-files` since sccache-setup writes credential files to disk.
- The upstream Presto `centos-dependency.dockerfile` evolves (e.g., added `COPY CMake/arrow/arrow-flight.patch`). The staging step now copies the entire `presto-native-execution` tree rather than cherry-picking files, to avoid breakage when upstream adds new `COPY` lines.
- `workflow_dispatch` only works when the workflow file is on `main`, so the `push` trigger is needed for testing on branches.

## Open Items and Risks

1. **Upstream Dockerfile evolution:** Since we use upstream Dockerfiles directly, changes to their expected build context or `FROM` references could break our builds. The staging step for presto-deps already accounts for this by copying the full tree.

2. **Registry storage:** GitHub Container Registry has storage limits. 20 per-arch images per run across multiple CUDA versions and architectures may consume significant space. Monitor usage after deployment.

3. **CUDA 13.1 compatibility:** CUDA 13.1 is newer than the upstream default (12.9). Verify that the Velox and Presto build systems compile cleanly against CUDA 13.1 with all dependencies.

4. **sccache OIDC prerequisites:** The `AWS_ROLE_ARN` and `AWS_REGION` repository variables must be configured. The workflow must declare `id-token: write` permission. If OIDC is misconfigured, the build will fail (no graceful degradation).

5. **Test coverage:** Smoke tests currently only run the `cuda13.1` GPU image on amd64/L4. Expand to cover CPU images, cuda12.9, and arm64 as runners become available.

6. **Remove `push` trigger:** The `push` trigger on `ci-images.yml` must be removed before merging to `main`.
