# GitHub Actions Workflows

This directory contains GitHub Actions workflows for automated testing, benchmarking, and branch management for the Velox and Presto projects.

## Workflow Summary

| Workflow | Purpose | Notes |
|----------|---------|-------|
| **Staging Branch Management** |||
| `create-staging-composite.yml` | Reusable workflow for creating staging branches | Supports additional repo merge + PR merging |
| `velox-create-staging.yml` | Creates Velox staging branch by merging cuDF PRs | Auto-fetches PRs with `cudf` label by default |
| `presto-create-staging.yml` | Creates Presto staging branch by merging specified PRs | Requires manual PR numbers (no auto-fetch) |
| **CI Images** |||
| `velox-nightly.yml` | Nightly Velox builds + tests + benchmarks (upstream) | Schedule (5am UTC) + manual dispatch |
| `presto-nightly.yml` | Nightly Presto builds + tests (upstream, pinned) | Schedule (5am UTC) + manual dispatch |
| `velox.yml` | Velox CI image build + test + benchmark pipeline | Called by nightly; also supports `workflow_dispatch` |
| `presto.yml` | Presto CI image build + test pipeline | Called by nightly; also supports `workflow_dispatch` |
| `actions/resolve-commits/` | Composite action to resolve Velox/Presto commit SHAs | Used by CI image workflows |
| `actions/resolve-run-id-suffix/` | Composite action to resolve final image tag suffixes | Used by test/benchmark workflows |
| `velox-build.yml` | Reusable workflow for Velox CI image builds + merge | Builds deps + build images, creates multi-arch manifests |
| `presto-build.yml` | Reusable workflow for Presto CI image builds + merge | Builds deps + build + coordinator, creates multi-arch manifests |
| `velox-test.yml` | Reusable workflow for Velox CI image tests | CPU + GPU tests; supports `workflow_dispatch` for test-only runs |
| `velox-benchmark.yml` | Reusable workflow for Velox GPU benchmarks | TPC-H benchmarks against pre-built CI images; supports `workflow_dispatch` |
| `presto-test.yml` | Reusable workflow for Presto CI image tests | Smoke + integration tests; supports `workflow_dispatch` for test-only runs |
| `ci-image-cleanup.yml` | Deletes CI images older than 30 days from GHCR | Weekly (Tuesdays) + manual dispatch |
| **Compute Sanitizer** |||
| `velox-compute-sanitizer-trigger.yaml` | Discovers cuDF tests and runs compute-sanitizer tools | Weekly (Saturdays) + manual dispatch |
| `velox-compute-sanitizer-run.yaml` | Reusable workflow to run a sanitizer tool on a matrix of tests | Called by trigger; also supports `workflow_dispatch` |

---

## Staging Branch Workflows

### Overview

The staging workflows create branches that aggregate PRs from upstream repositories, enabling integration testing before changes are merged upstream.

**Key Features:**
- Resets target branch to upstream HEAD
- Optionally merges an additional repository/branch (e.g., cuDF exchange integration)
- Auto-discovers or manually specifies PRs to merge
- Tests all PR pairs for merge conflicts before proceeding
- Creates dated snapshot branches (e.g., `staging_01-30-2026`) for rollback
- Generates `.staging-manifest.yaml` documenting merged PRs and additional merges

### Workflow Inputs

| Input | Description | Required |
|-------|-------------|----------|
| `base_repository` | Upstream repository to sync from (e.g., `facebookincubator/velox`) | Yes |
| `base_branch` | Branch from base repository (e.g., `main`) | Yes |
| `target_repository` | Fork repository to push staging branch to | Yes |
| `target_branch` | Name of the staging branch to create | Yes |
| `auto_fetch_prs` | Auto-fetch non-draft PRs with specified labels. **Note:** Auto-fetch is automatically disabled if `manual_pr_numbers` is specified. | No |
| `pr_labels` | Comma-separated PR labels to auto-fetch | No |
| `manual_pr_numbers` | Comma-separated PR numbers to merge. Providing this disables `auto_fetch_prs`. | No |
| `exclude_pr_numbers` | Comma-separated PR numbers to exclude from auto-fetch results | No |
| `force_push` | Force push to override existing target branch | No |
| `additional_repository` | Additional repository to merge from (e.g., `rapidsai/velox`) | No |
| `additional_branch` | Branch from additional repository to merge (e.g., `cudf-exchange`) | No |

### Additional Repository Merge

The `additional_repository` and `additional_branch` inputs allow merging code from a secondary source before PRs are applied. This is useful for:
- Integrating cuDF exchange or GPU-specific changes
- Testing feature branches that span multiple repositories
- Including work-in-progress changes not yet submitted as PRs

The additional merge happens **after** the base reset but **before** PR merging. The resulting commit becomes the new base for PR compatibility testing, ensuring all merged PRs are compatible with the additional changes.

### Local Usage

```bash
# Velox: auto-fetch PRs with "cudf" label
./velox/scripts/create_staging.sh

# Velox: specific PRs
./velox/scripts/create_staging.sh --manual-pr-numbers "16075,16050"

# Velox: auto-fetch but exclude specific PRs
./velox/scripts/create_staging.sh --exclude-pr-numbers "16264,16258"

# Velox: with additional repository merge
./velox/scripts/create_staging.sh \
  --additional-repository "rapidsai/velox" \
  --additional-branch "feature/cudf-exchange" \
  --manual-pr-numbers "16075"

# Presto: must specify PRs (no auto-fetch)
./presto/scripts/create_staging.sh --manual-pr-numbers "27057,27054"
```

**Note:** In local mode, push to remote is skipped. Use `--mode ci` to enable pushing.

---

## CI Images

### Overview

Multi-arch (amd64/arm64) Docker images for Velox and Presto are built and published to [GitHub Container Registry](https://github.com/orgs/rapidsai/packages?repo_name=velox-testing) under the `velox-testing-images` package.

- **`velox-nightly.yml`** — Nightly schedule (5am UTC). Calls `velox.yml` for the upstream variant.
- **`presto-nightly.yml`** — Nightly schedule (5am UTC). Calls `presto.yml` for upstream and pinned variants.
- **`velox.yml`** / **`presto.yml`** — Reusable pipelines that resolve commits, build images, run tests, and (for Velox) run benchmarks. Also support `workflow_dispatch` for manual runs.

  | Variant | Velox | Presto |
  |---------|-------|--------|
  | `upstream` | `facebookincubator/velox:main` | `prestodb/presto:master` |
  | `pinned` | Presto's pinned Velox submodule | `prestodb/presto:master` |
### CI Image Pipeline

The Velox pipeline (`velox.yml`):

```
                                 ┌─► velox-test
resolve-commits ──► velox-build ─┤
                                 └─► velox-benchmark
```

The Presto pipeline (`presto.yml`):

```
resolve-commits ──► presto-build ──► presto-test
```

The pipeline is split into focused reusable workflows:

| Workflow | Purpose |
|----------|---------|
| `actions/resolve-commits/` | Composite action: resolves Velox/Presto commit SHAs (including `presto-pinned` logic) and sets the build date |
| `actions/resolve-run-id-suffix/` | Composite action: resolves `-${BUILD_VARIANT}-${GITHUB_RUN_ID}` suffixes for manual final image tags |
| `velox-build.yml` | Builds velox-deps and velox images, creates multi-arch manifests |
| `presto-build.yml` | Builds presto-deps, presto native worker, and coordinator images, creates multi-arch manifests |
| `velox-test.yml` | Runs Velox CPU and GPU tests against built images |
| `velox-benchmark.yml` | Runs TPC-H GPU benchmarks against built Velox images using `benchmark_velox.sh --image` |
| `presto-test.yml` | Runs Presto smoke tests and TPC-H/TPC-DS integration tests against built images |

`velox-test.yml`, `velox-benchmark.yml`, and `presto-test.yml` all support `workflow_dispatch` for standalone runs. Dispatch inputs use the same commit/date/build-variant components as reusable workflow calls, plus an optional `run_id` that is required when testing images from manual builds (`build_variant: manual`).

### Image Tags

Images are tagged with commit SHAs, CUDA version, and build date:

- **Velox deps:** `velox-deps-${VELOX_SHA}-cuda${CUDA_VERSION}-${DATE}`
- **Velox build (GPU):** `velox-${VELOX_SHA}-gpu-cuda${CUDA_VERSION}-${DATE}`
- **Velox build (CPU):** `velox-${VELOX_SHA}-cpu-${DATE}`
- **Presto deps:** `presto-deps-${PRESTO_SHA}-velox-${VELOX_SHA}-cuda${CUDA_VERSION}-${DATE}`
- **Presto build (GPU):** `presto-${PRESTO_SHA}-velox-${VELOX_SHA}-gpu-cuda${CUDA_VERSION}-${DATE}`
- **Presto build (CPU):** `presto-${PRESTO_SHA}-velox-${VELOX_SHA}-cpu-${DATE}`
- **Presto coordinator:** `presto-coordinator-${PRESTO_SHA}-${DATE}`

Manual build runs append the build variant and GitHub run ID to final image tags, for example `velox-${VELOX_SHA}-gpu-cuda${CUDA_VERSION}-${DATE}-${BUILD_VARIANT}-${GITHUB_RUN_ID}`. Scheduled nightly final tags keep the date-only names above and update stable `latest` tags for the canonical variants. Intermediate arch-specific tags include the same build variant and GitHub run ID before the architecture suffix, for example `velox-${VELOX_SHA}-gpu-cuda${CUDA_VERSION}-${DATE}-${BUILD_VARIANT}-${GITHUB_RUN_ID}-${ARCH}`, so overlapping builds cannot delete each other's merge inputs.

Images are purged after 30 days by the `ci-image-cleanup.yml` workflow.

### Pulling Images

The container registry is private. You must be a member of the `rapidsai` GitHub organization to pull images.

1. Log into GitHub Container Registry with Docker:

```bash
# Ensure the gh token has read:packages scope, required for ghcr.io
if ! gh auth status 2>&1 | grep -q 'read:packages'; then
  echo "Token missing read:packages scope, refreshing..."
  gh auth refresh -s read:packages
fi

# Log into ghcr.io with the current gh credentials
echo $(gh auth token) | docker login ghcr.io -u $(gh api user -q .login) --password-stdin
```

2. Pull and run an image. For example:

```bash
docker run -it ghcr.io/rapidsai/velox-testing-images:velox-deps-8853645-cuda13.1-20260305
```

Browse available tags at [ghcr.io/rapidsai/velox-testing-images](https://github.com/orgs/rapidsai/packages?repo_name=velox-testing) (requires `rapidsai` org membership).

---

## Required Secrets

| Secret | Purpose |
|--------|---------|
| `VELOX_FORK_PAT` | GitHub PAT with write access to target Velox repository |
| `PRESTO_FORK_PAT` | GitHub PAT with write access to target Presto repository |
| `AWS_ARN_STRING` | AWS ARN for S3 access |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |

---

## Repository Variables

| Variable | Purpose |
|----------|---------|
| `STABLE_VELOX_REPO` | Stable Velox repository for nightly tests |
| `STABLE_VELOX_COMMIT` | Stable Velox commit/branch |
| `STABLE_PRESTO_REPO` | Stable Presto repository |
| `STABLE_PRESTO_COMMIT` | Stable Presto commit/branch |
| `S3_BUCKET_NAME` | S3 bucket for artifacts/cache |
| `S3_BUCKET_REGION` | S3 bucket region |

---

## Workflow Dependency Graph

```
STAGING
───────
velox-create-staging.yml ──► create-staging-composite.yml ──► [staging branch]
presto-create-staging.yml ──► create-staging-composite.yml ──► [staging branch]

CI IMAGES (VELOX)
─────────────────
                                                         ┌─► velox-test.yml
velox-nightly.yml ──► velox.yml ──► velox-build.yml ─────┤
                                                         └─► velox-benchmark.yml

velox-test.yml (workflow_dispatch) ──► [test images by SHA/date/variant]
velox-benchmark.yml (workflow_dispatch) ──► [benchmark images by SHA/date/variant]

CI IMAGES (PRESTO)
──────────────────
presto-nightly.yml ──► presto.yml ──► presto-build.yml ──► presto-test.yml

presto-test.yml (workflow_dispatch) ──► [test images by SHA/date/variant]

COMPUTE SANITIZER
─────────────────
velox-compute-sanitizer-trigger.yaml ──► [discover cuda_driver tests] ──► velox-compute-sanitizer-run.yaml (racecheck)
                                                                      └─► velox-compute-sanitizer-run.yaml (synccheck)

CLEANUP
───────
ci-image-cleanup.yml ──► [delete old images]
```
