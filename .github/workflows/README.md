# GitHub Actions Workflows

This directory contains GitHub Actions workflows for automated testing, benchmarking, and branch management for the Velox and Presto projects.

## Workflow Summary

| Workflow | Purpose | Notes |
|----------|---------|-------|
| **Staging Branch Management** |||
| `create-staging-composite.yml` | Reusable workflow for creating staging branches | Supports additional repo merge + PR merging |
| `velox-create-staging.yml` | Creates Velox staging branch by merging cuDF PRs | Auto-fetches PRs with `cudf` label by default |
| `presto-create-staging.yml` | Creates Presto staging branch by merging specified PRs | Requires manual PR numbers (no auto-fetch) |
| **Velox Testing** |||
| `velox-test.yml` | Builds and tests Velox (CPU and/or GPU) | Foundation workflow called by nightly jobs |
| `velox-nightly-upstream.yml` | Nightly tests against upstream `facebookincubator/velox:main` | Early detection of upstream regressions |
| `velox-nightly-staging.yml` | Nightly tests against the staging branch | Validates staging before cuDF merges |
| **Velox Benchmarking** |||
| `velox-benchmark-sanity-test.yml` | Validates benchmarking scripts execute correctly | **Not a representative benchmark** - uses minimal data |
| `velox-benchmark-nightly-staging.yml` | Nightly benchmark sanity test on staging | Catches script/infrastructure regressions |
| **Velox Dependencies** |||
| `velox-deps-upload.yml` | Builds and uploads Velox dependencies image to S3 | Run when base dependencies change |
| **Presto Testing** |||
| `presto-test.yml` | Tests Presto with Java, Native CPU, or Native GPU workers | Supports multiple worker configurations |
| `presto-test-composite.yml` | Reusable workflow implementing Presto test logic | Called by presto-test.yml |
| `presto-nightly-upstream.yml` | Nightly tests against upstream Presto and Velox | Tests latest upstream compatibility |
| `presto-nightly-staging.yml` | Nightly tests against staging/stable versions | Validates known-good configurations |
| `presto-nightly-pinned.yml` | Nightly tests using Presto's pinned Velox version | Tests exact Velox commit Presto depends on |
| **CI Images** |||
| `ci-images.yml` | Nightly builds of CI images for upstream, pinned, and staging | Schedule only (5am UTC) |
| `ci-images-manual.yml` | Manual builds of CI images with user-specified inputs | `workflow_dispatch` only |
| `build-and-test.yml` | Reusable workflow implementing CI image build/test logic | Called by ci-images workflows |
| `ci-image-cleanup.yml` | Deletes CI images older than 30 days from GHCR | Weekly (Tuesdays) + manual dispatch |
| **Preliminary Checks** |||
| `preliminary-checks.yml` | Runs tests when specific directories change | Triggers on `benchmark_data_tools/` or `presto/` changes |

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

## Velox Testing Workflows

### `velox-test.yml`
Core build and test workflow for Velox. Supports CPU-only, GPU-only, or both targets. Used as a foundation by nightly workflows.

### `velox-nightly-upstream.yml`
Tests against the latest upstream Velox (`facebookincubator/velox:main`). Catches regressions introduced in upstream before they affect our staging branch.

### `velox-nightly-staging.yml`
Tests against the staging branch with cuDF PRs integrated. Validates the staging configuration works before promoting to stable.

---

## Velox Benchmarking Workflows

### `velox-benchmark-sanity-test.yml`
**Important:** This is a sanity test of the benchmarking infrastructure, NOT a representative performance benchmark. It uses minimal data and iterations to verify scripts execute without errors.

### `velox-benchmark-nightly-staging.yml`
Runs benchmark sanity tests nightly on the staging branch to catch infrastructure regressions.

---

## CI Images

### Overview

Multi-arch (amd64/arm64) Docker images for Velox and Presto are built and published to [GitHub Container Registry](https://github.com/orgs/rapidsai/packages?repo_name=velox-testing) under the `velox-testing-images` package. Two separate workflows handle image builds:

- **`ci-images.yml`** — Nightly schedule (5am UTC). Builds images for three source combinations in parallel:

  | Job | Velox | Presto |
  |-----|-------|--------|
  | `nightly-upstream` | `facebookincubator/velox:main` | `prestodb/presto:master` |
  | `nightly-pinned` | Presto's pinned Velox submodule | `prestodb/presto:master` |
  | `nightly-staging` | `$STABLE_VELOX_REPO:$STABLE_VELOX_COMMIT` | `$STABLE_PRESTO_REPO:$STABLE_PRESTO_COMMIT` |

  A new nightly run cancels any in-progress nightly run.

- **`ci-images-manual.yml`** — Manual dispatch only. Builds a single image set with user-specified repository/commit inputs. Runs never cancel each other.

### Image Tags

Images are tagged with commit SHAs, CUDA version, and build date:

- **Velox deps:** `velox-deps-${VELOX_SHA}-cuda${CUDA_VERSION}-${DATE}`
- **Velox build (GPU):** `velox-${VELOX_SHA}-gpu-cuda${CUDA_VERSION}-${DATE}`
- **Velox build (CPU):** `velox-${VELOX_SHA}-cpu-${DATE}`
- **Presto deps:** `presto-deps-${PRESTO_SHA}-velox-${VELOX_SHA}-cuda${CUDA_VERSION}-${DATE}`
- **Presto build (GPU):** `presto-${PRESTO_SHA}-velox-${VELOX_SHA}-gpu-cuda${CUDA_VERSION}-${DATE}`
- **Presto build (CPU):** `presto-${PRESTO_SHA}-velox-${VELOX_SHA}-cpu-${DATE}`

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

## Presto Testing Workflows

### `presto-test.yml`
Main Presto testing workflow supporting three worker types:
- **Java Worker:** Traditional Presto Java execution
- **Native CPU Worker:** Prestissimo with Velox CPU
- **Native GPU Worker:** Prestissimo with Velox GPU/cuDF

### `presto-nightly-upstream.yml`
Tests latest upstream Presto (`prestodb/presto:master`) with latest upstream Velox. Catches compatibility issues early.

### `presto-nightly-staging.yml`
Tests known-good staging/stable configurations. Uses repository variables for version pinning.

### `presto-nightly-pinned.yml`
Tests Presto with its exact pinned Velox submodule version. Ensures Presto works with its declared Velox dependency.

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
VELOX WORKFLOWS
───────────────
velox-create-staging.yml ──► create-staging-composite.yml ──► [staging branch]

velox-nightly-upstream.yml ─┬──► velox-test.yml
velox-nightly-staging.yml ──┘

velox-benchmark-nightly-staging.yml ──► velox-benchmark-sanity-test.yml


PRESTO WORKFLOWS
────────────────
presto-create-staging.yml ──► create-staging-composite.yml ──► [staging branch]

presto-nightly-upstream.yml ─┬
presto-nightly-staging.yml ──┼──► presto-test.yml ──► presto-test-composite.yml
presto-nightly-pinned.yml ───┘


CI IMAGES
─────────
ci-images.yml (nightly) ─┬──► build-and-test.yml ──► [GHCR images]
ci-images-manual.yml ────┘
ci-image-cleanup.yml ──► [delete old images]
```
