# GitHub Actions Workflows

This directory contains GitHub Actions workflows for automated testing, benchmarking, and branch management for the Velox and Presto projects.

## Workflow Summary

| Workflow | Purpose | Notes |
|----------|---------|-------|
| **Staging Branch Management** |||
| `create-staging-composite.yml` | Reusable workflow for creating staging branches | Called by velox/presto staging workflows |
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
| **Preliminary Checks** |||
| `preliminary-checks.yml` | Runs tests when specific directories change | Triggers on `benchmark_data_tools/` or `presto/` changes |

---

## Staging Branch Workflows

### Overview

The staging workflows create branches that aggregate PRs from upstream repositories, enabling integration testing before changes are merged upstream.

**Key Features:**
- Resets target branch to upstream HEAD
- Auto-discovers or manually specifies PRs to merge
- Tests all PR pairs for merge conflicts before proceeding
- Creates dated snapshot branches (e.g., `staging_01-30-2026`) for rollback
- Generates `.staging-manifest.yaml` documenting merged PRs

### Local Usage

```bash
# Velox: auto-fetch PRs with "cudf" label
./velox/scripts/create_staging.sh

# Velox: specific PRs
./velox/scripts/create_staging.sh --manual-pr-numbers "16075,16050"

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
| `PRESTO_FORK_PAT` | GitHub PAT with write access to target Presto repository (falls back to `VELOX_FORK_PAT`) |
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
```
