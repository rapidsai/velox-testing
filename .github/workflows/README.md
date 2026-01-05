# GitHub Actions Workflows

This directory contains GitHub Actions workflows for automated testing, benchmarking, and branch management for the Velox and Presto projects. These workflows enable continuous integration and nightly testing to catch regressions early and ensure stability across CPU and GPU execution paths.

## Overview

The workflows are organized into three main categories:

| Category | Purpose |
|----------|---------|
| **Velox Testing** | Build and test Velox libraries (CPU/GPU) |
| **Velox Benchmarking** | Run TPC-H benchmarks on Velox GPU |
| **Presto Testing** | End-to-end testing with Presto Java/Native workers |

---

## Velox Workflows

### Core Build & Test

#### `velox-test.yml`
The foundational Velox build and test workflow. Supports both CPU and GPU builds with configurable targets.

| Input | Description | Default |
|-------|-------------|---------|
| `repository` | Velox repository to build | `facebookincubator/velox` |
| `velox_commit` | Commit SHA or branch | `main` |
| `build_target` | Build target: `all`, `cpu`, or `gpu` | `gpu` |

**Usage:**
- Triggered manually via `workflow_dispatch`
- Called by other workflows via `workflow_call`
- Runs on appropriate runners: `linux-amd64-cpu4` for CPU, `linux-amd64-gpu-l4-latest-1` for GPU

### Nightly Tests

#### `velox-nightly-upstream.yml`
Runs nightly tests against the **upstream Velox main branch** (`facebookincubator/velox:main`).

- **Schedule:** Daily at 04:00 UTC
- **Purpose:** Early detection of regressions in upstream Velox
- **Builds:** Both CPU and GPU (`build_target: all`)

#### `velox-nightly-staging.yml`
Runs nightly tests against the **staging branch** with cuDF PRs integrated.

- **Schedule:** Daily at 05:00 UTC
- **Purpose:** Validate the staging branch before merging cuDF changes
- **Configuration:** Uses `STABLE_VELOX_REPO` and `STABLE_VELOX_COMMIT` repository variables

### Branch Management

#### `velox-create-staging.yml`
Automates the creation and maintenance of the staging branch by aggregating cuDF-related PRs.

**Key Features:**
- Syncs with upstream `facebookincubator/velox:main`
- Auto-fetches PRs labeled `ready-to-merge` and `cudf`
- Merges PRs into staging branch with conflict detection
- Optionally triggers build and test workflow

| Input | Description | Default |
|-------|-------------|---------|
| `base_repository` | Base Velox repo | `facebookincubator/velox` |
| `base_velox_commit` | Base Velox commit SHA or branch | `main` |
| `target_repository` | Target repo for staging | `rapidsai/velox` |
| `target_branch` | Target branch (where cuDF PRs are merged) | `staging` |
| `auto_fetch_prs` | Auto-fetch labeled PRs | `true` |
| `manual_pr_numbers` | Manual PR list (space-separated) | `''` |
| `build_and_run_tests` | Run tests after update | `true` |
| `force_push` | Force push to target | `false` |

### Benchmarking

#### `velox-benchmark-sanity-test.yml`
Runs TPC-H benchmarks on Velox GPU builds as a sanity check.

| Input | Description | Default |
|-------|-------------|---------|
| `repository` | Velox repository | `facebookincubator/velox` |
| `velox_commit` | Commit SHA or branch | `main` |
| `benchmark_type` | Benchmark suite | `tpch` |

**Artifacts:**
- Build logs (`velox_gpu_build_log`)
- Benchmark results with 14-day retention

#### `velox-benchmark-nightly-staging.yml`
Nightly benchmark sanity tests on the staging branch.

- **Schedule:** Daily at 08:00 UTC
- **Purpose:** Track performance regressions in staging
- **Uses:** Stable staging configuration from repository variables

---

## Presto Workflows

### Core Build & Test

#### `presto-test.yml`
The main Presto testing workflow supporting multiple worker types.

| Input | Description | Default |
|-------|-------------|---------|
| `presto_repository` | Presto repository | `prestodb/presto` |
| `presto_commit` | Presto commit/branch | `master` |
| `velox_repository` | Velox repository | `facebookincubator/velox` |
| `velox_commit` | Velox commit/branch | `main` |
| `run_java_tests` | Test with Java worker | `false` |
| `run_cpu_tests` | Test with Native CPU worker | `false` |
| `run_gpu_tests` | Test with Native GPU worker | `true` |
| `set_velox_backward_compatible` | Enable backward compatibility | `false` |

**Worker Types:**
- **Java Worker:** Traditional Presto Java execution
- **Native CPU Worker:** Prestissimo with Velox CPU execution
- **Native GPU Worker:** Prestissimo with Velox GPU/cuDF execution

#### `presto-test-composite.yml`
Reusable composite workflow that implements the actual build-and-test logic for Presto. Used internally by `presto-test.yml`.

**Steps:**
1. Checkout Presto, Velox, and velox-testing repositories
2. Download Presto dependencies container image
3. Build Presto with the specified worker type
4. Run TPC-H integration tests
5. Shutdown Presto cleanly

### Nightly Tests

#### `presto-nightly-upstream.yml`
Nightly tests against **upstream Presto and Velox**.

- **Schedule:** Daily at 04:00 UTC
- **Repositories:** `prestodb/presto:master` + `facebookincubator/velox:main`
- **Tests:** Java, CPU, and GPU workers

#### `presto-nightly-staging.yml`
Nightly tests against **staging/stable versions**.

- **Schedule:** Daily at 04:00 UTC
- **Configuration:** Uses `STABLE_PRESTO_*` and `STABLE_VELOX_*` repository variables
- **Purpose:** Validate known-good configurations

#### `presto-nightly-pinned.yml`
Nightly tests using **Presto's pinned Velox version**.

- **Schedule:** Daily at 04:00 UTC
- **Purpose:** Test with the exact Velox commit that Presto depends on
- **Process:**
  1. Checks out Presto and extracts its pinned Velox submodule SHA
  2. Runs tests with that specific Velox version

---

## Workflow Dependency Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                     VELOX WORKFLOWS                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  velox-create-staging.yml ──────► velox-test.yml                │
│        │                              ▲                          │
│        │                              │                          │
│        ▼                              │                          │
│  [staging branch]                     │                          │
│        │                              │                          │
│        └──────────────────────────────┘                          │
│                                                                  │
│  velox-nightly-upstream.yml ────────► velox-test.yml            │
│  velox-nightly-staging.yml ─────────► velox-test.yml            │
│                                                                  │
│  velox-benchmark-nightly-staging.yml ► velox-benchmark-sanity-test.yml │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     PRESTO WORKFLOWS                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  presto-nightly-upstream.yml ───┐                               │
│  presto-nightly-staging.yml ────┼──► presto-test.yml            │
│  presto-nightly-pinned.yml ─────┘         │                     │
│                                           │                      │
│                                           ▼                      │
│                                  presto-test-composite.yml       │
│                                    (java | cpu | gpu)            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Repository Variables

The workflows use the following repository variables for configuration:

| Variable | Description |
|----------|-------------|
| `STABLE_VELOX_REPO` | Stable Velox repository (e.g., `rapidsai/velox`) |
| `STABLE_VELOX_COMMIT` | Stable Velox commit/branch (e.g., `staging`) |
| `STABLE_PRESTO_REPO` | Stable Presto repository |
| `STABLE_PRESTO_COMMIT` | Stable Presto commit/branch |
| `SET_PRESTO_VELOX_BACKWARD_COMPATIBLE` | Enable backward compatibility flag |
| `S3_BUCKET_NAME` | S3 bucket for artifacts/cache |
| `S3_BUCKET_REGION` | S3 bucket region |

---

## Required Secrets

| Secret | Purpose |
|--------|---------|
| `VELOX_TEST_PAT` | GitHub PAT for cross-repo operations |
| `AWS_ARN_STRING` | AWS ARN for S3 access |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |

---

## Running Workflows Manually

All workflows support manual triggering via the GitHub Actions UI:

1. Navigate to **Actions** tab in the repository
2. Select the desired workflow from the left sidebar
3. Click **Run workflow**
4. Configure inputs as needed
5. Click **Run workflow** to start

---

## Best Practices

1. **Use staging for integration testing:** Test cuDF changes on the staging branch before merging to upstream
2. **Monitor nightly runs:** Check nightly workflow results daily to catch regressions early
3. **Use pinned versions:** For production deployments, use Presto's pinned Velox version
4. **Review benchmark results:** Compare benchmark artifacts across runs to track performance

