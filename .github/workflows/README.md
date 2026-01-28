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
Automates the creation and maintenance of the staging branch by aggregating cuDF-related PRs from upstream Velox.

**How It Works:**

1. **Sync with Upstream:** Resets the target branch to match `upstream/main` (or specified base branch)
2. **Fetch PRs:** Auto-discovers non-draft PRs with `cudf` label, or uses manually specified PR numbers
3. **Pairwise Conflict Check:** Tests all PR combinations for merge conflicts before proceeding
4. **Merge PRs:** Sequentially merges all PRs into the staging branch
5. **Create Manifest:** Generates `.staging-manifest.yaml` documenting the branch recipe (base commit, merged PRs)
6. **Push Branches:** Pushes both `staging` and `staging_MM-DD-YYYY` dated snapshot branches
7. **Trigger Tests:** Optionally triggers the build and test workflow

**Key Features:**
- **Dated Branch Snapshots:** Creates daily snapshots (e.g., `staging_01-22-2026`) for rollback capability
- **Manifest File:** Each staging branch includes `.staging-manifest.yaml` documenting exactly how it was built
- **Conflict Detection:** Fails fast on merge conflicts with clear error messages
- **Pairwise Testing:** Validates all PR pairs can merge cleanly together

**Parameters:**

| Input | Description | Default |
|-------|-------------|---------|
| `base_repository` | Upstream Velox repository to sync from | `facebookincubator/velox` |
| `base_branch` | Upstream branch to use as base | `main` |
| `target_repository` | Target repository to push staging branch | `rapidsai/velox` |
| `target_branch` | Name of the staging branch | `staging` |
| `auto_fetch_prs` | Auto-fetch non-draft PRs with `cudf` label | `true` |
| `manual_pr_numbers` | Space-separated PR numbers (when `auto_fetch_prs=false`) | `''` |
| `build_and_run_tests` | Trigger test workflow after update | `true` |
| `force_push` | Force push to override existing branch | `false` |

**Manifest File (`.staging-manifest.yaml`):**

The staging branch includes a manifest file documenting its creation:

```yaml
metadata:
  generated_at: "2026-01-22T10:30:00Z"
  target_repository: "rapidsai/velox"
  target_branch: "staging"
  dated_branch: "staging_01-22-2026"

base:
  repository: "facebookincubator/velox"
  branch: "main"
  commit: "abc123..."
  url: "https://github.com/facebookincubator/velox/tree/abc123..."

merged_prs:
  - number: 1234
    commit: "def456..."
    author: "developer1"
    title: "Add cuDF support for feature X"
    url: "https://github.com/facebookincubator/velox/pull/1234"
```

**Parameter Reference:**

`base_repository` (string)
:   The upstream Velox repository to sync from. This is the source of truth for the
    base code before cuDF PRs are applied. This is where the cudf, non-draft PRs are read from. 
    
    Default: `facebookincubator/velox`

`base_branch` (string)
:   The branch in `base_repository` to use as the starting point. The staging branch
    is reset to this branch's HEAD before any PRs are merged.
    
    Default: `main`

`target_repository` (string)
:   The repository where the staging branch will be pushed. Must have write access
    configured via `GITHUB_TOKEN` or a PAT with appropriate permissions.
    
    Default: `rapidsai/velox`

`target_branch` (string)
:   The name of the staging branch to create or update. A dated snapshot branch
    (`<target_branch>_MM-DD-YYYY`) is also created alongside it for historical
    reference and rollback capability.
    
    Default: `staging`

`auto_fetch_prs` (boolean)
:   When `true`, automatically discovers and fetches all non-draft PRs from
    `facebookincubator/velox` that have the `cudf` label. When `false`, only PRs
    specified in `manual_pr_numbers` are used.
    
    Default: `true`

`manual_pr_numbers` (string)
:   Space-separated list of PR numbers to merge into the staging branch. Only used
    when `auto_fetch_prs` is `false`. PRs are merged in the order specified.
    
    Example: `"11443 11567 11892"`
    
    Default: `''` (empty)

`build_and_run_tests` (boolean)
:   When `true`, triggers the `velox-test.yml` workflow after successfully updating
    the staging branch. This validates the merged code builds and passes tests.
    Set to `false` to skip testing (useful for quick iterations).
    
    Default: `true`

`force_push` (boolean)
:   When `true`, force pushes to the target branch, overwriting any existing content.
    When `false`, the push may fail if the remote branch has diverged. Scheduled runs
    always use force push regardless of this setting.
    
    **Warning:** Use with caution as this can overwrite commits on the target branch.
    
    Default: `false`

**Setup Secret:**

Before triggering the workflow, ensure the `VELOX_FORK_PAT` secret is configured in the
repository where the workflow runs (e.g., `rapidsai/velox-testing`). The PAT should have
write access to the `target_repository`.

```bash
# Set the PAT secret in the workflow repo for push access to target repo
# Example: Setting secret in velox-testing to push to avinashraj/velox
gh secret set VELOX_FORK_PAT --repo rapidsai/velox-testing --body "$TOKEN_VAR"
```

**CLI Examples:**

```bash
# Basic: Create staging with auto-fetched cuDF PRs
gh workflow run velox-create-staging.yml \
  -R rapidsai/velox-testing

# Specify target branch name
gh workflow run velox-create-staging.yml \
  -R rapidsai/velox-testing \
  -f target_branch=test-staging

# Manual PR selection (disable auto-fetch)
gh workflow run velox-create-staging.yml \
  -R rapidsai/velox-testing \
  -f auto_fetch_prs=false \
  -f manual_pr_numbers="11443 11567 11892"

# Skip tests after staging creation
gh workflow run velox-create-staging.yml \
  -R rapidsai/velox-testing \
  -f build_and_run_tests=false

# Force push to override existing branch
gh workflow run velox-create-staging.yml \
  -R rapidsai/velox-testing \
  -f force_push=true

# Full custom configuration
gh workflow run velox-create-staging.yml \
  -R rapidsai/velox-testing \
  --ref ci-staging-branch-creation-followup \
  -f base_repository=facebookincubator/velox \
  -f base_branch=main \
  -f target_repository=rapidsai/velox \
  -f target_branch=staging \
  -f auto_fetch_prs=true \
  -f manual_pr_numbers="" \
  -f build_and_run_tests=false \
  -f force_push=true
```

**Scheduled Runs:**

The workflow can be scheduled (currently commented out) to run periodically:
```yaml
schedule:
  - cron: '0 */6 * * *'  # Run every 6 hours
```

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
| `VELOX_FORK_PAT` | GitHub PAT with write access to the target repository (default: `rapidsai/velox`) for staging branch push |
| `AWS_ARN_STRING` | AWS ARN for S3 access |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |

> **Note:** `VELOX_FORK_PAT` requires `repo` scope (or fine-grained token with Contents: Read and Write permission). This is used by `velox-create-staging.yml` to push the staging branch. If you set `target_repository` to your own fork, use a PAT with write access to that fork.

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

