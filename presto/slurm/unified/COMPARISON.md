# Unified Launcher: Overview and Motivation

The `presto/slurm/unified/` directory provides an alternative launcher for running Presto TPC-H benchmarks on Slurm. It is intended to produce identical benchmark results to the existing `presto/slurm/presto-nvl72/` scripts while consolidating the execution logic into fewer files with a more linear control flow.

## How the original launcher works

The existing launcher involves several files and external tools that coordinate across multiple directories in the repository:

1. **`launch-run.sh`** parses CLI arguments and submits the job via `sbatch`, passing parameters as exported environment variables.
2. **`run-presto-benchmarks.slurm`** receives those environment variables, sets up paths and computed values (e.g., `CONFIGS` pointing to `presto/docker/config/generated/gpu/`), then delegates to a shell script.
3. **`run-presto-benchmarks.sh`** sources two helper files (`echo_helpers.sh` and `functions.sh`) and orchestrates the benchmark phases: setup, coordinator launch, worker launch, schema creation, query execution, and result collection.
4. **`functions.sh`** contains the actual implementation of each phase. During the setup phase, if generated configs do not already exist, it invokes `presto/scripts/generate_presto_config.sh`.
5. **`generate_presto_config.sh`** calls the **`pbench`** binary (`presto/pbench/pbench genconfig`) to render Go-style templates from `presto/docker/config/template/` using parameters from `presto/docker/config/params.json`. After generation, it applies variant-specific patches (e.g., uncommenting GPU optimizer flags, toggling multi-worker settings) and duplicates per-worker configs via the `duplicate_worker_configs()` function, which applies `sed` transformations to adjust ports, node IDs, and multi-node flags.

In total, a single benchmark run traverses at least six files across four directories, plus the `pbench` binary and its template/parameter inputs.

## How the unified launcher works

The unified launcher consists of two files:

1. **`launch.sh`** parses CLI arguments and submits the job via `sbatch`.
2. **`run.slurm`** contains all logic in a single file: environment setup, config generation (from local templates in `config-templates/`), coordinator launch, worker launch, schema setup, and query execution.

Configuration templates live in `config-templates/` within the unified directory itself and use simple `__PLACEHOLDER__` substitution via `sed`, with no external tooling required. Memory settings are computed dynamically from the node's actual RAM using the same formulas defined in `params.json`.

## Key differences

| Aspect | Original | Unified |
|---|---|---|
| Files involved | 6+ files across 4 directories | 2 files in 1 directory |
| Config generation | `pbench` binary + Go templates + post-generation `sed` patches | Direct `sed` substitution from local templates |
| External dependencies | Requires `pbench` binary | None beyond standard tools |
| Container image loading | Sequential (host-side waits between phases) | Parallel (all containers launched simultaneously, waits moved inside containers) |
| Benchmark setup + queries | Two separate container launches | Single container launch |

## Parallel container loading

One structural improvement in the unified launcher is that all container images are loaded in parallel. The original launcher waits on the host for the coordinator to become active before launching workers, and waits for workers to register before launching the benchmark container. Each of these phases incurs a container image load delay. The unified launcher issues all `srun` commands immediately and moves the dependency waits inside the containers themselves, so image loading happens concurrently.

## A note on scope

This unified launcher was developed primarily for the use case of running TPC-H GPU benchmarks on Slurm with a straightforward configuration. The original implementation may have been architected with additional considerations in mind -- for example, supporting multiple variant types (CPU, GPU, Java), integration with broader infrastructure tooling via `pbench`, or other workflows that benefit from the separation of template rendering and config patching. There may be more advanced use cases where the original implementation's flexibility is preferred or required, and this unified alternative is not intended to replace it in those scenarios.
