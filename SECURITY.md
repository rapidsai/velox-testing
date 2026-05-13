# Security Policy

`velox-testing` is the CI / build / benchmark infrastructure used to test the
RAPIDS GPU integrations with Apache Velox, PrestoDB native (with the Velox
runtime), and Spark Gluten. The repository's "product" is the build and
test scripts, Dockerfiles, GitHub Actions workflows, benchmark data tools,
and reporting utilities — not a runtime library shipped to end users.

Because the workflows here drive cross-repo builds (cloning upstream Velox,
Presto, and Gluten sources), use a shared `sccache` cluster backed by S3
for distributed compile caching, and run on both GitHub Actions and Slurm
HPC schedulers, the security posture is shaped by build-time supply
chain, credential handling for the compile cache, and the same
GitHub-Actions-shape concerns as the rest of the RAPIDS CI estate.

## Reporting a Vulnerability

Please report security vulnerabilities privately through one of the channels
below. **Do not open a public GitHub issue, PR, or discussion** for a
suspected vulnerability.

1. **NVIDIA Vulnerability Disclosure Program (preferred)**
   <https://www.nvidia.com/en-us/security/>
   Submit through the NVIDIA PSIRT web form. This is the fastest path to
   triage and tracking.

2. **Email NVIDIA PSIRT**
   psirt@nvidia.com — encrypt sensitive reports with the
   [NVIDIA PSIRT PGP key](https://www.nvidia.com/en-us/security/pgp-key).

3. **GitHub Private Vulnerability Reporting**
   Use the **Security** tab on this repository → *Report a vulnerability*.

Please include, where possible:

- Affected component (a specific workflow, a Dockerfile, the sccache
  authentication scripts, a benchmark harness, a Slurm script)
- Whether the issue is in this repo or in how a caller / runner / shared
  service consumes it
- Reproduction steps, including any relevant env vars, branches, or
  upstream source revisions
- Impact assessment (credential exposure, cache poisoning, code execution
  on a runner, supply-chain weakness, workflow injection)
- Any relevant CWE / CVE identifiers

NVIDIA PSIRT will acknowledge receipt and coordinate triage, fix
development, and coordinated disclosure. More on NVIDIA's response
process: <https://www.nvidia.com/en-us/security/psirt-policies/>.

## Security Architecture & Context

**Classification:** CI / build / benchmark tooling. Distributed as scripts,
Dockerfiles, and GitHub Actions workflows in this repository; consumed by
RAPIDS engineers, GitHub Actions runners, and Slurm HPC jobs that exercise
the upstream Velox / Presto / Spark Gluten codebases against RAPIDS GPU
integrations.

**Primary security responsibility:** Provide build and benchmark
infrastructure that fetches and assembles upstream sources, runs them
through configurable test and benchmark harnesses, and reports results —
without leaking the credentials it handles, poisoning the shared compile
cache, or amplifying caller-workflow trust assumptions.

**Components:**

- **`velox/`** — Velox build, test, and benchmark scripts and
  Dockerfiles (`velox/docker/adapters_build.dockerfile`,
  `velox/scripts/build_velox.sh`, `velox/benchmarks/`).
- **`presto/`** — Presto native build (with Velox runtime) and test
  harnesses (`presto/docker/native_build.dockerfile`, `presto/pbench/`,
  `presto/slurm/`).
- **`spark_gluten/`** — Spark Gluten (Velox-backed Spark accelerator)
  build and test (`spark_gluten/docker/gluten_build.dockerfile`,
  `spark_gluten/docker/static_jar_test.dockerfile`).
- **`scripts/sccache/`** — sccache integration. `setup_sccache_auth.sh`
  guides an engineer through obtaining a 12-hour GitHub token and AWS
  credentials and stores them in `~/.sccache-auth/` (overrideable via
  `SCCACHE_AUTH_DIR`). `sccache_auth.dockerfile` provides the auth
  container. The configured S3 bucket is `rapids-sccache-devs`.
- **`scripts/`** — shared helpers (`fetch_docker_image_from_s3.sh`,
  `upload_docker_image_to_s3.sh`, `create_staging_branch.sh`,
  workflow_status helpers, py-env helpers).
- **`benchmark_data_tools/`** and **`benchmark_reporting_tools/`** —
  generate, validate, and summarize benchmark datasets and results.
- **`common/testing/`** — shared test harness pieces.
- **`template_rendering/`** — templating used by the benchmark and
  reporting tooling.
- **`.github/workflows/`** — reusable workflows for Velox / Presto /
  Gluten build, test, nightly, benchmark, staging-branch creation,
  CUDA compute-sanitizer runs, CI image cleanup, and a status report.
- **Cross-repo expectation.** The build infrastructure expects the
  `velox` (and optionally `presto`) source repositories cloned as
  siblings under a common parent directory.

**Out of scope for this policy:** vulnerabilities in upstream Apache
Velox, PrestoDB, Apache Spark, Spark Gluten, sccache itself, AWS, the
NVIDIA driver, or CUDA. Report those to their respective projects
(NVIDIA driver and CUDA bugs still go to PSIRT). Vulnerabilities in
*how* velox-testing assembles, authenticates against, or invokes those
upstreams — credential handling, workflow YAML, Docker build context,
cache integrity — are in scope.

## Threat Model

The threats below trace to specific patterns in this repository. The
[RAPIDS Security Audit](https://github.com/orgs/rapidsai/projects/207)
remediated four CI/CD findings against this repo (template injection,
mutable refs, `secrets: inherit`, missing permissions blocks).

1. **GitHub Actions template injection.**
   Workflows in `.github/workflows/` historically interpolated `${{ ... }}`
   values into shell `run:` blocks at evaluation time, including values
   derivable from PR metadata. The audit remediated specific instances;
   the risk class recurs on new workflow contributions. New `run:` blocks
   should consume PR-shaped or input-shaped values via `env:` rather
   than direct interpolation.

2. **Mutable references to external workflows and actions.**
   Reusable workflows or third-party actions referenced by branch or
   tag rather than commit SHA let upstream maintainers retroactively
   change the code that runs here, with access to this repo's secrets
   and the configured runner permissions. The audit produced SHA-pin
   fixes; re-introduction on new contributions is the recurring risk.

3. **`secrets: inherit` blast radius.**
   Reusable-workflow calls with `secrets: inherit` pass every
   repository secret to the called workflow — including the sccache
   GitHub token, AWS credentials, container-registry credentials, and
   any nightly-status tokens. The audit moved this repo toward
   explicit secret declaration; new callers must follow that pattern.

4. **Missing top-level `permissions:` blocks.**
   Workflows without an explicit top-level `permissions:` block
   receive a broader default `GITHUB_TOKEN` scope than most jobs
   need. The audit remediated specific workflows; new workflows
   should declare a minimal top-level `permissions:` block and only
   grant per-job elevations where required.

5. **sccache S3 cache poisoning.**
   The build pipeline routes C / C++ / CUDA compilations through
   sccache backed by `s3://rapids-sccache-devs`. A cache hit returns
   the previously-stored object file directly into the link. An
   attacker who obtains write access to the S3 bucket — through
   compromise of any user's AWS credentials issued by
   `setup_sccache_auth.sh`, or through a misconfigured bucket policy
   — can substitute object files that subsequent builds (anywhere
   that uses the cache) will silently incorporate. This is a
   build-time supply-chain attack with broad blast radius across
   RAPIDS engineering.

6. **Local credential storage.**
   `setup_sccache_auth.sh` writes a GitHub token and AWS credentials
   to `~/.sccache-auth/` (default) with default umask. Any process
   running as the engineer's user — including other CI tooling on a
   developer workstation, malicious dependencies installed into the
   user's environment, or a co-tenant on a shared dev box — can read
   those files for their 12-hour validity window. The script does
   not chmod the directory to `0700` itself.

7. **Build-time fetch of upstream sources.**
   The Velox, Presto, and Spark Gluten builds expect those projects'
   sources cloned as siblings. Workflows that perform the clone
   themselves do so over HTTPS, with the trust model that
   `github.com/{org}/{project}` is the authoritative source for
   each upstream. A compromise of any of those upstream
   repositories — or of `rapidsai/sccache` (the fork used as the
   cache binary source) — flows into the resulting test artifacts.

8. **CUDA compute-sanitizer runs surface bugs in upstream code.**
   `velox-compute-sanitizer-run.yaml` and the corresponding trigger
   workflow run CUDA compute-sanitizer against the Velox + cuDF
   integration. The findings are intentionally pre-disclosure
   information; the workflow run logs, the artifacts uploaded from
   the run, and any GitHub issues / PRs auto-created from results
   should be handled with the same care as any vulnerability
   triage product.

9. **Slurm scripts on shared HPC infrastructure.**
   The `*.slurm` scripts run on shared cluster resources. Job
   parameters that interpolate into shell context, paths that
   reference shared filesystems with permissive permissions, and
   credentials passed through Slurm's environment forwarding are
   all vectors common to HPC; new scripts should be reviewed with
   that lens.

## Critical Security Assumptions

The following are assumed of engineers, runner operators, and the
shared services this repository depends on. These are load-bearing —
violating them turns documented behavior into a vulnerability.

- **Engineer workstations protect `~/.sccache-auth/`.**
  The credentials written by `setup_sccache_auth.sh` are valid for
  12 hours and grant read/write access to the shared compile cache.
  Engineers should treat the directory like any credential store:
  set restrictive filesystem permissions, do not commit it to a
  repository, and rotate (re-run the script) if a workstation is
  ever shared or suspected compromised.

- **The S3 bucket policy enforces least privilege.**
  The integrity of every build that uses sccache rests on the
  `rapids-sccache-devs` bucket's access controls. Write access
  should be limited to authorized engineers and runners, and the
  bucket should have versioning and object-lock policies that
  permit forensic recovery if cache poisoning is discovered.

- **Caller workflows pin references by commit SHA.**
  Workflows in this repo that call third-party actions, and caller
  workflows that consume this repo's workflows, should pin to
  commit SHAs rather than tags or branches. Re-introduction of
  mutable refs reopens the audit-class supply-chain risk.

- **Workflow `run:` blocks consume inputs via `env:`.**
  Any value that originates from PR metadata, fork-supplied inputs,
  or other attacker-influenced context must reach a `run:` block
  through an `env:` mapping, not via direct `${{ ... }}` interpolation.

- **Reusable-workflow secret passing is explicit.**
  Callers should pass only the secrets a downstream workflow needs.
  `secrets: inherit` is a blast-radius multiplier and should not be
  reintroduced.

- **Top-level `permissions:` blocks are declared minimally.**
  GitHub's default `GITHUB_TOKEN` permissions are broader than most
  jobs need; workflows in this repo should declare a minimal
  top-level `permissions:` block and only grant per-job elevations
  where required.

- **Upstream sources are pinned per build.**
  Builds should consume specific commits of Velox, Presto, and
  Spark Gluten — not floating branches — so that a compromised
  upstream does not silently land in test artifacts.

- **Compute-sanitizer outputs are treated as pre-disclosure.**
  Findings produced by `velox-compute-sanitizer-run.yaml` may
  represent undisclosed memory-safety bugs. Logs, artifacts, and
  any automatically created issues should be reviewed and routed
  through PSIRT, not posted publicly.

- **Self-hosted runners do not retain state between jobs from
  forks.**
  Slurm and `nv-gha-runners` runners should not be reused across
  fork-PR jobs in a way that would let one PR's filesystem state
  influence another's build. Where caching is intentional
  (sccache), the cache's integrity controls are the load-bearing
  protection.

## Supported Versions

velox-testing follows a rolling-`main` model with periodic staging
branches created by `scripts/create_staging_branch.sh`. Security fixes
ship to `main`; staging branches receive backports only for the
specific RAPIDS release cut they support.

## Dependency Security

velox-testing depends on Docker, Bash, Python, the `nv-gha-runners`
runner controls, the `rapidsai/sccache` fork (binary source), AWS S3
(`rapids-sccache-devs`), GitHub Actions, Slurm, and the upstream
Velox / Presto / Spark Gluten projects. Upstream CVE-driven updates
should be applied as scripted version bumps; high-severity advisories
in sccache, Docker, or the runner OS may trigger out-of-band updates.
