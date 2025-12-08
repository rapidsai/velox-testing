# Velox / Presto Build Image Synchronization

This directory contains the automation that bridges GitHub Actions image builds and the
GitLab container registry. Two workflows are supported:

1. **Manual sync** – Trigger the build, inspect it with `gh_gitlab_sync.py`, then run
   `download → prepare → push → clean` yourself.
2. **Automated sync (daemon)** – Trigger the build and allow the daemon to detect the new tag and
   perform the full sync automatically.

> The `gh_gitlab_sync.py` tool must run on a machine that can reach both GitHub and the internal GitLab registry
> (i.e., typically a host inside the VPN). It should remain running to keep GitLab up to date.

The tooling assumes the standard repository layout in `rapidsai/velox-testing` and the
`presto-build-images.yml` workflow described below.

---

## Build Workflow (`presto-build-images.yml`)

The workflow accepts several inputs that control which commits and worker bundle to build.
A typical invocation for the feature branch `mattgara/presto-image-build` looks like:

```bash
gh workflow run presto-build-images.yml \
  --ref mattgara/presto-image-build \
  --field presto_repository=prestodb/presto \
  --field presto_commit=92865fbce0d2a056af5334a0afecc1f36853b657 \
  --field velox_repository=rapidsai/velox \
  --field velox_commit=65797d572e0a297fa898e4f6ab4c1afc75fe3419 \
  --field worker_bundle_mode=all_native \
  --field target_platform=linux/amd64 \
  --field runner_label=linux-amd64-cpu16 \
  --field set_velox_backward_compatible=false
```

### Worker Modes

- `single` (default): Builds only the worker specified via `worker_variant`.
- `all`: Builds coordinator + Java worker + native CPU worker + native GPU worker.
- `all_native`: Builds coordinator + native CPU worker + native GPU worker.

### Target Platforms

- `linux/amd64` (x86) and `linux/arm64` (ARM) are supported targets.
- The workflow uses BuildKit + QEMU to build ARM images on x86 GitHub runners, so you can request
  `linux/arm64` even if you run on the default x86 hardware (`runner_label=linux-amd64-cpu16`).
- The platform is included in the resulting GitHub image tag but is encoded in the **path** portion of
  the GitLab tag (`x86/<image>:<tag>` or `arm/<image>:<tag>`).

> **Warning:** When supplying `presto_commit` / `velox_commit`, use the full 40-character SHA. Short SHAs
> may not resolve when the workflow runs on GitHub-hosted runners.
> **Note:** Because this workflow currently resides on the branch `mattgara/presto-image-build`, the GitHub
> web UI does **not** show the “Run workflow” button. Use the `gh workflow run` command above (or cherry-pick
> the workflow onto your branch) to trigger builds.

### Workflow Inputs (Quick Reference)

| Input                         | Description                                                                                  |
|------------------------------|----------------------------------------------------------------------------------------------|
| `presto_repository`          | GitHub repo containing the Presto source (default `prestodb/presto`).                        |
| `presto_commit`              | Presto commit SHA/branch to build (use full SHA for reproducibility).                        |
| `velox_repository`           | GitHub repo containing the Velox source (default `rapidsai/velox`).                 |
| `velox_commit`               | Velox commit SHA/branch to build (use full SHA).                                             |
| `worker_variant`             | Worker to build when `worker_bundle_mode=single` (`native_cpu`, `native_gpu`, `java`).        |
| `worker_bundle_mode`         | Bundle mode (`single`, `all`, `all_native`).                                                 |
| `set_velox_backward_compatible` | Enables Velox backward-compatibility CMake option for native builds.                      |
| `target_platform`            | Target architecture (`linux/amd64` or `linux/arm64`).                                        |
| `runner_label`               | Desired GitHub runner label (defaults to GitHub-hosted x86 runner).                          |

> **Note:** Because this workflow currently resides on a non-default branch (`mattgara/presto-image-build`),
> the GitHub web UI will *not* show the “Run workflow” button. Use the `gh workflow run` command shown above.

### Workflow Inputs

- `presto_repository` / `velox_repository` — GitHub owner/repo for the source repositories.
- `presto_commit` / `velox_commit` — Full commit SHAs to build.
- `worker_bundle_mode` — Worker bundle selection (`single`, `all`, `all_native`).
- `worker_variant` — Worker variant when `worker_bundle_mode=single` (`native_cpu`, `native_gpu`, `java_worker`).
- `target_platform` — Desired output platform (`linux/amd64` or `linux/arm64`).
- `runner_label` — GitHub runner label (e.g., `linux-amd64-cpu16`).
- `set_velox_backward_compatible` — Whether to enable the Velox backward compatibility flag (boolean).

### Artifacts and Metadata

- Successful runs upload a single archive (`docker save | gzip`) that contains one tarball per image.
- The workflow logs print a *metadata block* with the Presto/Velox SHAs, target platform, build mode,
  and the derived **image tag** (e.g. `presto-<presto sha prefix>-velox-<velox sha prefix>-linux-amd64`).
- `gh_gitlab_sync.py` relies entirely on this metadata block when cross-referencing images or pushing to GitLab.

---

## `gh_gitlab_sync.py` Overview

The Python tool requires:

- The GitHub CLI (`gh`) installed and available on `PATH`. Authenticate via `gh auth login`, or supply
  an API token with `--gh-token` / `GH_TOKEN` / `GITHUB_TOKEN`.
- A GitLab registry token (`GITLAB_TOKEN` or the `--gitlab-token` flag) with
  `read_registry` permission at minimum; pushing also needs `write_registry`.
- Optional Docker registry credentials provided via `--registry-username` /
  `--registry-password` or corresponding environment variables. If omitted, Docker login uses
  the GitLab token.

### Subcommands

| Subcommand      | Purpose                                                                                          |
|-----------------|--------------------------------------------------------------------------------------------------|
| `gh-list`       | List recent GitHub workflow runs with parsed metadata.                                           |
| `gitlab-list`   | List repositories/tags currently in the GitLab container registry.                               |
| `xref`          | Cross-reference which GitHub builds are missing from GitLab (and optionally which tags are extra).|
| `download`      | Download the artifact archive for a specific image tag.                                          |
| `prepare`       | Unpack a previously downloaded archive, verify tarballs, and build an upload plan.               |
| `push`          | Load the tarballs into Docker and push them to the GitLab registry.                              |
| `clean`         | Remove all local data (artifacts, extracted tarballs, metadata) for a specific image tag.        |
| `daemon`        | Periodically run xref → download → prepare → push → clean to keep GitLab in sync.                |
| `retag`         | Point a new GitLab tag (e.g. `latest`) at an existing manifest without re-uploading layers.      |

### Basic Listing

```bash
python3 tools/gh_gitlab_sync.py gh-list \
  --limit 10 \
  --gh-token "$GH_TOKEN"

python3 tools/gh_gitlab_sync.py gitlab-list \
  --gitlab-token "$GITLAB_TOKEN"
```

The **GitHub listing** shows each run’s metadata block; look for the line
`Image tag: <value>`—that tag is the canonical identifier used throughout the tooling. The same tag is
also printed in the `xref` output when a run is missing from GitLab. The **GitLab listing** shows
available repositories and their tags (including size and creation date when available).

### Cross-Reference (`xref`)

```bash
python3 tools/gh_gitlab_sync.py xref \
  --gh-token "$GH_TOKEN" \
  --gitlab-token "$GITLAB_TOKEN"
```

`xref` reports:

- **Missing tags**: GitHub runs that produced images not yet present in GitLab.
- **Extra tags** (optional with `--list-extra`): GitLab tags with no corresponding GitHub run.

> **Note:** If more than one GitHub run shares the same image tag, the script always takes the most recent
> successful run.

---

## Manual GitHub → GitLab Transfer

To copy images manually:

1. **Download the artifact**
   ```bash
   python3 tools/gh_gitlab_sync.py download <image-tag> \
     --output-dir downloads \
     --gh-token "$GH_TOKEN"
   ```
   Downloads the GitHub Actions artifact (typically ~11 GiB for the full bundle), records the workflow
   metadata in `downloads/<image-tag>/metadata.json`, and stores the original archive for later steps.

2. **Prepare the extracted tarballs**
   ```bash
   python3 tools/gh_gitlab_sync.py prepare <image-tag> \
     --downloads-dir downloads
   ```
   Extracts the archive into `downloads/<image-tag>/extracted/`, verifies that all expected tarballs are
   present, and writes `upload-plan.json` describing which local files map to which GitLab tags.

3. **Push to GitLab**
   ```bash
   python3 tools/gh_gitlab_sync.py push <image-tag> \
     --downloads-dir downloads \
     --gitlab-token "$GITLAB_TOKEN" \
     --registry gitlab-master.nvidia.com:5005 \
     --registry-username <user> \
  [--retag-as-latest] \
     --verbose \
     --yes
   ```

   - `push` performs `docker login`, `docker load`, `docker tag`, and `docker push` for each tarball.
   - It skips tags already present in GitLab unless `--force` is supplied.
- Add `--retag-as-latest` if you want the tool to tag and push `<repo>:latest` alongside the commit-specific tag.
   - Docker command output (including layer progress) is streamed to stdout.

4. **Clean local files**
   ```bash
   python3 tools/gh_gitlab_sync.py clean <image-tag> \
     --downloads-dir downloads \
     --gh-token "$GH_TOKEN" \
     --yes
   ```
   Removes the downloaded archive, extracted tarballs, and any run-specific metadata, keeping the
   workspace tidy for future syncs.

### Retagging Existing Images

If you already have an image in GitLab and want to add another tag (e.g. set `latest` to point at a specific build)
without re-uploading layers, use the `retag` subcommand:

```bash
python3 tools/gh_gitlab_sync.py retag \
  x86/presto-coordinator:presto-92865fbce0d2-velox-65797d572e0a \
  x86/presto-coordinator:latest \
  --registry gitlab-master.nvidia.com:5005 \
  --registry-project hercules/veloxtesting \
  --gitlab-token "$GITLAB_TOKEN" \
  --registry-username <user> \
  --registry-password "$GITLAB_TOKEN"
```

Behind the scenes the tool runs `docker pull`, `docker tag`, and `docker push` using the existing credentials,
so only the manifest is re-tagged—layers are not duplicated.

> **Tagging behavior:** Docker (and OCI registries such as GitLab) treat each tag as a pointer to an image
> manifest. Multiple tags can reference the same manifest, which is how “soft links” such as `latest` work.
> Retagging does **not** remove the original tag; it simply creates or updates another tag to reference the
> same manifest digest. Use this subcommand to add tags like `latest` without rebuilding or re-uploading
> the image layers.

---

## Daemon Mode

Daemon mode automates the entire pipeline. It continuously:

1. Calls `xref` to determine which tags are missing.
2. For each missing tag: `download` → `prepare` → `push` → `clean`.
3. Sleeps for the configured interval and repeats.

```bash
python3 tools/gh_gitlab_sync.py daemon \
  --downloads-dir downloads \
  --interval 900 \
  --repo rapidsai/velox-testing \
  --workflow .github/workflows/presto-build-images.yml \
  --gitlab-token "$GITLAB_TOKEN" \
  --registry gitlab-master.nvidia.com:5005 \
  --registry-username <user> \
  [--retag-as-latest] \
  --verbose
```

Recommended first run with `--once` to verify credentials and workflow:

```bash
python3 tools/gh_gitlab_sync.py daemon \
  --downloads-dir downloads \
  --once \
  --interval 900 \
  --gitlab-token "$GITLAB_TOKEN" \
  --gh-token "$GH_TOKEN" \
  --registry gitlab-master.nvidia.com:5005 \
  --registry-username <user>
```

Daemon mode is non-interactive: it automatically accepts pushes, skips already-present tags (unless `--force-push` is set),
and cleans local files after a successful upload.

---

## CLI Option Reference

### Common GitHub Options

- `--repo` (default `rapidsai/velox-testing`) — Owner/repo of the workflow.
- `--workflow` (default `.github/workflows/presto-build-images.yml`) — Workflow filename.
- `--branch` — Filter runs to this branch (optional).
- `--limit` (default 20) — Maximum runs to inspect.
- `--gh-token` — GitHub token; otherwise rely on `gh auth login` or `GH_TOKEN`/`GITHUB_TOKEN`.

### GitLab & Registry Options

- `--gitlab-host` (default `gitlab-master.nvidia.com`) — GitLab server.
- `--gitlab-project` (default `hercules/veloxtesting`) — Project containing the registry.
- `--gitlab-token` — GitLab personal/deploy token (or `GITLAB_TOKEN` env). Requires `read_api`
  and `read_registry`; pushing also needs `write_registry`.
- `--registry` (default `gitlab-master.nvidia.com:5005`) — Registry hostname.
- `--registry-username` — Docker username (`GITLAB_REGISTRY_USERNAME` / `CI_REGISTRY_USER` fallback).
- `--registry-password` — Docker password/token (`GITLAB_REGISTRY_PASSWORD` / `CI_REGISTRY_PASSWORD` / GitLab token fallback).

### Command-Specific Flags

- `download`: `--output-dir`, `--force`.
- `prepare`: `--downloads-dir`, `--clean`.
- `push`: `--downloads-dir`, `--skip-login`, `--force`, `--yes`, `--verbose`, `--retag-as-latest`, `--registry-project`.
- `clean`: `--downloads-dir`, `--yes` (non-interactive).
- `daemon`: `--downloads-dir`, `--interval`, `--once`, `--skip-login`, `--force-push`, `--retag-as-latest`, `--verbose`.  
  (Automatically enables `--yes` when calling `push`/`clean`).
- `retag`: `--registry`, `--gitlab-host`, `--gitlab-token`, `--registry-username`, `--registry-password`, `--registry-project`.

---

## Tag Naming Conventions

- **GitHub metadata** encodes architecture in the image tag itself (e.g.
  `presto-…-velox-…-linux-amd64`).
- **GitLab tags** drop the platform suffix; architecture is encoded in the repository path
  (`x86/presto-coordinator:<tag>` or `arm/presto-coordinator:<tag>`).
- The Python tool parses the GitHub tag, normalizes it for GitLab, and maps back to the full metadata
  when performing cross-references or pushes.

By following the workflow and using the provided tooling, you can reliably mirror GitHub Actions
build artifacts into the GitLab registry with minimal manual intervention.

