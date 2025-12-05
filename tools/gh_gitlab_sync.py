#!/usr/bin/env python3
"""
Utility helpers for inspecting GitHub workflow runs and GitLab registry tags.

Current features:
  - gh-list      : list completed workflow runs with parsed metadata
  - gitlab-list  : list container images and tags in a GitLab project registry
  - xref         : cross reference expected images from GitHub runs with tags found in GitLab
  - download     : fetch artifacts for a specific image tag
  - prepare      : extract and validate artifacts, generating an upload plan
  - push         : load the prepared tarballs and push them to the GitLab registry
  - clean        : remove downloaded artifacts and prepared data for a tag
  - daemon       : periodically mirror missing GitHub builds into the GitLab registry

The script relies on the GitHub CLI (`gh`) for authenticated API calls so that
users can keep using their existing login session (`gh auth login`). For GitLab
calls, provide a personal or deploy token via `--gitlab-token` or the
environment variable `GITLAB_TOKEN`.

Example usage:

    # list recent successful runs (default workflow)
    python tools/gh_gitlab_sync.py gh-list --limit 10

    # list registry tags in GitLab
    python tools/gh_gitlab_sync.py gitlab-list --gitlab-project hercules/veloxtesting

    # find runs that produced images not yet present in the registry
    python tools/gh_gitlab_sync.py xref
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import textwrap
import zipfile
from pathlib import Path
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from urllib.parse import quote


DEFAULT_GITHUB_REPO = "rapidsai/velox-testing"
DEFAULT_WORKFLOW_FILE = ".github/workflows/presto-build-images.yml"
DEFAULT_GITLAB_HOST = "gitlab-master.nvidia.com"
DEFAULT_GITLAB_PROJECT = "hercules/veloxtesting"
DEFAULT_GITLAB_REGISTRY = "gitlab-master.nvidia.com:5005"

METADATA_KEYS = [
    "Presto repository",
    "Presto ref",
    "Velox repository",
    "Velox ref",
    "Target platform",
    "Worker bundle mode",
    "Build native CPU",
    "Build native GPU",
    "Build Java worker",
    "Image tag",
    "Registry push",
    "Generated at",
]


_GH_TOKEN: Optional[str] = None


def run_gh(args: List[str], capture_bytes: bool = False) -> subprocess.CompletedProcess:
    """Execute a GitHub CLI command, raising on failure."""
    cmd = ["gh"] + args
    stdout_option = subprocess.PIPE
    env = os.environ.copy()
    if _GH_TOKEN:
        env.setdefault("GH_TOKEN", _GH_TOKEN)
        env.setdefault("GITHUB_TOKEN", _GH_TOKEN)
    result = subprocess.run(
        cmd,
        stdout=stdout_option,
        stderr=subprocess.PIPE,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="ignore").strip()
        raise RuntimeError(f"Command {' '.join(cmd)} failed: {stderr}")
    return result


def gh_api_json(path: str, params: Optional[Iterable[Tuple[str, str]]] = None) -> dict:
    """Call `gh api` and parse JSON response."""
    cmd = ["api", path]
    if params:
        for key, value in params:
            cmd.extend(["-f", f"{key}={value}"])
    result = run_gh(cmd)
    return json.loads(result.stdout.decode("utf-8"))


def fetch_workflow_runs(
    repo: str,
    workflow: str,
    branch: Optional[str],
    limit: int,
) -> List[dict]:
    """Fetch workflow runs using the gh CLI (supports branch-scoped workflows)."""
    cmd = [
        "run",
        "list",
        "--workflow",
        workflow,
        "--limit",
        str(max(limit, 1)),
        "--json",
        ",".join(
            [
                "databaseId",
                "name",
                "displayTitle",
                "conclusion",
                "status",
                "createdAt",
                "updatedAt",
                "startedAt",
                "headBranch",
                "headSha",
                "event",
                "number",
                "url",
            ]
        ),
        "--repo",
        repo,
    ]
    if branch:
        cmd.extend(["--branch", branch])

    result = run_gh(cmd)
    runs = json.loads(result.stdout.decode("utf-8"))
    return runs


def download_run_logs(repo: str, run_id: int) -> bytes:
    """Download zipped logs for a workflow run."""
    result = run_gh(
        ["api", f"repos/{repo}/actions/runs/{run_id}/logs"],
        capture_bytes=True,
    )
    return result.stdout


def parse_metadata_from_logs(log_zip: bytes) -> Dict[str, str]:
    """Extract metadata lines from the workflow logs."""
    metadata: Dict[str, str] = {}
    with zipfile.ZipFile(io.BytesIO(log_zip)) as zf:
        for name in zf.namelist():
            with zf.open(name) as fh:
                for raw_line in fh:
                    try:
                        line = raw_line.decode("utf-8", errors="ignore").strip()
                    except UnicodeDecodeError:
                        continue
                    if not line:
                        continue
                    parts = line.split("\t")
                    content = parts[-1].strip() if len(parts) > 1 else line
                    for key in METADATA_KEYS:
                        if key in content:
                            start = content.find(key)
                            segment = content[start:]
                            key_parts = segment.split(":", 1)
                            if len(key_parts) == 2:
                                value = key_parts[1].strip()
                                metadata[key] = value
    return metadata


def summarize_run(run: dict) -> str:
    """Return a short textual summary for a workflow run."""
    started = run.get("startedAt") or run.get("createdAt")
    return (
        f"{run['databaseId']}: {run['displayTitle']} · branch={run['headBranch']} "
        f"· conclusion={run['conclusion']} · started={started}"
    )


def cmd_gh_list(args: argparse.Namespace) -> None:
    runs = fetch_workflow_runs(
        repo=args.repo,
        workflow=args.workflow,
        branch=args.branch,
        limit=args.limit,
    )
    if not runs:
        print("No workflow runs found.")
        return

    print(f"Found {len(runs)} workflow runs")
    print("-" * 80)
    for run in runs:
        if not args.include_failed and run.get("conclusion") != "success":
            continue
        print(summarize_run(run))
        try:
            logs = download_run_logs(args.repo, run["databaseId"])
            metadata = parse_metadata_from_logs(logs)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"  (metadata unavailable: {exc})")
            print("-" * 80)
            continue

        if not metadata:
            print("  (no metadata lines found in logs)")
            print("-" * 80)
            continue

        for key in METADATA_KEYS:
            if key in metadata:
                print(f"  {key:<18}: {metadata[key]}")
        print("-" * 80)


def ensure_requests() -> "module":
    try:
        import requests  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "The 'requests' package is required for GitLab interactions. "
            "Install it with 'pip install requests'."
        ) from exc
    return requests


def gitlab_api_json(
    requests_module,
    host: str,
    path: str,
    token: str,
    params: Optional[dict] = None,
) -> dict:
    url = f"https://{host}{path}"
    headers = {"PRIVATE-TOKEN": token}
    response = requests_module.get(url, headers=headers, params=params, timeout=30)
    if response.status_code >= 400:
        raise RuntimeError(f"GitLab API call failed ({response.status_code}): {response.text}")
    return response.json()


def cmd_gitlab_list(args: argparse.Namespace) -> None:
    requests_module = ensure_requests()
    token = args.gitlab_token or os.environ.get("GITLAB_TOKEN")
    if not token:
        raise RuntimeError("GitLab token not provided. Use --gitlab-token or set GITLAB_TOKEN.")

    project_path = args.gitlab_project or DEFAULT_GITLAB_PROJECT
    encoded_project = project_path.replace("/", "%2F")

    repos = gitlab_api_json(
        requests_module,
        args.gitlab_host,
        f"/api/v4/projects/{encoded_project}/registry/repositories",
        token,
        params={"per_page": 100},
    )
    if not repos:
        print("No GitLab registry repositories found.")
        return

    for repo in repos:
        repo_id = repo["id"]
        repo_name = repo.get("name") or repo.get("path")
        print(f"Repository: {repo_name} (id={repo_id})")
        tags = gitlab_api_json(
            requests_module,
            args.gitlab_host,
            f"/api/v4/projects/{encoded_project}/registry/repositories/{repo_id}/tags",
            token,
            params={"per_page": 100},
        )
        if not tags:
            print("  (no tags)")
        else:
            for tag in tags:
                created = tag.get("created_at", "?")
                size = tag.get("total_size", 0) or 0
                size_gb = size / (1024 ** 3)
                print(f"  {tag['name']:<40}  size={size_gb:6.2f} GiB  created={created}")
        print("-" * 80)


def expected_gitlab_tags_from_metadata(metadata: Dict[str, str]) -> List[str]:
    """
    Given metadata extracted from a workflow run, return the expected GitLab tags.
    """
    image_tag = metadata.get("Image tag")
    if not image_tag:
        return []
    platform = (metadata.get("Target platform") or "").lower()
    normalized_tag = normalize_gitlab_image_tag(image_tag, platform)
    if "arm" in platform:
        arch_prefix = "arm"
    elif "amd" in platform or "x86" in platform:
        arch_prefix = "x86"
    else:
        arch_prefix = ""

    def full_name(repo: str) -> str:
        return f"{arch_prefix}/{repo}:{normalized_tag}" if arch_prefix else f"{repo}:{normalized_tag}"

    tags = [
        full_name("presto-coordinator"),
    ]
    if metadata.get("Build Java worker", "").lower() == "true":
        tags.append(full_name("presto-java-worker"))
    if metadata.get("Build native CPU", "").lower() == "true":
        tags.append(full_name("presto-native-worker-cpu"))
    if metadata.get("Build native GPU", "").lower() == "true":
        tags.append(full_name("presto-native-worker-gpu"))
    return tags


def normalize_gitlab_image_tag(image_tag: str, platform: str) -> str:
    """
    Remove the platform suffix (e.g. '-linux-amd64') from the image tag when present so
    GitLab tags rely on the registry path for architecture differentiation.
    """
    if not image_tag or not platform:
        return image_tag
    normalized_platform = platform.lower().replace("/", "-")
    suffix = f"-{normalized_platform}"
    if normalized_platform and image_tag.endswith(suffix):
        return image_tag[: -len(suffix)]
    return image_tag


def format_command(cmd: List[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def docker_load_archive(archive_path: Path, verbose: bool) -> Tuple[List[str], str]:
    """
    Load a docker archive (gzip or plain tar) and return the tags reported by docker load.
    Progress from docker is streamed directly to stdout.
    """
    if not archive_path.exists():
        raise RuntimeError(f"Archive {archive_path} does not exist.")

    if not tarfile.is_tarfile(archive_path):
        raise RuntimeError(
            f"Archive {archive_path} appears to be corrupted or incomplete (not a valid tar file). "
            "Re-run the prepare step to extract the artifacts again."
        )

    cmd = ["docker", "load", "--input", str(archive_path)]
    if verbose:
        print("    $ " + format_command(cmd))
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    output_lines: List[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        print("      " + line.rstrip())
        output_lines.append(line)
    process.wait()
    if process.returncode:
        raise subprocess.CalledProcessError(process.returncode, cmd)

    output = "".join(output_lines)
    tags: List[str] = []
    for line in output.splitlines():
        line = line.strip()
        if "Loaded image:" in line:
            tags.append(line.split("Loaded image:", 1)[1].strip())
        elif "Loaded image ID:" in line:
            tags.append(line.split("Loaded image ID:", 1)[1].strip())
    return tags, output


def run_command(cmd: List[str], verbose: bool) -> None:
    if verbose:
        print("    $ " + format_command(cmd))
    subprocess.run(cmd, check=True)


def docker_login(registry: str, username: str, password: str, verbose: bool) -> None:
    cmd = ["docker", "login", registry, "-u", username, "--password-stdin"]
    if verbose:
        print("    $ " + format_command(cmd))
    subprocess.run(
        cmd,
        input=password.encode("utf-8"),
        stdout=None,
        stderr=None,
        check=True,
    )


def build_gitlab_repo_lookup(
    repos: Iterable[dict],
    project_path: str,
    registry_host: str,
) -> Dict[str, dict]:
    lookup: Dict[str, dict] = {}
    registry_host = registry_host.rstrip("/")
    for repo in repos:
        candidates = set()
        for key in ("path", "name"):
            value = repo.get(key)
            if value:
                candidates.add(value)

        location = repo.get("location")
        if location:
            loc = location
            if loc.startswith("http://") or loc.startswith("https://"):
                loc = loc.split("://", 1)[1]
            if registry_host and loc.startswith(registry_host + "/"):
                candidates.add(loc[len(registry_host) + 1 :])
            if "/" in loc:
                candidates.add(loc.split("/", 1)[1])

        for value in list(candidates):
            if project_path and value.startswith(project_path + "/"):
                candidates.add(value[len(project_path) + 1 :])

        for value in candidates:
            lookup.setdefault(value, repo)
    return lookup


def compose_registry_ref(registry: str, project: str, gitlab_tag: str) -> str:
    registry = registry.rstrip("/")
    project = project.strip("/")
    return f"{registry}/{project}/{gitlab_tag}"


def prompt_yes_no(prompt: str) -> bool:
    while True:
        try:
            response = input(prompt)
        except EOFError:
            return False
        answer = response.strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no", ""):
            return False
        print("Please respond with 'y' or 'n'.")


def find_run_metadata_for_tag(
    repo: str,
    workflow: str,
    branch: Optional[str],
    limit: int,
    image_tag: str,
) -> Tuple[dict, Dict[str, str]]:
    runs = fetch_workflow_runs(repo=repo, workflow=workflow, branch=branch, limit=limit)
    matches: List[Tuple[dict, Dict[str, str]]] = []
    errors: List[Tuple[dict, Exception]] = []

    for run in runs:
        if run.get("conclusion") != "success":
            continue
        try:
            logs = download_run_logs(repo, run["databaseId"])
            metadata = parse_metadata_from_logs(logs)
        except Exception as exc:  # pylint: disable=broad-except
            errors.append((run, exc))
            continue
        if metadata.get("Image tag") == image_tag:
            matches.append((run, metadata))

    if not matches:
        if errors:
            error_summaries = ", ".join(
                f"{run.get('databaseId')}: {type(exc).__name__}" for run, exc in errors
            )
            raise RuntimeError(
                f"No successful workflow run found with image tag '{image_tag}'. "
                f"Encountered errors while inspecting runs: {error_summaries}"
            ) from errors[0][1]
        raise RuntimeError(f"No successful workflow run found with image tag '{image_tag}'.")


def compute_registry_sync_state(
    repo: str,
    workflow: str,
    branch: Optional[str],
    limit: int,
    gitlab_host: str,
    gitlab_project: str,
    gitlab_token: str,
) -> Dict[str, Any]:
    runs = fetch_workflow_runs(
        repo=repo,
        workflow=workflow,
        branch=branch,
        limit=limit,
    )
    gh_tags: Dict[str, Dict[str, str]] = {}
    runs_by_image: Dict[str, List[str]] = {}
    for run in runs:
        if run.get("conclusion") != "success":
            continue
        try:
            logs = download_run_logs(repo, run["databaseId"])
            metadata = parse_metadata_from_logs(logs)
        except Exception:  # pylint: disable=broad-except
            continue
        tags = expected_gitlab_tags_from_metadata(metadata)
        if not tags:
            continue
        canonical_metadata = metadata.copy()
        canonical_metadata["_tags"] = tags
        canonical_metadata["_run_display"] = run.get("displayTitle")
        canonical_metadata["_run_id"] = run.get("databaseId")
        canonical_metadata["_run_info"] = {
            "databaseId": run.get("databaseId"),
            "displayTitle": run.get("displayTitle"),
            "headBranch": run.get("headBranch"),
            "conclusion": run.get("conclusion"),
            "htmlUrl": run.get("url"),
        }
        canonical_metadata["_run_branch"] = run.get("headBranch")
        canonical_metadata["_run_conclusion"] = run.get("conclusion")
        canonical_metadata["run_id"] = run.get("databaseId")
        canonical_metadata["run_name"] = run.get("displayTitle")
        for tag in tags:
            gh_tags[tag] = canonical_metadata
        key = metadata.get("Image tag", "unknown")
        runs_by_image.setdefault(key, []).append(run["displayTitle"])

    requests_module = ensure_requests()
    encoded_project = gitlab_project.replace("/", "%2F")
    repos = gitlab_api_json(
        requests_module,
        gitlab_host,
        f"/api/v4/projects/{encoded_project}/registry/repositories",
        gitlab_token,
        params={"per_page": 100},
    )

    gitlab_tags: Dict[str, dict] = {}
    for repo_entry in repos:
        repo_id = repo_entry["id"]
        repo_name = repo_entry.get("name") or repo_entry.get("path")
        tags = gitlab_api_json(
            requests_module,
            gitlab_host,
            f"/api/v4/projects/{encoded_project}/registry/repositories/{repo_id}/tags",
            gitlab_token,
            params={"per_page": 100},
        )
        for tag in tags or []:
            gitlab_tags[f"{repo_name}:{tag['name']}"] = tag

    missing_on_gitlab = sorted(set(gh_tags) - set(gitlab_tags))
    extra_on_gitlab = sorted(set(gitlab_tags) - set(gh_tags))

    grouped_missing: Dict[str, Dict[str, Any]] = {}
    for tag in missing_on_gitlab:
        metadata = gh_tags[tag]
        image_tag = metadata.get("Image tag", "unknown")
        info = grouped_missing.setdefault(
            image_tag,
            {
                "metadata": metadata,
                "tags": [],
                "runs": runs_by_image.get(image_tag, []),
            },
        )
        info["tags"].append(tag)

    return {
        "gh_tags": gh_tags,
        "gitlab_tags": gitlab_tags,
        "missing_grouped": grouped_missing,
        "extra_on_gitlab": extra_on_gitlab,
        "runs_by_image": runs_by_image,
    }


def cmd_xref(args: argparse.Namespace) -> None:
    token = args.gitlab_token or os.environ.get("GITLAB_TOKEN")
    if not token:
        raise RuntimeError("GitLab token not provided. Use --gitlab-token or set GITLAB_TOKEN.")

    state = compute_registry_sync_state(
        repo=args.repo,
        workflow=args.workflow,
        branch=args.branch,
        limit=args.limit,
        gitlab_host=args.gitlab_host,
        gitlab_project=args.gitlab_project or DEFAULT_GITLAB_PROJECT,
        gitlab_token=token,
    )

    print(f"Collected {len(state['gh_tags'])} expected image tags from GitHub runs.")
    print(f"Found {len(state['gitlab_tags'])} tags in GitLab registry.")

    grouped = state["missing_grouped"]
    if grouped:
        print("\nImages present in GitHub runs but missing on GitLab:")
        for image_tag, info in grouped.items():
            metadata = info["metadata"]
            print(f"  Image tag: {image_tag}")
            print(f"    Presto ref : {metadata.get('Presto ref', '?')}")
            print(f"    Velox ref  : {metadata.get('Velox ref', '?')}")
            print(f"    Platform   : {metadata.get('Target platform', '?')}")
            if info.get("runs"):
                print(f"    Runs       : {', '.join(info['runs'])}")
            for tag in sorted(info["tags"]):
                print(f"    missing tag: {tag}")
    else:
        print("\nNo missing GitLab images.")

    extra_on_gitlab = state["extra_on_gitlab"]
    if extra_on_gitlab and args.list_extra:
        print("\nImages on GitLab with no matching GitHub run:")
        for tag in extra_on_gitlab:
            print(f"  {tag}")

    def sort_key(item: Tuple[dict, Dict[str, str]]) -> str:
        run = item[0]
        return run.get("startedAt") or run.get("createdAt") or ""

    matches.sort(key=sort_key, reverse=True)

    if len(matches) > 1:
        run_ids = ", ".join(str(item[0]["databaseId"]) for item in matches)
        print(
            f"WARNING: Found {len(matches)} successful runs with image tag '{image_tag}'. "
            f"Using the most recent run (IDs: {run_ids})."
        )

    return matches[0]


def download_artifact_with_progress(url: str, token: str, dest_path: str) -> None:
    requests_module = ensure_requests()
    headers = {"Authorization": f"token {token}"}
    with requests_module.get(url, headers=headers, stream=True, timeout=60) as response:
        if response.status_code >= 400:
            raise RuntimeError(f"Failed to download artifact: {response.status_code} {response.text}")
        total = int(response.headers.get("Content-Length", "0"))
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        downloaded = 0
        chunk_size = 1024 * 1024
        with open(dest_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        percent = downloaded * 100 / total
                        mb_downloaded = downloaded / (1024 ** 2)
                        mb_total = total / (1024 ** 2)
                        sys.stdout.write(
                            f"\rDownloading {mb_downloaded:8.2f}/{mb_total:8.2f} MiB ({percent:5.1f}%)"
                        )
                        sys.stdout.flush()
        if total:
            sys.stdout.write("\n")
        print(f"Saved artifact to {dest_path}")


def cmd_download(args: argparse.Namespace) -> None:
    if not _GH_TOKEN:
        raise RuntimeError("GitHub token required for download. Provide --gh-token or set GH_TOKEN.")

    steps_total = 5

    def log(step: int, message: str) -> None:
        print(f"[{step}/{steps_total}] {message}")

    precomputed_run = getattr(args, "precomputed_run", None)
    precomputed_metadata = getattr(args, "precomputed_metadata", None)

    if precomputed_metadata:
        metadata = {**precomputed_metadata}
    if precomputed_run and precomputed_metadata:
        log(1, f"Using cached workflow run for image tag '{args.image_tag}' ...")
        run = precomputed_run
        metadata = {**metadata, **precomputed_metadata}
    else:
        log(1, f"Locating workflow run for image tag '{args.image_tag}' ...")
        run, metadata = find_run_metadata_for_tag(
            repo=args.repo,
            workflow=args.workflow,
            branch=args.branch,
            limit=args.limit,
            image_tag=args.image_tag,
        )

    print(
        f"        → run {run.get('databaseId')} ({run.get('displayTitle')}), "
        f"Presto={metadata.get('Presto ref', '?')}, Velox={metadata.get('Velox ref', '?')}, "
        f"platform={metadata.get('Target platform', '?')}"
    )

    log(2, "Ensuring download directory exists ...")
    tag_dir = os.path.join(args.output_dir, args.image_tag)
    os.makedirs(tag_dir, exist_ok=True)

    metadata_path = os.path.join(tag_dir, "metadata.json")

    log(3, "Retrieving artifact metadata from GitHub ...")
    artifacts = gh_api_json(f"repos/{args.repo}/actions/runs/{run['databaseId']}/artifacts")
    artifact_list = artifacts.get("artifacts", [])
    if not artifact_list:
        raise RuntimeError("No artifacts found for the selected run.")

    artifact = artifact_list[0]
    name = artifact["name"]
    url = artifact["archive_download_url"]
    expected_size = artifact.get("size_in_bytes", 0) or 0
    output_name = f"{args.image_tag}.zip"
    dest_path = os.path.join(tag_dir, output_name)

    if os.path.exists(dest_path):
        if not args.force:
            raise RuntimeError(
                f"Download target {dest_path} already exists. "
                "Delete the file or re-run with --force to overwrite."
            )
        os.remove(dest_path)

    log(4, f"Downloading artifact '{name}' (~{expected_size / (1024 ** 2):.2f} MiB) to {dest_path} ...")
    download_artifact_with_progress(url, _GH_TOKEN, dest_path)

    log(5, f"Writing metadata to {metadata_path} ...")
    metadata_record = dict(metadata)
    metadata_record["run_id"] = run.get("databaseId")
    metadata_record["run_name"] = run.get("displayTitle")
    metadata_record["repository"] = args.repo
    metadata_record["artifact_name"] = name
    metadata_record["artifact_size_bytes"] = expected_size
    metadata_record["artifact_zip"] = output_name
    if precomputed_run:
        metadata_record["_run_info"] = precomputed_run
        metadata_record["_run_branch"] = precomputed_run.get("headBranch")
        metadata_record["_run_conclusion"] = precomputed_run.get("conclusion")
    with open(metadata_path, "w", encoding="utf-8") as fh:
        json.dump(metadata_record, fh, indent=2, sort_keys=True)
    print("Download complete.")


def cmd_prepare(args: argparse.Namespace) -> None:
    steps_total = 6

    def log(step: int, message: str) -> None:
        print(f"[{step}/{steps_total}] {message}")

    tag_dir = Path(args.downloads_dir) / args.image_tag
    log(1, f"Verifying download directory at {tag_dir} ...")
    if not tag_dir.exists():
        raise RuntimeError(f"Download directory {tag_dir} not found. Run the download step first.")

    metadata_path = tag_dir / "metadata.json"
    if not metadata_path.exists():
        raise RuntimeError(f"Metadata file not found at {metadata_path}.")

    log(2, f"Loading metadata from {metadata_path} ...")
    with metadata_path.open("r", encoding="utf-8") as fh:
        metadata = json.load(fh)

    run_id = metadata.get("run_id")
    run_name = metadata.get("run_name")
    if run_id:
        descriptor = f"{run_id}"
        if run_name:
            descriptor += f" ({run_name})"
        print(f"Preparing artifacts from run {descriptor}.")
    else:
        print("WARNING: metadata does not include run_id; re-run download if you need run provenance.")

    image_tag = metadata.get("Image tag")
    if image_tag and image_tag != args.image_tag:
        print(
            f"WARNING: metadata tag '{image_tag}' does not match requested tag '{args.image_tag}'. "
            "Continuing with requested tag."
        )

    log(3, "Selecting artifact archive ...")
    preferred_zip = tag_dir / f"{args.image_tag}.zip"
    artifact_zip: Optional[Path] = None
    if preferred_zip.exists():
        artifact_zip = preferred_zip
        print(f"        → using archive {preferred_zip.name}")
    else:
        artifact_zips = sorted(tag_dir.glob("*.zip"))
        if not artifact_zips:
            raise RuntimeError(f"No artifact zip found in {tag_dir}.")
        artifact_zip = artifact_zips[0]
        if len(artifact_zips) > 1:
            print(
                "WARNING: Multiple zip files found; using "
                f"{artifact_zip.name}. Remove the extras or use --downloads-dir with a clean folder."
            )
        if artifact_zip != preferred_zip:
            print(
                f"WARNING: Expected archive named {preferred_zip.name}, "
                f"but using {artifact_zip.name}. Consider re-running download."
            )
        print(f"        → using archive {artifact_zip.name}")

    extract_dir = tag_dir / "extracted"
    if extract_dir.exists():
        if args.clean:
            log(4, f"Removing existing extraction directory {extract_dir} ...")
            shutil.rmtree(extract_dir)
        else:
            log(4, f"Extraction directory {extract_dir} already exists; reusing existing contents.")
    if not extract_dir.exists():
        log(4, f"Extracting {artifact_zip.name} to {extract_dir} ...")
        try:
            with zipfile.ZipFile(artifact_zip) as zf:
                bad_file = zf.testzip()
                if bad_file:
                    raise RuntimeError(
                        f"Archive {artifact_zip} appears corrupted (first bad file: {bad_file}). "
                        "Re-run download with --force."
                    )
                zf.extractall(extract_dir)
        except zipfile.BadZipFile as exc:
            raise RuntimeError(
                f"Archive {artifact_zip} is not a valid zip file. Re-run download with --force."
            ) from exc

    if not image_tag:
        raise RuntimeError("Metadata is missing 'Image tag'.")

    log(5, "Validating extracted tarballs ...")
    expected_tags = expected_gitlab_tags_from_metadata(metadata)
    files_found: List[Dict[str, str]] = []
    missing: List[str] = []

    for tag in expected_tags:
        repo_path, tag_value = tag.split(":", 1)
        if "/" in repo_path:
            arch, repo = repo_path.split("/", 1)
        else:
            arch, repo = "", repo_path
        expected_filename = f"{repo}-{image_tag}.tar.gz"
        file_path = next(extract_dir.glob(expected_filename), None)
        if file_path and file_path.exists():
            files_found.append(
                {
                    "gitlab_tag": tag,
                    "arch": arch,
                    "repo": repo,
                    "file": str(file_path.resolve()),
                }
            )
        else:
            missing.append(expected_filename)

    if missing:
        raise RuntimeError(f"Missing expected tarballs: {', '.join(missing)}")

    print(f"        → located {len(files_found)} tarballs.")

    log(6, "Writing upload plan ...")
    plan = {
        "image_tag": image_tag,
        "presto_ref": metadata.get("Presto ref"),
        "velox_ref": metadata.get("Velox ref"),
        "platform": metadata.get("Target platform"),
        "artifacts": files_found,
    }
    plan_path = tag_dir / "upload-plan.json"
    with plan_path.open("w", encoding="utf-8") as fh:
        json.dump(plan, fh, indent=2, sort_keys=True)

    print(f"Prepared upload plan written to {plan_path}")
    print("Artifacts ready for upload:")
    print("  ─────────────────────────────────────────────────────────────")
    for idx, item in enumerate(files_found, start=1):
        print(f"  Artifact #{idx}:")
        print(f"    Image        : {item['repo']}")
        print(f"    Arch         : {item['arch'] or '-'}")
        print(f"    GitLab tag   : {item['gitlab_tag']}")
        print(f"    Local tarball: {item['file']}")
        if idx != len(files_found):
            print("  ─────────────────────────────────────────────────────────────")
    print("Prepare complete.")


def cmd_clean(args: argparse.Namespace) -> None:
    downloads_dir = Path(args.downloads_dir)

    print("[1/3] Resolving run metadata from GitHub ...")
    precomputed_run = getattr(args, "precomputed_run", None)
    precomputed_metadata = getattr(args, "precomputed_metadata", None) or {}
    metadata = dict(precomputed_metadata)

    gh_repo = args.repo or metadata.get("repository") or DEFAULT_GITHUB_REPO
    gh_workflow = args.workflow or DEFAULT_WORKFLOW_FILE
    gh_branch = args.branch

    run_info = precomputed_run
    if run_info:
        print("    → using cached workflow run metadata.")
    else:
        try:
            run_info, gh_metadata = find_run_metadata_for_tag(
                repo=gh_repo,
                workflow=gh_workflow,
                branch=gh_branch,
                limit=args.limit,
                image_tag=args.image_tag,
            )
            metadata.update({k: v for k, v in gh_metadata.items() if v is not None})
        except Exception as exc:  # pylint: disable=broad-except
            fallback_run = {
                "databaseId": metadata.get("run_id"),
                "displayTitle": metadata.get("run_name"),
                "headBranch": metadata.get("_run_branch"),
                "conclusion": metadata.get("_run_conclusion"),
            }
            if fallback_run["databaseId"]:
                print(
                    "    → warning: failed to re-query GitHub; using stored metadata. "
                    f"Details: {exc}"
                )
                run_info = fallback_run
            else:
                raise RuntimeError(
                    "Failed to resolve workflow run for the specified tag. "
                    "Ensure the GitHub token is set (via --gh-token or GH_TOKEN / GITHUB_TOKEN). "
                    f"Details: {exc}"
                ) from exc

    resolved_tag = metadata.get("Image tag") or args.image_tag
    print(
        f"    → run {run_info.get('databaseId')} ({run_info.get('displayTitle')}), "
        f"branch={run_info.get('headBranch')}, conclusion={run_info.get('conclusion')}"
    )
    if resolved_tag != args.image_tag:
        print(f"    → normalized image tag: {resolved_tag}")

    candidates: List[Path] = []
    legacy_candidates: List[Path] = []

    primary_dir = downloads_dir / resolved_tag
    if primary_dir.exists():
        candidates.append(primary_dir)

    run_id = metadata.get("run_id") or run_info.get("databaseId")

    local_metadata = None
    primary_meta = primary_dir / "metadata.json"
    if primary_meta.exists():
        with primary_meta.open("r", encoding="utf-8") as fh:
            local_metadata = json.load(fh)
    elif run_id:
        legacy_meta = downloads_dir / str(run_id) / "metadata.json"
        if legacy_meta.exists():
            with legacy_meta.open("r", encoding="utf-8") as fh:
                local_metadata = json.load(fh)

    if local_metadata:
        metadata = {**metadata, **local_metadata}
        run_id = metadata.get("run_id") or run_id
        resolved_tag = metadata.get("Image tag") or resolved_tag

    run_id_str = str(run_id) if run_id is not None else None

    if run_id_str:
        legacy_dir = downloads_dir / run_id_str
        if legacy_dir.exists() and legacy_dir != primary_dir:
            legacy_candidates.append(legacy_dir)

    possible_paths: Set[Path] = set()
    name_candidates = [
        args.image_tag,
        resolved_tag,
        run_id_str,
        metadata.get("artifact_name"),
        metadata.get("artifact_zip"),
    ]
    for name in name_candidates:
        if not name:
            continue
        possible_paths.add(downloads_dir / name)
        possible_paths.add(downloads_dir / f"{name}.zip")
        possible_paths.add(downloads_dir / f"{name}.tar.gz")
        if run_id_str:
            possible_paths.add(downloads_dir / f"presto-images-{run_id_str}.zip")

    if run_id_str:
        possible_paths.update(downloads_dir.glob(f"*{run_id_str}*.zip"))
        possible_paths.update(downloads_dir.glob(f"*{run_id_str}*.tar.gz"))
    possible_paths.update(downloads_dir.glob(f"*{resolved_tag}*.zip"))
    possible_paths.update(downloads_dir.glob(f"*{resolved_tag}*.tar.gz"))

    for file_path in list(possible_paths):
        if file_path.exists():
            legacy_candidates.append(file_path)

    if not candidates and not legacy_candidates:
        raise RuntimeError(
            "No local artifacts found for this image tag. "
            "If old downloads exist elsewhere, remove them manually."
        )

    print("[2/3] The following paths will be removed:")
    for path in candidates:
        print(f"  - {path}/ (download + prepared data)")
    for path in legacy_candidates:
        suffix = "/" if path.is_dir() else ""
        label = "additional directory" if path.is_dir() else "downloaded artifact"
        print(f"  - {path}{suffix} ({label})")

    if not args.yes and not prompt_yes_no("[3/3] Proceed with deletion? [y/N]: "):
        print("Clean aborted.")
        return

    for path in candidates:
        shutil.rmtree(path)
        print(f"Removed directory {path}")

    for path in legacy_candidates:
        if path.is_dir():
            shutil.rmtree(path)
            print(f"Removed legacy directory {path}")
        else:
            path.unlink()
            print(f"Removed legacy file {path}")

    print("Clean complete.")


def cmd_daemon(args: argparse.Namespace) -> None:
    token = args.gitlab_token or os.environ.get("GITLAB_TOKEN")
    if not token:
        raise RuntimeError("GitLab token not provided. Use --gitlab-token or set GITLAB_TOKEN.")

    interval = max(args.interval, 1)
    downloads_dir = args.downloads_dir

    print(f"Starting daemon loop (interval={interval}s, downloads_dir='{downloads_dir}')")
    try:
        while True:
            cycle_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
            print(f"\n[{cycle_ts}] Checking for missing GitLab images ...")
            try:
                state = compute_registry_sync_state(
                    repo=args.repo,
                    workflow=args.workflow,
                    branch=args.branch,
                    limit=args.limit,
                    gitlab_host=args.gitlab_host,
                    gitlab_project=args.gitlab_project or DEFAULT_GITLAB_PROJECT,
                    gitlab_token=token,
                )
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[{cycle_ts}] ERROR: Failed to compute registry state: {exc}")
                if args.once:
                    raise
                print(f"[{cycle_ts}] Sleeping {interval}s before retry.")
                time.sleep(interval)
                continue

            missing = state["missing_grouped"]
            if not missing:
                print(f"[{cycle_ts}] No missing tags. Sleeping {interval}s.")
                if args.once:
                    break
                time.sleep(interval)
                continue

            for image_tag in sorted(missing.keys()):
                print(f"[{cycle_ts}] Processing image '{image_tag}'")
                try:
                    info = missing[image_tag]
                    metadata = info["metadata"]
                    precomputed_run = metadata.get("_run_info") or {
                        "databaseId": metadata.get("_run_id"),
                        "displayTitle": metadata.get("_run_display"),
                        "headBranch": metadata.get("_run_branch"),
                        "conclusion": metadata.get("_run_conclusion"),
                    }

                    download_args = SimpleNamespace(
                        image_tag=image_tag,
                        repo=args.repo,
                        workflow=args.workflow,
                        branch=args.branch,
                        limit=args.limit,
                        output_dir=downloads_dir,
                        force=True,
                        precomputed_run=precomputed_run,
                        precomputed_metadata=metadata,
                    )
                    cmd_download(download_args)

                    prepare_args = SimpleNamespace(
                        image_tag=image_tag,
                        downloads_dir=downloads_dir,
                        clean=True,
                    )
                    cmd_prepare(prepare_args)

                    push_args = SimpleNamespace(
                        image_tag=image_tag,
                        downloads_dir=downloads_dir,
                        gitlab_host=args.gitlab_host,
                        gitlab_project=args.gitlab_project or DEFAULT_GITLAB_PROJECT,
                        registry=args.registry or DEFAULT_GITLAB_REGISTRY,
                        gitlab_token=token,
                        repo=args.repo,
                        workflow=args.workflow,
                        branch=args.branch,
                        limit=args.limit,
                        registry_username=args.registry_username,
                        registry_password=args.registry_password,
                        skip_login=args.skip_login,
                        yes=True,
                        force=args.force_push,
                        verbose=args.verbose,
                    )
                    cmd_push(push_args)

                    clean_args = SimpleNamespace(
                        image_tag=image_tag,
                        downloads_dir=downloads_dir,
                        yes=True,
                        repo=args.repo,
                        workflow=args.workflow,
                        branch=args.branch,
                        limit=args.limit,
                        precomputed_run=precomputed_run,
                        precomputed_metadata=metadata,
                    )
                    cmd_clean(clean_args)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f"[{cycle_ts}] ERROR while processing '{image_tag}': {exc}")
                    continue

            if args.once:
                break

            print(f"[{cycle_ts}] Cycle complete. Sleeping {interval}s.")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nDaemon interrupted by user. Exiting.")


def cmd_push(args: argparse.Namespace) -> None:
    if shutil.which("docker") is None:
        raise RuntimeError("Docker CLI not found in PATH. Install Docker or ensure it is available.")

    tag_dir = Path(args.downloads_dir) / args.image_tag
    if not tag_dir.exists():
        raise RuntimeError(
            f"Download directory {tag_dir} not found. Run the download and prepare steps first."
        )

    metadata_path = tag_dir / "metadata.json"
    plan_path = tag_dir / "upload-plan.json"
    if not metadata_path.exists() or not plan_path.exists():
        raise RuntimeError(
            "Push requires metadata.json and upload-plan.json. "
            "Run `download` followed by `prepare` for this image tag before pushing."
        )

    with metadata_path.open("r", encoding="utf-8") as fh:
        metadata = json.load(fh)
    with plan_path.open("r", encoding="utf-8") as fh:
        plan = json.load(fh)

    artifacts = plan.get("artifacts") or []
    if not artifacts:
        raise RuntimeError(
            f"No artifacts found in {plan_path}. Re-run the prepare step to regenerate the upload plan."
        )

    for artifact in artifacts:
        file_path = Path(artifact.get("file", ""))
        if not file_path.exists():
            raise RuntimeError(
                f"Expected tarball {file_path} does not exist. Re-run the prepare step before pushing."
            )

    steps_total = 5

    def log(step: int, message: str) -> None:
        print(f"[{step}/{steps_total}] {message}")

    precomputed_run = getattr(args, "precomputed_run", None)
    precomputed_metadata = getattr(args, "precomputed_metadata", None)
    if precomputed_metadata:
        metadata.update({k: v for k, v in precomputed_metadata.items() if v is not None})

    run_info = precomputed_run

    # Step 1: verify GitHub run metadata (or use cached/fallback)
    gh_repo = args.repo or metadata.get("repository") or DEFAULT_GITHUB_REPO
    gh_workflow = args.workflow or DEFAULT_WORKFLOW_FILE
    gh_branch = args.branch

    if run_info:
        log(1, "Using cached workflow run metadata.")
    else:
        log(1, "Resolving run metadata from GitHub ...")
        try:
            run_info, gh_metadata = find_run_metadata_for_tag(
                repo=gh_repo,
                workflow=gh_workflow,
                branch=gh_branch,
                limit=args.limit,
                image_tag=args.image_tag,
            )
            metadata.update({k: v for k, v in gh_metadata.items() if v is not None})
        except Exception as exc:  # pylint: disable=broad-except
            fallback_run = {
                "databaseId": metadata.get("run_id"),
                "displayTitle": metadata.get("run_name"),
                "headBranch": metadata.get("_run_branch"),
                "conclusion": metadata.get("_run_conclusion"),
            }
            if fallback_run["databaseId"]:
                print(
                    "WARNING: Failed to re-query GitHub for run metadata. Falling back to stored metadata. "
                    f"Details: {exc}"
                )
                run_info = fallback_run
            else:
                raise RuntimeError(
                    "Failed to verify run metadata from GitHub. Ensure the gh CLI is authenticated or provide --gh-token. "
                    f"Details: {exc}"
                ) from exc

    print(
        f"    → run {run_info.get('databaseId')} ({run_info.get('displayTitle')}), "
        f"branch={run_info.get('headBranch')}, conclusion={run_info.get('conclusion')}"
    )

    stored_run_id = metadata.get("run_id")
    if stored_run_id and run_info.get("databaseId") and stored_run_id != run_info.get("databaseId"):
        print(
            "WARNING: Prepared metadata run_id differs from GitHub lookup "
            f"({stored_run_id} vs {run_info.get('databaseId')}). Using GitHub data."
        )

    # Common GitLab settings
    requests_module = ensure_requests()
    token = args.gitlab_token or os.environ.get("GITLAB_TOKEN")
    if not token:
        raise RuntimeError("GitLab token not provided. Use --gitlab-token or set GITLAB_TOKEN.")

    registry = (args.registry or DEFAULT_GITLAB_REGISTRY).rstrip("/")
    project = args.gitlab_project or DEFAULT_GITLAB_PROJECT
    encoded_project = project.replace("/", "%2F")

    registry_username = (
        args.registry_username
        or os.environ.get("GITLAB_REGISTRY_USERNAME")
        or os.environ.get("CI_REGISTRY_USER")
    )
    registry_password = (
        args.registry_password
        or os.environ.get("GITLAB_REGISTRY_PASSWORD")
        or os.environ.get("CI_REGISTRY_PASSWORD")
        or token
    )

    # Step 2: docker login (unless skipped)
    if args.skip_login:
        log(2, f"Skipping docker login for registry {registry} (user requested).")
    else:
        log(2, f"Logging into Docker registry {registry} ...")
        if not registry_username:
            raise RuntimeError(
                "Registry username is required for docker login. Provide --registry-username "
                "or set GITLAB_REGISTRY_USERNAME / CI_REGISTRY_USER."
            )
        if not registry_password:
            raise RuntimeError(
                "Registry password/token is required for docker login. Provide --registry-password "
                "or set GITLAB_REGISTRY_PASSWORD / CI_REGISTRY_PASSWORD / use --gitlab-token."
            )
        docker_login(registry, registry_username, registry_password, args.verbose)

    # Step 3: gather GitLab state
    log(3, "Gathering GitLab registry state ...")
    repos = gitlab_api_json(
        requests_module,
        args.gitlab_host,
        f"/api/v4/projects/{encoded_project}/registry/repositories",
        token,
        params={"per_page": 100},
    )
    repo_lookup = build_gitlab_repo_lookup(repos or [], project, registry)

    # Step 4: build push plan
    log(4, "Building push plan ...")
    push_entries = []
    existing = []

    for artifact in artifacts:
        gitlab_tag = artifact["gitlab_tag"]
        if ":" not in gitlab_tag:
            raise RuntimeError(f"Invalid GitLab tag format '{gitlab_tag}'.")
        repo_path, tag_value = gitlab_tag.split(":", 1)
        file_path = Path(artifact["file"])

        repo_info = repo_lookup.get(repo_path) or repo_lookup.get(f"{project}/{repo_path}")
        tag_exists = False
        repo_found = repo_info is not None

        if repo_info:
            repo_id = repo_info["id"]
            tag_url = (
                f"https://{args.gitlab_host}/api/v4/projects/{encoded_project}/"
                f"registry/repositories/{repo_id}/tags/{quote(tag_value, safe='')}"
            )
            response = requests_module.get(
                tag_url,
                headers={"PRIVATE-TOKEN": token},
                timeout=30,
            )
            if response.status_code == 200:
                tag_exists = True
            elif response.status_code == 404:
                tag_exists = False
            else:
                raise RuntimeError(
                    f"GitLab tag lookup failed for {gitlab_tag} ({response.status_code}): {response.text}"
                )

        remote_ref = compose_registry_ref(registry, project, gitlab_tag)
        entry = {
            "gitlab_tag": gitlab_tag,
            "repo_path": repo_path,
            "tag_value": tag_value,
            "file": file_path,
            "remote_ref": remote_ref,
            "repo_found": repo_found,
            "tag_exists": tag_exists,
        }
        push_entries.append(entry)
        if tag_exists:
            existing.append(remote_ref)

    print("Push plan:")
    print("  ─────────────────────────────────────────────────────────────")
    for idx, entry in enumerate(push_entries, start=1):
        print(f"  Artifact #{idx}:")
        print(f"    Remote tag   : {entry['remote_ref']}")
        print(f"    Local tarball: {entry['file']}")
        print("    Local tag    : (will be determined during docker load)")
        if entry["tag_exists"]:
            print("    GitLab status: already present (push will overwrite the existing tag)")
        elif entry["repo_found"]:
            print("    GitLab status: repository exists, tag missing")
        else:
            print("    GitLab status: repository not found (will be created on push)")
        if idx != len(push_entries):
            print("  ─────────────────────────────────────────────────────────────")

    if args.verbose:
        print("\nCommand preview:")
        for entry in push_entries:
            print(f"    $ docker load --input {shlex.quote(str(entry['file']))}")
            print("    $ docker tag <loaded-tag> " + shlex.quote(entry["remote_ref"]))
            print(f"    $ docker push {shlex.quote(entry['remote_ref'])}")

    if existing and not args.force:
        print(
            "\nSkipping already-present tags (use --force to push anyway):\n  "
            + "\n  ".join(existing)
        )
        push_entries = [entry for entry in push_entries if not entry["tag_exists"]]
        if not push_entries:
            print("Nothing to push.")
            return
    elif existing:
        print(
            "\nWARNING: The following tags already exist in GitLab and will be overwritten:\n  "
            + "\n  ".join(existing)
        )

    if not args.yes:
        if not prompt_yes_no("\nProceed with upload? [y/N]: "):
            print("Push aborted.")
            return

    # Step 5: execute docker operations
    log(5, "Executing docker push sequence ...")
    for entry in push_entries:
        print(f"\nPushing {entry['remote_ref']} ...")
        archive_size = entry["file"].stat().st_size if entry["file"].exists() else 0
        print(f"  archive: {entry['file']} ({archive_size / (1024 ** 2):.2f} MiB)")
        try:
            loaded_tags, _ = docker_load_archive(entry["file"], args.verbose)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"'docker load' failed for {entry['file']}: {exc}"
            ) from exc

        source_tag = loaded_tags[0] if loaded_tags else None
        if not source_tag:
            raise RuntimeError(
                f"Unable to determine source image tag after loading {entry['file']}."
            )
        if args.verbose and loaded_tags:
            print("    Loaded tags: " + ", ".join(loaded_tags))

        if source_tag != entry["remote_ref"]:
            run_command(["docker", "tag", source_tag, entry["remote_ref"]], args.verbose)
        run_command(["docker", "push", entry["remote_ref"]], args.verbose)

    print("\nPush complete.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Helpers for GitHub workflow runs and GitLab registry images.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Authentication:
              • GitHub: run `gh auth login` once or pass --gh-token / GH_TOKEN.
              • GitLab: supply a token via --gitlab-token or set GITLAB_TOKEN.
            """
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # gh-list
    gh_list = subparsers.add_parser("gh-list", help="List GitHub workflow runs with metadata.")
    gh_list.add_argument("--repo", default=DEFAULT_GITHUB_REPO, help="owner/repo (default: %(default)s)")
    gh_list.add_argument("--workflow", default=DEFAULT_WORKFLOW_FILE, help="workflow filename (default: %(default)s)")
    gh_list.add_argument("--branch", default=None, help="Only include runs from this branch.")
    gh_list.add_argument("--limit", type=int, default=20, help="Maximum runs to fetch (default: %(default)s)")
    gh_list.add_argument("--include-failed", action="store_true", help="Include non-successful runs.")
    gh_list.add_argument(
        "--gh-token",
        default=None,
        help="GitHub token (otherwise GH_TOKEN/GITHUB_TOKEN env vars or gh auth login are used).",
    )
    gh_list.set_defaults(func=cmd_gh_list)

    # gitlab-list
    gitlab_list = subparsers.add_parser("gitlab-list", help="List images/tags in the GitLab registry.")
    gitlab_list.add_argument("--gitlab-host", default=DEFAULT_GITLAB_HOST, help="GitLab host (default: %(default)s)")
    gitlab_list.add_argument("--gitlab-project", default=DEFAULT_GITLAB_PROJECT, help="GitLab project path (default: %(default)s)")
    gitlab_list.add_argument("--gitlab-token", default=None, help="GitLab access token (or set GITLAB_TOKEN).")
    gitlab_list.set_defaults(func=cmd_gitlab_list)

    # xref
    xref = subparsers.add_parser("xref", help="Cross reference GitHub runs and GitLab registry tags.")
    xref.add_argument("--repo", default=DEFAULT_GITHUB_REPO, help="owner/repo (default: %(default)s)")
    xref.add_argument("--workflow", default=DEFAULT_WORKFLOW_FILE, help="workflow filename (default: %(default)s)")
    xref.add_argument("--branch", default=None, help="Only include runs from this branch.")
    xref.add_argument("--limit", type=int, default=20, help="Maximum runs to consider (default: %(default)s)")
    xref.add_argument("--gitlab-host", default=DEFAULT_GITLAB_HOST, help="GitLab host (default: %(default)s)")
    xref.add_argument("--gitlab-project", default=DEFAULT_GITLAB_PROJECT, help="GitLab project path (default: %(default)s)")
    xref.add_argument("--gitlab-token", default=None, help="GitLab access token (or set GITLAB_TOKEN).")
    xref.add_argument("--list-extra", action="store_true", help="Show image tags that exist only on GitLab.")
    xref.add_argument(
        "--gh-token",
        default=None,
        help="GitHub token (otherwise GH_TOKEN/GITHUB_TOKEN env vars or gh auth login are used).",
    )
    xref.set_defaults(func=cmd_xref)

    # download
    download = subparsers.add_parser("download", help="Download artifact for a specific image tag.")
    download.add_argument("image_tag", help="Image tag (e.g. presto-<sha>-velox-<sha>-linux-amd64).")
    download.add_argument("--repo", default=DEFAULT_GITHUB_REPO, help="owner/repo (default: %(default)s)")
    download.add_argument("--workflow", default=DEFAULT_WORKFLOW_FILE, help="workflow filename (default: %(default)s)")
    download.add_argument("--branch", default=None, help="Only consider runs from this branch.")
    download.add_argument("--limit", type=int, default=20, help="Maximum runs to search (default: %(default)s)")
    download.add_argument("--output-dir", default="downloads", help="Directory to store downloads (default: %(default)s)")
    download.add_argument("--force", action="store_true", help="Overwrite existing download for the image tag.")
    download.add_argument(
        "--gh-token",
        default=None,
        help="GitHub token (otherwise GH_TOKEN/GITHUB_TOKEN env vars or gh auth login are used).",
    )
    download.set_defaults(func=cmd_download)

    # prepare
    prepare = subparsers.add_parser("prepare", help="Extract and validate downloaded artifact for a tag.")
    prepare.add_argument("image_tag", help="Image tag to prepare.")
    prepare.add_argument("--downloads-dir", default="downloads", help="Directory where downloads are stored (default: %(default)s)")
    prepare.add_argument("--clean", action="store_true", help="Remove existing extraction directory before extracting.")
    prepare.set_defaults(func=cmd_prepare)

    # push
    push = subparsers.add_parser("push", help="Push prepared artifacts to the GitLab registry.")
    push.add_argument("image_tag", help="Image tag to push.")
    push.add_argument("--downloads-dir", default="downloads", help="Directory where downloads are stored (default: %(default)s)")
    push.add_argument("--gitlab-host", default=DEFAULT_GITLAB_HOST, help="GitLab host for API calls (default: %(default)s)")
    push.add_argument("--gitlab-project", default=DEFAULT_GITLAB_PROJECT, help="GitLab project path (default: %(default)s)")
    push.add_argument("--registry", default=DEFAULT_GITLAB_REGISTRY, help="Docker registry hostname (default: %(default)s)")
    push.add_argument("--gitlab-token", default=None, help="GitLab access token (or set GITLAB_TOKEN).")
    push.add_argument("--repo", default=DEFAULT_GITHUB_REPO, help="GitHub owner/repo for verification (default: %(default)s)")
    push.add_argument("--workflow", default=DEFAULT_WORKFLOW_FILE, help="Workflow filename for verification (default: %(default)s)")
    push.add_argument("--branch", default=None, help="Only consider runs from this branch when verifying.")
    push.add_argument("--limit", type=int, default=20, help="Maximum workflow runs to search when verifying (default: %(default)s)")
    push.add_argument("--registry-username", default=None, help="Docker registry username (or set GITLAB_REGISTRY_USERNAME/CI_REGISTRY_USER).")
    push.add_argument("--registry-password", default=None, help="Docker registry password/token (or set GITLAB_REGISTRY_PASSWORD/CI_REGISTRY_PASSWORD).")
    push.add_argument("--skip-login", action="store_true", help="Skip docker login (assume already authenticated).")
    push.add_argument("--yes", action="store_true", help="Skip interactive confirmation.")
    push.add_argument("--force", action="store_true", help="Push even if tags already exist on the registry.")
    push.add_argument("--verbose", action="store_true", help="Show docker commands that will be executed.")
    push.add_argument(
        "--gh-token",
        default=None,
        help="GitHub token (otherwise GH_TOKEN/GITHUB_TOKEN env vars or gh auth login are used).",
    )
    push.set_defaults(func=cmd_push)

    # clean
    clean = subparsers.add_parser("clean", help="Remove downloaded artifacts and prepared data for a tag.")
    clean.add_argument("image_tag", help="Image tag to clean.")
    clean.add_argument("--downloads-dir", default="downloads", help="Directory where downloads are stored (default: %(default)s)")
    clean.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    clean.add_argument("--repo", default=DEFAULT_GITHUB_REPO, help="GitHub owner/repo for verification (default: %(default)s)")
    clean.add_argument("--workflow", default=DEFAULT_WORKFLOW_FILE, help="Workflow filename for verification (default: %(default)s)")
    clean.add_argument("--branch", default=None, help="Only consider runs from this branch when verifying.")
    clean.add_argument("--limit", type=int, default=20, help="Maximum workflow runs to search when verifying (default: %(default)s)")
    clean.add_argument(
        "--gh-token",
        default=None,
        help="GitHub token (otherwise GH_TOKEN/GITHUB_TOKEN env vars or gh auth login are used).",
    )
    clean.set_defaults(func=cmd_clean)

    # daemon
    daemon = subparsers.add_parser("daemon", help="Continuously mirror missing GitHub artifacts into GitLab.")
    daemon.add_argument("--interval", type=int, default=900, help="Sleep interval between checks in seconds (default: %(default)s)")
    daemon.add_argument("--downloads-dir", default="downloads", help="Directory where downloads are stored (default: %(default)s)")
    daemon.add_argument("--once", action="store_true", help="Run a single synchronization cycle and exit.")
    daemon.add_argument("--repo", default=DEFAULT_GITHUB_REPO, help="GitHub owner/repo (default: %(default)s)")
    daemon.add_argument("--workflow", default=DEFAULT_WORKFLOW_FILE, help="Workflow filename (default: %(default)s)")
    daemon.add_argument("--branch", default=None, help="Only consider runs from this branch.")
    daemon.add_argument("--limit", type=int, default=20, help="Maximum workflow runs to inspect (default: %(default)s)")
    daemon.add_argument("--gitlab-host", default=DEFAULT_GITLAB_HOST, help="GitLab host (default: %(default)s)")
    daemon.add_argument("--gitlab-project", default=DEFAULT_GITLAB_PROJECT, help="GitLab project path (default: %(default)s)")
    daemon.add_argument("--gitlab-token", default=None, help="GitLab access token (or set GITLAB_TOKEN).")
    daemon.add_argument("--registry", default=DEFAULT_GITLAB_REGISTRY, help="Docker registry hostname (default: %(default)s)")
    daemon.add_argument("--registry-username", default=None, help="Docker registry username (or set GITLAB_REGISTRY_USERNAME/CI_REGISTRY_USER).")
    daemon.add_argument("--registry-password", default=None, help="Docker registry password/token (or set GITLAB_REGISTRY_PASSWORD/CI_REGISTRY_PASSWORD).")
    daemon.add_argument("--skip-login", action="store_true", help="Skip docker login (assume already authenticated).")
    daemon.add_argument("--force-push", action="store_true", help="Push even if tags already exist on the registry.")
    daemon.add_argument("--verbose", action="store_true", help="Enable verbose output from nested commands.")
    daemon.add_argument(
        "--gh-token",
        default=None,
        help="GitHub token (otherwise GH_TOKEN/GITHUB_TOKEN env vars or gh auth login are used).",
    )
    daemon.set_defaults(func=cmd_daemon)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    global _GH_TOKEN  # pylint: disable=global-statement
    raw_token = getattr(args, "gh_token", None) or os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    gh_token = raw_token.strip() if isinstance(raw_token, str) else None
    if gh_token:
        _GH_TOKEN = gh_token

    try:
        args.func(args)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

