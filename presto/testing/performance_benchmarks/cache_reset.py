# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Cache reset for presto-native (Velox) workers.

Presto-native's admin operations live at GET /v1/operation/<target>/<action>
(PrestoServer.cpp registers this path with registerGet — POSTing to it 404s). A full
cold reset needs all of:

  - connector/clearCache: Hive connector's open file-handle cache.
  - server/clearCache?type=memory: Velox's in-memory AsyncDataCache.
  - server/clearCache?type=ssd: Velox's SsdCache index (not the on-disk bytes).
  - the OS page cache: none of the above touch it (SsdFile uses buffered I/O, no
    O_DIRECT), so previously-read/written blocks stay resident until it's dropped
    explicitly — drop it last.

The clear endpoints answer 200 even when they did nothing ("No memory cache set on
server"), so responses are matched against the exact success body rather than trusting
the status code. AsyncDataCache::clear() also keeps *pinned* entries, so a reset issued
while a query is still running is only partly cold — hence the idle-cluster check.

Limitation: drop_cache() writes /proc/sys/vm/drop_caches on the host running pytest
only. That covers workers sharing that kernel (the single-node Docker setup), but does
nothing for remote workers — their page cache stays warm, so "cold" numbers from a
multi-node cluster are not truly cold.
"""

import time
from concurrent.futures import ThreadPoolExecutor

from common.testing.performance_benchmarks.cache_utils import drop_cache

from .presto_api import fetch_json, fetch_text, get_nodes

DEFAULT_CONNECTOR_ID = "hive"

# Exact bodies from PrestoServerOperations::serverOperationClearCache. The endpoint
# answers 200 for no-ops too, so the body decides whether a tier was really cleared.
# A tier that is simply not configured is a valid deployment (this repo's GPU worker
# ships without an AsyncDataCache), so it is reported rather than failed — but it is
# never counted as cleared. Note the ssd request also answers with the memory-null body
# when no cache exists at all, because the null check precedes the type switch.
_MEMORY_CLEARED = "Cleared memory cache"
_SSD_CLEARED = "Cleared ssd cache"
_NOT_CONFIGURED = frozenset({"No memory cache set on server", "No ssd cache set on server"})

# Warn once per run rather than on every reset; cold mode resets many times.
_warned_no_memory_cache = False

MEMORY_LAYER = "velox-memory"
SSD_LAYER = "velox-ssd"
FILE_HANDLE_LAYER = "hive-file-handle"
OS_PAGE_CACHE_LAYER = "os-page-cache"

_TERMINAL_QUERY_STATES = frozenset({"FINISHED", "FAILED", "CANCELED"})
# The benchmark loop records timings without draining every cursor, so the previous
# iteration's query can still be settling. Wait briefly rather than failing instantly.
_IDLE_WAIT_SECONDS = 30.0
_IDLE_POLL_SECONDS = 0.25


class CacheResetError(RuntimeError):
    """Raised when worker cache state cannot be established safely."""


def _worker_uris(hostname: str, port: int) -> list[str]:
    """Return each worker's base URI from the coordinator's /v1/node view."""
    nodes = get_nodes(hostname, port)
    if not nodes:
        raise CacheResetError(f"No worker nodes found via /v1/node on {hostname}:{port}; cannot clear worker caches.")

    uris = []
    for node in nodes:
        uri = node.get("uri")
        if not uri:
            raise CacheResetError(f"Worker node has no 'uri' in /v1/node response: {node}")
        # /v1/node's "uri" is the status-heartbeat URL (.../v1/status), not a bare base
        # URI — strip that suffix before appending /v1/operation/... or every request 404s.
        uris.append(uri.rstrip("/").removesuffix("/v1/status"))
    return uris


def wait_for_idle_cluster(hostname: str, port: int, timeout_seconds: float = _IDLE_WAIT_SECONDS) -> None:
    """Block until no query on the coordinator is in a non-terminal state.

    AsyncDataCache::clear() does not evict pinned entries, so clearing while a query
    holds pins leaves a nominally cold reset partly warm. Raises CacheResetError if
    queries are still active after *timeout_seconds*.
    """
    deadline = time.monotonic() + timeout_seconds
    active = []
    while True:
        queries = fetch_json(f"http://{hostname}:{port}/v1/query")
        if queries is None:
            raise CacheResetError(f"Could not read /v1/query on {hostname}:{port} to check for active queries.")

        active = [
            (q.get("queryId"), q.get("state"))
            for q in queries
            if isinstance(q, dict) and str(q.get("state", "")).upper() not in _TERMINAL_QUERY_STATES
        ]
        if not active:
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(_IDLE_POLL_SECONDS)

    details = ", ".join(f"{query_id} ({state})" for query_id, state in sorted(active, key=lambda q: str(q[0])))
    raise CacheResetError(
        f"Refusing to clear worker caches while queries are active after {timeout_seconds:g}s: {details}. "
        "Cache-mode runs need exclusive use of the cluster."
    )


def _clear_tier(worker_uri: str, cache_type: str, cleared_body: str) -> str | None:
    """Clear one server cache tier, returning its body, or None if it is not configured."""
    body = fetch_text(f"{worker_uri}/v1/operation/server/clearCache?type={cache_type}")
    if body is None:
        raise CacheResetError(f"{worker_uri}: {cache_type} clear request failed")
    body = body.strip()
    if body == cleared_body:
        return body
    if body in _NOT_CONFIGURED:
        return None
    raise CacheResetError(f"{worker_uri}: {cache_type} clear returned {body!r}, expected {cleared_body!r}")


def _clear_one_worker(worker_uri: str, connector_id: str) -> set[str]:
    """Clear this worker's caches, returning the layers that were actually cleared."""
    handles = fetch_text(f"{worker_uri}/v1/operation/connector/clearCache?name=hive&id={connector_id}")
    if handles is None:
        raise CacheResetError(f"{worker_uri}: connector/clearCache failed (is --connector-id={connector_id} correct?)")

    layers = {FILE_HANDLE_LAYER}
    if _clear_tier(worker_uri, "memory", _MEMORY_CLEARED):
        layers.add(MEMORY_LAYER)
    if _clear_tier(worker_uri, "ssd", _SSD_CLEARED):
        layers.add(SSD_LAYER)
    return layers


def clear_worker_data_caches(
    hostname: str,
    port: int,
    connector_id: str = DEFAULT_CONNECTOR_ID,
    require_idle: bool = True,
) -> set[str]:
    """Clear the Hive file-handle cache and Velox memory/SSD data caches on every worker.

    Clears workers concurrently but does not return until all have replied, and raises
    unless *every* worker was cleared: data on an un-cleared worker is still warm, and
    reporting that as cold defeats the point. Clusters here are homogeneous
    (start_presto_helper.sh picks one of java/native-cpu/native-gpu), so a partial clear
    means something is wrong rather than a mixed-worker cluster.

    `name=hive` is the connector *type*, not the catalog: presto-native's
    clearConnectorCache only implements "hive" (anything else VELOX_USER_FAILs), while
    `id` / --connector-id selects which catalog of that type to clear.
    """
    if require_idle:
        wait_for_idle_cluster(hostname, port)

    uris = _worker_uris(hostname, port)
    errors = []
    layers: set[str] = set()
    with ThreadPoolExecutor(max_workers=min(32, len(uris)), thread_name_prefix="cache-reset") as executor:
        futures = {executor.submit(_clear_one_worker, uri, connector_id): uri for uri in uris}
        for future, uri in futures.items():
            try:
                layers |= future.result()
            except CacheResetError as error:
                errors.append(str(error))

    if errors:
        details = "\n".join(f"  {message}" for message in sorted(errors))
        raise CacheResetError(
            f"Cleared caches on only {len(uris) - len(errors)}/{len(uris)} worker(s); refusing to report these "
            f"timings as reset. Every worker must be presto-native (Java workers do not expose /v1/operation).\n"
            f"{details}"
        )

    global _warned_no_memory_cache
    if MEMORY_LAYER not in layers and not _warned_no_memory_cache:
        _warned_no_memory_cache = True
        print(
            "Warning: no worker has a Velox memory cache configured, so there was none to clear. "
            "Cache-mode runs on this cluster only control the file-handle and OS page caches."
        )
    return layers


def cold_run_reset(
    hostname: str,
    port: int,
    connector_id: str = DEFAULT_CONNECTOR_ID,
    skip_os_drop: bool = False,
) -> set[str]:
    """Full cache reset: clear worker data caches, then drop the OS page cache.

    Used by every --cache-mode that resets anything, at that mode's cadence — despite
    the name, not cold-specific. Does not restart Presto. Propagates CacheResetError if
    the cluster is busy or any worker could not be cleared.

    skip_os_drop mirrors --skip-drop-cache: worker caches are still cleared, but the
    privileged OS page-cache drop is skipped. Returns the layers actually cleared, which
    is what gets recorded in the run metadata.
    """
    layers = clear_worker_data_caches(hostname, port, connector_id)
    if skip_os_drop:
        print("[CacheReset] Skipping OS page-cache drop (--skip-drop-cache set).")
        return layers
    drop_cache()
    return layers | {OS_PAGE_CACHE_LAYER}
