# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for cache_reset.py's worker-clear semantics."""

import pytest

from presto.testing.performance_benchmarks import cache_reset

WORKERS = [{"uri": "http://worker-a:10000/v1/status"}, {"uri": "http://worker-b:10000/v1/status"}]
IDLE = [{"queryId": "q1", "state": "FINISHED"}]


def _patch(monkeypatch, nodes=WORKERS, queries=IDLE, bodies=None):
    """Stub discovery, the idle check, and per-endpoint response bodies.

    *bodies* maps a substring of the request URL to the body returned for it; anything
    unmatched gets that endpoint's normal success body.
    """
    calls = []
    monkeypatch.setattr(cache_reset, "get_nodes", lambda hostname, port: nodes)
    monkeypatch.setattr(cache_reset, "fetch_json", lambda url: queries)

    def fake_fetch_text(url):
        calls.append(url)
        for fragment, body in (bodies or {}).items():
            if fragment in url:
                return body
        if "type=memory" in url:
            return "Cleared memory cache"
        if "type=ssd" in url:
            return "Cleared ssd cache"
        return "file handle cache stats"

    monkeypatch.setattr(cache_reset, "fetch_text", fake_fetch_text)
    return calls


def test_clears_every_worker_and_strips_status_suffix(monkeypatch):
    calls = _patch(monkeypatch)

    layers = cache_reset.clear_worker_data_caches("coord", 8080)
    assert layers == {cache_reset.FILE_HANDLE_LAYER, cache_reset.MEMORY_LAYER, cache_reset.SSD_LAYER}
    # Three endpoints per worker, addressed at the base URI (no /v1/status).
    assert len(calls) == 6
    assert all("/v1/status" not in url for url in calls)
    assert sum("worker-a:10000/v1/operation" in url for url in calls) == 3


def test_partial_clear_raises(monkeypatch):
    """An un-cleared worker still serves warm data, so this must not be reported as cold."""
    _patch(monkeypatch, bodies={"worker-b": None})

    with pytest.raises(cache_reset.CacheResetError, match="only 1/2"):
        cache_reset.clear_worker_data_caches("coord", 8080)


def test_no_nodes_raises(monkeypatch):
    _patch(monkeypatch, nodes=[])

    with pytest.raises(cache_reset.CacheResetError, match="No worker nodes found"):
        cache_reset.clear_worker_data_caches("coord", 8080)


def test_unconfigured_tier_is_reported_not_cleared(monkeypatch):
    """The endpoint answers 200 with a no-op body when a tier isn't configured. That is a
    valid deployment, but it must not be counted as cleared — trusting the status code
    alone would report warm numbers as cold."""
    _patch(monkeypatch, bodies={"type=memory": "No memory cache set on server"})

    layers = cache_reset.clear_worker_data_caches("coord", 8080)
    assert cache_reset.MEMORY_LAYER not in layers
    assert cache_reset.SSD_LAYER in layers


def test_unexpected_clear_body_raises(monkeypatch):
    """Anything that is neither a success nor a known not-configured body is a failure."""
    _patch(monkeypatch, bodies={"type=memory": "Something went sideways"})

    with pytest.raises(cache_reset.CacheResetError, match="Something went sideways"):
        cache_reset.clear_worker_data_caches("coord", 8080)


def test_active_query_blocks_the_clear(monkeypatch):
    """AsyncDataCache::clear() keeps pinned entries, so clearing mid-query is only partly cold."""
    _patch(monkeypatch, queries=[{"queryId": "q9", "state": "RUNNING"}])

    with pytest.raises(cache_reset.CacheResetError, match="q9 \\(RUNNING\\)"):
        cache_reset.wait_for_idle_cluster("coord", 8080, timeout_seconds=0.01)
