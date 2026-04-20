"""Tests for ``GET /api/stats``.

Covers three things:

* the empty-index case (fresh app, no documents, all counters zero);
* counters moving after a direct :class:`InvertedIndex.add_document`
  call — which bypasses the Redis consumer and exercises the stats
  response wiring end-to-end;
* response-shape parity with the :class:`StatsResponse` pydantic
  model so future schema drift is caught loudly.

Documents are added *directly* via ``app.state.index`` rather than
through Redis — that keeps the test hermetic (no stream timing
dependency) and still exercises the code path the real consumer
goes through. The consumer task idling on XREADGROUP in the
background is harmless; it sees no messages and simply blocks until
teardown.
"""

from __future__ import annotations

import time

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from src.models import LogEntry


async def test_stats_empty_index(async_client: AsyncClient) -> None:
    """A fresh app reports zero-valued counters for every per-tier field."""
    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200

    body = resp.json()
    assert body["docs_indexed"] == 0
    assert body["current_segment_docs"] == 0
    assert body["flushed_memory_segments"] == 0
    assert body["disk_segments"] == 0
    # Throughput may be 0.0 or arbitrarily small; we just assert the
    # field is present and the response carries a positive uptime.
    assert "throughput_1m" in body
    assert body["uptime_s"] >= 0


async def test_stats_after_direct_add(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """Adding documents via app.state.index reflects in /api/stats."""
    index = app_instance.state.index

    for _ in range(5):
        await index.add_document(
            LogEntry(
                doc_id=0,
                message="hello world error",
                timestamp=time.time(),
                service="svc",
                level="INFO",
            )
        )

    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200

    body = resp.json()
    assert body["docs_indexed"] == 5
    assert body["current_segment_docs"] == 5
    # ``hello``, ``world``, ``error`` are all indexed, so vocab is
    # strictly positive; exact value depends on the tokenizer's stop
    # list so we assert >= 1 instead.
    assert body["vocab_size"] >= 1


async def test_stats_shape_matches_model(async_client: AsyncClient) -> None:
    """The response body must expose exactly the StatsResponse fields."""
    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200

    body = resp.json()
    required = {
        "docs_indexed",
        "current_segment_docs",
        "flushed_memory_segments",
        "disk_segments",
        "vocab_size",
        "memory_bytes",
        "throughput_1m",
        "ingest_lag",
        "query_p95_ms",
        "errors",
        "uptime_s",
    }
    assert set(body.keys()) == required
