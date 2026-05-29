"""API integration tests for the C7 FastAPI surface.

These drive the *real* application (``src.main.app``) through its lifespan so the
background optimization loop, ``app.state.batcher`` wiring, dependency providers,
and every router are exercised end-to-end in-process — no network, no Docker.

Determinism note
----------------
The live background loop in the test image uses the 5.0s default interval, so it
fires at most once during a sub-second test and we never *wait* on it. Where a
test needs populated metrics we instead drive the loop deterministically by
calling ``app.state.batcher.tick(timestamp=float(i), interval=1.0)`` a handful of
times. asyncio is single-threaded, so the background tick can only interleave at
an ``await`` boundary; assertions that must observe an exact post-mutation state
(e.g. the reset clearing the collector) are made immediately after the awaited
request returns, before any further await.

The async-FastAPI pattern (lifespan context + ASGITransport AsyncClient) mirrors
``log-sys-performance-profiler/tests/e2e/test_optimization_improves.py``.
"""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


def _client() -> AsyncClient:
    """An in-process ASGI client bound to the real app."""
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://t")


async def _pause_background_loop() -> None:
    """Cancel the live optimization loop so the batcher only changes when we tick.

    The lifespan starts a background task that ticks every ``optimization_interval``
    seconds (5.0s default in the tester). Even within a sub-second test that task
    can fire one tick at an ``await`` boundary and nudge the batch size off its
    initial seed. For the deterministic tests we cancel it after startup so the
    *only* mutations are our explicit ``batcher.tick(...)`` calls — the spec is
    explicit that we should drive determinism ourselves and never wait on the
    live loop. The task is recreated fresh on the next lifespan entry, so this is
    isolated to the current test.
    """
    task = app.state.optimization_task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# --- /health ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_returns_exact_healthy_payload() -> None:
    async with app.router.lifespan_context(app):
        async with _client() as ac:
            r = await ac.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "healthy"}


# --- /api/optimizer (initial status; no ticks needed) -----------------------


@pytest.mark.asyncio
async def test_optimizer_initial_status() -> None:
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        # Ensure a clean slate even if a prior test in the module advanced state.
        app.state.batcher.reset()
        async with _client() as ac:
            r = await ac.get("/api/optimizer")
    assert r.status_code == 200
    body = r.json()
    # Valid OptimizerStatus shape.
    expected_keys = {
        "state",
        "batch_size",
        "last_gradient",
        "smoothing_alpha",
        "min_batch_size",
        "max_batch_size",
        "constraint_active",
        "reason",
    }
    assert expected_keys.issubset(body.keys())
    assert body["state"] == "learning"
    assert body["batch_size"] == 100  # initial seed
    assert body["min_batch_size"] == 50
    assert body["max_batch_size"] == 5000
    assert isinstance(body["last_gradient"], (int, float))
    assert body["constraint_active"] is False


# --- /api/metrics before any ticks ------------------------------------------


@pytest.mark.asyncio
async def test_metrics_before_ticks() -> None:
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        app.state.batcher.reset()
        async with _client() as ac:
            r = await ac.get("/api/metrics")
    assert r.status_code == 200
    body = r.json()
    # `current` may be null before any tick has recorded a snapshot.
    assert "current" in body
    # `series` present with the chartable parallel lists (possibly empty).
    series = body["series"]
    assert isinstance(series, dict)
    for key in ("throughput", "batch_size", "timestamp", "latency_ms"):
        assert key in series
        assert isinstance(series[key], list)
    # `status` present and well-formed.
    assert "status" in body
    assert body["status"]["batch_size"] == 100


# --- /api/metrics after seeding the batcher deterministically ---------------


@pytest.mark.asyncio
async def test_metrics_after_seeding() -> None:
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        batcher = app.state.batcher
        batcher.reset()
        for i in range(10):
            batcher.tick(timestamp=float(i), interval=1.0)
        async with _client() as ac:
            r = await ac.get("/api/metrics")
    assert r.status_code == 200
    body = r.json()
    # Current snapshot now populated.
    assert body["current"] is not None
    assert body["current"]["batch_size"] >= 50
    # Series populated, capped at dashboard_points (20).
    series = body["series"]
    assert len(series["throughput"]) > 0
    assert len(series["batch_size"]) > 0
    assert len(series["throughput"]) <= 20
    assert len(series["batch_size"]) <= 20
    # Optimizer batch size stays within the configured bounds.
    assert 50 <= body["status"]["batch_size"] <= 5000


# --- POST /api/load ---------------------------------------------------------


@pytest.mark.asyncio
async def test_post_load_applies_rate() -> None:
    async with app.router.lifespan_context(app):
        batcher = app.state.batcher
        async with _client() as ac:
            r = await ac.post(
                "/api/load",
                json={"messages_per_second": 500, "burst_probability": 0.3},
            )
            assert r.status_code == 200
            body = r.json()
            assert body["applied"]["messages_per_second"] == 500
            assert body["applied"]["burst_probability"] == 0.3
            assert "message" in body
            # The live simulator was actually retargeted.
            assert batcher.load_simulator.messages_per_second == 500
            assert batcher.load_simulator.burst_probability == 0.3


# --- POST /api/optimizer/config (live retune) -------------------------------


@pytest.mark.asyncio
async def test_post_optimizer_config_live_retune() -> None:
    async with app.router.lifespan_context(app):
        batcher = app.state.batcher
        async with _client() as ac:
            r = await ac.post(
                "/api/optimizer/config",
                json={
                    "smoothing_alpha": 0.4,
                    "max_batch_size": 1000,
                    "optimization_interval": 2.0,
                },
            )
            assert r.status_code == 200
            # Optimizer fields applied in place.
            assert batcher.optimizer.smoothing_alpha == 0.4
            assert batcher.optimizer.max_batch_size == 1000
            # Live loop cadence retuned on app.state.
            assert app.state.loop_interval == 2.0
            # Returned status reflects the new bound.
            assert r.json()["max_batch_size"] == 1000


# --- POST /api/optimizer/reset ----------------------------------------------


@pytest.mark.asyncio
async def test_post_optimizer_reset_clears_state() -> None:
    async with app.router.lifespan_context(app):
        await _pause_background_loop()
        batcher = app.state.batcher
        # Advance well past LEARNING and accumulate history.
        for i in range(10):
            batcher.tick(timestamp=float(i), interval=1.0)
        assert len(batcher.collector) > 0  # sanity: there is something to clear
        async with _client() as ac:
            r = await ac.post("/api/optimizer/reset")
            # Assert the cleared state immediately, before any further await that
            # could let the 5s background loop fire a tick.
            assert r.status_code == 200
            body = r.json()
            assert body["state"] == "learning"
            assert body["batch_size"] == 100
            assert len(batcher.collector) == 0


# --- Validation (Pydantic constraints -> 422) -------------------------------


@pytest.mark.asyncio
async def test_post_load_negative_rate_is_422() -> None:
    async with app.router.lifespan_context(app):
        async with _client() as ac:
            r = await ac.post(
                "/api/load",
                json={"messages_per_second": -5},
            )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_post_load_burst_probability_above_one_is_422() -> None:
    async with app.router.lifespan_context(app):
        async with _client() as ac:
            r = await ac.post(
                "/api/load",
                json={"messages_per_second": 100, "burst_probability": 2},
            )
    assert r.status_code == 422
