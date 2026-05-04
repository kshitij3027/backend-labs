"""Failover behaviour — monitor-driven re-election + kill/heal endpoints.

Two flavours of test live here:

1. **Monitor-driven tests** (the bulk): build a bare cluster
   (``Region`` + ``ReplicationController`` + ``ReplicationStatsTracker``
   + ``HealthMonitor``) without the FastAPI app, start the monitor with
   a very short interval (50ms), mark the primary offline, and assert
   the monitor re-elects within a couple of ticks. This bypasses the
   HTTP layer so we exercise the *real* failover path without a TestClient.

2. **HTTP-shape tests** (the kill endpoint): drive ``POST
   /api/regions/{id}/kill`` through the FastAPI ``TestClient`` so the
   ``allow_kill_endpoint`` config gate and the route registration are
   covered.

Why we don't use ``httpx.AsyncClient + ASGITransport`` for the
monitor-driven tests:
  ``httpx.ASGITransport`` does not run the FastAPI ``lifespan``, which
  is what would otherwise start the monitor. Rather than pull in
  ``asgi-lifespan`` as a new dep, we just construct the cluster
  directly and call ``await monitor.start() / .stop()`` manually. That
  keeps the dependency surface flat and the test paths obvious.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Tuple

import pytest
from fastapi.testclient import TestClient

from src.config import AppConfig
from src.health_monitor import HealthMonitor
from src.http_server import create_app
from src.region import Region
from src.replication_controller import ReplicationController
from src.replication_stats import ReplicationStatsTracker


REGION_IDS = ["us-east", "europe", "asia"]
PRIMARY_PREFERENCE = ["us-east", "europe", "asia"]

# 50ms tick — the monitor needs two consecutive unhealthy ticks to fire
# failover, so worst-case detection time is ~150ms (mark_offline lands
# mid-tick + two more ticks). Tests sleep 0.5s to be comfortably safe.
FAST_INTERVAL = 0.05


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


def _build_cluster(
    interval: float = FAST_INTERVAL,
) -> Tuple[
    dict[str, Region],
    ReplicationController,
    ReplicationStatsTracker,
    HealthMonitor,
]:
    """Construct a fresh 3-region cluster wired to a fast HealthMonitor."""
    regions: dict[str, Region] = {rid: Region(rid) for rid in REGION_IDS}
    stats = ReplicationStatsTracker(regions=REGION_IDS)
    controller = ReplicationController(
        regions=regions,
        primary_preference=PRIMARY_PREFERENCE,
        stats=stats,
    )
    monitor = HealthMonitor(
        regions=regions,
        controller=controller,
        stats=stats,
        check_interval_sec=interval,
    )
    return regions, controller, stats, monitor


@pytest.fixture
async def fast_cluster() -> AsyncIterator[
    Tuple[
        dict[str, Region],
        ReplicationController,
        ReplicationStatsTracker,
        HealthMonitor,
    ]
]:
    """Spin up a fast-tick cluster, hand it to the test, then stop the monitor."""
    regions, controller, stats, monitor = _build_cluster()
    await monitor.start()
    try:
        yield regions, controller, stats, monitor
    finally:
        await monitor.stop()


# ---------------------------------------------------------------------
# Monitor-driven failover behaviour
# ---------------------------------------------------------------------


async def test_monitor_does_not_failover_when_primary_healthy(
    fast_cluster,
) -> None:
    """No failover should fire while every region is healthy."""
    _regions, controller, _stats, monitor = fast_cluster

    # Wait through enough ticks (≥4 with 50ms interval) for the
    # background loop to clearly observe the steady state.
    await asyncio.sleep(0.3)

    assert controller.primary_id == "us-east"
    assert monitor.failover_events() == []


async def test_failover_promotes_next_in_preference(fast_cluster) -> None:
    """Killing us-east hands the primary to europe (next in preference)."""
    regions, controller, _stats, monitor = fast_cluster

    regions["us-east"].mark_offline()
    # Two consecutive unhealthy ticks at 50ms = ~100ms; allow plenty of
    # slack so CI flake doesn't bite.
    await asyncio.sleep(0.5)

    assert controller.primary_id == "europe"
    events = monitor.failover_events()
    assert len(events) == 1
    assert events[0]["old_primary"] == "us-east"
    assert events[0]["new_primary"] == "europe"


async def test_failover_skips_unhealthy_secondaries(fast_cluster) -> None:
    """If europe is also down, failover lands on asia."""
    regions, controller, _stats, monitor = fast_cluster

    regions["europe"].mark_offline()
    regions["us-east"].mark_offline()
    # Wait long enough for the monitor to: see us-east unhealthy,
    # bump streak to 2, then re-elect (which iterates the preference
    # list and skips europe because it's also offline).
    await asyncio.sleep(0.5)

    assert controller.primary_id == "asia"
    events = monitor.failover_events()
    assert len(events) == 1
    assert events[0]["new_primary"] == "asia"


async def test_failover_records_event_in_history(fast_cluster) -> None:
    """Failover events expose the right keys + a reasonable elapsed_ms."""
    regions, _controller, _stats, monitor = fast_cluster

    regions["us-east"].mark_offline()
    await asyncio.sleep(0.5)

    events = monitor.failover_events()
    assert len(events) == 1
    event = events[0]
    assert set(event.keys()) == {"at", "old_primary", "new_primary", "elapsed_ms"}
    assert event["old_primary"] == "us-east"
    assert event["new_primary"] == "europe"
    # Election is a few dict lookups — well under 100ms in any
    # plausible environment. We keep the bound generous so a slow CI
    # box still passes.
    assert 0.0 <= event["elapsed_ms"] < 100.0


async def test_writes_after_failover_go_to_new_primary(fast_cluster) -> None:
    """Writes routed through the controller after failover land on the new primary."""
    regions, controller, _stats, monitor = fast_cluster

    regions["us-east"].mark_offline()
    await asyncio.sleep(0.5)
    assert controller.primary_id == "europe"

    entry = await controller.write(
        {"message": "after", "level": "info", "service": "t"}
    )

    # The new primary (europe) should hold the entry; us-east is
    # offline so it's an unhealthy secondary and skipped on the
    # fan-out side; asia is a healthy secondary and should also have
    # received it.
    assert entry.region == "europe"
    assert entry.log_id in regions["europe"].log_store
    assert entry.log_id in regions["asia"].log_store
    assert entry.log_id not in regions["us-east"].log_store
    # And the failover itself was recorded.
    assert len(monitor.failover_events()) == 1


async def test_heal_does_not_auto_promote(fast_cluster) -> None:
    """Healing the original primary should NOT re-promote it (one-way)."""
    regions, controller, _stats, monitor = fast_cluster

    regions["us-east"].mark_offline()
    await asyncio.sleep(0.5)
    assert controller.primary_id == "europe"

    regions["us-east"].mark_online()
    # Wait through several more ticks; the monitor should not flip
    # primary back to us-east.
    await asyncio.sleep(0.4)

    assert controller.primary_id == "europe"
    assert regions["us-east"].is_healthy is True
    assert len(monitor.failover_events()) == 1


async def test_failover_history_is_bounded(fast_cluster) -> None:
    """The deque caps history length at FAILOVER_HISTORY_MAXLEN."""
    _regions, _controller, _stats, monitor = fast_cluster

    # Fabricate more than maxlen synthetic events. The monitor's
    # ``_failover_history`` is a ``collections.deque(maxlen=10)`` so any
    # excess should be silently evicted from the head.
    for i in range(monitor.FAILOVER_HISTORY_MAXLEN + 5):
        monitor._failover_history.append(  # noqa: SLF001 — we're whitebox testing the bound
            {
                "at": float(i),
                "old_primary": "us-east",
                "new_primary": "europe",
                "elapsed_ms": 0.5,
            }
        )
    events = monitor.failover_events()
    assert len(events) == monitor.FAILOVER_HISTORY_MAXLEN
    # Newest event should still be present (the bounded deque drops
    # the oldest entries, not the newest).
    assert events[-1]["at"] == float(monitor.FAILOVER_HISTORY_MAXLEN + 4)


# ---------------------------------------------------------------------
# HTTP endpoints — kill / heal / per-region read
# ---------------------------------------------------------------------


def _build_app(allow_kill: bool = True) -> Tuple[object, TestClient]:
    """TestClient-driven app, with the ``ALLOW_KILL_ENDPOINT`` flag flipped."""
    config = AppConfig.from_env(
        env={
            "REGIONS": "us-east,europe,asia",
            "PRIMARY_PREFERENCE": "us-east,europe,asia",
            "ALLOW_KILL_ENDPOINT": "true" if allow_kill else "false",
        }
    )
    app = create_app(config)
    return app, TestClient(app)


def test_kill_endpoint_marks_region_offline() -> None:
    """``POST /api/regions/us-east/kill`` flips ``is_healthy`` to False."""
    app, client = _build_app(allow_kill=True)
    with client as opened:
        res = opened.post("/api/regions/us-east/kill")
        assert res.status_code == 200
        body = res.json()
        assert body == {"region_id": "us-east", "is_healthy": False}
        # And the underlying region is actually marked offline.
        assert app.state.regions["us-east"].is_healthy is False


def test_kill_endpoint_returns_403_when_disabled() -> None:
    """When ``ALLOW_KILL_ENDPOINT=false`` the route returns 403, not 404."""
    _app, client = _build_app(allow_kill=False)
    with client as opened:
        res = opened.post("/api/regions/us-east/kill")
        assert res.status_code == 403


def test_kill_endpoint_404_for_unknown_region() -> None:
    """An unknown region id returns 404, not 500."""
    _app, client = _build_app(allow_kill=True)
    with client as opened:
        res = opened.post("/api/regions/nope/kill")
        assert res.status_code == 404


def test_heal_endpoint_marks_region_online() -> None:
    """``POST /api/regions/{id}/heal`` flips ``is_healthy`` back to True."""
    app, client = _build_app(allow_kill=True)
    with client as opened:
        opened.post("/api/regions/europe/kill")
        assert app.state.regions["europe"].is_healthy is False

        res = opened.post("/api/regions/europe/heal")
        assert res.status_code == 200
        assert res.json() == {"region_id": "europe", "is_healthy": True}
        assert app.state.regions["europe"].is_healthy is True


def test_per_region_logs_endpoint_returns_replicated_entries() -> None:
    """``GET /api/regions/{id}/logs`` returns entries from that region's local log_store."""
    _app, client = _build_app(allow_kill=True)
    with client as opened:
        write = opened.post(
            "/api/logs",
            json={"message": "x", "level": "info", "service": "t"},
        )
        assert write.status_code == 200
        log_id = write.json()["log_id"]

        # Both secondaries should have received the entry via fan-out.
        for region in ("europe", "asia"):
            res = opened.get(f"/api/regions/{region}/logs?limit=10")
            assert res.status_code == 200
            ids = {e["log_id"] for e in res.json()}
            assert log_id in ids


def test_per_region_logs_endpoint_404_for_unknown() -> None:
    """Unknown region ids 404 from the per-region logs route too."""
    _app, client = _build_app(allow_kill=True)
    with client as opened:
        res = opened.get("/api/regions/nowhere/logs")
        assert res.status_code == 404
