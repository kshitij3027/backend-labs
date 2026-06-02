"""Tests for the ``/ws/metrics`` WebSocket stream (C17).

Two layers:

1. A **unit-style** :class:`~src.websocket.ConnectionManager` test with fake
   sockets (no real server / event-loop transport). It proves that a broadcast
   reaches a healthy client, that a client whose ``send_json`` raises is pruned
   from ``mgr.active``, and that the broadcast itself never raises.

2. An **integration** test that drives the real ASGI app through its lifespan
   with Starlette's (sync) ``TestClient`` — which supports in-process
   WebSockets — connecting to the real compose Redis + Postgres. It seeds a
   small ``raw_logs`` corpus, issues one ``/query`` so the metrics have data,
   then connects to ``/ws/metrics`` and asserts both the immediate push and the
   next periodic tick carry the expected payload shape.

``pytest.ini`` sets ``asyncio_mode = auto``; the unit ``async def`` test runs
without an explicit marker, while the integration test is intentionally **sync**
because ``TestClient.websocket_connect`` is a synchronous context manager.
"""

from __future__ import annotations

import asyncio
import os

import asyncpg
from starlette.testclient import TestClient

from src.db.seed import seed_raw_logs
from src.main import app
from src.websocket import ConnectionManager

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://cache:cache@postgres:5432/cache"
)

# A query window that brackets the seeded corpus (end_ts = 1_780_000_000),
# matching the other API integration tests.
_QUERY_BODY = {
    "query": "error_rate",
    "params": {"source": "api", "start": 1_779_000_000, "end": 1_781_000_000},
}


# --------------------------------------------------------------------------- #
# Unit-style ConnectionManager test (no real server)
# --------------------------------------------------------------------------- #
class _FakeWS:
    """A stand-in WebSocket that records every JSON message it receives."""

    def __init__(self) -> None:
        self.accepted = False
        self.received: list[dict] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, message: dict) -> None:
        self.received.append(message)


class _BadWS(_FakeWS):
    """A WebSocket whose ``send_json`` always raises (a dead client)."""

    async def send_json(self, message: dict) -> None:  # type: ignore[override]
        raise RuntimeError("client gone")


async def test_broadcast_prunes_dead_clients_without_raising() -> None:
    """Broadcast reaches the good client and silently prunes the bad one."""
    mgr = ConnectionManager()
    good = _FakeWS()
    bad = _BadWS()

    await mgr.connect(good)  # type: ignore[arg-type]
    await mgr.connect(bad)  # type: ignore[arg-type]
    assert good.accepted and bad.accepted
    assert mgr.active == {good, bad}  # type: ignore[comparison-overlap]

    # Must not raise even though one client's send_json blows up.
    await mgr.broadcast({"x": 1})

    # The healthy client received the message...
    assert good.received == [{"x": 1}]
    # ...and the dead client was pruned from the active set.
    assert bad not in mgr.active
    assert mgr.active == {good}  # type: ignore[comparison-overlap]


# --------------------------------------------------------------------------- #
# Integration test against the real app + compose Redis/Postgres
# --------------------------------------------------------------------------- #
def _seed_dataset_sync() -> None:
    """Seed a small deterministic ``raw_logs`` corpus via a short-lived pool.

    Runs the async seed against ``DATABASE_URL`` from a synchronous context
    (the test below is sync because ``TestClient`` websockets are sync). Mirrors
    the seeding the other API integration tests perform so ``/query`` returns
    real data and the metrics snapshot is non-empty.
    """

    async def _seed() -> None:
        pool = await asyncpg.create_pool(dsn=DATABASE_URL)
        try:
            async with pool.acquire() as conn:
                await conn.execute("TRUNCATE raw_logs, precomputed_aggregates")
            await seed_raw_logs(pool, 400, seed=7, end_ts=1_780_000_000)
        finally:
            await pool.close()

    asyncio.run(_seed())


def test_ws_metrics_streams_ticks() -> None:
    """Connecting clients get an immediate tick, then a periodic one.

    ``with TestClient(app)`` runs the real lifespan (connecting to the compose
    Redis + Postgres and starting the broadcast loop). We first drive one
    ``/query`` so the metrics have data, then open ``/ws/metrics`` and assert
    the immediate-push payload shape, then assert a *second* payload arrives
    from the background broadcast loop (proving the loop runs).
    """
    _seed_dataset_sync()

    with TestClient(app) as client:
        # Seed metrics with a real served query so the snapshot is populated.
        resp = client.post("/query", json=_QUERY_BODY)
        assert resp.status_code == 200

        with client.websocket_connect("/ws/metrics") as ws:
            # 1) Immediate push on connect.
            msg = ws.receive_json()
            assert msg["type"] == "tick"
            assert "stats" in msg
            assert "series" in msg
            assert "recommendations" in msg
            assert "degraded" in msg

            # 2) Next periodic tick from the broadcast loop (default 2s
            #    interval). Receiving a second payload proves the loop runs.
            nxt = ws.receive_json()
            assert nxt["type"] == "tick"
            assert "stats" in nxt
            assert "series" in nxt
            assert "recommendations" in nxt
            assert "degraded" in nxt
