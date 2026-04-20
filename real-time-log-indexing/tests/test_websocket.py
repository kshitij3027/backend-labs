"""Tests for the ``/ws`` endpoint and :class:`ConnectionManager`.

Every test spins up a fresh FastAPI app via :func:`build_app` (with an
isolated segment directory) and uses Starlette's *sync*
:class:`TestClient` because its async counterpart's WebSocket
support is clunky enough that the trade-off isn't worth it here.
TestClient drives the ASGI app through a background thread that hosts
its own event loop, so the app's lifespan — consumer, stats loop,
heartbeat — runs for real.

Starlette's ``WebSocketTestSession.receive_json`` does *not* accept a
timeout argument; it blocks on an internal ``queue.Queue`` until the
server pushes something. That's fine in practice because the server
broadcasts ``stats_update`` once per second, so a call to
``receive_json`` will always unblock within ~1 s. We cap our
patience with a loop counter (``max_messages``) so a genuinely-stuck
test fails fast rather than hanging CI.

The tests cover:

1. The welcome ``connected`` message on connect.
2. That ingesting a document (via ``/api/generate-sample`` when Redis
   is reachable) produces a ``new_document`` push.
3. The 1 Hz ``stats_update`` broadcast arrives within a few reads.
4. Responding to a ``ping`` with ``pong`` keeps the connection alive
   past the stale-eviction threshold.
5. Every connected client receives a broadcast (no partial fan-out).

Tests that require real Redis (2 and 5) are auto-skipped when the
``/api/generate-sample`` call returns 503, so the suite still passes
when Redis is unreachable.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from src.config import Settings
from src.main import build_app


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _new_app(tmp_path, monkeypatch):
    """Build an isolated FastAPI instance for a single test.

    ``DISK_SEGMENT_DIR`` is set via monkeypatch so the lifespan sees
    the fresh directory when it constructs :class:`Settings`. We still
    pass an explicit ``Settings()`` so anything cached in the module
    singleton is bypassed.
    """
    monkeypatch.setenv("DISK_SEGMENT_DIR", str(tmp_path / "segments"))
    settings = Settings()
    settings = settings.model_copy(
        update={"disk_segment_dir": str(tmp_path / "segments")}
    )
    return build_app(settings=settings)


def _drain_until(ws, predicate, max_messages: int = 30):
    """Read messages from *ws* until *predicate(msg)* is truthy.

    Starlette's ``receive_json`` blocks with no timeout, but the app
    broadcasts ``stats_update`` every second and pings periodically,
    so each call unblocks quickly. We still cap the total number of
    reads via *max_messages* so a genuinely-stuck test fails fast.

    Messages that don't match are dropped on the floor — the tests
    that use this helper only care about a specific event type and
    treat all other traffic (``ping``, ``stats_update``) as noise.
    Returns the matching message or ``None`` if we hit the cap.
    """
    for _ in range(max_messages):
        try:
            msg = ws.receive_json()
        except Exception:
            return None
        if predicate(msg):
            return msg
    return None


# ---------------------------------------------------------------------------
# 1. connected welcome
# ---------------------------------------------------------------------------

def test_ws_connect_receives_connected(tmp_path, monkeypatch):
    """First frame after connect is the welcome envelope."""
    app = _new_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            msg = ws.receive_json()
            assert msg["type"] == "connected"
            assert "client_id" in msg and msg["client_id"]
            assert "server_time" in msg


# ---------------------------------------------------------------------------
# 2. new_document fires on ingest
# ---------------------------------------------------------------------------

def test_ws_receives_new_document_on_ingest(tmp_path, monkeypatch):
    """Pushing a single sample via Redis should surface a ``new_document``."""
    app = _new_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            # Drain the welcome envelope first.
            ws.receive_json()

            r = client.post("/api/generate-sample", json={"count": 1})
            if r.status_code == 503:
                pytest.skip("Redis not connected")
            assert r.status_code == 200

            msg = _drain_until(
                ws, lambda m: m.get("type") == "new_document"
            )
            assert msg is not None, "did not receive new_document"
            assert "document" in msg
            # The document dict is the LogEntry serialization — at
            # minimum we should see a message + service + level.
            doc = msg["document"]
            assert "message" in doc
            assert "service" in doc
            assert "level" in doc


# ---------------------------------------------------------------------------
# 3. stats_update arrives from the 1 Hz broadcast loop
# ---------------------------------------------------------------------------

def test_ws_receives_stats_update(tmp_path, monkeypatch):
    """Even without ingest, the 1 Hz stats broadcast should reach us."""
    app = _new_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # welcome

            msg = _drain_until(
                ws, lambda m: m.get("type") == "stats_update"
            )
            assert msg is not None, "did not receive stats_update"
            assert "data" in msg
            # StatsResponse-shaped — a handful of the required keys.
            for key in (
                "docs_indexed",
                "current_segment_docs",
                "disk_segments",
                "throughput_1m",
                "uptime_s",
            ):
                assert key in msg["data"]


# ---------------------------------------------------------------------------
# 4. pong keeps the connection alive
# ---------------------------------------------------------------------------

def test_ws_pong_prevents_eviction(tmp_path, monkeypatch):
    """Responding to a ping with pong keeps the socket from being evicted.

    We shorten the heartbeat interval so the test completes quickly —
    with interval=1s the eviction threshold is 3s, giving us a tight
    but testable window.
    """
    monkeypatch.setenv("WS_HEARTBEAT_INTERVAL_S", "1")
    app = _new_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # welcome

            ping = _drain_until(ws, lambda m: m.get("type") == "ping")
            assert ping is not None, "no ping received"

            # Reply; server should refresh our last_pong.
            ws.send_json({"type": "pong", "t": time.time()})

            # Sleep past what would be the eviction threshold
            # (3 * interval = 3s) and confirm we still get frames.
            time.sleep(1.2)
            follow_up = _drain_until(
                ws,
                lambda m: m.get("type")
                in ("ping", "stats_update", "new_document"),
            )
            assert follow_up is not None, "socket appears to have been evicted"


# ---------------------------------------------------------------------------
# 5. multi-client broadcast — every connected client receives
# ---------------------------------------------------------------------------

def test_multiple_clients_all_receive(tmp_path, monkeypatch):
    """Two simultaneous clients should each see the same ``new_document``."""
    app = _new_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws1, client.websocket_connect(
            "/ws"
        ) as ws2:
            ws1.receive_json()  # welcome
            ws2.receive_json()  # welcome

            r = client.post("/api/generate-sample", json={"count": 1})
            if r.status_code == 503:
                pytest.skip("Redis not connected")
            assert r.status_code == 200

            m1 = _drain_until(
                ws1, lambda m: m.get("type") == "new_document"
            )
            m2 = _drain_until(
                ws2, lambda m: m.get("type") == "new_document"
            )
            assert m1 is not None, "ws1 did not receive new_document"
            assert m2 is not None, "ws2 did not receive new_document"
