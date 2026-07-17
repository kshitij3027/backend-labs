"""Integration tests for the C9 ``/ws`` live feed against a fully loaded NLPEngine.

Drive the real ``/ws`` route through the session-scoped ``loaded_client`` (``conftest.py``):

* the ``"ping"`` -> ``"pong"`` keepalive;
* the POST -> WS broadcast: analyzing a line pushes an ``{"type": "analysis", "data": ...}``
  frame followed by an ``{"type": "stats", "data": ...}`` frame to a connected client;
* disconnect cleanup: leaving the ``with`` block removes the socket from the manager (the live
  set never leaks a dead connection).

**WebSocket + event-loop note.** Starlette's ``TestClient`` runs the ASGI app on an anyio
*portal* (a background thread with its own event loop). A **bare** ``TestClient`` (the way
``loaded_client`` is built) starts a *fresh* portal per call, so an HTTP POST issued while a
WebSocket is open would run on a *different* loop than the WS session — and the server-side
``send_json`` from the analyze handler could then never reach that socket. A self-contained WS
exchange (ping/pong) lives entirely inside the one WS session's loop, so the bare client is fine
there. The broadcast test, which must POST *while a WebSocket is open*, therefore wraps the
already-loaded app in a **context-managed** ``TestClient(loaded_client.app)`` — entering it pins
a single shared portal for both the WS session and the POST, so the broadcast actually reaches
the socket. Reusing ``loaded_client.app`` keeps the expensive engine load shared (no re-load).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

MESSAGE = "auth-svc rejected login for user 4821 from 10.0.0.1"


# --- WebSocket keepalive ----------------------------------------------------------


def test_ws_ping_pong(loaded_client):
    with loaded_client.websocket_connect("/ws") as ws:
        ws.send_text("ping")
        assert ws.receive_text() == "pong"


# --- Broadcast on analyze (POST -> WS) --------------------------------------------


def test_analyze_broadcasts_analysis_and_stats_frames(loaded_client):
    # Context-managed TestClient over the already-loaded app => one shared loop/portal for both
    # the WebSocket session AND the POST, so the broadcast reaches the socket (see module docstring).
    with TestClient(loaded_client.app) as client:
        with client.websocket_connect("/ws") as ws:
            response = client.post("/api/analyze", json={"message": MESSAGE})
            assert response.status_code == 200
            # Exactly two frames are pushed per analyze (one analysis, then one stats); the POST
            # awaits the broadcast before returning, so both are already queued here.
            frames = [ws.receive_json(), ws.receive_json()]

    by_type = {frame["type"]: frame for frame in frames}
    assert set(by_type) == {"analysis", "stats"}

    # The analysis frame carries the full result for the exact line we posted.
    analysis = by_type["analysis"]["data"]
    assert analysis["message"] == MESSAGE
    assert set(analysis) == {"message", "entities", "intent", "sentiment", "keywords"}

    # The stats frame carries a well-formed rolling-stats snapshot.
    stats = by_type["stats"]["data"]
    assert isinstance(stats, dict)
    assert "total_analyzed" in stats
    assert stats["total_analyzed"] >= 1


def test_analyze_broadcasts_to_every_client(loaded_client):
    # Two listeners on one shared portal both receive the analysis frame.
    with TestClient(loaded_client.app) as client:
        with client.websocket_connect("/ws") as ws_a:
            with client.websocket_connect("/ws") as ws_b:
                response = client.post("/api/analyze", json={"message": MESSAGE})
                assert response.status_code == 200
                frame_a = ws_a.receive_json()
                frame_b = ws_b.receive_json()

    # The first frame each client sees is the analysis for the posted line.
    assert frame_a["type"] == frame_b["type"] == "analysis"
    assert frame_a["data"]["message"] == frame_b["data"]["message"] == MESSAGE


# --- Disconnect cleanup -----------------------------------------------------------


def test_ws_disconnect_cleans_up(loaded_client):
    # The manager is reachable off the shared runtime; assert a relative delta (other tests share
    # this session-scoped client) so we never depend on an absolute zero.
    manager = loaded_client.app.state.runtime.connections
    before = manager.count

    with loaded_client.websocket_connect("/ws") as ws:
        # A completed ping/pong round-trip guarantees the server has finished manager.connect()
        # (it only answers "pong" after registering and looping to receive), so counting now is
        # race-free.
        ws.send_text("ping")
        assert ws.receive_text() == "pong"
        assert manager.count == before + 1  # registered while the socket is open

    # Leaving the block disconnects the client (the /ws handler's `finally: manager.disconnect`).
    # TestClient's WS __exit__ blocks until the server coroutine unwinds, so the count is settled.
    assert manager.count == before
