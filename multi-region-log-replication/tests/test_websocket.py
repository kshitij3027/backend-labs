"""WebSocket tests for the ``/ws`` broadcaster endpoint.

We use FastAPI's :class:`~fastapi.testclient.TestClient.websocket_connect`
context manager. It blocks until the server sends the immediate
on-connect snapshot, so a single ``receive_text()`` is guaranteed to
return without us needing a manual sleep or polling loop.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from src.config import AppConfig
from src.http_server import create_app


def _build_client() -> TestClient:
    """Build a fresh TestClient with the canonical 3-region config."""
    config = AppConfig.from_env(
        env={
            "REGIONS": "us-east,europe,asia",
            "PRIMARY_PREFERENCE": "us-east,europe,asia",
        }
    )
    return TestClient(create_app(config))


@pytest.fixture
def client() -> TestClient:
    """Yield a TestClient inside a ``with`` block to fire lifespan events."""
    with _build_client() as c:
        yield c


# ---------------------------------------------------------------------
# /ws
# ---------------------------------------------------------------------


def test_ws_receives_initial_snapshot(client: TestClient) -> None:
    """On connect, the broadcaster pushes a snapshot before any interval tick.

    This is the path the dashboard relies on for first paint — without
    the immediate snapshot the UI would sit blank for up to 5s after
    page load.
    """
    with client.websocket_connect("/ws") as ws:
        raw = ws.receive_text()
        payload = json.loads(raw)
        # The payload must look like a HealthSnapshot.
        assert "overall_status" in payload
        assert "regions" in payload
        assert isinstance(payload["regions"], list)
        assert len(payload["regions"]) == 3


def test_ws_disconnects_cleanly(client: TestClient) -> None:
    """Closing the WS doesn't leak an exception out of the context manager."""
    with client.websocket_connect("/ws") as ws:
        # Drain the immediate snapshot so the server-side ``receive_text``
        # is idle when the context manager closes the connection on exit.
        ws.receive_text()
    # If we got here without a raised exception, disconnect was clean.
    # We don't assert against ``len(connections)`` because the cleanup
    # is asynchronous and the context manager exits before the server
    # task observes the close in some test harness configurations.
