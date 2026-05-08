"""Tests for WebSocket /ws/metrics broadcaster."""
import time

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app


@pytest.fixture
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


def test_websocket_emits_initial_snapshot(client):
    """A freshly-connected client should receive a snapshot immediately."""
    with client.websocket_connect("/ws/metrics") as ws:
        snapshot = ws.receive_json()
        assert isinstance(snapshot, dict)
        assert "circuits" in snapshot
        assert "processing" in snapshot
        assert "generated_at" in snapshot


def test_websocket_snapshot_contains_all_breakers(client):
    """First frame should include all four registered breakers."""
    with client.websocket_connect("/ws/metrics") as ws:
        snapshot = ws.receive_json()
        circuits = snapshot["circuits"]
        assert "database_primary" in circuits
        assert "database_backup" in circuits
        assert "queue_main" in circuits
        assert "external_api" in circuits


def test_websocket_disconnect_does_not_break_server(client):
    """Disconnecting a client should not break the server; new clients still get frames."""
    with client.websocket_connect("/ws/metrics") as ws:
        first = ws.receive_json()
        assert "circuits" in first
    # Reconnect — server must still serve frames.
    with client.websocket_connect("/ws/metrics") as ws2:
        second = ws2.receive_json()
        assert "circuits" in second


def test_history_accumulates_after_broadcaster_runs(monkeypatch):
    """With a fast broadcast interval, the history ring buffer should fill up."""
    monkeypatch.setenv("WEBSOCKET_BROADCAST_INTERVAL", "0.1")
    app = create_app()
    with TestClient(app) as c:
        time.sleep(0.5)
        r = c.get("/api/metrics/history")
        assert r.status_code == 200
        history = r.json()["history"]
        assert len(history) >= 2
        assert "circuits" in history[0]
