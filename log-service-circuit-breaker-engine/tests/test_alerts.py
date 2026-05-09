"""Tests for state-change alerter."""
import asyncio
import pytest
from fastapi.testclient import TestClient
from src.api.app import create_app
from src.alerts import StateChangeAlerter
from src.state import CircuitState


def test_alerter_records_event():
    """A single transition produces a single event with the correct fields."""
    alerter = StateChangeAlerter()
    alerter("svc", CircuitState.CLOSED, CircuitState.OPEN, "test")
    events = alerter.events()
    assert len(events) == 1
    assert events[0]["name"] == "svc"
    assert events[0]["from"] == "CLOSED"
    assert events[0]["to"] == "OPEN"
    assert events[0]["reason"] == "test"
    assert "ts" in events[0]


def test_alerter_capped_at_maxlen():
    """The deque caps at maxlen; only the latest entries are retained."""
    alerter = StateChangeAlerter(maxlen=3)
    for i in range(5):
        alerter(f"svc-{i}", CircuitState.CLOSED, CircuitState.OPEN, f"reason-{i}")
    events = alerter.events()
    assert len(events) == 3
    # The latest three entries (i=2,3,4) survived.
    assert [e["name"] for e in events] == ["svc-2", "svc-3", "svc-4"]


def test_alerts_endpoint_returns_events_after_force_open():
    """Driving a real transition surfaces an event through /api/alerts."""
    app = create_app()
    with TestClient(app) as c:
        registry = app.state.registry
        breaker = registry.get("database_primary")
        # Run the async force_open in a fresh loop to avoid loop conflicts:
        asyncio.run(breaker.force_open())
        r = c.get("/api/alerts")
        assert r.status_code == 200
        events = r.json()["events"]
        assert any(e["name"] == "database_primary" for e in events)
