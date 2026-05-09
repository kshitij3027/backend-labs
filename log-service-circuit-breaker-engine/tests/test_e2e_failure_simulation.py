"""End-to-end test that drives a full failure cycle through the FastAPI app."""
import asyncio
import pytest
from fastapi.testclient import TestClient
from src.api.app import create_app


@pytest.fixture
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


@pytest.mark.e2e
def test_full_failure_simulation_cycle(client):
    # 1. Health
    r = client.get("/health")
    assert r.status_code == 200

    # 2. Process logs cleanly
    r = client.post("/api/process/logs", json={"count": 25})
    assert r.status_code == 200
    body = r.json()
    assert body["processed"] == 25

    # 3. Drive primary to OPEN by directly toggling the injector — simpler than waiting on the simulate endpoint.
    services = client.app.state.services
    primary = services["database_primary"]
    primary.injector.set_failure_rate(1.0)

    # Drive enough calls to trip the breaker
    for _ in range(8):
        client.post("/api/process/logs", json={"count": 3})

    # 4. Verify breaker is OPEN
    metrics = client.get("/api/metrics").json()
    assert metrics["circuits"]["database_primary"]["state"] == "OPEN"

    # 5. Reset — simulating recovery
    primary.injector.set_failure_rate(0.0)
    asyncio.run(client.app.state.registry.reset_all())

    # 6. Process again and verify CLOSED + counters cleared
    r = client.post("/api/process/logs", json={"count": 5})
    assert r.status_code == 200
    metrics = client.get("/api/metrics").json()
    assert metrics["circuits"]["database_primary"]["state"] == "CLOSED"

    # 7. Verify alerts captured the OPEN transition
    alerts = client.get("/api/alerts").json()["events"]
    assert any(e["name"] == "database_primary" and e["to"] == "OPEN" for e in alerts)

    # 8. Verify Prometheus output exists
    prom = client.get("/metrics").text
    assert "circuit_breaker_state" in prom
