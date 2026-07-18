"""Unit tests for GET /health — the unversioned, dependency-free liveness contract.

The probe is consumed by the container HEALTHCHECK and by compose's
``condition: service_healthy``, so the two properties under test here are non-negotiable:
it returns 200 while the process is alive **whatever** the runtime looks like, and it carries
a correlation id on every response.
"""

from fastapi.testclient import TestClient

from src.main import API_VERSION, Runtime, create_app


def test_health_returns_healthy(client):
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["version"] == API_VERSION
    # Injected runtimes are unseeded (seed_entries=0), so the ring is empty and uptime is
    # a small non-negative float rather than a fixed value.
    assert body["store_entries"] == 0
    assert body["uptime_sec"] >= 0.0
    assert set(body) == {"status", "version", "uptime_sec", "store_entries"}


def test_health_sets_request_id_header(client):
    """A client that supplies no id still gets one back, so every response is correlatable."""
    response = client.get("/health")

    request_id = response.headers.get("X-Request-ID")
    assert request_id
    # uuid4().hex — 32 lower-case hex characters.
    assert len(request_id) == 32

    # Two requests must not share an id, or correlation is meaningless.
    other = client.get("/health").headers["X-Request-ID"]
    assert other != request_id


def test_health_echoes_supplied_request_id(client):
    """A caller-supplied id is echoed verbatim so a trace can span services."""
    supplied = "caller-supplied-trace-id-42"

    response = client.get("/health", headers={"X-Request-ID": supplied})

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == supplied


def test_health_works_without_runtime(settings):
    """No runtime on app.state must degrade to zeroed vitals — never a 500.

    Simulates a half-wired or pre-startup process by deleting the injected runtime. The route
    reads everything through ``getattr(..., default)`` precisely so this case stays a 200: a
    liveness probe that 500s when the app is degraded reports the opposite of the truth to the
    orchestrator.
    """
    app = create_app(runtime=Runtime.build(settings))
    del app.state.runtime
    assert getattr(app.state, "runtime", None) is None

    response = TestClient(app).get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["uptime_sec"] == 0.0
    assert body["store_entries"] == 0
