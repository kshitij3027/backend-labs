"""Unit tests for GET /health — the C1 spec-verbatim health contract."""


def test_health_returns_200(client):
    assert client.get("/health").status_code == 200


def test_health_spec_verbatim_identity(client):
    body = client.get("/health").json()
    # Exact contract values — the C8 E2E verifier asserts these verbatim too.
    assert body["status"] == "healthy"
    assert body["service"] == "correlation-analysis"
    assert body["version"] == "0.1.0"


def test_health_components_shape(client):
    body = client.get("/health").json()
    components = body["components"]
    assert isinstance(components, dict)
    assert "pipeline_running" in components
    assert isinstance(components["pipeline_running"], bool)
    # Redis is unwired until C3, so the key must exist but may be null.
    assert "redis" in components


def test_health_uptime_and_memory(client):
    body = client.get("/health").json()
    assert body["uptime_seconds"] >= 0
    # memory_mb comes from /proc/self/status (Linux); null on platforms without procfs.
    assert body["memory_mb"] is None or isinstance(body["memory_mb"], (int, float))
