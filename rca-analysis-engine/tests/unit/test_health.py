"""Unit tests for GET /api/health — the C1 spec-verbatim health contract."""


def test_health_returns_200(client):
    assert client.get("/api/health").status_code == 200


def test_health_spec_verbatim_body(client):
    body = client.get("/api/health").json()
    # Exact contract — the two keys, nothing more. The C10 E2E verifier asserts this
    # verbatim too, so it must never change.
    assert body == {"status": "healthy", "analyzer_ready": True}


def test_health_is_dependency_free(client):
    # No runtime dependency: repeated calls always return the same 200 body.
    first = client.get("/api/health")
    second = client.get("/api/health")
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json() == {"status": "healthy", "analyzer_ready": True}
