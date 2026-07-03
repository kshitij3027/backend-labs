"""Unit tests for the health endpoint.

C13 replaced the C1 shallow liveness probe with a **deep** readiness ``/health``
that additionally reports per-subsystem booleans (``components``) and the corpus
size. The new body is a strict SUPERSET of the C1 payload
(``status`` / ``service`` / ``version`` are retained). These tests run in the
profile-gated ``test`` container where Postgres + Redis ARE up, so ``status`` is
still ``"ok"`` and the required-dependency components are ``True``.

The endpoint always returns HTTP 200 while the process is alive; a degraded
dependency is signalled in the body (``status: "degraded"``), never as a non-2xx.
"""

from __future__ import annotations


def test_health_returns_200(client) -> None:
    """GET /health responds with HTTP 200 (never 500), even to a deep probe."""
    response = client.get("/health")
    assert response.status_code == 200


def test_health_payload(client) -> None:
    """GET /health returns the deep C13 payload; the C1 fields are retained.

    In the ``test`` profile Postgres + Redis are reachable, so ``status`` is
    ``"ok"`` and the required-dependency components probe ``True``.
    """
    body = client.get("/health").json()

    # C1 fields preserved.
    assert body["status"] == "ok"
    assert body["service"] == "log-recommendation-engine"
    assert body["version"] == "0.1.0"

    # C13 deep-probe additions: a `components` object of documented booleans + a
    # best-effort corpus size.
    assert "components" in body
    components = body["components"]
    for field in ("database", "vector_extension", "redis", "embedding_model"):
        assert field in components, f"missing component field {field!r}"
        assert isinstance(components[field], bool)

    # Required dependencies for status=="ok" must be up in the test profile.
    assert components["database"] is True
    assert components["redis"] is True
    assert components["vector_extension"] is True

    assert "corpus_size" in body
    assert isinstance(body["corpus_size"], int)
    assert body["corpus_size"] >= 0
