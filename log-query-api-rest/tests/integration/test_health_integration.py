"""Integration tests for the C1 scaffold: the full ASGI stack, not just the handler.

Where ``tests/unit/test_health.py`` pokes the ``/health`` handler through an app built with a
cheap injected Runtime, these tests drive the **whole** stack the production app assembles —
the CORS middleware, the request-context middleware, the router, and FastAPI's own generated
documentation surface — via the ``seeded_client`` fixture, which builds the app through
:meth:`src.main.Runtime.build_seeded` (the same constructor the production lifespan uses).

``tests/integration/`` grows substantially in C5, when the ``/api/v1`` router lands and the
pagination, filter and envelope contracts get their real coverage. Until then **this module is
the floor**: without it ``tests/integration/`` contains no test at all, ``pytest tests/integration``
exits 5 (``NO_TESTS_COLLECTED``), and ``make test-int`` — a per-commit verification gate for the
whole project — is red for reasons that have nothing to do with the code. Keeping a real,
non-trivial integration test here from C1 keeps that gate meaningful instead of merely green.
"""

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from src.config import Settings
from src.main import API_TITLE, API_VERSION, Runtime, create_app
from src.models import LogEntry


def test_health_through_full_stack(seeded_client):
    """The liveness contract, served through the production-shaped app."""
    response = seeded_client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"status", "version", "uptime_sec", "store_entries"}
    assert body["status"] == "healthy"
    assert body["version"] == API_VERSION == "1.0.0"
    assert isinstance(body["uptime_sec"], float)
    assert body["uptime_sec"] >= 0.0
    # Not pinned to 0: the fixture runs with seed_entries=0 today, but C4 makes build_seeded
    # actually populate the ring and this assertion must survive that.
    assert isinstance(body["store_entries"], int)
    assert body["store_entries"] >= 0


def test_openapi_document_is_served(seeded_client):
    """The self-documenting surface is part of the deliverable, so it is part of the suite."""
    response = seeded_client.get("/openapi.json")

    assert response.status_code == 200
    document = response.json()
    assert document["info"]["title"] == API_TITLE == "Log Query API (REST)"
    assert document["info"]["version"] == API_VERSION == "1.0.0"
    # /health is unversioned on purpose — it is a liveness contract, not a data route, and
    # must not move when /api/v2 lands. This pins that it is documented at the root.
    assert "/health" in document["paths"]
    assert "get" in document["paths"]["/health"]


def test_docs_and_redoc_are_served(seeded_client):
    """Swagger UI and ReDoc both render — the README advertises both, so both are tested."""
    for path in ("/docs", "/redoc"):
        response = seeded_client.get(path)

        assert response.status_code == 200, path
        assert response.headers["content-type"].startswith("text/html"), path


def test_cors_exposes_ratelimit_headers_on_actual_response(seeded_client):
    """Pin the ``expose_headers`` wiring that C8's rate-limit headers depend on.

    Without an explicit ``expose_headers``, the CORS spec limits browser JS to a handful of
    safelisted response headers — so the dashboard could *receive* ``X-RateLimit-Remaining``
    and still be unable to read it, which would quietly defeat the entire "advertise the limit
    on every response" design.

    ``Access-Control-Expose-Headers`` is asserted on the **actual** cross-origin GET, which is
    the only place it means anything: per the CORS spec it applies to real responses, and
    Starlette's ``CORSMiddleware`` accordingly never puts it on a preflight under any
    configuration. The preflight is still exercised here — it has to answer 200 with the
    origin and method it allows, or the browser never issues the real request at all — but it
    is checked for exactly what Starlette does set on it.
    """
    preflight = seeded_client.options(
        "/health",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "*"
    assert "GET" in preflight.headers["access-control-allow-methods"]

    actual = seeded_client.get("/health", headers={"Origin": "http://localhost:5173"})

    assert actual.status_code == 200
    exposed_on_response = actual.headers["access-control-expose-headers"]
    for header in (
        "X-Request-ID",
        "X-RateLimit-Limit",
        "X-RateLimit-Remaining",
        "X-RateLimit-Reset",
        "Retry-After",
        "X-Page-Limit-Clamped",
        "X-Cursor-Truncated",
    ):
        assert header in exposed_on_response


def test_request_id_is_unique_across_requests(seeded_client):
    """A minted correlation id must be fresh per request, or correlation is meaningless."""
    first = seeded_client.get("/health").headers["X-Request-ID"]
    second = seeded_client.get("/health").headers["X-Request-ID"]

    assert first and second
    assert first != second


def test_health_reports_non_zero_store_entries(settings: Settings):
    """``store_entries`` must be the real resident count, not a defensively-zeroed placeholder.

    Every other assertion about this field is ``== 0`` or ``>= 0``, which is satisfied whether
    the probe works or silently fails — and it *did* silently fail: ``_store_entries`` calls
    ``len(store)`` behind ``except (TypeError, ValueError): return 0``, so a store without
    ``__len__`` reported 0 forever while holding thousands of entries. Nothing caught it because
    the store is empty at C4 and 0 was right by accident.

    So this test puts a **known non-zero** count into the ring and demands the route echo it
    exactly. It is written against ``runtime.store`` rather than the seeder, so it keeps holding
    when C5 wires ``build_seeded`` to ``generate_entries`` and the number stops being one this
    test chose.
    """
    runtime = Runtime.build_seeded(settings)
    assert runtime.store is not None
    baseline = len(runtime.store)

    base_ts = datetime(2026, 7, 27, 10, 0, 0, tzinfo=UTC)
    runtime.store.append_many(
        LogEntry(
            id=f"health-{i:04d}",
            ts=base_ts + timedelta(seconds=i),
            level="ERROR",
            service="auth-svc",
            host="node-3",
            message="invalid token",
        )
        for i in range(37)
    )
    expected = baseline + 37
    assert expected <= runtime.store.capacity(), (
        "the appended entries must fit without eviction, or `expected` is not the resident count"
    )

    client = TestClient(create_app(runtime=runtime))
    body = client.get("/health").json()

    assert body["store_entries"] == expected != 0
    assert body["store_entries"] == runtime.store.size()
    assert body["status"] == "healthy"
