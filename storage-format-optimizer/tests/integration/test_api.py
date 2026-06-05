"""Integration tests for the FastAPI app surface.

Originally these only exercised the C2 wiring (``/health``, lifespan settings,
OpenAPI). As of C19 the lifespan builds the full object graph and mounts the
ingest / query routers, so this module also drives the end-to-end HTTP
ingest -> query data flow: data POSTed to ``/api/ingest`` must come back out of a
subsequent ``/api/query`` POST.

Every test runs against the shared ``client`` fixture (see ``tests/conftest.py``),
which points ``DATA_DIR`` / ``LOG_DIR`` at a per-test temp dir and pins the
migration loop to an hour so state is deterministic — no migration fires
underneath a test. The fixture enters ``TestClient(app)`` as a context manager so
the app's lifespan runs and the object graph is published on ``app.state``.
"""
from __future__ import annotations


# --------------------------------------------------------------------------- #
# C2 surface — now run against the isolated ``client`` fixture.
# --------------------------------------------------------------------------- #
def test_health(client):
    """GET /health returns 200 with exactly the healthy body."""
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "healthy"}


def test_lifespan_sets_settings(client):
    """The lifespan stashes a Settings instance on app.state with defaults."""
    assert client.app.state.settings is not None
    assert client.app.state.settings.api_port == 8000


def test_openapi_available(client):
    """The OpenAPI schema is served, confirming the app is fully wired."""
    assert client.get("/openapi.json").status_code == 200


# --------------------------------------------------------------------------- #
# Shared payloads / helpers for the data-flow tests.
# --------------------------------------------------------------------------- #
# Two entries in the same time bucket (partition_bucket_seconds default = 3600,
# so ts 3600.0 and 3601.0 both land in partition ``p_1``).
ACME_TWO = {
    "tenant": "acme",
    "entries": [
        {"ts": 3600.0, "fields": {"user": "a", "level": "INFO"}},
        {"ts": 3601.0, "fields": {"user": "b", "level": "ERROR"}},
    ],
}


def _ingest(client, body):
    """POST a batch and return the parsed JSON, asserting a 200."""
    r = client.post("/api/ingest", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def _query(client, body):
    """POST a query and return the parsed JSON, asserting a 200."""
    r = client.post("/api/query", json=body)
    assert r.status_code == 200, r.text
    return r.json()


# --------------------------------------------------------------------------- #
# Ingest -> query data flow (all over HTTP).
# --------------------------------------------------------------------------- #
def test_ingest_returns_summary(client):
    """POST /api/ingest reports rows landed, the tenant, and touched partitions."""
    body = _ingest(client, ACME_TWO)
    assert body["ingested"] == 2
    assert body["tenant"] == "acme"
    assert isinstance(body["partitions_touched"], list)
    assert body["partitions_touched"]  # non-empty


def test_ingest_then_query_full_record(client):
    """A full-record query returns every ingested row, flattened, no aggregates."""
    _ingest(client, ACME_TWO)

    body = _query(client, {"tenant": "acme"})
    assert body["aggregates"] is None
    assert body["meta"]["query_class"] == "full_record"

    rows = body["rows"]
    assert len(rows) == 2
    # Rows are flattened to {"ts": ..., **fields}; compare on the field content.
    by_user = {r["user"]: r for r in rows}
    assert set(by_user) == {"a", "b"}
    assert by_user["a"] == {"ts": 3600.0, "user": "a", "level": "INFO"}
    assert by_user["b"] == {"ts": 3601.0, "user": "b", "level": "ERROR"}


def test_query_projection(client):
    """A narrow projection returns only the projected key and classes analytical."""
    _ingest(client, ACME_TWO)

    body = _query(client, {"tenant": "acme", "columns": ["user"]})
    assert body["meta"]["query_class"] == "analytical"  # <= 3 columns
    rows = body["rows"]
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == {"user"}
    assert {r["user"] for r in rows} == {"a", "b"}


def test_query_filter(client):
    """An equality filter returns only the matching row(s)."""
    _ingest(client, ACME_TWO)

    body = _query(
        client,
        {"tenant": "acme", "filters": [{"column": "level", "op": "eq", "value": "ERROR"}]},
    )
    rows = body["rows"]
    assert len(rows) == 1
    assert rows[0]["level"] == "ERROR"
    assert rows[0]["user"] == "b"


def test_query_aggregation(client):
    """count + avg over a numeric column come back as aggregates (rows null)."""
    _ingest(
        client,
        {
            "tenant": "acme",
            "entries": [
                {"ts": 3600.0, "fields": {"latency": 10}},
                {"ts": 3601.0, "fields": {"latency": 20}},
                {"ts": 3602.0, "fields": {"latency": 30}},
            ],
        },
    )

    body = _query(
        client,
        {
            "tenant": "acme",
            "aggregations": [
                {"op": "count"},
                {"op": "avg", "column": "latency"},
            ],
        },
    )
    assert body["rows"] is None
    assert body["meta"]["query_class"] == "analytical"
    aggregates = body["aggregates"]
    assert aggregates["count_all"] == 3
    assert aggregates["avg_latency"] == 20.0


def test_multi_tenant_isolation(client):
    """Each tenant only sees its own data on query."""
    _ingest(client, ACME_TWO)
    _ingest(
        client,
        {
            "tenant": "globex",
            "entries": [
                {"ts": 3600.0, "fields": {"user": "z", "level": "WARN"}},
            ],
        },
    )

    acme = _query(client, {"tenant": "acme"})
    assert {r["user"] for r in acme["rows"]} == {"a", "b"}

    globex = _query(client, {"tenant": "globex"})
    assert len(globex["rows"]) == 1
    assert globex["rows"][0]["user"] == "z"
    assert globex["rows"][0]["level"] == "WARN"


def test_bad_ingest_422(client):
    """Malformed ingest bodies are rejected by Pydantic validation as 422."""
    # Empty entries list violates IngestRequest.entries min_length=1.
    r = client.post("/api/ingest", json={"tenant": "x", "entries": []})
    assert r.status_code == 422, r.text

    # Missing the required ``entries`` key entirely.
    r = client.post("/api/ingest", json={"tenant": "x"})
    assert r.status_code == 422, r.text


def test_query_empty_tenant(client):
    """Querying a tenant with no data returns empty rows and reads no partitions."""
    body = _query(client, {"tenant": "ghost"})
    assert body["rows"] == []
    assert body["meta"]["partitions_read"] == 0
