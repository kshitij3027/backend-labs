"""Integration tests for ``GET /api/v1/stats`` — the aggregate panel, over real HTTP.

The headline test in this file is
:func:`test_stats_total_equals_list_page_total_for_same_filter`. Everything else supports it.

That equality is the reason the endpoint is designed the way it is: stats are computed on demand
over ``store.iter_matching`` with the *same* filter object the list route walks, rather than
maintained as incremental counters, so ``StatsSnapshot.total`` and ``LogPage.page.total`` cannot
drift. A design that "guarantees by construction" is only worth the phrase if something checks
the construction, and a unit test over ``compute_stats`` is not enough — the two numbers reach a
client through two *routes*, each with its own query parsing, its own filter build and its own
response model, and any of those is a place for the sets to diverge. So it is asserted end to
end, parametrized over several filters, and then asserted again against ``POST /logs/search``
with an equivalent boolean tree.

Nothing here sleeps.
"""

from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.auth import DEV_PASSWORDS
from src.deps import REQUIRED_ROLE_EXTENSION
from src.main import API_V1_PREFIX
from src.models import LogEntry
from src.ratelimit import RATE_LIMIT_HEADERS

STATS = f"{API_V1_PREFIX}/stats"
LOGS = f"{API_V1_PREFIX}/logs"
SEARCH = f"{API_V1_PREFIX}/logs/search"
TOKEN_URL = f"{API_V1_PREFIX}/auth/token"

#: Filters swept by the agreement tests, as query-parameter bundles. Chosen to cover every shape
#: the flat vocabulary has: no filter at all, a single value, a multi-value OR, a substring, a
#: two-field AND, and one that matches nothing — the last being the case where an incremental
#: aggregate would be most likely to report the whole store instead of the empty set.
AGREEMENT_FILTERS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("unfiltered", {}),
    ("single-level", {"level": ["ERROR"]}),
    ("level-or", {"level": ["ERROR", "FATAL"]}),
    ("substring", {"q": "e"}),
    ("level-and-substring", {"level": ["INFO"], "q": "a"}),
    ("matches-nothing", {"service": ["no-such-service"]}),
)


def headers_for(client: TestClient, username: str) -> dict[str, str]:
    """``Authorization`` header for a demo account, minted through the real token route."""
    response = client.post(
        TOKEN_URL, data={"username": username, "password": DEV_PASSWORDS[username]}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def get_stats(client: TestClient, **params: Any) -> dict[str, Any]:
    """GET the stats route, assert ``200``, and return the decoded snapshot."""
    response = client.get(STATS, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def brute_force_count(corpus: tuple[LogEntry, ...], params: dict[str, Any]) -> int:
    """Count the corpus the dumb way, from the same query bundle the routes were given.

    Independent of ``Filter``, ``compile_filter`` and ``compute_stats`` — a plain loop over the
    fixture's own entries. If it shared an evaluator with the code under test it could only
    confirm self-consistency, which is the one thing an agreement test must not settle for.
    """
    levels = params.get("level")
    services = params.get("service")
    needle = params.get("q")
    return sum(
        1
        for entry in corpus
        if (levels is None or entry.level.value in levels)
        and (services is None or entry.service in services)
        and (needle is None or needle.lower() in entry.message.lower())
    )


# ---------------------------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------------------------


def test_stats_requires_viewer_401_without_token(client):
    """No credential is a ``401`` with a challenge — not a ``403``, and not a public snapshot.

    Aggregates are still corpus data: the count of FATALs per service tells an unauthenticated
    caller a great deal about a system it is not entitled to read. "I don't know who you are"
    is the honest answer, and the ``WWW-Authenticate`` header is what tells a client how to fix
    it.
    """
    response = client.get(STATS)

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"].startswith("Bearer")


def test_viewer_can_read_stats(seeded_client):
    """The bottom of the ladder can read stats — the README's viewer row, exercised.

    A role that may page through every entry may certainly see their sum, so putting stats above
    ``viewer`` would gate a strictly weaker capability more tightly than the thing it summarises.
    """
    response = seeded_client.get(STATS, headers=headers_for(seeded_client, "viewer"))

    assert response.status_code == 200, response.text
    assert response.json()["total"] > 0


def test_openapi_documents_stats_route(client):
    """The route publishes its role requirement and its response schema.

    ``x-required-role`` is derived by :class:`~src.deps.RoleDocumentedRoute` from the dependency
    tree it is about to enforce, so this assertion is not restating a decorator — it is proving
    the gate is a dependency rather than an ``if`` inside the handler, which is the entire claim
    the README makes about this API's authorization.
    """
    spec = client.get("/openapi.json").json()

    assert STATS in spec["paths"], sorted(spec["paths"])
    operation = spec["paths"][STATS]["get"]

    assert operation[REQUIRED_ROLE_EXTENSION] == "viewer"
    assert operation["security"] == [{"bearerAuth": []}]
    assert "401" in operation["responses"]
    assert "429" in operation["responses"]
    assert "StatsSnapshot" in spec["components"]["schemas"], sorted(
        spec["components"]["schemas"]
    )


# ---------------------------------------------------------------------------------------------
# The guarantee
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "params", [pytest.param(p, id=name) for name, p in AGREEMENT_FILTERS]
)
def test_stats_total_equals_list_page_total_for_same_filter(seeded_client, corpus, params):
    """**The headline guarantee.** ``/stats`` total == ``/logs`` ``page.total``, same filter.

    Both numbers are also compared against a brute-force tally of the fixture corpus, so a bug
    that made *both* routes agree on the wrong number cannot pass. Two implementations agreeing
    is necessary; two implementations agreeing with an independent count is the actual claim.

    The ``matches-nothing`` case is in the sweep on purpose: an aggregate maintained incrementally
    would most plausibly fail exactly there, reporting the whole store because the filter never
    reached the counter.
    """
    snapshot = get_stats(seeded_client, **params)
    page = seeded_client.get(LOGS, params={**params, "limit": 1})
    assert page.status_code == 200, page.text

    assert snapshot["total"] == page.json()["page"]["total"]
    assert snapshot["total"] == brute_force_count(corpus, params)


def test_stats_total_equals_search_page_total(seeded_client):
    """The same equality against ``POST /logs/search`` and an equivalent boolean tree.

    Worth its own test because search reaches the store through a *different* filter object — a
    compiled tree rather than the flat bundle. The two compile to the same predicate by design,
    and this is where "by design" is checked: a client that switches from the query string to the
    search body must not see its totals move.
    """
    tree = {"any": [{"field": "level", "op": "eq", "value": "ERROR"},
                    {"field": "level", "op": "eq", "value": "FATAL"}]}

    snapshot = get_stats(seeded_client, level=["ERROR", "FATAL"])
    searched = seeded_client.post(SEARCH, json={"filter": tree, "limit": 1})
    assert searched.status_code == 200, searched.text

    assert snapshot["total"] == searched.json()["page"]["total"]


def test_stats_by_level_matches_the_corpus(seeded_client, corpus):
    """``by_level`` equals a brute-force tally of the fixture, key for key.

    Observed keys only: a level nobody logged is absent rather than present with a zero, which is
    what makes ``by_level`` readable as "what is in this set" rather than "what could have been".
    """
    expected: dict[str, int] = {}
    for entry in corpus:
        expected[entry.level.value] = expected.get(entry.level.value, 0) + 1

    assert get_stats(seeded_client)["by_level"] == expected


# ---------------------------------------------------------------------------------------------
# Shape, liveness and resolution
# ---------------------------------------------------------------------------------------------


def test_stats_shape_matches_model(seeded_client):
    """Every documented field is present, typed as the model says, and internally consistent.

    The consistency checks are the interesting half: the buckets must sum to ``total`` and the
    per-service counts must too, so a response that merely *has* the right keys but computed them
    over different sets fails here rather than in a dashboard three weeks later.
    """
    body = get_stats(seeded_client)

    assert set(body) == {
        "total",
        "by_level",
        "by_service",
        "buckets",
        "top_errors",
        "window",
        "ingest",
    }
    assert isinstance(body["total"], int)
    assert sum(body["by_level"].values()) == body["total"]
    assert sum(body["by_service"].values()) == body["total"]
    assert sum(point["count"] for point in body["buckets"]) == body["total"]

    for point in body["buckets"]:
        assert set(point) == {"bucket_start", "count"}
        assert point["bucket_start"].endswith("Z"), point

    for message in body["top_errors"]:
        assert set(message) == {"message", "count"}
        assert message["count"] >= 1

    window = body["window"]
    assert set(window) == {
        "earliest",
        "latest",
        "bucket_sec",
        "requested_bucket_sec",
        "generated_at",
    }
    assert window["earliest"].endswith("Z") and window["latest"].endswith("Z")
    assert window["generated_at"].endswith("Z")
    assert window["earliest"] <= window["latest"], "RFC-3339 Z sorts lexicographically"

    ingest = body["ingest"]
    assert set(ingest) == {"entries_total", "resident", "capacity", "evicted", "per_sec"}
    assert ingest["entries_total"] == ingest["resident"] + ingest["evicted"]
    assert ingest["capacity"] >= ingest["resident"]


def test_stats_updates_after_append(seeded_client):
    """A write moves the counters — the endpoint reads the live store, not a cached snapshot.

    A read-only aggregate can look perfectly healthy while being completely inert, which is the
    failure this project's tests exist to catch. One append, one re-read, three numbers that must
    all have moved by exactly one.
    """
    before = get_stats(seeded_client, service=["stats-probe"])
    assert before["total"] == 0

    created = seeded_client.post(
        LOGS,
        json={
            "level": "ERROR",
            "service": "stats-probe",
            "host": "node-probe",
            "message": "stats liveness marker",
        },
    )
    assert created.status_code == 201, created.text

    after = get_stats(seeded_client, service=["stats-probe"])

    assert after["total"] == 1
    assert after["by_level"] == {"ERROR": 1}
    assert after["by_service"] == {"stats-probe": 1}
    assert [m["message"] for m in after["top_errors"]] == ["stats liveness marker"]
    assert after["ingest"]["entries_total"] == before["ingest"]["entries_total"] + 1


def test_stats_bucket_sec_override_is_echoed(seeded_client):
    """An explicit ``bucket_sec`` is honoured, and the effective width comes back in the body.

    Echoing the resolution is not decoration: a client that asked for 5-second buckets and
    silently received 300-second ones would label its own x-axis wrongly and have no way to find
    out. The requested value is echoed too, so "you got what you asked for" is distinguishable
    from "you got something coarser".
    """
    fine = get_stats(seeded_client, bucket_sec=5)
    coarse = get_stats(seeded_client, bucket_sec=3600)

    assert fine["window"]["requested_bucket_sec"] == 5
    assert fine["window"]["bucket_sec"] == 5
    assert coarse["window"]["bucket_sec"] == 3600
    # Finer resolution over the same corpus means at least as many points, and the same total.
    assert len(fine["buckets"]) >= len(coarse["buckets"])
    assert fine["total"] == coarse["total"]
    assert sum(p["count"] for p in fine["buckets"]) == sum(
        p["count"] for p in coarse["buckets"]
    )


def test_stats_rejects_out_of_range_bucket_sec(seeded_client):
    """``bucket_sec`` is bounded at the edge — a ``422``, before any aggregation runs.

    The one place this endpoint does reject rather than degrade. ``bucket_sec=0`` has no honest
    interpretation (a zero-width bucket is not a coarser answer, it is no answer), so it is
    refused by the parameter declaration rather than silently rewritten into something the client
    did not ask for.
    """
    assert seeded_client.get(STATS, params={"bucket_sec": 0}).status_code == 422
    assert seeded_client.get(STATS, params={"bucket_sec": 100_000}).status_code == 422


def test_stats_on_impossible_filter_returns_zeros_not_500(seeded_client):
    """An over-narrow filter is a well-formed zero snapshot at ``200``.

    Every dashboard's filter bar can produce an empty set — it is what happens the moment someone
    types a service name that has been quiet for an hour. A ``500`` there would make an ordinary
    interaction look like an outage.
    """
    body = get_stats(seeded_client, service=["definitely-not-a-service"], q="zzzz-nope")

    assert body["total"] == 0
    assert body["by_level"] == {}
    assert body["by_service"] == {}
    assert body["buckets"] == []
    assert body["top_errors"] == []
    assert body["window"]["earliest"] is None
    assert body["window"]["latest"] is None
    # The store is not empty, and `ingest` still says so — it describes the store, not the query.
    assert body["ingest"]["resident"] > 0


def test_stats_rejects_incoherent_bounds_the_same_way_the_list_route_does(seeded_client):
    """``since`` after ``until`` is a ``400`` here exactly as it is on ``GET /logs``.

    Both routes validate through the same :class:`~src.models.LogQuery`, so a bound pair the list
    route refuses cannot be quietly accepted by the route that summarises the same set — which
    would be the worst kind of disagreement, since it would return a confident number for a query
    the rest of the API considers meaningless.
    """
    params = {"since": "2026-07-27T12:00:00Z", "until": "2026-07-27T10:00:00Z"}

    stats_response = seeded_client.get(STATS, params=params)
    list_response = seeded_client.get(LOGS, params=params)

    assert stats_response.status_code == 400, stats_response.text
    assert list_response.status_code == 400, list_response.text
    assert stats_response.json()["detail"] == list_response.json()["detail"]


def test_stats_carries_ratelimit_headers(seeded_client):
    """The metering triple rides on the stats response like every other response.

    The README's promise is that limits are advertised on *every* response, not only on
    rejection, so a dashboard polling this route every five seconds can pace itself from the
    headers instead of discovering the ceiling by hitting it.
    """
    response = seeded_client.get(STATS)

    assert response.status_code == 200, response.text
    for header in RATE_LIMIT_HEADERS:
        assert header in response.headers, dict(response.headers)
    assert int(response.headers["X-RateLimit-Remaining"]) >= 0
