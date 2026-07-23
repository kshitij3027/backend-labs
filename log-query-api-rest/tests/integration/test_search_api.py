"""Integration tests for C9's ``POST /api/v1/logs/search`` — the structured boolean search.

These drive the whole ASGI stack: the ``analyst`` gate, the body schema, the compiler, the shared
pager and the published OpenAPI document, against the same deterministic corpus every other
integration suite uses (``tests/integration/conftest.py``).

Two tests carry most of the weight:

* :func:`test_search_agrees_with_equivalent_get_filter` — for every predicate both routes can
  express, ``POST /logs/search`` and ``GET /logs`` must return the same ids in the same order.
  That is the executable form of "one evaluator, two vocabularies"; without it, the claim that
  the flat filter and the tree describe the same sets is prose.
* :func:`test_search_boolean_tree_matches_brute_force` — the tree is evaluated a **second time**,
  in plain Python over the fixture entries, by an implementation that shares no code with the
  compiler. Two implementations agreeing is evidence; one implementation agreeing with itself is
  not.

Expected counts are never literals. Each is a tally over the ``corpus`` fixture, so a change to
the fixture's size or seed cannot quietly turn an assertion into a lie.
"""

from collections import Counter
from datetime import UTC, datetime
from typing import Any

import pytest

from src.deps import REQUIRED_ROLE_EXTENSION
from src.main import API_V1_PREFIX
from src.models import CLAMPED_HEADER, MAX_FILTER_DEPTH, LogEntry
from src.ratelimit import RATE_LIMIT_HEADERS

from .conftest import bearer_token

SEARCH = f"{API_V1_PREFIX}/logs/search"
LOGS = f"{API_V1_PREFIX}/logs"

#: Page size for the multi-page walks. Small, so a walk over a *filtered* set still spans several
#: pages and ends on a partial one — the boundary where an off-by-one in ``has_more`` would hide.
WALK_LIMIT = 6

#: Hard stop for every cursor loop, so a pager that fails to advance fails the suite instead of
#: hanging it.
MAX_WALK_PAGES = 100

#: Severity order, spelled out here rather than imported. The brute-force evaluator below is only
#: worth having if it is genuinely independent of the code it checks — reusing ``LEVEL_ORDER``
#: would make a mistake in that map invisible to both sides.
SEVERITY = ("DEBUG", "INFO", "WARN", "ERROR", "FATAL")


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


def headers_for(client, username: str) -> dict[str, str]:
    """An ``Authorization`` header for a demo account, minted through the real token route."""
    return {"Authorization": f"Bearer {bearer_token(client, username)}"}


def search(client, body: dict[str, Any], **kwargs) -> dict[str, Any]:
    """POST a search body, assert ``200``, and return the decoded envelope."""
    response = client.post(SEARCH, json=body, **kwargs)
    assert response.status_code == 200, response.text
    return response.json()


def get_logs(client, **params) -> dict[str, Any]:
    """GET the list route with query params, assert ``200``, and return the decoded envelope."""
    response = client.get(LOGS, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def ids(body: dict[str, Any]) -> list[str]:
    """The ids in one page, in wire order."""
    return [item["id"] for item in body["items"]]


def leaf(field: str, op: str, value: Any) -> dict[str, Any]:
    """A leaf node, spelled the way a client would."""
    return {"field": field, "op": op, "value": value}


def nest(depth: int) -> dict[str, Any]:
    """A tree exactly ``depth`` levels deep: ``depth - 1`` nested ``all``s around one leaf."""
    node: dict[str, Any] = leaf("level", "eq", "ERROR")
    for _ in range(depth - 1):
        node = {"all": [node]}
    return node


def brute_force(entry: LogEntry, node: dict[str, Any] | None) -> bool:
    """Evaluate a filter tree against one entry in plain Python. **Shares no code with C9.**

    Deliberately naive — it re-derives everything the compiler precomputes (the severity ordinal,
    the lower-casing, the epoch) from the entry itself. That is the point: an independent
    implementation is only useful as a check if it is actually independent, so this one is written
    the obvious way and is allowed to be slow.
    """
    if node is None:
        return True
    if "all" in node:
        return all(brute_force(entry, child) for child in node["all"])
    if "any" in node:
        return any(brute_force(entry, child) for child in node["any"])
    if "not" in node:
        return not brute_force(entry, node["not"])

    field, op, value = node["field"], node["op"], node["value"]
    if field == "level":
        actual: Any = entry.level.value
    elif field == "service":
        actual = entry.service
    elif field == "host":
        actual = entry.host
    elif field == "message":
        actual = entry.message
    else:
        actual = entry.ts

    if op == "eq":
        return actual == _operand(field, value)
    if op == "ne":
        return actual != _operand(field, value)
    if op == "in":
        return actual in [_operand(field, item) for item in value]
    if op == "nin":
        return actual not in [_operand(field, item) for item in value]
    if op == "contains":
        return str(value).lower() in str(actual).lower()

    # The four order comparisons. `level` compares by severity rank, never alphabetically.
    left, right = (
        (SEVERITY.index(actual), SEVERITY.index(value))
        if field == "level"
        else (actual, _operand(field, value))
    )
    if op == "gt":
        return left > right
    if op == "gte":
        return left >= right
    if op == "lt":
        return left < right
    if op == "lte":
        return left <= right
    raise AssertionError(f"the brute-force evaluator does not implement {op!r}")


def _operand(field: str, value: Any) -> Any:
    """Parse a leaf value the naive way — only ``ts`` needs any work."""
    if field != "ts":
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def expected_ids(corpus, node: dict[str, Any] | None, *, descending: bool = True) -> list[str]:
    """The ids a tree must return, tallied over the fixture corpus in the requested order.

    The corpus is generated oldest-first and appended in that order, so ascending ``seq`` is
    ascending time — which makes "reverse the list" the correct expectation for ``desc``.
    """
    matched = [entry.id for entry in corpus if brute_force(entry, node)]
    return list(reversed(matched)) if descending else matched


# ---------------------------------------------------------------------------------------------
# The gate — analyst, and the first route a viewer token cannot reach
# ---------------------------------------------------------------------------------------------


def test_search_requires_analyst_403_for_viewer(seeded_client):
    """A ``viewer`` holds a perfectly good token that simply does not out-rank this route.

    ``403``, never ``401``: the credential is valid and refreshing it would not help, and the
    absence of the ``WWW-Authenticate`` challenge is the machine-readable half of "do not retry".
    Search is the first route on the ladder where ``viewer`` — the floor, which reads everything
    else — is genuinely refused.
    """
    response = seeded_client.post(
        SEARCH, headers=headers_for(seeded_client, "viewer"), json={}
    )

    assert response.status_code == 403, response.text
    assert "WWW-Authenticate" not in response.headers
    assert "analyst" in response.json()["detail"]


def test_analyst_can_search(seeded_client, corpus):
    """The role the route exists for gets a real, filtered answer."""
    body = {"filter": leaf("level", "eq", "ERROR"), "limit": len(corpus)}
    expected = sum(1 for entry in corpus if entry.level.value == "ERROR")
    assert expected > 0, "the fixture corpus must contain ERROR entries"

    response = seeded_client.post(
        SEARCH, headers=headers_for(seeded_client, "analyst"), json=body
    )

    assert response.status_code == 200, response.text
    page = response.json()
    assert page["page"]["total"] == expected
    assert {item["level"] for item in page["items"]} == {"ERROR"}


def test_writer_and_admin_can_search(seeded_client, corpus):
    """The ladder is inclusive: every role **above** analyst reaches the route too.

    A ladder that admitted only the exact role would mean an admin token could not search, which
    is not a stricter policy — it is a broken one.
    """
    for username in ("writer", "admin"):
        response = seeded_client.post(
            SEARCH, headers=headers_for(seeded_client, username), json={}
        )

        assert response.status_code == 200, f"{username}: {response.text}"
        assert response.json()["page"]["total"] == len(corpus)


def test_search_without_token_is_401(client):
    """No credential at all is "I don't know who you are", with the RFC 9110 challenge.

    Distinct from the ``403`` above, and the distinction is what tells a client whether to
    re-authenticate or to give up.
    """
    response = client.post(SEARCH, json={})

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert "detail" in response.json()


# ---------------------------------------------------------------------------------------------
# The envelope — search and list are the same response, because they are the same code
# ---------------------------------------------------------------------------------------------


def test_search_returns_same_envelope_as_list(seeded_client):
    """Byte-for-byte the same shape as ``GET /logs``: same keys, same item schema, same order.

    Both routes go through the one ``_paginate`` helper, so this is really an assertion that
    nobody has re-implemented pagination inside the search handler. A second pager is how two
    routes end up disagreeing about ``has_more`` on an exact-boundary page.
    """
    searched = search(seeded_client, {"limit": 10})
    listed = get_logs(seeded_client, limit=10)

    assert set(searched) == set(listed) == {"items", "page"}
    assert set(searched["page"]) == set(listed["page"])
    assert searched["items"] == listed["items"]
    assert searched["items"], "the fixture corpus must not be empty"
    assert set(searched["items"][0]) == {
        "id",
        "ts",
        "level",
        "service",
        "host",
        "message",
        "attrs",
    }

    for key in ("limit", "returned", "has_more", "total"):
        assert searched["page"][key] == listed["page"][key], key
    # `next_cursor` is the one field that differs, and it must: a cursor is bound to the
    # fingerprint of the filter that minted it, and the two routes fingerprint differently even
    # for the same match set. That is what stops a list cursor from resuming a search.
    assert searched["page"]["next_cursor"] != listed["page"]["next_cursor"]


#: Predicates every one of which both routes can express. Each row is
#: ``(label, search_body_filter, get_query_params)``.
EQUIVALENT_FILTERS: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
    ("single-level", leaf("level", "eq", "ERROR"), {"level": "ERROR"}),
    (
        "level-disjunction",
        leaf("level", "in", ["ERROR", "FATAL"]),
        {"level": ["ERROR", "FATAL"]},
    ),
    (
        "level-and-service",
        {"all": [leaf("level", "eq", "ERROR"), leaf("service", "eq", "auth-svc")]},
        {"level": "ERROR", "service": "auth-svc"},
    ),
    (
        # Spelled in the other case on purpose: `contains` and `q` are both case-insensitive, so
        # the two must agree even when the requests disagree about capitalisation.
        "substring-over-message",
        leaf("message", "contains", "CONNECTION"),
        {"q": "connection"},
    ),
    (
        "any-over-one-field-is-the-flat-OR",
        {"any": [leaf("host", "eq", "node-1"), leaf("host", "eq", "node-2")]},
        {"host": ["node-1", "node-2"]},
    ),
]


@pytest.mark.parametrize(
    ("label", "tree", "params"),
    EQUIVALENT_FILTERS,
    ids=[label for label, _, _ in EQUIVALENT_FILTERS],
)
def test_search_agrees_with_equivalent_get_filter(seeded_client, corpus, label, tree, params):
    """**The headline test.** The same predicate, two vocabularies, identical rows.

    ``GET /logs`` ANDs across fields and ORs within one; every such query has an exact spelling as
    a tree, and the two must be indistinguishable in the response — same ids, same order, same
    ``total``. This is what "the flat filter and the boolean tree compile to one predicate" means
    operationally, and it is the property that keeps the search route, the list route and (from
    C11) the stats route describing the same set.
    """
    limit = len(corpus)
    searched = search(seeded_client, {"filter": tree, "limit": limit})
    listed = get_logs(seeded_client, limit=limit, **params)

    assert ids(searched) == ids(listed), label
    assert searched["page"]["total"] == listed["page"]["total"], label
    assert searched["page"]["total"] > 0, f"{label}: the predicate must match something"
    assert searched["items"] == listed["items"], label


def test_search_agrees_with_get_on_a_time_range(seeded_client, corpus):
    """``ts gte``/``lte`` and ``?since=``/``?until=`` are the same **inclusive** window.

    Both bounds are inclusive on both routes, and the two boundary entries below are chosen from
    the corpus itself — so an exclusive bound on either side drops a row the tally still counts.
    """
    low, high = corpus[10], corpus[60]
    tree = {
        "all": [
            leaf("ts", "gte", low.ts.isoformat()),
            leaf("ts", "lte", high.ts.isoformat()),
        ]
    }

    searched = search(seeded_client, {"filter": tree, "limit": len(corpus)})
    listed = get_logs(
        seeded_client, since=low.ts.isoformat(), until=high.ts.isoformat(), limit=len(corpus)
    )

    assert ids(searched) == ids(listed)
    assert searched["page"]["total"] == 51, "indices 10..60 inclusive"
    assert low.id in ids(searched)
    assert high.id in ids(searched)


# ---------------------------------------------------------------------------------------------
# What only the tree can say
# ---------------------------------------------------------------------------------------------

#: Trees that a query string cannot express — the reason this route exists. Every one mixes at
#: least two combinators or a negation.
BOOLEAN_TREES: list[tuple[str, dict[str, Any]]] = [
    (
        "or-and-not",
        {
            "all": [
                {"any": [leaf("level", "eq", "ERROR"), leaf("level", "eq", "FATAL")]},
                {"not": leaf("service", "eq", "auth-svc")},
            ]
        },
    ),
    (
        "severity-threshold-with-exclusion",
        {
            "all": [
                leaf("level", "gte", "WARN"),
                {"not": {"any": [leaf("host", "eq", "node-1"), leaf("host", "eq", "node-2")]}},
            ]
        },
    ),
    (
        "three-levels-deep",
        {
            "all": [
                {
                    "any": [
                        {"all": [leaf("level", "eq", "ERROR"), leaf("host", "eq", "node-1")]},
                        leaf("level", "eq", "FATAL"),
                    ]
                },
                {"not": leaf("message", "contains", "zzz-never-occurs")},
            ]
        },
    ),
    ("negated-membership", {"not": leaf("level", "in", ["DEBUG", "INFO"])}),
    (
        "substring-or-severity",
        {
            "any": [
                leaf("message", "contains", "TIMED OUT"),
                leaf("level", "eq", "FATAL"),
            ]
        },
    ),
]


@pytest.mark.parametrize(
    ("label", "tree"), BOOLEAN_TREES, ids=[label for label, _ in BOOLEAN_TREES]
)
def test_search_boolean_tree_matches_brute_force(seeded_client, corpus, label, tree):
    """Every tree is evaluated twice: once by the compiler, once naively. They must agree.

    A differential test rather than a golden one. Hardcoding the expected ids would pin today's
    corpus and would prove only that the answer has not changed; re-deriving them from an
    independent evaluator proves the answer is *right*, for whatever corpus the fixture holds.
    """
    body = search(seeded_client, {"filter": tree, "limit": len(corpus)})
    expected = expected_ids(corpus, tree)

    assert expected, f"{label}: the tree must match something or it proves nothing"
    assert len(expected) < len(corpus), f"{label}: ...and must exclude something too"
    assert ids(body) == expected, label
    assert body["page"]["total"] == len(expected), label


def test_search_empty_filter_returns_everything(seeded_client, corpus):
    """An omitted filter, ``null``, and ``{"all": []}`` all mean the whole corpus.

    The empty conjunction is vacuously true, which is what lets a UI start with an empty ``all``
    and push conditions into it as boxes are ticked without showing an empty result first.
    """
    for body in ({}, {"filter": None}, {"filter": {"all": []}}):
        page = search(seeded_client, {**body, "limit": len(corpus)})

        assert page["page"]["total"] == len(corpus), body
        assert page["page"]["returned"] == len(corpus), body
        assert Counter(ids(page)) == Counter(entry.id for entry in corpus), body


def test_search_empty_any_returns_nothing(seeded_client):
    """``{"any": []}`` is the mirror rule: vacuously false, so the page is honestly empty.

    And ``total`` is ``0``, not the corpus size — a filter that matches nothing must never be
    mistaken for an unconstrained one, which is exactly what would happen if the compiler
    reported it as "empty".
    """
    page = search(seeded_client, {"filter": {"any": []}})

    assert page["items"] == []
    assert page["page"]["total"] == 0
    assert page["page"]["has_more"] is False
    assert page["page"]["next_cursor"] is None


# ---------------------------------------------------------------------------------------------
# Pagination — the same cursor contract as the list route
# ---------------------------------------------------------------------------------------------


def test_search_cursor_pagination_walks_result_set_once(seeded_client, corpus):
    """A full cursor walk over a filtered set visits every match exactly once — no gap, no repeat.

    The same property ``test_cursor_walk_covers_corpus_exactly_once`` pins for ``GET /logs``, now
    over a *filtered* walk, which is the harder case: the anchor advances by ``seq`` while the
    page is measured in matches, so an implementation that confused the two would drift.
    """
    tree = {"any": [leaf("level", "eq", "ERROR"), leaf("level", "eq", "FATAL")]}
    expected = expected_ids(corpus, tree)
    assert len(expected) > WALK_LIMIT, "the walk must span several pages to prove anything"

    seen: list[str] = []
    cursor: str | None = None
    pages = 0

    while True:
        body = search(
            seeded_client,
            {"filter": tree, "limit": WALK_LIMIT, **({"cursor": cursor} if cursor else {})},
        )
        seen.extend(ids(body))
        pages += 1
        assert pages <= MAX_WALK_PAGES, "cursor walk failed to terminate"
        # `total` is frozen as of walk start and carried inside the cursor, so it is the same
        # number on every page even though the compiler recomputes nothing between them.
        assert body["page"]["total"] == len(expected)

        cursor = body["page"]["next_cursor"]
        if cursor is None:
            assert body["page"]["has_more"] is False
            break
        assert body["page"]["has_more"] is True

    assert pages > 1
    assert len(set(seen)) == len(seen), "the walk emitted duplicates"
    assert seen == expected


def test_search_cursor_rejected_against_different_filter(seeded_client):
    """A cursor is bound to its search. Replaying it elsewhere is a ``400``, never a wrong page.

    Without the fingerprint the anchor is still a perfectly well-formed integer, so the store
    would serve a page that is internally consistent and completely wrong — the client would
    silently skip or repeat an arbitrary slice of the corpus and have no way to notice.
    """
    first = search(seeded_client, {"filter": leaf("level", "eq", "ERROR"), "limit": 1})
    cursor = first["page"]["next_cursor"]
    assert cursor is not None

    foreign = seeded_client.post(
        SEARCH, json={"filter": leaf("level", "eq", "INFO"), "cursor": cursor}
    )

    assert foreign.status_code == 400, foreign.text
    assert "different filter" in foreign.json()["detail"]

    # Same filter, other direction: also refused, and with the message that names the real cause.
    reversed_walk = seeded_client.post(
        SEARCH,
        json={
            "filter": leaf("level", "eq", "ERROR"),
            "sort": {"order": "asc"},
            "cursor": cursor,
        },
    )
    assert reversed_walk.status_code == 400, reversed_walk.text
    assert "sort order" in reversed_walk.json()["detail"]

    # And the cursor still works against the search that minted it — the rejection above is about
    # identity, not about the cursor being malformed.
    assert (
        seeded_client.post(
            SEARCH, json={"filter": leaf("level", "eq", "ERROR"), "cursor": cursor}
        ).status_code
        == 200
    )


@pytest.mark.parametrize("cursor", ["garbage", "b64:!!!!", ""])
def test_search_malformed_cursor_is_400(seeded_client, cursor):
    """Every malformed cursor shape is a ``400`` with an explanation — never a ``500``, never
    a silent fall back to page one."""
    response = seeded_client.post(SEARCH, json={"cursor": cursor})

    assert response.status_code == 400, response.text
    assert response.json()["detail"]


def test_search_sort_asc_and_desc(seeded_client, corpus):
    """``sort.order`` reverses the same match set, and ``desc`` is the default when omitted."""
    tree = leaf("level", "gte", "WARN")
    limit = len(corpus)

    descending = search(seeded_client, {"filter": tree, "limit": limit})
    explicit_desc = search(
        seeded_client, {"filter": tree, "limit": limit, "sort": {"order": "desc"}}
    )
    ascending = search(
        seeded_client,
        {"filter": tree, "limit": limit, "sort": {"field": "ts", "order": "asc"}},
    )

    assert ids(descending) == ids(explicit_desc), "desc is the default"
    assert ids(ascending) == list(reversed(ids(descending)))
    assert ids(descending) == expected_ids(corpus, tree)
    assert ids(ascending) == expected_ids(corpus, tree, descending=False)

    stamps = [item["ts"] for item in ascending["items"]]
    assert stamps == sorted(stamps)


def test_search_limit_clamped(seeded_client, settings):
    """An over-large ``limit`` is clamped and reported — **a 422 here would be a regression**.

    Identical to the list route's contract, because it is the identical code: ``clamp_limit``
    never raises, the header carries what was asked for, and ``page.limit`` carries what was
    served. One value alone cannot tell a client it was adjusted.
    """
    response = seeded_client.post(SEARCH, json={"limit": 100_000})

    assert response.status_code == 200, response.text
    assert response.json()["page"]["limit"] == settings.max_page_size
    assert response.headers[CLAMPED_HEADER] == "100000"

    # The floor clamps the same way, and expressing no preference adjusts nothing.
    floor = seeded_client.post(SEARCH, json={"limit": 0})
    assert floor.status_code == 200, floor.text
    assert floor.json()["page"]["limit"] == 1
    assert floor.headers[CLAMPED_HEADER] == "0"
    assert CLAMPED_HEADER not in seeded_client.post(SEARCH, json={}).headers


# ---------------------------------------------------------------------------------------------
# Rejected bodies — a 422 before a single record is read
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"filter": leaf("severity", "eq", "ERROR")}, id="unknown-field"),
        pytest.param({"filter": leaf("message", "regex", "^a")}, id="unknown-op"),
        pytest.param({"filter": leaf("level", "contains", "ERR")}, id="op-field-mismatch"),
        pytest.param({"filter": leaf("level", "in", "ERROR")}, id="in-with-a-scalar"),
        pytest.param({"filter": leaf("level", "eq", ["ERROR"])}, id="eq-with-a-list"),
        pytest.param({"filter": leaf("level", "eq", "TRACE")}, id="value-not-a-level"),
        pytest.param({"filter": {"all": [], "any": []}}, id="two-combinators-in-one-node"),
        pytest.param({"offset": 5}, id="unknown-top-level-key-offset"),
        pytest.param({"filters": {"all": []}}, id="unknown-top-level-key-typo"),
        pytest.param({"sort": {"field": "level"}}, id="unsupported-sort-field"),
        pytest.param({"filter": {"all": [leaf("level", "eq", "ERROR")] * 200}}, id="too-wide"),
        pytest.param({"filter": nest(MAX_FILTER_DEPTH + 1)}, id="too-deep"),
    ],
)
def test_search_malformed_body_is_422(seeded_client, body):
    """A body the schema refuses is a ``422`` — a client mistake, distinct from an auth failure
    and distinct from an incoherent-but-valid request (which is a ``400``).

    Every one of these is rejected at **parse time**, before a record is touched. A filter that
    only failed on the thousandth row would already have burned the request, and one that
    silently matched nothing would be indistinguishable from "there are no such logs" to the
    person reading the empty page during an incident.
    """
    response = seeded_client.post(SEARCH, json=body)

    assert response.status_code == 422, response.text
    assert response.json()["detail"]


def test_search_deeply_nested_body_is_422_not_a_500(seeded_client):
    """A tree far past the cap is refused cheaply, and the refusal names the limit.

    Depth is measured on the raw decoded JSON by an **iterative** walk before pydantic recurses
    into the tree, so rejecting a hostile body costs one pass and no stack. The depth here is
    deliberately hostile-but-modest (~25x the cap) rather than astronomical: FastAPI's own ``422``
    renderer json-encodes the rejected input *recursively*, so the transport's practical ceiling
    is lower than the validator's, and a test that went to five thousand levels would be measuring
    the framework's error formatter rather than this project's cap. The unbounded case is pinned
    where it belongs and where it is truly stack-free —
    ``tests/unit/test_filters.py::test_deeply_nested_tree_does_not_recurse_to_death``.
    """
    response = seeded_client.post(SEARCH, json={"filter": nest(200)})

    assert response.status_code == 422, response.status_code
    assert "nested deeper" in response.text


def test_search_rejects_a_bare_array_body(seeded_client):
    """The body is an object with a ``filter`` key, not a bare tree and not an array."""
    assert seeded_client.post(SEARCH, json=[leaf("level", "eq", "ERROR")]).status_code == 422
    assert seeded_client.post(SEARCH, json="ERROR").status_code == 422


# ---------------------------------------------------------------------------------------------
# Cross-cutting: metering and the published document
# ---------------------------------------------------------------------------------------------


def test_search_carries_ratelimit_headers(seeded_client):
    """Search is metered like every other gated route, and says so on every response.

    The ``X-RateLimit-*`` triple rides on successes as well as failures — a limit a client can
    only discover by tripping it is a limit it will discover by tripping it. They are attached by
    the middleware rather than the handler precisely so the ``403`` below carries them too.
    """
    ok = seeded_client.post(SEARCH, json={"limit": 1})
    assert ok.status_code == 200, ok.text
    for header in RATE_LIMIT_HEADERS:
        assert header in ok.headers, header

    assert int(ok.headers["X-RateLimit-Limit"]) > 0
    assert int(ok.headers["X-RateLimit-Remaining"]) >= 0
    assert ok.headers["X-Request-ID"]

    denied = seeded_client.post(SEARCH, headers=headers_for(seeded_client, "viewer"), json={})
    assert denied.status_code == 403
    for header in RATE_LIMIT_HEADERS:
        assert header in denied.headers, header


def test_openapi_documents_search_route(seeded_client):
    """The route, its role and its whole body vocabulary are in the published document.

    The recursive filter union is the part worth asserting: ``FilterAll`` references
    ``FilterNode`` before it exists, so without the ``model_rebuild()`` calls in ``src/models.py``
    this document would not generate at all and ``/openapi.json`` would answer ``500``. A schema
    that fails to publish is a contract nobody can generate a client from.
    """
    document = seeded_client.get("/openapi.json").json()

    assert SEARCH in document["paths"]
    operation = document["paths"][SEARCH]["post"]
    assert operation[REQUIRED_ROLE_EXTENSION] == "analyst"
    assert operation["security"] == [{"bearerAuth": []}]
    assert operation["tags"] == ["logs"]
    for code in ("200", "400", "401", "403", "422", "429"):
        assert code in operation["responses"], code

    schemas = document["components"]["schemas"]
    for name in ("SearchRequest", "SortSpec", "FilterLeaf", "FilterAll", "FilterAny", "FilterNot"):
        assert name in schemas, name

    # The response is the same envelope the list route publishes — one schema, two routes.
    assert (
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        == document["paths"][LOGS]["get"]["responses"]["200"]["content"]["application/json"][
            "schema"
        ]["$ref"]
    )

    # `offset` is absent from the request schema, deliberately: a nested filter over a live ring
    # is a stream the caller walks with a cursor, not a table to jump around in.
    assert set(schemas["SearchRequest"]["properties"]) == {
        "filter",
        "sort",
        "limit",
        "cursor",
    }


# ---------------------------------------------------------------------------------------------
# Hostile bodies — the refusal has to survive the code that reports it
#
# A depth cap that answers `500` on the exact input it was written to refuse is not a cap, it is
# an advertisement. FastAPI's *default* validation handler json-encodes the rejected input
# recursively, so a body deep enough to matter blew the stack inside the error handler and the
# caller was told the server broke. `src.main.validation_exception_handler` reports `type`, `loc`
# and `msg` and never touches the input, which is what these pin.
# ---------------------------------------------------------------------------------------------


#: A string that stands in for what a real search body carries — a customer id, an internal
#: hostname, the term someone is grepping production for during an incident. It has to be
#: distinctive enough that finding it anywhere in a response is unambiguous.
CANARY = "SUPERSECRET-CANARY-9f3a1b"

#: Depths walked by the regression pin. Well past anything a client could mean and past every
#: cap the model publishes; the point is not the number but that no number produces a ``500``.
HOSTILE_DEPTHS = (500, 1_000, 5_000)


def deep_body_bytes(depth: int) -> bytes:
    """Serialise a ``depth``-level ``{"all": [...]}`` tree **without recursing**.

    :func:`nest` builds the dict iteratively, but posting it with httpx's ``json=`` would run it
    through :func:`json.dumps`, whose C encoder recurses once per container — twice per level
    here, for the object and its array — against the interpreter's recursion limit. The *test*
    would then die of precisely the failure mode it exists to check, and it would die in the
    client. Concatenating the wire bytes keeps the test's own stack flat and puts the body on the
    wire exactly as a hostile client would.
    """
    leaf_json = '{"field":"level","op":"eq","value":"ERROR"}'
    body = '{"filter":' + '{"all":[' * (depth - 1) + leaf_json + "]}" * (depth - 1) + "}"
    return body.encode()


def post_raw_json(client, url: str, payload: bytes, headers: dict[str, str] | None = None):
    """POST pre-serialised bytes under the content type httpx's ``json=`` would have set."""
    merged = {"content-type": "application/json"}
    if headers:
        merged.update(headers)
    return client.post(url, content=payload, headers=merged)


def test_extremely_deep_body_is_422_not_500(seeded_client):
    """A body nested thousands of levels deep is **refused**, and refusing it is never a ``500``.

    The regression pin for the whole section. Two refusals are legitimate here and which one
    arrives depends on the interpreter, not on this project:

    * ``422`` — the body decoded, ``SearchRequest``'s iterative depth walk refused it, and
      :func:`~src.main.validation_exception_handler` rendered that refusal without re-encoding
      the rejected tree. This is what the fix buys; before it the same body was a ``500``.
    * ``400`` — the stdlib JSON decoder itself refused to parse a document this deep (its C
      scanner recurses once per container), and FastAPI maps that to "there was an error parsing
      the body" before any model is consulted.

    Both are the server saying *no* about the client's body. ``500`` is the server saying it
    broke, and that is the assertion that matters — it is written first and separately from the
    status-set check so a failure reads as "it 500s again" rather than as a set mismatch. The
    exact depth at which the second outcome takes over from the first moves between CPython
    releases (3.11 refuses to decode at a few hundred levels; 3.12 and 3.14 parse thousands), so
    pinning one status per depth would pin the interpreter's recursion accounting rather than
    this project's behaviour.
    """
    analyst = headers_for(seeded_client, "analyst")

    for depth in HOSTILE_DEPTHS:
        response = post_raw_json(seeded_client, SEARCH, deep_body_bytes(depth), analyst)

        assert response.status_code != 500, (depth, response.text[:300])
        assert response.status_code in (400, 422), (depth, response.status_code)

        # Whichever refusal it is, it stays small: a hostile body must not be able to buy itself
        # a hostile *response*, which is what echoing the input back would have made it.
        assert len(response.content) < 4096, (depth, len(response.content))

        if response.status_code == 422:
            assert "nested deeper" in response.text, depth
        else:
            assert "parsing the body" in response.text, depth


def test_validation_error_body_does_not_echo_the_input(seeded_client):
    """The rejected body is never quoted back — not the tree, not the terms inside it.

    Dropping ``input`` from the reported errors is what makes the deep-body case above cheap, but
    it is worth having on its own account. ``POST /logs/search`` takes its filter in a **body**
    partly so that search terms stay out of proxy access logs, browser history and referrers,
    which a query string cannot promise; echoing the body back inside a ``422`` would hand those
    same terms to every error-tracking sink a client pipes failures into, and quietly undo the
    reason the route is a ``POST`` at all.

    The honest boundary: this removes the *wholesale* echo of the rejected structure. A validator
    that quotes an offending scalar in its own message — ``'TRACE' is not a log level`` — still
    surfaces that one scalar, which is bounded, deliberate and what makes the message useful. So
    the canary here sits in a body whose refusal is structural, which is the shape a real search
    body has.
    """
    analyst = headers_for(seeded_client, "analyst")

    # One level past the cap, with the sensitive term where a real client would put it.
    tree: dict[str, Any] = leaf("message", "contains", CANARY)
    for _ in range(MAX_FILTER_DEPTH):
        tree = {"all": [tree]}

    response = seeded_client.post(SEARCH, headers=analyst, json={"filter": tree})

    assert response.status_code == 422, response.text
    assert CANARY not in response.text, response.text[:500]

    body = response.json()
    assert body["errors"], body
    for error in body["errors"]:
        assert "input" not in error, error
        assert "ctx" not in error, error
        assert set(error) == {"type", "loc", "msg"}, error
    # Belt and braces: not under some other key, not nested, not anywhere.
    assert '"input"' not in response.text

    # The same holds for a body rejected on a key the schema does not know: the *value* of that
    # key is the client's data, and a refusal has no reason to repeat it.
    unknown_key = seeded_client.post(SEARCH, headers=analyst, json={"offset": CANARY})

    assert unknown_key.status_code == 422, unknown_key.text
    assert CANARY not in unknown_key.text, unknown_key.text[:500]


def test_validation_error_body_has_detail_and_request_id(seeded_client):
    """A ``422`` is the project's error envelope, not FastAPI's — and it is correlatable.

    Every route in ``src/api/v1.py`` publishes :class:`~src.models.ErrorBody` as its ``422``
    model, whose ``detail`` is a **string**; FastAPI's default handler puts a *list* there, so
    until this handler existed the wire contradicted the document it published. The ``request_id``
    is the same one the middleware stamps on the header, so the line in the operator's log and
    the body the client kept are the same request without anyone having to correlate by timestamp.
    """
    from src.main import VALIDATION_ERROR_CODE  # local: keeps src.main out of collection

    response = seeded_client.post(
        SEARCH, headers=headers_for(seeded_client, "analyst"), json={"filters": {"all": []}}
    )

    assert response.status_code == 422, response.text
    body = response.json()

    assert isinstance(body["detail"], str), body
    assert body["detail"].strip()
    assert body["code"] == VALIDATION_ERROR_CODE
    assert body["request_id"] == response.headers["X-Request-ID"]


def test_ordinary_422_still_reports_useful_location(seeded_client):
    """Hardening the renderer must not flatten an everyday mistake into "something was wrong".

    The counterweight to everything above. A client that misspells an operator has to be told
    *where* — ``loc`` is the only part of a validation error that answers that, and a handler
    that dropped it in the name of safety would trade a rare ``500`` for a permanent debugging
    tax on ordinary callers.
    """
    response = seeded_client.post(
        SEARCH,
        headers=headers_for(seeded_client, "analyst"),
        json={"filter": leaf("message", "regex", "^a")},
    )

    assert response.status_code == 422, response.text
    locations = [error["loc"] for error in response.json()["errors"]]

    assert locations, response.text
    # Every path is anchored where the failure was — the body, not a query param or a header.
    assert all(location and location[0] == "body" for location in locations)
    assert any("filter" in location for location in locations), locations
    # And the offending key itself is named, not merely the object containing it.
    assert any("op" in location for location in locations), locations
