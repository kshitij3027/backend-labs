"""Integration tests for the C5 ``/api/v1`` read surface: list, paginate, filter, fetch.

These drive the **whole** ASGI stack — middleware, router, handlers, response models and
FastAPI's generated OpenAPI document — against a store holding a known, deterministic corpus (see
``tests/integration/conftest.py``).

C7 gated these routes at the ``viewer`` role, so ``seeded_client`` now carries a default bearer
token; nothing else in this file changed, because the read contract did not. What a *missing* or
*insufficient* credential does to these routes is ``tests/integration/test_rbac_api.py``'s
subject, not this file's.

.. rubric:: Why almost nothing here is a hardcoded number

Every expected count is a brute-force tally over the ``corpus`` fixture: ``sum(1 for e in corpus
if …)``. A literal like ``assert body["page"]["total"] == 14`` passes today and silently becomes
a lie the moment the fixture's size or seed changes — and the failure it then produces points at
the test, not at the code. Deriving the expectation from the same entries the store holds keeps
the assertion true for any corpus and keeps a failure meaningful.

The single most important test in this module is
:func:`test_cursor_walk_covers_corpus_exactly_once`. It is the executable form of the whole
pagination design: a cursor walk must visit the corpus **exactly once** — no duplicate, no gap.
"""

import base64
from collections import Counter
from datetime import datetime

import pytest

from src.api.v1 import router as v1_router
from src.config import Settings
from src.main import API_V1_PREFIX
from src.models import CLAMPED_HEADER, LogLevel

LOGS = f"{API_V1_PREFIX}/logs"

#: Page size for the multi-page walks. Deliberately NOT a divisor of the corpus size, so the walk
#: ends on a partial page — the boundary where an off-by-one in `has_more` would hide.
WALK_LIMIT = 17

#: Hard stop for every cursor loop. A pager bug that fails to advance would otherwise hang the
#: suite forever instead of failing it.
MAX_WALK_PAGES = 100


def _ts(item: dict) -> datetime:
    """Parse an item's wire ``ts`` (RFC-3339 with a ``Z`` suffix) back into a datetime."""
    return datetime.fromisoformat(item["ts"].replace("Z", "+00:00"))


def _ids(body: dict) -> list[str]:
    """The ids in one page response, in wire order."""
    return [item["id"] for item in body["items"]]


def _get(client, **params) -> dict:
    """GET /api/v1/logs with ``params``, assert 200, return the decoded body."""
    response = client.get(LOGS, params=params)
    assert response.status_code == 200, response.text
    return response.json()


# ---------------------------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------------------------


def test_list_logs_returns_envelope(seeded_client):
    """Every list response is the `LogPage` envelope — never a bare top-level array."""
    response = seeded_client.get(LOGS)

    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body) == {"items", "page"}
    assert isinstance(body["items"], list)
    # All five keys, always present. `next_cursor` is null rather than absent when the walk is
    # exhausted, so a client can read `page.next_cursor` unconditionally.
    assert set(body["page"]) == {"limit", "returned", "next_cursor", "has_more", "total"}
    assert body["page"]["returned"] == len(body["items"])
    assert set(body["items"][0]) == {
        "id",
        "ts",
        "level",
        "service",
        "host",
        "message",
        "attrs",
    }


def test_list_logs_default_limit_applies(seeded_client, settings: Settings, corpus):
    """No `limit` means the configured default — and nothing of the client's was adjusted."""
    body = _get(seeded_client)

    assert body["page"]["limit"] == settings.default_page_size
    assert body["page"]["returned"] == min(settings.default_page_size, len(corpus))
    assert body["page"]["total"] == len(corpus)
    # The clamp header is a report of an ADJUSTMENT. The client expressed no preference, so
    # there was nothing to adjust and the header must be absent.
    assert CLAMPED_HEADER not in seeded_client.get(LOGS).headers


# ---------------------------------------------------------------------------------------------
# Clamping — the README's explicit "not a 422" requirement
# ---------------------------------------------------------------------------------------------


def test_limit_is_clamped_not_rejected(seeded_client, settings: Settings):
    """`limit=100000` returns the ceiling plus a header. **A 422 here would be a regression.**"""
    response = seeded_client.get(LOGS, params={"limit": 100000})

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["page"]["limit"] == settings.max_page_size
    # The header reports what was REQUESTED; page.limit reports what was served. One value alone
    # cannot tell a client it was adjusted.
    assert response.headers[CLAMPED_HEADER] == "100000"


def test_limit_zero_is_clamped_to_one(seeded_client):
    """The floor clamps the same way the ceiling does — there is no useful "give me zero rows"."""
    response = seeded_client.get(LOGS, params={"limit": 0})

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["page"]["limit"] == 1
    assert body["page"]["returned"] == 1
    assert response.headers[CLAMPED_HEADER] == "0"


# ---------------------------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------------------------


def test_newest_first_by_default(seeded_client):
    """Newest-first is the default, and it is what makes cursor pagination safe."""
    body = _get(seeded_client, limit=50)
    stamps = [_ts(item) for item in body["items"]]

    assert stamps[0] >= stamps[-1]
    assert stamps == sorted(stamps, reverse=True)


def test_order_asc_reverses(seeded_client, corpus):
    """`order=asc` returns the same set in the opposite order.

    The limit covers the whole corpus so both directions describe the *same* match set — with a
    smaller page the two would return different halves and comparing them would prove nothing.
    """
    limit = len(corpus)
    descending = _ids(_get(seeded_client, limit=limit, order="desc"))
    ascending = _ids(_get(seeded_client, limit=limit, order="asc"))

    assert len(descending) == len(corpus)
    assert ascending == list(reversed(descending))
    # The generator emits oldest-first, and the store appends in that order, so ascending seq
    # order is ascending time order.
    assert ascending == [entry.id for entry in corpus]


# ---------------------------------------------------------------------------------------------
# Cursor pagination — the correctness core of the route
# ---------------------------------------------------------------------------------------------


def test_cursor_walk_covers_corpus_exactly_once(seeded_client, corpus):
    """**The most important test in this module.** A full walk visits every entry exactly once.

    No duplicates and no gaps: the id multiset collected across every page must equal the
    corpus's id multiset. Anything else — a mis-signed anchor, an inclusive instead of exclusive
    resume, an `has_more` derived from `len(items) == limit` — shows up right here.
    """
    seen: list[str] = []
    cursor = None
    pages = 0

    while True:
        params: dict[str, object] = {"limit": WALK_LIMIT}
        if cursor is not None:
            params["cursor"] = cursor
        body = _get(seeded_client, **params)

        seen.extend(_ids(body))
        pages += 1
        assert pages <= MAX_WALK_PAGES, "cursor walk failed to terminate"
        # `total` is frozen as of walk start and carried in the cursor, so every page of one
        # walk reports the same number.
        assert body["page"]["total"] == len(corpus)

        cursor = body["page"]["next_cursor"]
        if cursor is None:
            assert body["page"]["has_more"] is False
            break
        assert body["page"]["has_more"] is True

    assert len(seen) == len(corpus)
    assert len(set(seen)) == len(seen), "the walk emitted duplicate entries"
    assert Counter(seen) == Counter(entry.id for entry in corpus)
    assert pages > 1, "the walk must span several pages or it proves nothing about resuming"


def test_cursor_walk_terminates_with_null_next_cursor(seeded_client, corpus):
    """An exact-boundary page must not advertise a page that does not exist.

    `limit == len(corpus)` is the case where `has_more = len(items) == limit` would be wrong: it
    would claim a further page, and a client trusting it would issue a request guaranteed to come
    back empty. `has_more` is decided by reading one record *past* the page instead.
    """
    body = _get(seeded_client, limit=len(corpus))

    assert body["page"]["returned"] == len(corpus)
    assert body["page"]["has_more"] is False
    assert body["page"]["next_cursor"] is None


def test_offset_pagination_matches_cursor_page_one(seeded_client):
    """With no concurrent writes, offset and cursor paging must agree page for page."""
    page_one = _get(seeded_client, limit=10)
    offset_zero = _get(seeded_client, limit=10, offset=0)

    assert _ids(offset_zero) == _ids(page_one)

    page_two = _get(seeded_client, limit=10, cursor=page_one["page"]["next_cursor"])
    offset_ten = _get(seeded_client, limit=10, offset=10)

    assert _ids(offset_ten) == _ids(page_two)
    assert not set(_ids(page_one)) & set(_ids(page_two))


# ---------------------------------------------------------------------------------------------
# Incoherent queries — 400, never a plausible-looking wrong answer
# ---------------------------------------------------------------------------------------------


def test_cursor_and_offset_together_is_400(seeded_client):
    """A cursor already encodes a position, so pairing it with an offset can only be a guess."""
    cursor = _get(seeded_client, limit=5)["page"]["next_cursor"]
    assert cursor is not None

    response = seeded_client.get(LOGS, params={"limit": 5, "cursor": cursor, "offset": 5})

    assert response.status_code == 400, response.text
    assert "mutually exclusive" in response.json()["detail"]


@pytest.mark.parametrize(
    ("cursor", "label"),
    [
        ("garbage", "not base64 and has no prefix"),
        ("b64:!!!!", "prefixed but outside the urlsafe-base64 alphabet"),
        (
            "b64:" + base64.urlsafe_b64encode(b"not-json-at-all").decode().rstrip("="),
            "well-formed base64 whose payload is not JSON",
        ),
        (
            base64.urlsafe_b64encode(b'{"s":1,"o":"desc","f":"deadbeef","t":10}')
            .decode()
            .rstrip("="),
            "a valid payload missing the b64: prefix",
        ),
    ],
)
def test_malformed_cursor_is_400(seeded_client, cursor, label):
    """Every malformed shape is a 400 with an explanation, never a 500 and never page one."""
    response = seeded_client.get(LOGS, params={"cursor": cursor})

    assert response.status_code == 400, f"{label}: {response.text}"
    assert response.json()["detail"], label


def test_cursor_from_different_filter_is_400(seeded_client, corpus):
    """Replaying a cursor against another filter is an error, not a wrong page.

    This is what the fingerprint inside the cursor buys. Without it the anchor is still a
    perfectly well-formed integer, so the store would serve a page that is internally consistent
    and completely wrong — the client would silently skip or repeat an arbitrary slice.
    """
    errors = sum(1 for entry in corpus if entry.level is LogLevel.ERROR)
    assert errors >= 2, "the fixture corpus needs >= 2 ERROR entries for this walk to continue"

    cursor = _get(seeded_client, level="ERROR", limit=1)["page"]["next_cursor"]
    assert cursor is not None

    response = seeded_client.get(LOGS, params={"level": "INFO", "cursor": cursor})

    assert response.status_code == 400, response.text
    assert "different filter" in response.json()["detail"]


def test_since_after_until_is_400(seeded_client):
    """An empty range must not look identical to "no matching logs"."""
    response = seeded_client.get(
        LOGS,
        params={"since": "2026-07-18T12:00:00Z", "until": "2026-07-18T11:00:00Z"},
    )

    assert response.status_code == 400, response.text
    assert "until" in response.json()["detail"]


# ---------------------------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------------------------


def test_filter_by_level(seeded_client, corpus):
    """`?level=` narrows to that level and reports the filtered total."""
    expected = sum(1 for entry in corpus if entry.level is LogLevel.ERROR)
    assert expected > 0, "the fixture corpus must contain ERROR entries"

    body = _get(seeded_client, level="ERROR", limit=len(corpus))

    assert body["page"]["total"] == expected
    assert body["page"]["returned"] == expected
    assert {item["level"] for item in body["items"]} == {"ERROR"}


def test_filter_by_service(seeded_client, corpus):
    """`?service=` narrows to that service, counted against a brute-force tally."""
    service, expected = Counter(entry.service for entry in corpus).most_common(1)[0]

    body = _get(seeded_client, service=service, limit=len(corpus))

    assert body["page"]["total"] == expected
    assert {item["service"] for item in body["items"]} == {service}


def test_filter_by_host(seeded_client, corpus):
    """`?host=` narrows to that host, counted against a brute-force tally."""
    host, expected = Counter(entry.host for entry in corpus).most_common(1)[0]

    body = _get(seeded_client, host=host, limit=len(corpus))

    assert body["page"]["total"] == expected
    assert {item["host"] for item in body["items"]} == {host}


def test_filters_are_anded(seeded_client, corpus):
    """Two filters return the INTERSECTION, not the union — the ANDed contract, verified.

    The pair is chosen as the most common `(ERROR, service)` combination in the fixture, so the
    intersection is guaranteed non-empty *and* guaranteed to be a strict subset of both single
    filters — which is what makes the assertion mean "AND" rather than merely "some rows".
    """
    error_services = Counter(
        entry.service for entry in corpus if entry.level is LogLevel.ERROR
    )
    assert error_services, "the fixture corpus must contain ERROR entries"
    service, expected = error_services.most_common(1)[0]

    body = _get(seeded_client, level="ERROR", service=service, limit=len(corpus))

    assert body["page"]["total"] == expected
    assert body["page"]["returned"] == expected
    for item in body["items"]:
        assert item["level"] == "ERROR"
        assert item["service"] == service

    # And it really is narrower than either filter alone, i.e. an intersection not a union.
    level_only = _get(seeded_client, level="ERROR", limit=len(corpus))["page"]["total"]
    service_only = _get(seeded_client, service=service, limit=len(corpus))["page"]["total"]
    assert expected <= level_only
    assert expected <= service_only
    assert expected < level_only + service_only


def test_time_range_filter_is_inclusive(seeded_client, corpus):
    """`since` and `until` are BOTH inclusive — the boundary entries come back."""
    low, high = corpus[10], corpus[60]
    expected = sum(1 for entry in corpus if low.ts <= entry.ts <= high.ts)

    body = _get(
        seeded_client,
        since=low.ts.isoformat(),
        until=high.ts.isoformat(),
        limit=len(corpus),
    )

    # 51 = indices 10..60 inclusive. Generated timestamps are strictly increasing, so an
    # exclusive bound on either end would drop one and land on 50 or 49.
    assert expected == 51
    assert body["page"]["total"] == expected
    returned = set(_ids(body))
    assert low.id in returned, "`since` must be inclusive"
    assert high.id in returned, "`until` must be inclusive"


def test_substring_filter_is_case_insensitive(seeded_client, corpus):
    """`q` matches a substring of `message` regardless of case, in either direction."""
    message, occurrences = Counter(entry.message for entry in corpus).most_common(1)[0]
    assert occurrences >= 2, "pick a recurring message so the filter has something to find"

    needle = message.lower()
    expected = sum(1 for entry in corpus if needle in entry.message.lower())

    for spelling in (message, message.upper(), message.lower()):
        body = _get(seeded_client, q=spelling, limit=len(corpus))
        assert body["page"]["total"] == expected, spelling
        for item in body["items"]:
            assert needle in item["message"].lower()


def test_total_matches_brute_force_count(seeded_client, corpus):
    """`page.total` is the filtered match count — and values within a field are ORed."""
    wanted = {LogLevel.ERROR, LogLevel.FATAL}
    expected = sum(1 for entry in corpus if entry.level in wanted)
    assert expected > 0, "the fixture corpus must contain ERROR or FATAL entries"

    body = _get(seeded_client, level=["ERROR", "FATAL"], limit=len(corpus))

    assert body["page"]["total"] == expected
    assert body["page"]["returned"] == expected
    assert {item["level"] for item in body["items"]} <= {"ERROR", "FATAL"}


# ---------------------------------------------------------------------------------------------
# Single fetch
# ---------------------------------------------------------------------------------------------


def test_get_entry_by_id_round_trips(seeded_client):
    """An entry read from a page and fetched by id must be byte-identical.

    One schema, two delivery paths — a client writes one parser.
    """
    listed = _get(seeded_client, limit=5)["items"][0]

    response = seeded_client.get(f"{LOGS}/{listed['id']}")

    assert response.status_code == 200, response.text
    assert response.json() == listed


def test_get_unknown_entry_is_404(seeded_client):
    """Unknown (or evicted) ids answer 404 — the ring cannot tell the two apart."""
    response = seeded_client.get(f"{LOGS}/does-not-exist")

    assert response.status_code == 404, response.text


def test_404_body_has_detail(seeded_client):
    """The error envelope's required half is always present and human-readable."""
    body = seeded_client.get(f"{LOGS}/does-not-exist").json()

    assert isinstance(body.get("detail"), str)
    assert body["detail"].strip()


# ---------------------------------------------------------------------------------------------
# Cross-cutting: middleware and the published document
# ---------------------------------------------------------------------------------------------


def test_every_response_carries_request_id(seeded_client):
    """C1's correlation middleware still wraps the v1 routes — including their error paths.

    Correlation ids are only useful if they survive the whole request, so this checks a 200, a
    404 and a 400 rather than just the happy path.
    """
    responses = [
        seeded_client.get(LOGS),
        seeded_client.get(f"{LOGS}/does-not-exist"),
        seeded_client.get(LOGS, params={"cursor": "garbage"}),
    ]

    ids = []
    for response in responses:
        assert response.headers.get("X-Request-ID"), response.request.url
        ids.append(response.headers["X-Request-ID"])
    assert len(set(ids)) == len(ids), "each request must mint its own id"


def test_openapi_documents_v1_routes(seeded_client):
    """The generated document is the deliverable the README advertises, so it is asserted on.

    Checking the *schemas* as well as the paths is what proves the `response_model=` wiring is
    real: a handler that returned a bare dict would still show up in `paths` and would publish
    no schema at all.
    """
    document = seeded_client.get("/openapi.json").json()

    assert LOGS in document["paths"]
    assert "get" in document["paths"][LOGS]
    assert f"{LOGS}/{{entry_id}}" in document["paths"]
    assert "get" in document["paths"][f"{LOGS}/{{entry_id}}"]

    schemas = document["components"]["schemas"]
    for name in ("LogPage", "PageInfo", "LogEntry", "ErrorBody"):
        assert name in schemas, name

    # The prefix is spelled in two places (src/main.API_V1_PREFIX documents it; src/api/v1.py
    # applies it) because importing one from the other would be a cycle. Pin them together.
    assert v1_router.prefix == API_V1_PREFIX == "/api/v1"
