"""Integration tests for C7: the role ladder over HTTP, and the writer append route.

``tests/unit/test_rbac.py`` proves the *decision* — sixteen (held, minimum) pairs against
``require_role`` called as a plain coroutine. This file proves the **chain**: a real form login
mints a real token, the token walks ``HTTPBearer`` -> ``current_principal`` -> the role guard ->
the handler, and every rejection comes back as the status code the README specifies rather than
as its neighbour.

The distinction that matters throughout, and the one the README states outright, is that
``401`` means "I don't know who you are" and ``403`` means "I know, and no". Almost every
assertion below is ultimately about keeping those two apart: a ``403`` that should have been a
``401`` sends a client hunting for credentials it already has, and a ``401`` that should have
been a ``403`` sends it into a re-authentication loop that can never succeed.

Tokens are obtained through ``POST /auth/token`` rather than by calling
:func:`~src.auth.create_access_token` directly. Minting one in-process would be faster, but it
would also test a token this API never actually issues; the ``bcrypt_rounds=4`` the ``settings``
fixture pins makes the real login cost ~2 ms, which is cheap enough that the more honest option
is also the practical one.

Nothing here sleeps.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from src.auth import DEV_ACCOUNTS, DEV_PASSWORDS, Role, role_satisfies
from src.deps import (
    REQUIRED_ROLE_EXTENSION,
    role_denied_detail,
    role_requirement_note,
)
from src.main import API_V1_PREFIX, Runtime, create_app
from src.models import MAX_ATTR_VALUE_LEN, MAX_ATTRS_KEYS, LogEntry

TOKEN_URL = f"{API_V1_PREFIX}/auth/token"
ME_URL = f"{API_V1_PREFIX}/auth/me"
LOGS_URL = f"{API_V1_PREFIX}/logs"
SEARCH_URL = f"{API_V1_PREFIX}/logs/search"
STREAM_URL = f"{API_V1_PREFIX}/logs/stream"
STATS_URL = f"{API_V1_PREFIX}/stats"
MEMORY_URL = f"{API_V1_PREFIX}/debug/memory"

#: The single-fetch route as it appears in ``/openapi.json``. ``.format(entry_id=…)`` turns it
#: into a concrete path, so the RBAC matrix and the OpenAPI assertions can share one spelling.
ENTRY_PATH_TEMPLATE = f"{API_V1_PREFIX}/logs/{{entry_id}}"


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


def token_for(client: TestClient, username: str) -> str:
    """Log in as a demo account and return the bearer token, asserting the login worked."""
    response = client.post(
        TOKEN_URL, data={"username": username, "password": DEV_PASSWORDS[username]}
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


def auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def headers_for(client: TestClient, username: str) -> dict[str, str]:
    """``Authorization`` header for a demo account, in one step."""
    return auth_header(token_for(client, username))


def append_body(**overrides: object) -> dict[str, object]:
    """A minimal valid ``LogCreate`` body, with per-test overrides merged on top.

    Built by a function rather than shared as a module constant because several tests mutate
    their copy — a shared dict would let one test's ``attrs`` bag leak into the next one's
    assertion.
    """
    body: dict[str, object] = {
        "level": "INFO",
        "service": "c7-svc",
        "host": "c7-node",
        "message": "append from the C7 writer route",
    }
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------------------------
# 401 before 403 — the gate can only refuse someone it has already identified
# ---------------------------------------------------------------------------------------------


def test_logs_requires_token_401(client):
    """The read route is gated now. No token at all is a ``401`` with the RFC 9110 challenge."""
    response = client.get(LOGS_URL)

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert "detail" in response.json()


def test_append_without_token_is_401_not_403(client):
    """Even on a route almost nobody may use, "who are you" is answered before "may you".

    Returning ``403`` here would be the tempting shortcut — the anonymous caller certainly is not
    a writer — but it would tell a client with an expired token that its credentials are
    irrelevant, when in fact refreshing them is exactly the fix.
    """
    response = client.post(LOGS_URL, json=append_body())

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_expired_or_garbage_token_on_append_is_401(client):
    response = client.post(
        LOGS_URL, headers={"Authorization": "Bearer not-a-jwt"}, json=append_body()
    )

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"


# ---------------------------------------------------------------------------------------------
# viewer — the floor of the ladder reads everything and writes nothing
# ---------------------------------------------------------------------------------------------


def test_viewer_can_list_logs(seeded_client, corpus):
    response = seeded_client.get(LOGS_URL, headers=headers_for(seeded_client, "viewer"))

    assert response.status_code == 200, response.text
    assert response.json()["page"]["total"] == len(corpus)


def test_viewer_can_get_single_entry(seeded_client, corpus):
    expected = corpus[0]

    response = seeded_client.get(
        f"{LOGS_URL}/{expected.id}", headers=headers_for(seeded_client, "viewer")
    )

    assert response.status_code == 200, response.text
    assert response.json()["id"] == expected.id


def test_viewer_can_read_me(client):
    response = client.get(ME_URL, headers=headers_for(client, "viewer"))

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["subject"] == "viewer"
    assert body["role"] == Role.VIEWER.value


def test_viewer_cannot_append_403(client):
    """A perfectly good token that simply does not out-rank the route. ``403``, never ``401``."""
    response = client.post(
        LOGS_URL, headers=headers_for(client, "viewer"), json=append_body()
    )

    assert response.status_code == 403, response.text
    assert response.status_code != 401
    # The absence of the challenge is the machine-readable half of "do not retry this".
    assert "WWW-Authenticate" not in response.headers
    assert response.json()["detail"] == role_denied_detail(Role.VIEWER, Role.WRITER)


def test_analyst_cannot_append_403(client):
    """One rung up the ladder and still short: ``analyst`` buys search and SSE, not writes."""
    response = client.post(
        LOGS_URL, headers=headers_for(client, "analyst"), json=append_body()
    )

    assert response.status_code == 403, response.text
    assert response.json()["detail"] == role_denied_detail(Role.ANALYST, Role.WRITER)


def test_viewer_with_invalid_body_still_gets_403(client):
    """The gate runs **before** the body is validated, so a refusal leaks no schema information.

    FastAPI solves sub-dependencies ahead of body binding, which is the ordering this asserts: an
    unauthorized caller must not be able to probe the accepted field set by watching ``422``s and
    ``403``s alternate. (Malformed *JSON* is still a ``422`` — that failure happens in the ASGI
    body reader, before any dependency exists to consult.)
    """
    response = client.post(
        LOGS_URL,
        headers=headers_for(client, "viewer"),
        json={"level": "NOT-A-LEVEL", "nonsense": True},
    )

    assert response.status_code == 403, response.text


# ---------------------------------------------------------------------------------------------
# writer — the append route
# ---------------------------------------------------------------------------------------------


def test_writer_can_append_201(client):
    """The happy path: ``201``, a ``Location`` header, and a server-minted id and timestamp."""
    sent_at = datetime.now(UTC)

    response = client.post(
        LOGS_URL, headers=headers_for(client, "writer"), json=append_body()
    )

    assert response.status_code == 201, response.text
    body = response.json()

    # The body is a full LogEntry, so a client never has to follow `Location` to see what it
    # wrote. Validating through the model (rather than eyeballing keys) also pins the response
    # against the published schema.
    entry = LogEntry.model_validate(body)
    assert set(body) == {"id", "ts", "level", "service", "host", "message", "attrs"}

    assert response.headers["Location"] == f"{LOGS_URL}/{entry.id}"

    # Server-minted id: a uuid4 hex, 32 characters of pure hex.
    assert len(entry.id) == 32
    assert all(char in "0123456789abcdef" for char in entry.id)

    # Server-minted ts: right now, in the API's one timestamp format.
    assert body["ts"].endswith("Z")
    assert "+00:00" not in body["ts"]
    assert abs((entry.ts - sent_at).total_seconds()) < 5

    assert entry.message == "append from the C7 writer route"
    assert entry.attrs == {}


def test_writer_can_append_with_explicit_id_and_ts(client):
    """Both defaults are overridable, because a shipper replaying its buffer needs them to be.

    The original event time and the shipper's own idempotency key are exactly what would be lost
    if the server insisted on minting both.
    """
    response = client.post(
        LOGS_URL,
        headers=headers_for(client, "writer"),
        json=append_body(
            id="c7-explicit-id-0001",
            ts="2026-01-02T03:04:05.678Z",
            attrs={"request_id": "b1c2d3", "status": "401"},
        ),
    )

    assert response.status_code == 201, response.text
    body = response.json()

    assert body["id"] == "c7-explicit-id-0001"
    assert body["ts"] == "2026-01-02T03:04:05.678Z"
    assert body["attrs"] == {"request_id": "b1c2d3", "status": "401"}
    assert response.headers["Location"] == f"{LOGS_URL}/c7-explicit-id-0001"


def test_explicit_ts_in_another_zone_is_normalised_to_utc(client):
    """The store must never hold a mixed-timezone corpus; the model normalises on the way in."""
    response = client.post(
        LOGS_URL,
        headers=headers_for(client, "writer"),
        json=append_body(ts="2026-01-02T05:04:05.678+02:00"),
    )

    assert response.status_code == 201, response.text
    assert response.json()["ts"] == "2026-01-02T03:04:05.678Z"


def test_admin_inherits_writer_permissions(client):
    """The ladder is inclusive: ``admin`` out-ranks ``writer``, so it may append."""
    response = client.post(
        LOGS_URL, headers=headers_for(client, "admin"), json=append_body()
    )

    assert response.status_code == 201, response.text


def test_appended_entry_is_immediately_readable(client):
    """Write then read: the round trip C10's SSE test and C12's E2E marker both rest on."""
    headers = headers_for(client, "writer")

    created = client.post(
        LOGS_URL, headers=headers, json=append_body(message="readable immediately")
    )
    assert created.status_code == 201, created.text
    entry = created.json()

    fetched = client.get(f"{LOGS_URL}/{entry['id']}", headers=headers)

    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == entry


def test_append_location_header_addresses_the_new_entry(client):
    """``Location`` is not decoration — following it verbatim must return the created entry."""
    headers = headers_for(client, "writer")

    created = client.post(LOGS_URL, headers=headers, json=append_body())
    assert created.status_code == 201, created.text

    followed = client.get(created.headers["Location"], headers=headers)

    assert followed.status_code == 200, followed.text
    assert followed.json() == created.json()


def test_appended_entry_increments_total(seeded_client):
    """``page.total`` is the size of the match set, so a write moves it by exactly one."""
    headers = headers_for(seeded_client, "writer")

    before = seeded_client.get(LOGS_URL, headers=headers).json()["page"]["total"]

    created = seeded_client.post(LOGS_URL, headers=headers, json=append_body())
    assert created.status_code == 201, created.text

    after = seeded_client.get(LOGS_URL, headers=headers).json()["page"]["total"]

    assert after == before + 1


def test_appended_entry_appears_in_filtered_list(seeded_client):
    """The write lands in the same indexes the read path scans — not in a side channel."""
    marker = "c7-marker-4f2a9b1d"
    headers = headers_for(seeded_client, "writer")

    created = seeded_client.post(
        LOGS_URL,
        headers=headers,
        json=append_body(level="ERROR", message=f"boom {marker} boom"),
    )
    assert created.status_code == 201, created.text

    page = seeded_client.get(
        LOGS_URL, headers=headers, params={"level": "ERROR", "q": marker}
    )

    assert page.status_code == 200, page.text
    body = page.json()
    assert body["page"]["total"] == 1
    assert [item["id"] for item in body["items"]] == [created.json()["id"]]


def test_duplicate_client_supplied_id_is_accepted(client):
    """A repeated id is appended, not rejected — the documented contract of an append-only ring.

    Both records stay resident and both are counted; the id map simply points at the newer one.
    Rejecting instead would require an unbounded "ids ever seen" index behind a bounded store,
    which is the memory leak the ring exists to prevent.
    """
    headers = headers_for(client, "writer")

    first = client.post(LOGS_URL, headers=headers, json=append_body(id="dup-c7", message="first"))
    second = client.post(
        LOGS_URL, headers=headers, json=append_body(id="dup-c7", message="second")
    )

    assert first.status_code == 201, first.text
    assert second.status_code == 201, second.text

    fetched = client.get(f"{LOGS_URL}/dup-c7", headers=headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["message"] == "second"

    # Two records, not one overwritten record. The `client` fixture starts from an empty ring.
    assert client.get(LOGS_URL, headers=headers).json()["page"]["total"] == 2


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(append_body(level="TRACE"), id="unknown-level"),
        pytest.param(append_body(message=""), id="empty-message"),
        pytest.param(append_body(service=""), id="empty-service"),
        pytest.param(append_body(severity="INFO"), id="unknown-field-extra-forbid"),
        pytest.param({"service": "s", "host": "h", "message": "m"}, id="missing-level"),
        pytest.param(
            append_body(attrs={f"k{i}": "v" for i in range(MAX_ATTRS_KEYS + 1)}),
            id="too-many-attrs",
        ),
        pytest.param(
            append_body(attrs={"big": "x" * (MAX_ATTR_VALUE_LEN + 1)}),
            id="oversized-attr-value",
        ),
    ],
)
def test_append_rejects_invalid_body_422(client, body):
    """A body the model refuses is a ``422`` — a client mistake, distinct from an auth failure.

    The caps on ``attrs`` are the interesting half: the ring bounds the entry *count* but not the
    per-entry *size*, so without them a writer could grow the process without bound while never
    exceeding ``STORE_CAPACITY``.
    """
    response = client.post(LOGS_URL, headers=headers_for(client, "writer"), json=body)

    assert response.status_code == 422, response.text


def test_append_degrades_to_503_without_a_store(settings):
    """A write must never claim success it cannot deliver — and must never be a ``500`` either.

    Every read route in ``src/api/v1.py`` degrades to an honest empty page on a half-wired
    runtime, because "there is nothing to read" is true of a store that does not exist. There is
    no equivalent true statement for a write, so this is the one handler that answers ``503``:
    distinct, retryable, and never mistakable for "accepted".
    """
    store_less = TestClient(create_app(runtime=Runtime(settings=settings)))

    response = store_less.post(
        LOGS_URL, headers=headers_for(store_less, "writer"), json=append_body()
    )

    assert response.status_code == 503, response.text
    assert "store" in response.json()["detail"]


# ---------------------------------------------------------------------------------------------
# The shape of a 403
# ---------------------------------------------------------------------------------------------


def test_403_body_carries_detail(client):
    """The refusal is actionable: it names the role required and the role held."""
    response = client.post(
        LOGS_URL, headers=headers_for(client, "viewer"), json=append_body()
    )

    assert response.status_code == 403, response.text
    detail = response.json()["detail"]
    assert detail == role_denied_detail(Role.VIEWER, Role.WRITER)
    assert "'writer' or higher required" in detail


def test_403_response_has_request_id_header(client):
    """C1's ``RequestContextMiddleware`` wraps the authorization failure path too.

    A correlation id is worth least on the response nobody investigates and most on the one
    somebody files a ticket about, so the middleware has to cover ``403`` as well as ``200``.
    """
    response = client.post(
        LOGS_URL, headers=headers_for(client, "viewer"), json=append_body()
    )

    assert response.status_code == 403
    assert response.headers["X-Request-ID"]

    # And a client-supplied id is echoed, so the caller can correlate its own trace.
    echoed = client.post(
        LOGS_URL,
        headers={**headers_for(client, "viewer"), "X-Request-ID": "trace-403-abc"},
        json=append_body(),
    )
    assert echoed.status_code == 403
    assert echoed.headers["X-Request-ID"] == "trace-403-abc"


# ---------------------------------------------------------------------------------------------
# The RBAC table itself
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class GuardedRoute:
    """One row of the README's role table, in a form both tests below can drive.

    ``path`` is the template exactly as it appears in ``/openapi.json``, so the same field serves
    the OpenAPI assertions and — after ``.format(entry_id=…)`` — the live HTTP calls.
    """

    method: str
    path: str
    minimum: Role
    success_status: int
    body: dict[str, object] | None = None


#: **The RBAC contract, as data.** Every route gated in C7, with the role it demands and the
#: status a permitted caller should see.
#:
#: This tuple is the single place the table is written down, and both tests below iterate it: the
#: live matrix sweeps all four demo roles across every row, and the OpenAPI test asserts every row
#: is discoverable in the published document. C9 added its row and got the whole sweep for free;
#: C11 has now added ``GET /stats`` (viewer) and ``GET /debug/memory`` (admin) the same way.
#:
#: .. rubric:: One route is deliberately excluded: ``GET /logs/stream``
#:
#: It is gated (``analyst``, via ``StreamGuard``) and it belongs in spirit, but it **cannot be
#: driven by this table**, and the reason is structural rather than a matter of effort:
#:
#: * Both tests here drive ``TestClient``, which buffers the entire response body before
#:   returning. ``tests/integration/test_stream_api.py``'s module docstring explains at length
#:   why that deadlocks on an open-ended SSE response — it is why that suite starts its own
#:   in-process uvicorn server on an ephemeral port.
#: * ``?max_events=1`` does not rescue it. The generator emits its ``ready`` frame, finds nothing
#:   to replay, and then parks on ``await sub.queue.get()`` forever, because nothing in this
#:   sweep appends. Every *permitted*-role cell would hang rather than fail, and this project
#:   pins no pytest timeout — so the failure mode is a CI job killed by the runner with no
#:   failing test to point at, which is strictly worse than a missing row.
#: * Forcing termination through ``?last_event_id=…`` so a replayed frame satisfies ``max_events``
#:   would work, but it would turn a role assertion into a replay assertion that silently depends
#:   on the fixture corpus still being resident in the ring.
#:
#: Neither half of the coverage is actually lost. The live gate is asserted by
#: ``test_stream_api.py::test_stream_requires_analyst_403_for_viewer`` against a real socket, and
#: the published requirement is asserted by
#: :func:`test_openapi_documents_the_excluded_stream_route` below — which exists precisely so this
#: exclusion is a recorded decision with a test attached, not an oversight the next reader has to
#: rediscover. A route added here that *can* be driven by ``TestClient`` still belongs in the
#: tuple: adding a row is two lines and buys the whole 4x1 sweep.
GUARDED_ROUTES: tuple[GuardedRoute, ...] = (
    GuardedRoute("GET", ME_URL, Role.VIEWER, 200),
    GuardedRoute("GET", LOGS_URL, Role.VIEWER, 200),
    GuardedRoute("GET", ENTRY_PATH_TEMPLATE, Role.VIEWER, 200),
    GuardedRoute("POST", LOGS_URL, Role.WRITER, 201, body=append_body()),
    # C9. The empty body is a complete, valid `SearchRequest`: every field defaults, so `{}` means
    # "everything, newest first" — which makes this row a role assertion and nothing else.
    GuardedRoute("POST", SEARCH_URL, Role.ANALYST, 200, body={}),
    # C11. Both take no body and no required parameter, so each row is a pure role assertion:
    # `/stats` defaults to the whole corpus, and the memory probe takes nothing at all.
    GuardedRoute("GET", STATS_URL, Role.VIEWER, 200),
    GuardedRoute("GET", MEMORY_URL, Role.ADMIN, 200),
)


def route_id(route: GuardedRoute) -> str:
    return f"{route.method}-{route.path}"


@pytest.mark.parametrize("route", GUARDED_ROUTES, ids=route_id)
@pytest.mark.parametrize("username", sorted(DEV_ACCOUNTS))
def test_role_matrix_across_every_guarded_route(seeded_client, corpus, username, route):
    """The whole RBAC table, swept: four roles x every guarded route, over real HTTP.

    This is the test that pins the README's contract. A route whose gate is loosened, tightened,
    or removed changes a cell here, and a cell that changes without the table changing is a
    failure — which is precisely the property "enforcement is a route dependency" is supposed to
    buy and which no amount of prose can guarantee on its own.

    The expectation is computed from :func:`~src.auth.role_satisfies` rather than typed out, so
    the ladder's inclusiveness is asserted the same way in every cell without one chance per cell
    to mistype it. (Deliberately not a literal count: the matrix is ``len(GUARDED_ROUTES) x 4``
    and grows with every gated route, so a number written here would go stale on the next commit
    — as it did once already.)
    """
    held = DEV_ACCOUNTS[username][1]

    response = seeded_client.request(
        route.method,
        route.path.format(entry_id=corpus[0].id),
        headers=headers_for(seeded_client, username),
        json=route.body,
    )

    if role_satisfies(held, route.minimum):
        assert response.status_code == route.success_status, response.text
    else:
        assert response.status_code == 403, response.text
        assert response.json()["detail"] == role_denied_detail(held, route.minimum)
        assert "WWW-Authenticate" not in response.headers


@pytest.mark.parametrize("route", GUARDED_ROUTES, ids=route_id)
def test_openapi_documents_role_requirements(client, route):
    """The README's claim, proven: the required role is *in the published document*.

    "Enforcement is a route dependency, so the required role is part of the generated OpenAPI
    document rather than tribal knowledge" is only true if something checks it. If the gates were
    inline ``if`` statements inside the handlers, the runtime behaviour would be identical and
    every assertion here would fail — which is the difference the design exists to make visible.

    Two mechanisms, because they serve two readers: the ``description`` carries the human
    sentence that ``/docs`` renders, and ``x-required-role`` carries the machine-readable value a
    generated client or policy linter can read without parsing English. Both are derived by
    :class:`~src.deps.RoleDocumentedRoute` from the dependency tree it is about to enforce, so
    neither can drift away from the code.

    Note the deliberate absence of scopes. ``Security(current_principal, scopes=[...])`` is the
    idiomatic answer and it does not work here: FastAPI copies scopes into an operation's
    ``security`` block only for ``OAuth2``/``OpenIdConnect`` schemes, and ours is an
    ``HTTPBearer`` — so the requirement would be silently dropped and every operation would
    publish the same bare ``[{"bearerAuth": []}]``, which is what the last assertion pins.
    """
    spec = client.get("/openapi.json").json()

    assert route.path in spec["paths"], sorted(spec["paths"])
    operation = spec["paths"][route.path][route.method.lower()]

    note = role_requirement_note(route.minimum)
    description = operation["description"]
    assert note in description
    # Exactly once. `include_router` re-creates every route with the already-rendered
    # description, so a non-idempotent append would publish the sentence twice.
    assert description.count(note) == 1
    # The handler's own prose survives alongside it — the note is an addition, not a replacement.
    assert len(description) > len(note)

    assert operation[REQUIRED_ROLE_EXTENSION] == route.minimum.value

    assert operation["security"] == [{"bearerAuth": []}]
    assert "401" in operation["responses"]


def test_openapi_marks_the_append_route_as_a_writer_creation(client):
    """The one route that creates something documents its ``201`` and its reachable ``403``."""
    operation = client.get("/openapi.json").json()["paths"][LOGS_URL]["post"]

    assert "201" in operation["responses"]
    assert "403" in operation["responses"]
    assert operation[REQUIRED_ROLE_EXTENSION] == Role.WRITER.value
    assert operation["tags"] == ["logs"]


def test_openapi_documents_the_excluded_stream_route(client):
    """The one gated route :data:`GUARDED_ROUTES` cannot sweep still publishes its requirement.

    ``GET /logs/stream`` is excluded from the table for the structural reason set out above its
    definition — ``TestClient`` cannot drive an open-ended SSE response. That exclusion costs the
    live 4x1 sweep, which ``test_stream_api.py`` covers against a real socket; it must not also
    cost the *documentation* assertion, which needs nothing but a ``GET /openapi.json`` and is
    therefore perfectly expressible here.

    This matters more for this route than for any other in the file. ``/logs/stream`` is the only
    one that cannot compose one of the four ``*Guard`` aliases — its principal comes from
    ``?access_token=`` as well as the header, so it builds its own guard and stamps
    ``_MINIMUM_ROLE_ATTR`` by hand. A hand-stamped attribute is exactly the kind that can be
    dropped in a refactor without anything visibly breaking: enforcement would carry on working
    while the route quietly vanished from the published ladder and started looking ungated to a
    policy linter. This is the assertion that would fail.
    """
    operation = client.get("/openapi.json").json()["paths"][STREAM_URL]["get"]

    assert operation[REQUIRED_ROLE_EXTENSION] == Role.ANALYST.value
    assert role_requirement_note(Role.ANALYST) in operation["description"]
    assert "403" in operation["responses"]


def test_openapi_leaves_the_public_token_route_ungated(client):
    """``POST /auth/token`` must publish no role requirement and no security block.

    Gating the route that mints principals would be a deadlock, and a generated client that saw a
    requirement here would demand a token in order to get a token.
    """
    operation = client.get("/openapi.json").json()["paths"][TOKEN_URL]["post"]

    assert REQUIRED_ROLE_EXTENSION not in operation
    assert not operation.get("security")
    for role in Role:
        assert role_requirement_note(role) not in operation.get("description", "")
