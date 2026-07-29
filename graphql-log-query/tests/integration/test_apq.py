"""Automatic persisted queries over the real HTTP surface — spec §2 item 36.

Every test here drives ``POST /graphql`` on an assembled application with its lifespan running, so
what is under test is the whole path: the router mount, ``get_context`` carrying
``app.state.apq`` onto the :class:`~src.graphql.context.Context`, the extension's substitution into
``execution_context.query`` before parsing, the real Redis the compose ``test`` service provides,
and the JSON envelope that comes back. Nothing calls the extension directly — the unit suite
(``tests/unit/test_apq.py``) owns the pure logic, and everything a schema-level test could assert
about APQ would still be green with the extension unmounted.

.. rubric:: THE ASSERTION THAT MAKES THIS FILE WORTH HAVING IS THE THIRD REQUEST

Steps 1 and 2 of the handshake prove very little on their own: an error and a successful query are
both things a server does anyway. The claim is that the **third** request — which carries a hash
and no document at all — returns the *same bytes* as the second. That is the whole feature, and it
cannot be faked by a server that quietly ignored the protocol, because such a server has nothing to
execute.

.. rubric:: THE SECOND ONE IS THAT A PERSISTED QUERY IS STILL COST-GATED

:func:`test_a_persisted_query_is_still_refused_by_the_cost_gate` persists a document C8's budget
refuses and then sends it **by hash**, and asserts the rejection is still
``COST_LIMIT_EXCEEDED`` rather than ``PERSISTED_QUERY_NOT_FOUND``. Both halves matter: the code
being ``COST_LIMIT_EXCEEDED`` proves the document really was found and re-priced, and the code *not*
being ``PERSISTED_QUERY_NOT_FOUND`` is what distinguishes "the gate held" from "the lookup missed
and we congratulated ourselves". A persisted query that skipped validation would be a way through
C8's gate for the price of one registration.

.. rubric:: Isolation without a per-test namespace

The store's key **is** ``sha256(document)`` and its value is that same document, so two tests
registering the same text collide *correctly* — the answer is the same either way. The only hazard
is a test asserting "not found" for a document some other test (or some earlier ``make test`` run,
since Redis is not flushed between them and the TTL is an hour) already registered. Every test
therefore builds its documents through :func:`unique_document`, which stamps a fresh UUID into the
operation name — so a miss is a miss because nothing registered it, not because the suite happened
to run in a particular order.

.. rubric:: What is deliberately not tested here

The ``GET /graphql?extensions=…`` spelling of the protocol — the CDN-cacheable one — is covered at
unit level in ``tests/unit/test_apq.py`` against the extraction function, not over HTTP. Driving it
through the transport would additionally be a test of how Strawberry's GET handler treats a request
with no ``query`` parameter, which is not this commit's contract to pin.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any, Optional
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI

from src.cache import ResultCache
from src.config import Settings
from src.db.models import LogRecord
from src.db.session import Database
from src.graphql.apq import (
    PERSISTED_QUERY_NOT_FOUND_MESSAGE,
    PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE,
    PersistedQueryStore,
    compute_query_hash,
)
from src.graphql.errors import MASKED_ERROR_MESSAGE, ErrorCode
from src.main import create_app

#: The shipped complexity budget, read off the declared default rather than off the environment —
#: the compose ``test`` service raises ``MAX_QUERY_COMPLEXITY`` to 980,000 for the rest of the
#: suite, and the cost-gate test here needs the number a clean clone boots with. Constructor
#: arguments outrank the environment in pydantic-settings, so no container variable can reach it.
SHIPPED_COMPLEXITY = Settings.model_fields["max_query_complexity"].default

#: Two levels of correlation under an unbounded parent list — ``10 + 10x100 + 10x100x100 +
#: 1x100x100x100`` = 1,101,010, forty-four times the shipped budget. Four fields deep, so the depth
#: limiter (10) is nowhere near it: this document is over budget for exactly one reason, which is
#: what makes it usable as evidence about the *cost* gate.
OVER_BUDGET_SELECTION = "logs { relatedLogs { relatedLogs { id } } }"
OVER_BUDGET_COST = 10 + (10 * 100) + (10 * 100 * 100) + (1 * 100 * 100 * 100)


def unique_document(*, selection: str = "logs(filters: {limit: 3}) { id service level }") -> str:
    """A document no other test (or earlier run) can have registered. See the module docstring.

    The uniqueness lives in the **operation name**, so the selection set — and therefore the rows
    that come back — is whatever the caller asked for and is unaffected by the stamping.
    """
    return f"query Apq_{uuid4().hex} {{ {selection} }}"


def apq_extension(digest: str, *, version: Optional[int] = 1) -> dict[str, Any]:
    """The ``extensions`` object an Apollo client sends, in the shape it sends it."""
    payload: dict[str, Any] = {"sha256Hash": digest}
    if version is not None:
        payload["version"] = version
    return {"persistedQuery": payload}


async def post_graphql(
    client: httpx.AsyncClient,
    *,
    query: Optional[str] = None,
    extensions: Optional[dict[str, Any]] = None,
    variables: Optional[dict[str, Any]] = None,
) -> httpx.Response:
    """POST one GraphQL request, omitting ``query`` entirely when it is ``None``.

    Omitting the key rather than sending ``"query": null`` is what an APQ client actually does —
    ``createPersistedQueryLink`` deletes the document from the body — and it is the shape this whole
    feature has to survive: a request with no document at all.
    """
    body: dict[str, Any] = {}
    if query is not None:
        body["query"] = query
    if variables is not None:
        body["variables"] = variables
    if extensions is not None:
        body["extensions"] = extensions
    return await client.post("/graphql", json=body)


def errors_of(response: httpx.Response) -> list[dict[str, Any]]:
    """The ``errors`` array, asserting the transport-level promises on the way past.

    Spec §2 item 35 is checked here rather than in a test of its own so that **every** failure this
    module produces is held to it: 200, no traceback in the body, and the failure inside the
    envelope.
    """
    assert response.status_code == 200, response.text
    assert "Traceback" not in response.text
    payload = response.json()
    assert payload.get("data") is None
    errors = payload.get("errors")
    assert errors, f"expected a GraphQL error envelope, got {payload!r}"
    return errors


def codes_of(response: httpx.Response) -> list[Optional[str]]:
    """Every ``extensions.code`` in a failed response."""
    return [(error.get("extensions") or {}).get("code") for error in errors_of(response)]


def messages_of(response: httpx.Response) -> list[str]:
    """Every message in a failed response."""
    return [error.get("message", "") for error in errors_of(response)]


def data_of(response: httpx.Response) -> dict[str, Any]:
    """The ``data`` object of a successful response, failing loudly on any error."""
    assert response.status_code == 200, response.text
    payload = response.json()
    assert "errors" not in payload, payload["errors"]
    assert payload.get("data") is not None
    return payload["data"]


# =================================================================================================
# Fixtures
# =================================================================================================


def make_settings(**overrides: Any) -> Settings:
    """Settings for an APQ application, with the cost budget pinned to what we ship.

    ``DATABASE_URL`` and ``REDIS_URL`` still come from the environment compose injects (the test
    database, Redis logical DB 1), so this is the real stack. ``cache_enabled`` stays **false** —
    the compose ``test`` service's own setting, and the combination that matters most here: the
    shared request-path Redis client must be built for persisted queries even when the cache that
    normally owns it is switched off.
    """
    fields: dict[str, Any] = {
        "_env_file": None,
        "seed_entries": 0,
        "seed_orders": 0,
        "log_level": "WARNING",
        "cache_enabled": False,
        "persisted_queries_enabled": True,
        "max_query_complexity": SHIPPED_COMPLEXITY,
        "default_query_limit": Settings.model_fields["default_query_limit"].default,
        "max_query_limit": Settings.model_fields["max_query_limit"].default,
        "subscription_channel": f"test:apq:{uuid4().hex}",
    }
    fields.update(overrides)
    return Settings(**fields)


@pytest.fixture()
def make_apq_app(database: Database) -> Any:  # noqa: ANN401 - a factory
    """Build an application whose APQ configuration the test chooses.

    Depends on ``database`` so ``log_entries`` exists and has been truncated before the app's own
    lifespan opens an engine over it.
    """

    def _make(**overrides: Any) -> FastAPI:
        return create_app(settings=make_settings(**overrides))

    return _make


@pytest.fixture()
def apq_app(make_apq_app: Any) -> FastAPI:  # noqa: ANN401
    """The default application: persisted queries on, cache off, shipped cost budget."""
    return make_apq_app()


@pytest.fixture()
async def apq_client(apq_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """An async HTTP client over ``apq_app``, with the lifespan entered by hand.

    ``ASGITransport`` never sends lifespan events, and ``get_context`` needs ``app.state.db`` — and,
    for everything in this module, ``app.state.apq``. Same construction as the shared
    ``http_client`` fixture; only the settings differ.
    """
    async with apq_app.router.lifespan_context(apq_app):
        transport = httpx.ASGITransport(app=apq_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://graphql-apq.test") as c:
            yield c


def store_of(app: FastAPI) -> PersistedQueryStore:
    """The application's persisted query store, asserting it was really built."""
    store = app.state.apq
    assert isinstance(store, PersistedQueryStore), (
        "the lifespan did not build a persisted query store — app.state.apq is "
        f"{store!r}"
    )
    return store


# =================================================================================================
# The three-step handshake
# =================================================================================================


async def test_the_three_step_handshake_ends_with_the_client_sending_no_document(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI
) -> None:
    """Miss, register, then execute by hash — and the third answer equals the second.

    THE test for this feature. The third request's body carries no ``query`` key at all (see
    :func:`post_graphql`), so a server that had ignored the protocol would have nothing to run; the
    only way to produce the second response's bytes a third time is to have remembered the document
    and substituted it before parsing.

    The counters are the second instrument. Asserting only on payloads would leave "the store was
    consulted" and "the resolver ran twice from the client's own document" indistinguishable.
    """
    document = unique_document()
    digest = compute_query_hash(document)
    store = store_of(apq_app)

    assert store.enabled is True, "the suite's Redis must be reachable for this test to mean anything"

    # 1. Hash only, nothing registered. A normal protocol step, not a fault.
    first = await post_graphql(apq_client, extensions=apq_extension(digest))

    assert messages_of(first) == [PERSISTED_QUERY_NOT_FOUND_MESSAGE], (
        "the message is a WIRE CONSTANT — Apollo's persisted-query link compares it with ==="
    )
    assert codes_of(first) == [ErrorCode.PERSISTED_QUERY_NOT_FOUND.value]

    # 2. Document + hash. The server verifies, registers, and executes.
    second = await post_graphql(apq_client, query=document, extensions=apq_extension(digest))
    registered_data = data_of(second)

    assert len(registered_data["logs"]) == 3

    # 3. Hash only again. No document is sent, and the same answer comes back.
    third = await post_graphql(apq_client, extensions=apq_extension(digest))

    assert third.status_code == 200
    assert third.json() == second.json(), (
        "a hash-only request must produce byte-identical output to the request that registered it"
    )

    stats = store.stats
    assert stats.registered == 1
    assert stats.hits == 1, "the third request must have been answered from the store"
    assert stats.misses == 1, "exactly one lookup missed — the first"
    assert (stats.mismatches, stats.protocol_errors, stats.errors) == (0, 0, 0)


async def test_a_registered_document_survives_for_a_second_client(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI
) -> None:
    """Registration is process-wide state in Redis, not per-connection memory.

    Sent over a **separate** HTTP client against the same app, because a store that had cached the
    document on the request or on the connection would pass every test above and fail the moment a
    second client sent the hash — which is the entire deployment story for APQ.
    """
    document = unique_document(selection="logStats { totalLogs errorCount services }")
    digest = compute_query_hash(document)

    data_of(await post_graphql(apq_client, query=document, extensions=apq_extension(digest)))

    transport = httpx.ASGITransport(app=apq_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://graphql-apq-2.test") as other:
        by_hash = await post_graphql(other, extensions=apq_extension(digest))

    assert data_of(by_hash)["logStats"]["totalLogs"] >= 0
    assert store_of(apq_app).stats.hits == 1


# =================================================================================================
# The security property: the server recomputes the hash
# =================================================================================================


async def test_a_hash_that_does_not_name_the_document_is_refused_and_nothing_is_stored(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI
) -> None:
    """Cache poisoning, attempted and refused — **and the follow-up proves nothing was written**.

    The attack: register document *B* under the hash of document *A*, so that the next hash-only
    request for *A* — from any client — executes *B*. A server that trusted the client's
    ``sha256Hash`` would store it and never notice.

    Two assertions, and the second is the one that matters. Refusing the request is easy; the
    security property is that the mismatched document was **not persisted**, which is why this test
    follows the refusal with a hash-only request for the victim hash and requires it to still be a
    miss. Without that follow-up, an implementation that stored first and validated afterwards would
    pass.
    """
    victim = unique_document(selection="logs(filters: {limit: 1}) { id }")
    attacker = unique_document(selection="logs(filters: {limit: 500}) { id message metadata }")
    victim_digest = compute_query_hash(victim)
    store = store_of(apq_app)

    assert compute_query_hash(attacker) != victim_digest

    refused = await post_graphql(
        apq_client, query=attacker, extensions=apq_extension(victim_digest)
    )

    assert codes_of(refused) == [ErrorCode.VALIDATION_ERROR.value]
    assert "sha256Hash" in messages_of(refused)[0]
    assert messages_of(refused)[0] != MASKED_ERROR_MESSAGE, "a client mistake is not a server fault"

    # THE FOLLOW-UP: the victim hash must still resolve to nothing at all.
    probe = await post_graphql(apq_client, extensions=apq_extension(victim_digest))

    assert messages_of(probe) == [PERSISTED_QUERY_NOT_FOUND_MESSAGE], (
        "the mismatched document was stored under the hash it did not name"
    )
    assert store.stats.mismatches == 1
    assert store.stats.registered == 0, "no registration may survive a hash mismatch"


async def test_a_verified_registration_of_the_same_hash_still_works_after_a_mismatch(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient
) -> None:
    """A refused attempt must not poison the hash against its legitimate owner.

    A defensive implementation that blacklisted a hash after one mismatch would deny service to the
    real client for the whole TTL — turning the protection into the denial it was meant to prevent.
    """
    document = unique_document(selection="logs(filters: {limit: 2}) { id }")
    digest = compute_query_hash(document)

    mismatch = await post_graphql(
        apq_client, query=unique_document(), extensions=apq_extension(digest)
    )
    assert codes_of(mismatch) == [ErrorCode.VALIDATION_ERROR.value]

    data_of(await post_graphql(apq_client, query=document, extensions=apq_extension(digest)))
    by_hash = await post_graphql(apq_client, extensions=apq_extension(digest))

    assert len(data_of(by_hash)["logs"]) == 2


# =================================================================================================
# THE HOLE THIS COMMIT HAD TO PROVE CLOSED
# =================================================================================================


async def test_a_persisted_query_is_still_refused_by_the_cost_gate(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI
) -> None:
    """Sending a document by hash is not a way around C8.

    The substitution happens in the **pre-yield** half of ``on_operation``, and Strawberry parses
    inside that context manager — so the persisted document is what the parser, the depth/token/alias
    limiters and the complexity walker all see. This test is what fails if that ordering is ever
    changed to "substitute after parse".

    Both halves of the final assertion carry weight. ``COST_LIMIT_EXCEEDED`` proves the document was
    **found** and re-priced (a miss would have said ``PersistedQueryNotFound``), and the computed
    cost proves it was priced as *itself* rather than as the placeholder a rejection substitutes.

    Note the registration in step 1 is itself refused by the gate and the document is stored anyway —
    that is the documented behaviour (see :class:`src.graphql.apq.PersistedQueries`): the pair was
    verified, its residency is bounded by the TTL, and refusing to store anything that does not
    execute cleanly would mean a query with a runtime error could never be sent by hash.
    """
    document = unique_document(selection=OVER_BUDGET_SELECTION)
    digest = compute_query_hash(document)
    store = store_of(apq_app)

    registration = await post_graphql(apq_client, query=document, extensions=apq_extension(digest))

    assert codes_of(registration) == [ErrorCode.COST_LIMIT_EXCEEDED.value]
    assert store.stats.registered == 1, "the verified pair should still have been registered"

    by_hash = await post_graphql(apq_client, extensions=apq_extension(digest))

    assert codes_of(by_hash) == [ErrorCode.COST_LIMIT_EXCEEDED.value], (
        "a persisted query bypassed the complexity gate — the document is being substituted after "
        "validation instead of before parsing"
    )
    extensions = errors_of(by_hash)[0]["extensions"]
    assert extensions["computedCost"] == OVER_BUDGET_COST == 1_101_010
    assert extensions["maxCost"] == SHIPPED_COMPLEXITY
    assert store.stats.hits == 1, "the gate must have run on a document that was really loaded"


# =================================================================================================
# Failure modes
# =================================================================================================


async def test_redis_down_is_a_graceful_miss_and_the_full_document_path_still_works(
    seeded: list[LogRecord], make_apq_app: Any  # noqa: ANN401
) -> None:
    """**Losing Redis costs bandwidth, never availability.**

    Pointed at a port nothing listens on, so every command fails at connect. The three assertions
    are the whole never-raises contract at the HTTP boundary: a hash-only request is answered
    ``PersistedQueryNotFound`` (so the client resends), a full document is served completely
    normally, and a second hash-only request is *still* a miss — because the registration could not
    be written either, and pretending otherwise would strand the client in a retry loop.

    The error counter is asserted too: without it, a store that silently short-circuited every
    lookup without ever attempting one would produce exactly these three responses.
    """
    app = make_apq_app(redis_url="redis://127.0.0.1:6399/0")
    document = unique_document()
    digest = compute_query_hash(document)

    async with app.router.lifespan_context(app):
        store = store_of(app)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://graphql-apq-down.test") as client:
            first = await post_graphql(client, extensions=apq_extension(digest))
            registration = await post_graphql(
                client, query=document, extensions=apq_extension(digest)
            )
            second = await post_graphql(client, extensions=apq_extension(digest))

        assert messages_of(first) == [PERSISTED_QUERY_NOT_FOUND_MESSAGE]
        assert len(data_of(registration)["logs"]) == 3, (
            "an unreachable Redis must not affect a request that carried its own document"
        )
        assert messages_of(second) == [PERSISTED_QUERY_NOT_FOUND_MESSAGE]

        stats = store.stats
        assert store.configured is True, "the FEATURE is on; only the transport is broken"
        assert stats.errors >= 2, "the outage was never actually attempted, so nothing was survived"
        assert stats.registered == 0


async def test_persisted_queries_disabled_answers_not_supported_and_still_serves_documents(
    seeded: list[LogRecord], make_apq_app: Any  # noqa: ANN401
) -> None:
    """``PERSISTED_QUERIES_ENABLED=false``: the documented, protocol-correct behaviour.

    A hash-only request is answered ``PersistedQueryNotSupported`` — deliberately **not**
    ``PersistedQueryNotFound``. Apollo's link retries a *NotFound* once per cold operation forever,
    but disables APQ for the rest of the session on a *NotSupported*, which is exactly what an
    operator who switched the feature off wants every client to do.

    A request that carries both a document and a hash is simply executed and nothing is stored, so a
    client with an APQ link keeps working against a server without the feature — which is the entire
    point of the negotiation, and the reason "disabled" is not spelled "reject everything".
    """
    app = make_apq_app(persisted_queries_enabled=False)
    document = unique_document()
    digest = compute_query_hash(document)

    async with app.router.lifespan_context(app):
        assert app.state.apq is None, "a disabled feature must not build a store at all"
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://graphql-apq-off.test") as client:
            first = await post_graphql(client, extensions=apq_extension(digest))
            registration = await post_graphql(
                client, query=document, extensions=apq_extension(digest)
            )
            second = await post_graphql(client, extensions=apq_extension(digest))

    assert messages_of(first) == [PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE]
    assert codes_of(first) == [ErrorCode.VALIDATION_ERROR.value]
    assert len(data_of(registration)["logs"]) == 3
    assert messages_of(second) == [PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE], (
        "a disabled server must not have remembered the document it was just sent"
    )


@pytest.mark.parametrize("version", [2, 0, "1"])
async def test_an_unsupported_protocol_version_is_a_clean_typed_error(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI, version: Any
) -> None:
    """Version 2 is refused with a message naming both versions — not a crash, not a 500.

    The document is sent alongside the hash on purpose: a payload that is wrong about the protocol
    must be refused even when everything else about the request is fine, because executing it would
    mean silently ignoring a handshake the client believes it completed.
    """
    document = unique_document()
    digest = compute_query_hash(document)

    refused = await post_graphql(
        apq_client, query=document, extensions=apq_extension(digest, version=version)
    )

    assert codes_of(refused) == [ErrorCode.VALIDATION_ERROR.value]
    message = messages_of(refused)[0]
    assert "version" in message and repr(version) in message
    assert message != MASKED_ERROR_MESSAGE

    # And nothing was registered: a refused handshake must leave no state behind.
    probe = await post_graphql(apq_client, extensions=apq_extension(digest))
    assert messages_of(probe) == [PERSISTED_QUERY_NOT_FOUND_MESSAGE]
    assert store_of(apq_app).stats.protocol_errors == 1


async def test_a_malformed_hash_is_refused_before_it_can_reach_a_redis_key(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI
) -> None:
    """A payload that announces APQ and then supplies junk is named as the client bug it is."""
    refused = await post_graphql(
        apq_client, extensions={"persistedQuery": {"version": 1, "sha256Hash": "not-a-digest"}}
    )

    assert codes_of(refused) == [ErrorCode.VALIDATION_ERROR.value]
    assert "sha256Hash" in messages_of(refused)[0]
    assert store_of(apq_app).stats.protocol_errors == 1


# =================================================================================================
# The rest of the API is unaffected
# =================================================================================================


async def test_an_ordinary_request_is_untouched_by_the_extension(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI
) -> None:
    """The common path: no payload, no lookup, no counter movement, no behaviour change.

    This extension runs on **every** operation the server serves, so "it does nothing when it has
    nothing to do" is a load-bearing property rather than an obvious one — and the counters staying
    at zero is what proves the body was not parsed and Redis was not consulted.
    """
    store = store_of(apq_app)

    plain = await post_graphql(apq_client, query="{ logs(filters: {limit: 4}) { id service } }")
    unrelated = await post_graphql(
        apq_client,
        query="{ logs(filters: {limit: 4}) { id service } }",
        extensions={"tracing": {"version": 1}},
    )

    assert len(data_of(plain)["logs"]) == 4
    assert unrelated.json() == plain.json()

    stats = store.stats
    assert (stats.hits, stats.misses, stats.registered) == (0, 0, 0)
    assert (stats.mismatches, stats.protocol_errors, stats.errors) == (0, 0, 0)


async def test_a_persisted_query_miss_logs_one_concise_line_and_no_stack_trace(
    apq_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A hash-only probe is a routine protocol step, and a client sends one per cold operation.

    C4's whole argument (see :mod:`src.graphql.errors`): Strawberry logs every error with
    ``exc_info``, which is right for a crash and wrong for a client-side step. Because the rejection
    is raised as a :class:`~src.graphql.errors.DomainError`, it is classified as expected, logged as
    **one INFO line** on ``src.graphql.errors``, and never reaches ``strawberry.execution``.
    """
    caplog.set_level(logging.INFO, logger="src.graphql.errors")
    digest = compute_query_hash(unique_document())

    miss = await post_graphql(apq_client, extensions=apq_extension(digest))

    assert messages_of(miss) == [PERSISTED_QUERY_NOT_FOUND_MESSAGE]

    expected = [record for record in caplog.records if record.name == "src.graphql.errors"]
    assert len(expected) == 1, [record.getMessage() for record in expected]
    assert expected[0].levelno == logging.INFO
    assert expected[0].exc_info is None, "a persisted query miss logged a stack trace"
    assert ErrorCode.PERSISTED_QUERY_NOT_FOUND.value in expected[0].getMessage()

    unexpected = [record for record in caplog.records if record.name == "strawberry.execution"]
    assert unexpected == [], "a normal protocol step was logged as a server fault"


# =================================================================================================
# The lifespan wiring
# =================================================================================================


async def test_the_cache_and_the_persisted_query_store_share_one_redis_client(
    make_apq_app: Any,  # noqa: ANN401
) -> None:
    """One request-path connection pool, two consumers — asserted **by identity**.

    C7 justified two clients (one with a socket timeout for the request path, one without for the
    pub/sub long poll). C9 does not add a third: a persisted-query lookup wants exactly the same
    connection behaviour as a cache lookup, so the lifespan builds one and hands it to both. Both
    borrow it, so neither closes it, and the lifespan closes it once.
    """
    app = make_apq_app(cache_enabled=True)

    async with app.router.lifespan_context(app):
        cache = app.state.cache
        store = store_of(app)

        assert isinstance(cache, ResultCache)
        assert app.state.request_redis is not None
        assert cache.redis_client is app.state.request_redis
        assert store.redis_client is app.state.request_redis, (
            "the persisted query store opened a third connection pool"
        )
        assert cache.enabled and store.enabled

    assert cache.redis_client is None, "the cache must release the shared client on shutdown"
    assert store.redis_client is None, "the store must release the shared client on shutdown"


async def test_turning_the_cache_off_does_not_turn_persisted_queries_off(
    seeded: list[LogRecord], apq_client: httpx.AsyncClient, apq_app: FastAPI
) -> None:
    """The gate on the shared client is the **OR** of the two features, not the cache alone.

    Not hypothetical: the compose ``test`` service runs with ``CACHE_ENABLED=false``, so a client
    built only when the cache was on would leave persisted queries permanently broken in exactly the
    environment the suite runs in — and every APQ test would report a miss and look like a store
    that simply had nothing registered.
    """
    cache = apq_app.state.cache
    store = store_of(apq_app)
    document = unique_document()
    digest = compute_query_hash(document)

    assert cache.enabled is False, "this test is meaningless unless the cache really is off"
    assert cache.redis_client is None, "a disabled cache must still hold no client"
    assert store.enabled is True
    assert store.redis_client is apq_app.state.request_redis

    data_of(await post_graphql(apq_client, query=document, extensions=apq_extension(digest)))
    by_hash = await post_graphql(apq_client, extensions=apq_extension(digest))

    assert len(data_of(by_hash)["logs"]) == 3
