"""Automatic persisted queries as pure logic — spec §2 item 36.

No Redis, no HTTP, no schema. Everything here drives the three pieces
:mod:`src.graphql.apq` deliberately kept free of the transport:

* :func:`~src.graphql.apq.compute_query_hash` and :func:`~src.graphql.apq.normalise_hash` — the
  hashing, pinned against a digest computed outside this project so a change to the encoding or the
  algorithm fails here rather than silently invalidating every client's registration.
* :func:`~src.graphql.apq.plan_persisted_query` — the **protocol state machine**, which is a pure
  function of "what did the client send" and therefore testable without a request at all. Every
  branch is exercised, including the two nobody writes by accident: a hash that does not name the
  document, and a version this server does not implement.
* :class:`~src.graphql.apq.PersistedQueryStore` — driven with a stub that answers ``get``/``setex``,
  which is what lets this module produce the failures a healthy Redis cannot: a client that raises
  on every call, a value that is not decodable, a store with no client at all.

.. rubric:: What is asserted here that an "it did not raise" test would miss

The never-raises tests assert **three** things each, because "survived" has three halves and each
regresses on its own: the caller got the right answer (``None`` — a miss), the ``errors`` counter
moved (so the failure was *seen* rather than skipped), and the ``misses`` counter moved (so the
lookup was accounted for rather than dropped). A test that only checked "no exception escaped"
would stay green against a store that swallowed everything and reported nothing.

The mismatch test asserts the same way: it is not enough that the plan says ``MISMATCH``. The whole
security property is that **nothing is written**, so the test that matters is the one in
``tests/integration/test_apq.py`` which follows the mismatch with a hash-only request and finds it
still a miss. What is pinned here is the decision that leads to it.
"""

from __future__ import annotations

import hashlib
from typing import Any, Optional

import pytest

from src.config import Settings
from src.graphql.apq import (
    APQ_PROTOCOL_VERSION,
    DEFAULT_PERSISTED_QUERY_NAMESPACE,
    MAX_PERSISTED_DOCUMENT_BYTES,
    PERSISTED_QUERY_FORMAT_VERSION,
    PERSISTED_QUERY_NOT_FOUND_MESSAGE,
    PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE,
    PersistedQueryAction,
    PersistedQueryStore,
    compute_query_hash,
    create_persisted_query_store,
    hash_matches_document,
    normalise_hash,
    plan_persisted_query,
    read_persisted_query_payload,
    read_request_extensions,
)

#: The spec's own §5 acceptance document, and its sha256 computed **outside this module** (with
#: `python3 -c 'import hashlib; ...'`). Pinned as a literal on purpose: deriving the expectation
#: with the same function under test would assert only that the function is deterministic, which is
#: true of every wrong implementation too.
SPEC_DOCUMENT = "{ logs { id service level message } }"
SPEC_DIGEST = "6286e47a3b2294553750a5e0b4f404012df8facf310deb73d26b17c0813eccd2"

#: A different document, for the mismatch cases.
OTHER_DOCUMENT = "{ logStats { totalLogs } }"

#: Sixty-four hex characters that are not the digest of anything under test.
FOREIGN_DIGEST = "0" * 64


def make_settings(
    *, enabled: bool = True, ttl_seconds: int = 3600, cache_enabled: bool = False
) -> Settings:
    """Settings for a store under test, built directly rather than read from the environment.

    ``_env_file=None`` so a stray ``.env`` cannot perturb the suite; every value a test's
    expectations depend on is passed explicitly so it is visible in the test.
    """
    return Settings(
        _env_file=None,
        seed_entries=0,
        seed_orders=0,
        log_level="WARNING",
        cache_enabled=cache_enabled,
        persisted_queries_enabled=enabled,
        persisted_query_ttl_seconds=ttl_seconds,
    )


def persisted_query_extension(
    digest: str, *, version: Optional[int] = APQ_PROTOCOL_VERSION
) -> dict[str, Any]:
    """A request ``extensions`` object carrying an APQ payload, as a client would send it."""
    payload: dict[str, Any] = {"sha256Hash": digest}
    if version is not None:
        payload["version"] = version
    return {"persistedQuery": payload}


class StubRedis:
    """An in-memory stand-in for Redis, recording what it was asked to do.

    Duck-typed on ``get``/``setex`` — the only two methods
    :class:`~src.graphql.apq.PersistedQueryStore` uses — which is the whole reason the store takes a
    client rather than a URL.
    """

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []
        self.ttls: dict[str, int] = {}

    async def get(self, key: str) -> Optional[bytes]:
        self.calls.append(("get", key))
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: Any) -> bool:  # noqa: ANN401
        self.calls.append(("setex", key))
        self.ttls[key] = ttl
        self.store[key] = value if isinstance(value, bytes) else str(value).encode("utf-8")
        return True


class ExplodingRedis:
    """A client that fails every call — the outage the store has to survive."""

    def __init__(self) -> None:
        self.calls = 0

    async def get(self, key: str) -> Optional[bytes]:
        self.calls += 1
        raise ConnectionError("redis is not answering")

    async def setex(self, key: str, ttl: int, value: Any) -> bool:  # noqa: ANN401
        self.calls += 1
        raise ConnectionError("redis is not answering")


# =================================================================================================
# Hashing — the security property of step 2 starts here
# =================================================================================================


def test_the_hash_is_sha256_of_the_exact_utf8_document() -> None:
    """Pinned against a digest computed outside this project. See :data:`SPEC_DIGEST`."""
    assert compute_query_hash(SPEC_DOCUMENT) == SPEC_DIGEST
    assert len(SPEC_DIGEST) == 64
    assert SPEC_DIGEST == SPEC_DIGEST.lower()


def test_the_document_is_hashed_verbatim_with_no_normalisation() -> None:
    """Whitespace is part of the document, because it is part of what the client hashed.

    Apollo hashes the exact string it is about to send. Any normalisation here — trimming,
    collapsing runs of spaces, reformatting — would make every client's hash a mismatch, so the
    absence of normalisation is a compatibility requirement rather than laziness.
    """
    spaced = "{ logs  { id service level message } }"

    assert spaced != SPEC_DOCUMENT
    assert compute_query_hash(spaced) != compute_query_hash(SPEC_DOCUMENT)


def test_a_non_ascii_document_hashes_as_utf8() -> None:
    """The encoding is pinned, not inherited. ``str.encode()`` defaults are not a contract."""
    document = '{ logs(filters: {searchText: "café"}) { id } }'

    assert compute_query_hash(document) == hashlib.sha256(document.encode("utf-8")).hexdigest()


def test_hash_matches_document_accepts_only_the_real_digest() -> None:
    assert hash_matches_document(SPEC_DOCUMENT, SPEC_DIGEST) is True
    assert hash_matches_document(OTHER_DOCUMENT, SPEC_DIGEST) is False
    assert hash_matches_document(SPEC_DOCUMENT, FOREIGN_DIGEST) is False


@pytest.mark.parametrize(
    ("supplied", "expected"),
    [
        (SPEC_DIGEST, SPEC_DIGEST),
        # Some clients emit upper-case hex. Folded rather than refused: it is the same digest.
        (SPEC_DIGEST.upper(), SPEC_DIGEST),
        (f"  {SPEC_DIGEST}  ", SPEC_DIGEST),
        # Everything below is refused BEFORE it can reach the Redis key builder.
        ("", None),
        ("deadbeef", None),
        (SPEC_DIGEST[:-1], None),
        (SPEC_DIGEST + "0", None),
        (SPEC_DIGEST[:-1] + "z", None),
        ("../" + SPEC_DIGEST[3:], None),
        (f"{SPEC_DIGEST[:-1]}*", None),
        (None, None),
        (12345, None),
        ({"sha256Hash": SPEC_DIGEST}, None),
    ],
)
def test_only_a_real_sha256_hex_digest_survives_normalisation(
    supplied: Any, expected: Optional[str]
) -> None:
    """The hash is interpolated into a Redis key, so its shape is validated rather than assumed.

    "It comes from a hash function" is a statement about the honest client. The wildcard and the
    path-traversal cases are here because they are what a key built from unvalidated input would
    accept, and a ``*`` in a key is a ``KEYS``-pattern-shaped hole in somebody's operational tooling.
    """
    assert normalise_hash(supplied) == expected


# =================================================================================================
# The protocol state machine
# =================================================================================================


@pytest.mark.parametrize(
    "extensions",
    [
        None,
        {},
        {"tracing": {"version": 1}},
        {"persistedQuery": None},
        {"persistedQuery": "not-an-object"},
        # Not a mapping at all — a client that put a list under `extensions`.
        [{"persistedQuery": {"version": 1, "sha256Hash": SPEC_DIGEST}}],
        "extensions",
    ],
)
def test_a_request_without_an_apq_payload_passes_straight_through(extensions: Any) -> None:
    """``extensions`` is an open extension point; an unrecognised one is not an error.

    A server that rejected every ``extensions`` object it did not understand would break clients
    that are doing nothing wrong — tracing links and Apollo Studio both put their own keys there.
    """
    plan = plan_persisted_query(extensions, SPEC_DOCUMENT)

    assert plan.action is PersistedQueryAction.PASS_THROUGH
    assert plan.sha256_hash is None
    assert read_persisted_query_payload(extensions) is None


def test_a_hash_with_no_document_is_a_lookup() -> None:
    """Step 1 and step 3 of the handshake are the same request; only the store's answer differs."""
    plan = plan_persisted_query(persisted_query_extension(SPEC_DIGEST), None)

    assert plan.action is PersistedQueryAction.LOOKUP
    assert plan.sha256_hash == SPEC_DIGEST
    assert plan.document is None


def test_an_empty_query_string_counts_as_no_document() -> None:
    """``{"query": ""}`` is what a client that stripped the document leaves behind.

    Strawberry treats an empty query as missing (``if not execution_context.query``), so the plan
    has to agree — otherwise the empty string would be hashed and compared, always mismatch, and a
    client that sent it would be told its hash was wrong instead of being told to resend.
    """
    plan = plan_persisted_query(persisted_query_extension(SPEC_DIGEST), "")

    assert plan.action is PersistedQueryAction.LOOKUP


def test_a_document_with_its_own_hash_is_a_registration() -> None:
    """Step 2: the server recomputes, agrees, and only then agrees to remember."""
    plan = plan_persisted_query(persisted_query_extension(SPEC_DIGEST), SPEC_DOCUMENT)

    assert plan.action is PersistedQueryAction.REGISTER
    assert plan.sha256_hash == SPEC_DIGEST
    assert plan.document == SPEC_DOCUMENT


def test_a_document_under_someone_elses_hash_is_refused() -> None:
    """**The attack this whole design exists to prevent**, decided here.

    A client sends document *B* under the hash of document *A*. If the server trusted the client's
    hash, the next hash-only request for *A* — from anybody — would execute *B*. So the hash is
    recomputed, the pair is refused, and the message says why. ``tests/integration/test_apq.py``
    proves the other half: nothing was stored.
    """
    plan = plan_persisted_query(persisted_query_extension(SPEC_DIGEST), OTHER_DOCUMENT)

    assert plan.action is PersistedQueryAction.MISMATCH
    assert plan.reason is not None
    assert "sha256Hash" in plan.reason
    assert "NOT registered" in plan.reason


@pytest.mark.parametrize("version", [0, 2, 7, -1, "1", 1.0, True, None])
def test_a_version_this_server_does_not_implement_is_a_clean_typed_error(version: Any) -> None:
    """Version 2 (and every other value) is refused with a message, not a crash.

    ``True`` is in the list because ``isinstance(True, int)`` is ``True`` in Python, so a bare
    integer check would read a boolean as version 1 and proceed. ``None`` is here as the explicit
    ``"version": null`` a hand-rolled client might send — distinct from *omitting* the key, which
    the test below shows is read as version 1.
    """
    extensions = {"persistedQuery": {"version": version, "sha256Hash": SPEC_DIGEST}}

    plan = plan_persisted_query(extensions, None)

    assert plan.action is PersistedQueryAction.PROTOCOL_ERROR
    assert plan.reason is not None
    assert str(APQ_PROTOCOL_VERSION) in plan.reason
    assert repr(version) in plan.reason


def test_an_omitted_version_is_read_as_version_one() -> None:
    """Announcing nothing is not the same as announcing something incompatible.

    Every shipped Apollo client sends ``version: 1``; a hand-rolled one that omits it is not
    claiming a different protocol, so it is served rather than refused.
    """
    plan = plan_persisted_query(persisted_query_extension(SPEC_DIGEST, version=None), None)

    assert plan.action is PersistedQueryAction.LOOKUP


@pytest.mark.parametrize("digest", ["", "deadbeef", None, 42, SPEC_DIGEST[:-1] + "!"])
def test_a_malformed_hash_is_refused_before_it_can_reach_a_key(digest: Any) -> None:
    """A payload that announces APQ and then supplies junk is a client bug, and is named as one."""
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": digest}}

    plan = plan_persisted_query(extensions, SPEC_DOCUMENT)

    assert plan.action is PersistedQueryAction.PROTOCOL_ERROR
    assert plan.reason is not None
    assert "sha256Hash" in plan.reason


def test_the_version_is_checked_before_the_hash() -> None:
    """A payload wrong in two ways reports the *protocol* problem, which is the actionable one.

    Telling a version-2 client that its hash is malformed sends it looking in the wrong place; the
    version is what has to change before the hash could matter at all.
    """
    extensions = {"persistedQuery": {"version": 2, "sha256Hash": "junk"}}

    plan = plan_persisted_query(extensions, None)

    assert plan.action is PersistedQueryAction.PROTOCOL_ERROR
    assert "version" in (plan.reason or "")


def test_an_uppercase_hash_still_registers_the_document() -> None:
    """Normalisation happens before the comparison, so case cannot cause a false mismatch."""
    plan = plan_persisted_query(persisted_query_extension(SPEC_DIGEST.upper()), SPEC_DOCUMENT)

    assert plan.action is PersistedQueryAction.REGISTER
    assert plan.sha256_hash == SPEC_DIGEST, "the stored key must be the canonical lower-case form"


# =================================================================================================
# The store
# =================================================================================================


async def test_a_registered_document_comes_back_verbatim_and_moves_the_hit_counter() -> None:
    """Round trip through the store, with the counters as the second instrument.

    Byte-for-byte, because the document is about to be *hashed by the next client* to check against
    this same digest — a store that normalised on the way out would break step 2 for the very
    document step 2 registered.
    """
    client = StubRedis()
    store = PersistedQueryStore(make_settings(), redis_client=client)

    assert await store.put(SPEC_DIGEST, SPEC_DOCUMENT) is True
    assert await store.get(SPEC_DIGEST) == SPEC_DOCUMENT

    stats = store.stats
    assert (stats.hits, stats.misses, stats.registered, stats.errors) == (1, 0, 1, 0)


async def test_an_unregistered_hash_is_a_miss_rather_than_an_error() -> None:
    """Step 1 of the handshake. A miss is the protocol working, not a fault."""
    store = PersistedQueryStore(make_settings(), redis_client=StubRedis())

    assert await store.get(FOREIGN_DIGEST) is None

    stats = store.stats
    assert (stats.hits, stats.misses, stats.errors) == (0, 1, 0)


async def test_the_key_is_namespaced_and_versioned_and_carries_the_digest() -> None:
    """The key shape is pinned so a namespace change is a deliberate act with a visible diff.

    It must not collide with the result cache's keyspace: the two share one Redis client and one
    logical database, and an operational ``SCAN`` aimed at one would otherwise sweep the other.
    """
    client = StubRedis()
    store = PersistedQueryStore(make_settings(), redis_client=client)

    await store.put(SPEC_DIGEST, SPEC_DOCUMENT)
    (key,) = list(client.store)
    expected = (
        f"{DEFAULT_PERSISTED_QUERY_NAMESPACE}:v{PERSISTED_QUERY_FORMAT_VERSION}:{SPEC_DIGEST}"
    )

    assert key == expected
    assert store.make_key(SPEC_DIGEST) == key
    assert key.startswith("graphql-log-query:apq:")
    assert not key.startswith("graphql-log-query:cache")


async def test_the_document_is_stored_under_the_configured_ttl() -> None:
    """``PERSISTED_QUERY_TTL_SECONDS`` reaches ``SETEX``, rather than a hard-coded number."""
    client = StubRedis()
    store = PersistedQueryStore(make_settings(ttl_seconds=1234), redis_client=client)

    await store.put(SPEC_DIGEST, SPEC_DOCUMENT)

    assert store.ttl_seconds == 1234
    assert list(client.ttls.values()) == [1234]


async def test_a_non_positive_ttl_stores_nothing_and_is_not_an_error() -> None:
    """``PERSISTED_QUERY_TTL_SECONDS=0`` means "never remember", not "fail every write".

    Redis' ``SETEX`` rejects a non-positive expiry outright, so passing one through would turn a
    plainly-intended configuration into an error on every registration.
    """
    client = StubRedis()
    store = PersistedQueryStore(make_settings(ttl_seconds=0), redis_client=client)

    assert await store.put(SPEC_DIGEST, SPEC_DOCUMENT) is False
    assert client.calls == []
    assert store.stats.errors == 0


async def test_an_oversized_document_is_not_stored_and_the_request_is_unaffected() -> None:
    """Registration happens BEFORE parsing, so ``MAX_QUERY_TOKENS`` is not yet in the way.

    Without this cap a client could spend a megabyte of Redis per request on documents that never
    execute. Refusing to store one is not an error for the client: it sent the document, so the
    operation runs; it simply never gets a hash-only round trip for that document.
    """
    client = StubRedis()
    store = PersistedQueryStore(make_settings(), redis_client=client)
    huge = "#" * (MAX_PERSISTED_DOCUMENT_BYTES + 1) + "\n{ logs { id } }"

    assert await store.put(compute_query_hash(huge), huge) is False
    assert client.calls == []
    assert store.stats.oversized == 1
    assert store.stats.errors == 0


async def test_a_redis_that_raises_on_get_is_a_miss_that_is_counted_as_an_error() -> None:
    """**Never raises.** Three assertions, because "survived" has three halves.

    The caller gets a miss (so the client is told to resend the document), the error counter moves
    (so the outage is visible rather than skipped), and the miss counter moves (so the lookup is
    accounted for). A test asserting only "no exception escaped" would pass against a store that
    swallowed everything and reported nothing at all.
    """
    client = ExplodingRedis()
    store = PersistedQueryStore(make_settings(), redis_client=client)

    assert await store.get(SPEC_DIGEST) is None

    stats = store.stats
    assert client.calls == 1, "the store did not actually attempt the lookup"
    assert (stats.errors, stats.misses, stats.hits) == (1, 1, 0)


async def test_a_redis_that_raises_on_setex_leaves_the_request_unaffected() -> None:
    """A failed registration is a ``False``, not an exception into the middle of an operation."""
    client = ExplodingRedis()
    store = PersistedQueryStore(make_settings(), redis_client=client)

    assert await store.put(SPEC_DIGEST, SPEC_DOCUMENT) is False

    stats = store.stats
    assert client.calls == 1
    assert (stats.errors, stats.registered) == (1, 0)


async def test_a_blob_that_is_not_a_document_is_a_miss_rather_than_a_500() -> None:
    """A value written by a build with a different idea of the format is recomputable, not fatal."""
    client = StubRedis()
    store = PersistedQueryStore(make_settings(), redis_client=client)
    client.store[store.make_key(SPEC_DIGEST)] = b"\xff\xfe not utf-8 at all"

    assert await store.get(SPEC_DIGEST) is None

    stats = store.stats
    assert (stats.errors, stats.misses, stats.hits) == (1, 1, 0)


async def test_a_store_with_no_client_misses_everything_without_touching_anything() -> None:
    """A broken ``REDIS_URL`` is a different fault with identical, correct behaviour."""
    store = PersistedQueryStore(make_settings(), redis_client=None)

    assert store.configured is True, "the FEATURE is on; only the transport is missing"
    assert store.enabled is False
    assert await store.get(SPEC_DIGEST) is None
    assert await store.put(SPEC_DIGEST, SPEC_DOCUMENT) is False
    assert store.stats.misses == 1


async def test_configured_and_enabled_answer_different_questions() -> None:
    """The distinction decides which of the two well-known messages a client is sent.

    ``configured`` is "the operator switched the feature on" and ``enabled`` adds "and Redis is
    reachable". A store that is configured but disconnected must answer ``PersistedQueryNotFound``
    (temporary; keep using the protocol), never ``PersistedQueryNotSupported`` (permanent; stop) —
    otherwise a Redis blip would turn every APQ client into a full-document client for the rest of
    its session.
    """
    connected = PersistedQueryStore(make_settings(), redis_client=StubRedis())
    disconnected = PersistedQueryStore(make_settings(), redis_client=None)

    assert (connected.configured, connected.enabled) == (True, True)
    assert (disconnected.configured, disconnected.enabled) == (True, False)
    assert PERSISTED_QUERY_NOT_FOUND_MESSAGE != PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE


async def test_a_disabled_store_is_not_built_at_all() -> None:
    """``PERSISTED_QUERIES_ENABLED=false`` yields ``None``, which the extension reads as a decision.

    ``None`` — rather than a disabled instance — is what
    :class:`~src.graphql.apq.PersistedQueries` turns into ``PersistedQueryNotSupported``. Returning a
    disabled store instead would make "switched off" indistinguishable from "Redis is down".
    """
    assert create_persisted_query_store(make_settings(enabled=False)) is None

    built = create_persisted_query_store(make_settings(enabled=True), redis_client=StubRedis())
    assert built is not None
    assert built.enabled is True


async def test_the_shared_client_is_borrowed_and_never_closed_by_the_store() -> None:
    """The store must not close a pool the result cache is still reading through.

    ``aclose`` drops the reference either way — so ``enabled`` goes false, which is how a test
    observes the release — but the client itself is left open for its owner, the lifespan.
    """

    class ClosableRedis(StubRedis):
        def __init__(self) -> None:
            super().__init__()
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    client = ClosableRedis()
    borrowed = PersistedQueryStore(make_settings(), redis_client=client, owns_client=False)
    await borrowed.aclose()

    assert client.closed is False, "a borrowed client must outlive the store that borrowed it"
    assert borrowed.enabled is False

    owned = PersistedQueryStore(make_settings(), redis_client=client, owns_client=True)
    await owned.aclose()

    assert client.closed is True


# =================================================================================================
# Reading the payload off a request
# =================================================================================================


class FakeRequest:
    """The two attributes :func:`read_request_extensions` uses, and nothing else.

    A stand-in rather than a real Starlette ``Request`` because what is under test is the *policy* —
    which transports are read, when the body is parsed, what happens when it cannot be — and a real
    request would need an ASGI scope and a receive channel to express any of it.
    """

    def __init__(
        self,
        *,
        body: bytes = b"",
        method: str = "POST",
        scope_type: str = "http",
        query_params: Optional[dict[str, str]] = None,
        raises: Optional[BaseException] = None,
    ) -> None:
        self._body = body
        self.method = method
        self.scope = {"type": scope_type}
        self.query_params = query_params or {}
        self._raises = raises
        self.body_reads = 0

    async def body(self) -> bytes:
        self.body_reads += 1
        if self._raises is not None:
            raise self._raises
        return self._body


class FakeContext:
    """Just enough of :class:`src.graphql.context.Context` to carry a request."""

    def __init__(self, request: Any = None) -> None:  # noqa: ANN401
        self.request = request


async def test_the_body_is_not_parsed_when_it_does_not_mention_a_persisted_query() -> None:
    """The hot path: a plain request costs a substring search, not a second ``json.loads``.

    This runs on **every** operation, including the thousands the C14 load harness sends. Asserted
    by handing it a body that is not valid JSON at all: if the marker check were absent the parse
    would fail (or, worse, succeed and cost real time), and either way the ``None`` below would be
    reached for the wrong reason — so the body is deliberately parseable-looking but broken.
    """
    request = FakeRequest(body=b'{"query": "{ logs { id } }", "variables": {,,,}}')

    assert await read_request_extensions(FakeContext(request)) is None
    assert request.body_reads == 1, "the cached body should be read exactly once"


async def test_a_post_body_carrying_the_payload_is_read() -> None:
    request = FakeRequest(
        body=(
            b'{"variables":{},"extensions":{"persistedQuery":'
            b'{"version":1,"sha256Hash":"' + SPEC_DIGEST.encode() + b'"}}}'
        )
    )

    extensions = await read_request_extensions(FakeContext(request))

    assert read_persisted_query_payload(extensions) == {
        "version": 1,
        "sha256Hash": SPEC_DIGEST,
    }


async def test_the_get_spelling_of_the_protocol_is_read_from_the_query_string() -> None:
    """``GET /graphql?extensions={"persistedQuery":{...}}`` — the CDN-cacheable form.

    Half the reason APQ exists is that a hash-only request is short enough to be a cacheable ``GET``,
    so the ``GET`` spelling is not an afterthought.
    """
    request = FakeRequest(
        method="GET",
        query_params={
            "extensions": '{"persistedQuery":{"version":1,"sha256Hash":"' + SPEC_DIGEST + '"}}'
        },
    )

    extensions = await read_request_extensions(FakeContext(request))

    assert read_persisted_query_payload(extensions) == {"version": 1, "sha256Hash": SPEC_DIGEST}
    assert request.body_reads == 0, "a GET has no body to read"


async def test_a_websocket_connection_is_never_read() -> None:
    """Subscriptions always send their document; the transport carries its own extensions.

    Returning ``None`` here is what keeps this module out of the WebSocket entirely — see the
    module docstring of :mod:`src.graphql.apq`.
    """
    request = FakeRequest(scope_type="websocket")

    assert await read_request_extensions(FakeContext(request)) is None
    assert request.body_reads == 0


@pytest.mark.parametrize(
    "request_object",
    [
        None,
        FakeRequest(body=b"persistedQuery but not JSON at all"),
        FakeRequest(body=b'"persistedQuery"'),
        FakeRequest(body=b'["persistedQuery"]'),
        FakeRequest(body=b"persistedQuery", raises=RuntimeError("Stream consumed")),
        FakeRequest(method="GET", query_params={"extensions": "not json"}),
    ],
)
async def test_an_unreadable_request_is_no_payload_rather_than_a_failure(
    request_object: Any,
) -> None:
    """**Never raises.** Every failure here means "this is not an APQ request".

    Failing *closed* is the important half: the request is then handled as an ordinary one, so the
    worst case of a body this function cannot understand is a client that has to send its document —
    never a 500, and never a request handled as APQ on a guess.
    """
    assert await read_request_extensions(FakeContext(request_object)) is None


async def test_a_context_with_no_request_at_all_is_handled() -> None:
    """``schema.execute()`` with a hand-built context has no request, and that is not an error."""

    class Bare:
        pass

    assert await read_request_extensions(Bare()) is None
    assert await read_request_extensions(None) is None
