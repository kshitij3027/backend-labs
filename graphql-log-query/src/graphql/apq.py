"""Automatic persisted queries — spec §2 item 36, §7 ``PERSISTED_QUERIES_ENABLED``.

The Apollo APQ protocol, implemented as a :class:`~strawberry.extensions.SchemaExtension`. A client
sends ``extensions.persistedQuery = {version: 1, sha256Hash: "<64 hex>"}`` alongside — or
**instead of** — the query document, and the server keeps a hash-to-document map in Redis so the
second and every later request for the same operation can omit the document entirely. That is a
bandwidth optimisation for the client and, because the document text is what a CDN would otherwise
have to see, the reason APQ requests can be sent as cacheable ``GET``s at all.

.. rubric:: THE THREE-STEP HANDSHAKE, AND THE ONE THAT MUST NOT BE SKIPPED

1. **Hash only, nothing registered.** The server answers with a single error whose message is the
   literal string ``PersistedQueryNotFound``. It is not a server fault and it is not an outage — it
   is the *normal* first step of the protocol, and the client's correct response is to resend the
   same request with the document attached. Both the message and
   :attr:`~src.graphql.errors.ErrorCode.PERSISTED_QUERY_NOT_FOUND` in ``extensions.code`` are part
   of the wire contract: Apollo's ``createPersistedQueryLink`` keys its retry on exactly that
   message, so "improving" it breaks every client that speaks the protocol.
2. **Document + hash.** The server **recomputes** ``sha256(document)`` and compares. On a match the
   document is registered under the hash and executed normally.
3. **Hash only, registered.** The document is loaded out of Redis, substituted, and executed. The
   client sent no document at all and receives the same answer as step 2.

.. rubric:: STEP 2 RECOMPUTES THE HASH, AND THAT IS A SECURITY PROPERTY RATHER THAN A NICETY

If the server trusted the client's ``sha256Hash`` and stored the document under it unverified, any
client could register a document of its choosing under **somebody else's** hash — including the
hash of an operation a privileged client is about to send by hash alone. The next hash-only request
would then execute the attacker's document under the victim's request. The whole value of APQ rests
on "the hash names this exact document", so the server is the party that has to establish it.

Two consequences that are asserted by name in ``tests/integration/test_apq.py``:

* a mismatch is **refused**, and
* the mismatched document is **not stored** — the follow-up hash-only request is still a miss.

.. rubric:: WHERE THE SUBSTITUTION HAPPENS, AND WHY IT HAS TO BE THERE

The document is written into ``execution_context.query`` from the **pre-yield half of**
``on_operation``. Strawberry's ``execute`` runs ``if not execution_context.query: raise
MissingQueryError()`` and then the parse step *inside* the ``extensions_runner.operation()`` context
manager, so every extension's pre-yield code has already run by the time the document is read. A
persisted document is therefore parsed, depth-limited, token-limited, alias-limited and **cost-gated
exactly like a document sent inline** — there is no path by which a hash is cheaper to execute than
the text it stands for. That is the hole this ordering exists to close: a persisted query that
skipped validation would be a way through C8's gate for the price of one registration, and
``tests/integration/test_apq.py`` proves it closed by persisting an over-budget document and then
sending it by hash.

.. rubric:: A REJECTION IS REPORTED THROUGH A VALIDATION RULE, NOT BY RAISING

An exception raised from an extension hook is not a shape this project can promise anything about:
it escapes ``schema.execute`` and reaches the ASGI layer, where it is an HTTP 500 with a traceback —
the one outcome spec §2 item 35 forbids. So a rejection instead does what C8's cost gate does, and
for the same reason: it substitutes a trivially cheap placeholder document (``{ __typename }``,
which parses, prices at zero and touches nothing) and appends a
:class:`~graphql.validation.ValidationRule` that reports the real error. The operation is then
refused **during validation**, before any resolver runs, and the client receives a 200 with a
well-formed ``errors`` envelope carrying the code — the identical machinery, and therefore the
identical guarantees, as every other refusal this server issues.

That also settles the logging: the error is a :class:`~src.graphql.errors.DomainError`, so
:func:`~src.graphql.errors.is_expected_error` classifies it as expected,
:class:`~src.graphql.errors.MaskInternalErrors` leaves it alone, and the ``process_errors`` override
records **one INFO line with no traceback**. A hash-only probe is a routine protocol step and a
client under an APQ link sends one per cold operation; printing a stack trace for each would be the
C4 log-spam problem all over again.

.. rubric:: The store never raises, exactly like the result cache

Redis unreachable, a timeout, a value written by an older build — every one of them is a **miss**.
A miss answers ``PersistedQueryNotFound``, the client resends with the document, and the request
succeeds. So the failure mode of losing Redis is "APQ stops saving bandwidth", never "the API stops
answering". :meth:`PersistedQueryStore.get` and :meth:`PersistedQueryStore.put` have no failure that
reaches a caller.

.. rubric:: What ``PERSISTED_QUERIES_ENABLED=false`` does, and why it is not simply ignored

When the feature is off the lifespan builds **no store at all** (``app.state.apq is None``), and
this extension then answers a hash-only request with the protocol's other well-known message,
``PersistedQueryNotSupported``. That is deliberately different from ``PersistedQueryNotFound``:
Apollo's link retries a *NotFound* forever (once per cold operation) but disables APQ for the rest
of the session on a *NotSupported*, which is precisely the behaviour an operator who switched the
feature off wants from every client. A request that carries **both** a document and a hash is simply
executed and nothing is stored — a client with an APQ link keeps working against a server that does
not have the feature, which is the entire point of the negotiation.

.. rubric:: HTTP only

The payload is read off the HTTP request, so APQ covers ``POST /graphql`` and ``GET /graphql``.
Subscriptions negotiate over ``graphql-transport-ws``, where the ``subscribe`` message carries its
own ``payload.extensions`` that Strawberry consumes before an extension can see it; a subscription
therefore always sends its document, which costs nothing (one document per socket rather than one
per operation) and is why this module does not reach into the WebSocket at all.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from graphql import GraphQLError
from graphql.validation import ValidationContext, ValidationRule
from strawberry.extensions import SchemaExtension

from src.config import Settings
from src.graphql.errors import PersistedQueryNotFoundError, ValidationError

if TYPE_CHECKING:  # pragma: no cover - annotations only; `from __future__` makes them strings
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)

#: The only protocol version this server implements. Apollo has never shipped another one; a client
#: announcing a different number is announcing a handshake we cannot honour, so it is told so rather
#: than served under an assumption.
APQ_PROTOCOL_VERSION = 1

#: The literal message a client's APQ link matches on to decide "resend with the document". It is a
#: WIRE CONSTANT, not prose: Apollo compares it with ``===``. Changing it turns every APQ client
#: into one that retries nothing and fails.
PERSISTED_QUERY_NOT_FOUND_MESSAGE = "PersistedQueryNotFound"

#: The other well-known message: "this server does not do APQ, stop asking". Apollo disables the
#: link for the session on it. Sent when ``PERSISTED_QUERIES_ENABLED`` is false.
PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE = "PersistedQueryNotSupported"

#: Key prefix. Deliberately NOT the result cache's namespace (``graphql-log-query:cache``): the two
#: share one Redis client and one logical database, and a shared prefix would put documents and
#: cached result blobs in one keyspace where a ``KEYS``/``SCAN`` sweep intended for one would sweep
#: the other.
DEFAULT_PERSISTED_QUERY_NAMESPACE = "graphql-log-query:apq"

#: Bumped if the stored VALUE stops being "the document text, UTF-8". It sits in the key, so a bump
#: strands every existing entry behind a key nothing asks for; they expire on their own TTL. Same
#: migration story as :data:`src.cache.CACHE_FORMAT_VERSION`, for the same reason: the data is
#: derived and the client can always resend the document.
PERSISTED_QUERY_FORMAT_VERSION = 1

#: The largest document that will be **registered**, in UTF-8 bytes.
#:
#: Registration happens BEFORE the document is parsed — it has to, because the substitution has to
#: precede parsing — so ``MAX_QUERY_TOKENS`` is not yet standing between a client and this store.
#: Without a cap, a client could spend a megabyte of Redis per request on documents that never
#: execute. 100 kB is roughly fifty times the largest document a 2,000-token budget admits, so no
#: legitimate operation is anywhere near it. An oversized document is executed normally and simply
#: not stored: the request is unaffected, the client just never gets a hash-only round trip for it.
MAX_PERSISTED_DOCUMENT_BYTES = 100_000

#: The placeholder substituted for a rejected request. It parses, it is valid against every schema
#: this project builds, the C8 cost model prices introspection at zero, and it never runs because
#: validation reports the real error first. See the module docstring.
REJECTION_DOCUMENT = "{ __typename }"

#: A sha256 hex digest, lower-cased. Anchored, so a hash carrying a path separator or a wildcard
#: cannot reach the key builder — the value is interpolated into a Redis key, and "it comes from a
#: hash function" is a statement about the honest client rather than about the hostile one.
_SHA256_HEX = re.compile(r"\A[0-9a-f]{64}\Z")

#: The marker looked for in the raw request body before it is parsed a second time. See
#: :func:`read_request_extensions`.
_APQ_BODY_MARKER = b"persistedQuery"


# =================================================================================================
# Hashing — pure, and the security property of step 2
# =================================================================================================


def compute_query_hash(document: str) -> str:
    """``sha256(document)`` as a lower-case hex digest — what the client claims and we verify.

    Over the **exact text** the client sent, UTF-8 encoded, with no normalisation of whitespace or
    of the operation's spelling. That is the contract Apollo implements client-side, so any
    normalisation here would make every hash a mismatch; it also means two documents that differ
    only in indentation are two entries, which is correct (they are two different strings, and a
    client that generated both meant to).
    """
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def hash_matches_document(document: str, supplied_hash: str) -> bool:
    """Does ``supplied_hash`` really name ``document``?

    Plain equality rather than a constant-time comparison, deliberately: the value being compared is
    the digest of a document the *client itself* supplied in the same request, so there is no secret
    to leak by timing and a constant-time compare would only suggest there was one.
    """
    return compute_query_hash(document) == supplied_hash


def normalise_hash(value: Any) -> Optional[str]:  # noqa: ANN401 - whatever arrived in the JSON
    """``value`` as a validated sha256 hex digest, or ``None`` if it is not one.

    Upper-case hex is accepted and folded down (some clients emit it); anything else — a short
    string, a number, a digest with a colon in it — is rejected outright rather than passed to the
    key builder.
    """
    if not isinstance(value, str):
        return None
    candidate = value.strip().lower()
    return candidate if _SHA256_HEX.match(candidate) else None


# =================================================================================================
# The protocol state machine — pure, so it is unit-testable without Redis or a request
# =================================================================================================


class PersistedQueryAction(Enum):
    """What the request's ``persistedQuery`` payload asks the server to do.

    Note what is **not** here: ``NOT_FOUND``. Whether a lookup misses is not knowable from the
    request alone — it needs Redis — so :meth:`PersistedQueries.on_operation` decides it after
    :attr:`LOOKUP` returns nothing. Everything this enum names is decidable from the request, which
    is exactly what makes :func:`plan_persisted_query` a pure function.
    """

    #: No usable APQ payload. Execute the client's own document; touch nothing.
    PASS_THROUGH = "pass_through"
    #: Hash only. Resolve it from the store; a miss is ``PersistedQueryNotFound``.
    LOOKUP = "lookup"
    #: Document **and** a hash that verifiably names it. Register, then execute the document.
    REGISTER = "register"
    #: Document and a hash that does **not** name it. Refuse, and store nothing.
    MISMATCH = "mismatch"
    #: The payload is malformed or announces a version this server does not implement.
    PROTOCOL_ERROR = "protocol_error"


@dataclass(frozen=True, slots=True)
class PersistedQueryPlan:
    """The decision :func:`plan_persisted_query` reached, and the values the caller needs to act.

    Attributes:
        action: What to do. See :class:`PersistedQueryAction`.
        sha256_hash: The validated digest, for :attr:`~PersistedQueryAction.LOOKUP` and
            :attr:`~PersistedQueryAction.REGISTER`. ``None`` otherwise.
        document: The document to register, for :attr:`~PersistedQueryAction.REGISTER`.
        reason: The client-facing message for :attr:`~PersistedQueryAction.MISMATCH` and
            :attr:`~PersistedQueryAction.PROTOCOL_ERROR`. Built here rather than at the call site so
            the *whole* decision — including what the client is told and why — is one pure function
            a unit test can drive.
    """

    action: PersistedQueryAction
    sha256_hash: Optional[str] = None
    document: Optional[str] = None
    reason: Optional[str] = None


def read_persisted_query_payload(request_extensions: Any) -> Optional[Mapping[str, Any]]:  # noqa: ANN401
    """The ``persistedQuery`` object out of a request's ``extensions``, or ``None``.

    Everything that is not a mapping containing a mapping under ``persistedQuery`` is "this is not
    an APQ request" rather than an error. GraphQL's ``extensions`` field is an open extension point
    and other tooling puts its own keys there; a server that rejected an ``extensions`` object it
    did not recognise would break clients that are doing nothing wrong.
    """
    if not isinstance(request_extensions, Mapping):
        return None
    payload = request_extensions.get("persistedQuery")
    return payload if isinstance(payload, Mapping) else None


def plan_persisted_query(request_extensions: Any, document: Optional[str]) -> PersistedQueryPlan:  # noqa: ANN401
    """Decide what an APQ request is asking for. **Pure**: no Redis, no request, no I/O.

    The order of the checks is the design:

    1. No payload at all -> :attr:`~PersistedQueryAction.PASS_THROUGH`, and nothing else is looked
       at. The overwhelming majority of requests take this branch and it costs two type checks.
    2. Version, then hash shape. A malformed payload is refused *before* the hash is used to build a
       key or to compare against anything.
    3. **Document present -> verify.** The comparison happens before the store is consulted or
       written, which is what makes cache poisoning impossible rather than merely unlikely.
    4. Document absent -> :attr:`~PersistedQueryAction.LOOKUP`.

    Args:
        request_extensions: The request's ``extensions`` object, in whatever shape it arrived.
        document: The query document the client sent, or ``None``/``""`` when it sent none. An
            empty string is treated as absent — it is what a client that stripped the query leaves
            behind, and ``MissingQueryError`` treats it the same way.
    """
    payload = read_persisted_query_payload(request_extensions)
    if payload is None:
        return PersistedQueryPlan(PersistedQueryAction.PASS_THROUGH)

    # A missing `version` is read as 1: every shipped Apollo client sends it, and the one that does
    # not is announcing nothing rather than announcing something incompatible.
    version = payload.get("version", APQ_PROTOCOL_VERSION)
    if not isinstance(version, int) or isinstance(version, bool) or version != APQ_PROTOCOL_VERSION:
        return PersistedQueryPlan(
            PersistedQueryAction.PROTOCOL_ERROR,
            reason=(
                f"unsupported persisted query version {version!r}: this server implements version "
                f"{APQ_PROTOCOL_VERSION} of the automatic persisted queries protocol. Send "
                f'`extensions.persistedQuery.version = {APQ_PROTOCOL_VERSION}`, or omit the '
                "`persistedQuery` extension entirely and send the document."
            ),
        )

    sha256_hash = normalise_hash(payload.get("sha256Hash"))
    if sha256_hash is None:
        return PersistedQueryPlan(
            PersistedQueryAction.PROTOCOL_ERROR,
            reason=(
                "`extensions.persistedQuery.sha256Hash` must be the SHA-256 of the query document "
                "as 64 hexadecimal characters"
            ),
        )

    if not document:
        return PersistedQueryPlan(PersistedQueryAction.LOOKUP, sha256_hash=sha256_hash)

    if not hash_matches_document(document, sha256_hash):
        return PersistedQueryPlan(
            PersistedQueryAction.MISMATCH,
            sha256_hash=sha256_hash,
            document=document,
            reason=(
                "the supplied `extensions.persistedQuery.sha256Hash` is not the SHA-256 of the "
                "query document in this request, so the document was NOT registered. A persisted "
                "query is stored under the hash of its exact text; registering it under any other "
                "hash would let one client decide what a later hash-only request executes."
            ),
        )

    return PersistedQueryPlan(
        PersistedQueryAction.REGISTER, sha256_hash=sha256_hash, document=document
    )


# =================================================================================================
# Counters
# =================================================================================================


@dataclass(frozen=True, slots=True)
class PersistedQueryStats:
    """A point-in-time snapshot of the store's counters.

    Shaped, like :class:`src.cache.CacheStats` and :class:`src.broker.BrokerStats`, to be lifted
    straight into Prometheus by :mod:`src.metrics`: every field is a monotonic counter except
    :attr:`enabled`, and the names are the metric names minus their prefix. Nothing here carries a
    label, so the whole family costs a fixed number of series.

    ``hits + misses`` is the number of hash-only requests served. ``registered`` counts successful
    step-2 registrations, so ``registered`` climbing while ``hits`` stays flat means clients are
    registering documents they never send again — a hash that changes on every deploy, usually.

    Attributes:
        enabled: ``PERSISTED_QUERIES_ENABLED`` **and** a Redis client that could be built. A store
            that exists with ``enabled`` false answers every lookup as a miss, which is correct and
            simply saves nothing.
        hits: Hash-only requests answered from a registered document.
        misses: Hash-only requests with nothing registered — **including** every request during a
            Redis outage. A miss is a normal protocol step, not a failure.
        registered: Documents stored after their hash was verified.
        mismatches: Registrations refused because the supplied hash did not name the document. Any
            value above zero is worth looking at: it is either a broken client or an attempt at the
            poisoning attack step 2 exists to prevent.
        protocol_errors: Payloads refused for their shape — an unsupported ``version``, a
            ``sha256Hash`` that is not a digest.
        oversized: Documents that verified but were too large to store. See
            :data:`MAX_PERSISTED_DOCUMENT_BYTES`.
        errors: Redis or decode failures. Every one was survived — this counter moving is evidence
            that the never-raises contract was exercised, not that a request failed.
    """

    enabled: bool
    hits: int
    misses: int
    registered: int
    mismatches: int
    protocol_errors: int
    oversized: int
    errors: int


# =================================================================================================
# The store
# =================================================================================================


class PersistedQueryStore:
    """The hash-to-document map, in Redis. **Never raises.**

    One instance per process, built in :func:`src.main.lifespan` and reached by the extension
    through ``info.context.persisted_queries``.

    .. rubric:: It borrows the result cache's Redis client rather than opening a third pool

    C7 justified two clients: one for the **request path**, carrying a ``socket_timeout`` so a Redis
    that accepts connections and then stops answering cannot park a resolver forever, and one for
    the **long-poll path**, where the pub/sub reader's steady state is a blocking read that such a
    timeout would turn into a reconnect loop. A persisted-query lookup is squarely on the request
    path, so it belongs to the first client and :func:`src.main.lifespan` hands the same object to
    both. The one thing that is NOT shared is the gate: the client is built when *either*
    ``CACHE_ENABLED`` or ``PERSISTED_QUERIES_ENABLED`` is set, so turning the cache off does not
    silently turn persisted queries off with it — which matters, because the compose ``test``
    service runs with exactly that combination.

    The counters are here rather than on the extension because a ``SchemaExtension`` is constructed
    **per operation**: anything counted there would be discarded with the request that counted it.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        redis_client: Optional["Redis"] = None,
        namespace: str = DEFAULT_PERSISTED_QUERY_NAMESPACE,
        owns_client: bool = False,
    ) -> None:
        """Build a store.

        Args:
            settings: Supplies ``PERSISTED_QUERIES_ENABLED`` and ``PERSISTED_QUERY_TTL_SECONDS``.
                Carried rather than read from :func:`src.config.get_settings` so a test can run one
                app with the feature on and the next with it off without touching a process-wide
                LRU cache.
            redis_client: The store, or ``None`` for one that always misses. Duck-typed on ``get()``
                and ``setex()`` so the unit suite can drive every failure branch with a stub that
                raises, rather than asserting "no exception was raised" against a healthy server.
            namespace: Key prefix. Overridden per test so one test cannot answer another's lookup.
            owns_client: Whether :meth:`aclose` closes ``redis_client``. ``False`` for the shared
                request-path client, which the lifespan built and the lifespan closes — a store that
                closed it would take the result cache down as a side effect of its own shutdown.
        """
        self._settings = settings
        self._redis = redis_client
        self._namespace = namespace
        self._owns_client = owns_client
        self._configured_enabled = bool(settings.persisted_queries_enabled)

        self._hits = 0
        self._misses = 0
        self._registered = 0
        self._mismatches = 0
        self._protocol_errors = 0
        self._oversized = 0
        self._errors = 0

        #: ``None`` until the first observation, so the first transition always logs. Same
        #: once-per-state-change discipline as the cache and the broker's bridge: a Redis outage
        #: under load must cost one log line, not one per request.
        self._healthy: Optional[bool] = None

    # -- identity and counters ---------------------------------------------------------------

    @property
    def configured(self) -> bool:
        """Is the feature switched on? Independent of whether Redis is reachable.

        The extension consults this rather than :attr:`enabled` to decide between
        ``PersistedQueryNotFound`` (configured, but nothing registered — resend with the document)
        and passing through. "Enabled but Redis is down" must behave as a permanent miss, NOT as
        "not supported": the outage is ours and temporary, and a client that disabled its APQ link
        over it would keep sending full documents long after Redis came back.
        """
        return self._configured_enabled

    @property
    def enabled(self) -> bool:
        """Is this store actually able to remember anything? Configured **and** holding a client."""
        return self._configured_enabled and self._redis is not None

    @property
    def namespace(self) -> str:
        """The key prefix this store writes under."""
        return self._namespace

    @property
    def redis_client(self) -> Optional["Redis"]:
        """The client this store reads through, or ``None``.

        The counterpart of :attr:`src.cache.ResultCache.redis_client`, and exposed for the same
        reason: it lets an integration test assert by **identity** that the lifespan handed one pool
        to both consumers instead of opening a second.
        """
        return self._redis

    @property
    def ttl_seconds(self) -> int:
        """``PERSISTED_QUERY_TTL_SECONDS`` — how long a registered document is retained."""
        return int(self._settings.persisted_query_ttl_seconds)

    @property
    def stats(self) -> PersistedQueryStats:
        """A snapshot of every counter. See :class:`PersistedQueryStats`."""
        return PersistedQueryStats(
            enabled=self.enabled,
            hits=self._hits,
            misses=self._misses,
            registered=self._registered,
            mismatches=self._mismatches,
            protocol_errors=self._protocol_errors,
            oversized=self._oversized,
            errors=self._errors,
        )

    def record_mismatch(self) -> None:
        """Count a refused registration. Called by the extension, which is per-operation."""
        self._mismatches += 1

    def record_protocol_error(self) -> None:
        """Count a payload refused for its shape. Called by the extension."""
        self._protocol_errors += 1

    # -- keys ---------------------------------------------------------------------------------

    def make_key(self, sha256_hash: str) -> str:
        """The Redis key a document is stored under.

        ``"<namespace>:v<format>:<64 hex chars>"``. The digest is already validated by
        :func:`normalise_hash` before it reaches here, so nothing a client controls can widen the
        key beyond one namespace.
        """
        return f"{self._namespace}:v{PERSISTED_QUERY_FORMAT_VERSION}:{sha256_hash}"

    # -- the store path -------------------------------------------------------------------------

    async def get(self, sha256_hash: str) -> Optional[str]:
        """The document registered under ``sha256_hash``, or ``None``. **Never raises.**

        Every failure — no client, Redis unreachable, a timeout, bytes that are not UTF-8 — is a
        ``None``, which the caller turns into ``PersistedQueryNotFound`` and the client answers by
        resending the document. Losing Redis therefore costs bandwidth and nothing else.

        The hit/miss counters move here rather than in the extension so they count *lookups*
        regardless of what the caller then does with the answer.
        """
        client = self._redis
        if not self._configured_enabled or client is None:
            self._misses += 1
            return None

        try:
            raw = await client.get(self.make_key(sha256_hash))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a Redis fault is a miss, never a failed request
            self._errors += 1
            self._misses += 1
            self._note_health(healthy=False, operation="get", reason=exc)
            return None

        if raw is None:
            self._misses += 1
            self._note_health(healthy=True)
            return None

        try:
            document = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:  # noqa: BLE001 - a blob that is not a document is a miss, not a 500
            self._errors += 1
            self._misses += 1
            logger.warning(
                "discarding an undecodable persisted query document (hash=%s)", sha256_hash
            )
            return None

        self._hits += 1
        self._note_health(healthy=True)
        return document

    async def put(self, sha256_hash: str, document: str) -> bool:
        """Register ``document`` under ``sha256_hash``. **Never raises.**

        The caller has already verified the hash — this method does not re-check it, and it must not
        be called on an unverified pair.

        Returns:
            ``True`` when the document was really written. ``False`` for every reason it was not
            (no client, a non-positive TTL, an oversized document, a Redis failure), all of which
            leave the request itself completely unaffected: the client sent the document, so the
            operation runs either way and only the *next* hash-only request pays for the failure.
        """
        client = self._redis
        ttl = self.ttl_seconds
        if not self._configured_enabled or client is None or ttl <= 0:
            return False

        encoded = document.encode("utf-8")
        if len(encoded) > MAX_PERSISTED_DOCUMENT_BYTES:
            self._oversized += 1
            logger.warning(
                "refusing to persist a %d-byte query document (limit %d) — the operation runs "
                "normally, it simply cannot be sent by hash",
                len(encoded),
                MAX_PERSISTED_DOCUMENT_BYTES,
            )
            return False

        try:
            await client.setex(self.make_key(sha256_hash), ttl, encoded)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never raises
            self._errors += 1
            self._note_health(healthy=False, operation="setex", reason=exc)
            return False

        self._registered += 1
        self._note_health(healthy=True)
        return True

    # -- lifecycle ------------------------------------------------------------------------------

    def _note_health(
        self,
        *,
        healthy: bool,
        operation: str = "",
        reason: Optional[BaseException] = None,
    ) -> None:
        """Log Redis health **once per transition**, never once per request."""
        if self._healthy is healthy:
            return
        self._healthy = healthy
        if healthy:
            logger.info("persisted query store connected (namespace=%s)", self._namespace)
        else:
            logger.warning(
                "persisted query store degraded (namespace=%s, operation=%s): %s: %s — hash-only "
                "requests will answer %s until Redis returns, and clients will resend their "
                "documents, which is correct and simply costs bandwidth",
                self._namespace,
                operation,
                type(reason).__name__ if reason is not None else "unknown",
                reason,
                PERSISTED_QUERY_NOT_FOUND_MESSAGE,
            )

    async def aclose(self) -> None:
        """Release the Redis client **if this store built it**. Idempotent; never raises."""
        client, self._redis = self._redis, None
        if client is None or not self._owns_client:
            return
        try:
            closer = getattr(client, "aclose", None) or getattr(client, "close", None)
            if closer is None:
                return
            result = closer()
            if asyncio.iscoroutine(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - teardown is best-effort
            logger.debug("failed to close the persisted query store's Redis client", exc_info=True)


def create_persisted_query_store(
    settings: Settings,
    *,
    redis_client: Optional["Redis"] = None,
    namespace: str = DEFAULT_PERSISTED_QUERY_NAMESPACE,
) -> Optional[PersistedQueryStore]:
    """The process's store, or ``None`` when ``PERSISTED_QUERIES_ENABLED`` is false.

    ``None`` rather than a disabled instance, and the distinction is load-bearing: ``None`` is what
    :class:`PersistedQueries` reads as "this server does not do APQ" and answers
    ``PersistedQueryNotSupported`` to, while a store that exists but cannot reach Redis answers
    ``PersistedQueryNotFound``. See :attr:`PersistedQueryStore.configured`.

    ``redis_client`` is the shared request-path client :func:`src.main.lifespan` also hands the
    result cache; the store borrows it and never closes it.
    """
    if not settings.persisted_queries_enabled:
        return None
    return PersistedQueryStore(
        settings, redis_client=redis_client, namespace=namespace, owns_client=False
    )


# =================================================================================================
# The rejection path — a validation rule, exactly like C8's cost gate
# =================================================================================================


def create_rejecting_rule(error: GraphQLError) -> type[ValidationRule]:
    """A ``ValidationRule`` class that reports ``error`` and looks at nothing else.

    A factory returning a class because ``graphql.validate`` instantiates each rule with nothing but
    the validation context, so the error has to be closed over. The work happens in ``__init__`` for
    the same reason it does in :func:`src.graphql.cost.create_cost_validator`: there is no node to
    wait for, the decision was already made before parsing.
    """

    class PersistedQueryRejection(ValidationRule):
        """Refuses the operation with a decision the APQ extension already reached."""

        def __init__(self, validation_context: ValidationContext) -> None:
            super().__init__(validation_context)
            self.report_error(error)

    return PersistedQueryRejection


class PersistedQueries(SchemaExtension):
    """The APQ protocol. Substitutes the document **before parsing**; rejects through validation.

    Registered on the schema as a **class**, never an instance: Strawberry constructs one per
    execution and this extension holds per-operation state (the resolved document). See the ordering
    and factory notes in :mod:`src.graphql.schema`.

    Its whole contribution is in the pre-yield half of :meth:`on_operation`, which is why its
    position in the extension list is free as long as it is anywhere ahead of parsing — and parsing
    happens after *every* extension's pre-yield code has run.
    """

    async def on_operation(self) -> AsyncIterator[None]:
        """Resolve the request's persisted-query payload, then let the operation proceed.

        Async because two steps of the protocol are I/O: reading the request body and talking to
        Redis. That is the same reason :class:`src.graphql.context.PerOperationResources` is async,
        and it costs nothing extra — the schema has been async-only since C5.
        """
        execution_context = self.execution_context
        context = execution_context.context
        store: Optional[PersistedQueryStore] = getattr(context, "persisted_queries", None)

        request_extensions = await read_request_extensions(context)
        plan = plan_persisted_query(request_extensions, execution_context.query)

        if plan.action is PersistedQueryAction.PASS_THROUGH:
            # The overwhelmingly common branch, and the cheap one: two type checks and no I/O.
            yield
            return

        if store is None or not store.configured:
            # The feature is off. A request that also carried a document just runs — a client with
            # an APQ link must keep working against a server without the feature, which is the whole
            # point of the negotiation.
            #
            # A request with NO document is answered `PersistedQueryNotSupported` whatever else was
            # wrong with its payload. The test is "is there anything to execute", not "was the plan
            # a LOOKUP": a malformed payload with no document would otherwise fall through to
            # Strawberry's `MissingQueryError` and reach the client as a plain-text HTTP 400 —
            # exactly the shape spec §2 item 35 says a failure must never take.
            if not execution_context.query:
                self._reject(ValidationError(PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE))
            yield
            return

        if plan.action is PersistedQueryAction.PROTOCOL_ERROR:
            store.record_protocol_error()
            self._reject(ValidationError(plan.reason or "invalid persisted query payload"))
            yield
            return

        if plan.action is PersistedQueryAction.MISMATCH:
            # NOTHING IS STORED ON THIS PATH. See the module docstring: storing an unverified pair
            # is the one bug that turns APQ from an optimisation into a remote code-selection
            # primitive.
            store.record_mismatch()
            self._reject(ValidationError(plan.reason or "persisted query hash mismatch"))
            yield
            return

        if plan.action is PersistedQueryAction.REGISTER:
            # Registered BEFORE the document is parsed, because that is the only point at which the
            # substitution could have happened and the two have to agree about which text the hash
            # names. A document that then fails validation is still stored: it is the client's own
            # document under its own hash, its residency is bounded by the TTL, and refusing to
            # store anything that does not execute cleanly would mean a query with a runtime error
            # could never be sent by hash.
            await store.put(plan.sha256_hash or "", plan.document or "")
            yield
            return

        # LOOKUP.
        document = await store.get(plan.sha256_hash or "")
        if document is None:
            # Not a fault. The client's correct next move is to resend with the document attached,
            # which the message tells its APQ link to do.
            self._reject(PersistedQueryNotFoundError(PERSISTED_QUERY_NOT_FOUND_MESSAGE))
            yield
            return

        # THE SUBSTITUTION. Pre-yield, so parsing, the depth/token/alias limiters and the C8 cost
        # gate all see this document exactly as if the client had sent it inline.
        execution_context.query = document
        yield

    def _reject(self, error: GraphQLError) -> None:
        """Refuse the operation with ``error``, during validation, before any resolver runs.

        Two steps, and both are necessary. The placeholder document is what gives the parser
        something valid to work with — without it Strawberry raises ``MissingQueryError`` and the
        ASGI layer answers a plain-text 400 instead of a GraphQL error envelope. The appended rule
        is what actually reports the failure, in the same ``graphql.validate`` pass the cost gate
        reports through, so the response shape and the logging discipline are identical to every
        other refusal this server issues.
        """
        execution_context = self.execution_context
        execution_context.query = REJECTION_DOCUMENT
        existing = tuple(execution_context.validation_rules)
        execution_context.validation_rules = (*existing, create_rejecting_rule(error))


# =================================================================================================
# Reading the payload off the HTTP request
# =================================================================================================


async def read_request_extensions(context: Any) -> Any:  # noqa: ANN401 - any context object
    """The request's GraphQL ``extensions`` object, or ``None``. **Never raises.**

    .. rubric:: Why this reads the request rather than the execution context

    Strawberry's ``ExecutionContext`` carries the query, the variables and the operation name, but
    not the request's ``extensions`` — ``GraphQLRequestData`` does not thread them through to
    ``schema.execute``. The payload therefore has to come from the transport, and
    :class:`~src.graphql.context.Context` already carries the request because it subclasses
    ``BaseContext``.

    .. rubric:: THE BODY IS NOT RE-PARSED ON THE COMMON PATH, AND THAT IS THE POINT OF THE MARKER

    This runs on **every** operation, including the thousands the C14 load harness sends without any
    APQ payload at all. ``await request.body()`` returns Starlette's already-cached bytes (Strawberry
    read them through the same method, which memoises into ``request._body``), so the steady-state
    cost here is a substring search over a small ``bytes`` object — not a second ``json.loads`` of
    every request body in the system. Only a body that actually mentions ``persistedQuery`` is
    parsed.

    The marker can produce a false negative for a body that escapes the key
    (``"persisted\\u0051uery"``), which no client emits and which fails **closed**: the request is
    handled as an ordinary non-APQ request rather than being mishandled as an APQ one.

    Returns ``None`` for a WebSocket connection: ``graphql-transport-ws`` carries its own
    ``payload.extensions``, which Strawberry consumes before an extension sees it, so a subscription
    always sends its document. See the module docstring.
    """
    request = getattr(context, "request", None)
    if request is None:
        return None

    scope = getattr(request, "scope", None)
    if isinstance(scope, Mapping) and scope.get("type") != "http":
        return None

    try:
        if str(getattr(request, "method", "")).upper() == "GET":
            # The GET spelling of the protocol: `?extensions={"persistedQuery":{...}}`. This is the
            # form that makes an APQ request cacheable by a CDN, which is half of why APQ exists.
            raw = request.query_params.get("extensions")
            if not raw:
                return None
            body: Any = {"extensions": json.loads(raw)}
        else:
            payload = await request.body()
            if not payload or _APQ_BODY_MARKER not in payload:
                return None
            body = json.loads(payload)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - an unreadable body is "no APQ payload", never a failure
        logger.debug("could not read persisted query extensions off the request", exc_info=True)
        return None

    return body.get("extensions") if isinstance(body, Mapping) else None


__all__ = [
    "APQ_PROTOCOL_VERSION",
    "DEFAULT_PERSISTED_QUERY_NAMESPACE",
    "MAX_PERSISTED_DOCUMENT_BYTES",
    "PERSISTED_QUERY_FORMAT_VERSION",
    "PERSISTED_QUERY_NOT_FOUND_MESSAGE",
    "PERSISTED_QUERY_NOT_SUPPORTED_MESSAGE",
    "REJECTION_DOCUMENT",
    "PersistedQueries",
    "PersistedQueryAction",
    "PersistedQueryPlan",
    "PersistedQueryStats",
    "PersistedQueryStore",
    "compute_query_hash",
    "create_persisted_query_store",
    "create_rejecting_rule",
    "hash_matches_document",
    "normalise_hash",
    "plan_persisted_query",
    "read_persisted_query_payload",
    "read_request_extensions",
]
