"""Redis cache-aside for the read resolvers — spec §2 items 30-31, §5 "caching measurably reduces
database load".

Adapted from ``faceted-log-search-engine/src/storage/redis_cache.py`` rather than invented: the
sorted-JSON hash key, the never-raises ``get_or_compute`` and the single-flight future map are all
that module's, and all three exist because of a failure it met first.

.. rubric:: THE FOUR PROPERTIES. Everything in this module is one of them.

1. **A key is a deterministic hash of everything that changes the answer.** Two logically identical
   filter sets must produce the same key whatever order their dicts were built in; two that differ
   *at all* must not. The failure mode of getting this wrong is not a slow server, it is one
   query's rows served as another query's answer — see :func:`make_cache_key`.
2. **A hit reconstructs fully typed objects and touches no database.** Not a dict, not a partial
   entry: a :class:`~src.graphql.types.LogEntry` that resolves ``relatedLogs`` exactly as a
   database-loaded one does. The integration suite proves the "no database" half by counting the
   statements PostgreSQL actually received, which is the only assertion that can fail for the right
   reason (the response JSON is identical either way).
3. **The cache can never fail a request.** :meth:`ResultCache.get_or_compute` does not raise. Every
   Redis error — unreachable, timed out, OOM, a blob written by an older build — falls through to
   the source and bumps a counter. A cache that can take the API down is worse than no cache, and
   this one is a §2 nice-to-have sitting in front of every read the service serves.
4. **N concurrent misses on one key compute once.** Without the single-flight map, a popular key
   expiring under load means every in-flight request runs the same query at the same instant —
   the cache stampede, which turns the moment a cache *helps* least into the moment it hurts most.

.. rubric:: STALENESS IS BOUNDED BY THE TTL. THIS IS NOT WRITE-THROUGH, AND THAT IS THE DESIGN.

``createLog`` does **not** invalidate anything. A cached ``logs`` or ``logStats`` result therefore
keeps answering without the new row for up to ``CACHE_TTL_SECONDS`` (30) or
``AGG_CACHE_TTL_SECONDS`` (60) respectively. That is the spec's own choice — item 30 asks for "a
short TTL (article uses 30s)" and says nothing about invalidation — and it is the right one here:

* **Correct invalidation is not cheap.** A write invalidates every cached key whose *filters* the
  new row satisfies. Since keys are opaque hashes of arbitrary filter combinations, finding them
  means either a reverse index from filter dimensions to keys (a second consistency problem, in a
  store that can be down) or a `SCAN` of the keyspace per write (which costs more than the query
  it is saving). Both are elaborate machinery in service of an inconsistency window that is already
  30 seconds long.
* **The window is bounded and uniform.** "Up to 30s stale" is a property a dashboard can be built
  against. "Usually fresh, except for the filter shapes the invalidator forgot" is not.
* **Live data has a different route.** The dashboard's real-time view is
  ``Subscription.logStream`` (C6), which is fed by the broker directly from the mutation and is
  never cached. Freshness is a *streaming* concern here, not a caching one.

The one thing this module refuses to do is hide that. The behaviour is asserted by name in
``tests/integration/test_cache.py`` (a ``createLog`` is invisible to an already-cached query) and
stated in the README, so nobody reads a stale response as a bug.

.. rubric:: A note on what a coalesced caller receives

Single-flight means N callers share one computed value — the *same Python objects*, not N copies.
That is safe because every consumer treats them as immutable: Strawberry serialises them,
``related_logs`` only reads ``self.trace_id`` and ``self.id``, and nothing in the project mutates a
published type after construction. It also means a coalesced caller shares the leader's fate: if the
leader's query raises, every waiter sees that exception. That is the correct trade — the alternative
is N callers each retrying the query that just failed, which is the stampede again with an error
budget attached.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

from src.config import Settings
from src.db.repository import (
    FunnelBucket,
    LogQuery,
    LogStatsResult,
    OrderEventQuery,
    OrderStatusBucket,
    PaymentEventQuery,
    PaymentOutcomeBucket,
    ServiceLevelCount,
    as_utc,
    clamp_limit,
)
from src.graphql.types import LogEntry, from_wire_timestamp, to_wire_timestamp

if TYPE_CHECKING:  # pragma: no cover - annotations only
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Bumped when the **stored value** shape or the **key payload** shape changes in a way that makes
#: an old blob wrong rather than merely absent. It participates in both the key prefix and the
#: hashed payload, so bumping it strands every existing entry behind a key nothing will ever ask
#: for — they expire on their own TTL and are never read. That is the whole migration story for a
#: cache, and it is why the alternative (teaching a new build to read an old blob) is not worth
#: writing: the data is derived, reproducible, and at most 60 seconds from being regenerated.
CACHE_FORMAT_VERSION = 1

#: Key prefix. Namespaced by project because one Redis instance serves this stack's cache, its
#: persisted-query documents (C9) and its subscription channel (C6) — and, in this monorepo, may
#: one day serve a sibling project on the same logical database.
DEFAULT_CACHE_NAMESPACE = "graphql-log-query:cache"

#: The cached query kinds. A kind is part of the key **and** part of the hashed payload, so
#: ``logs`` and ``logStats`` cannot collide even when handed byte-identical filters — which they
#: routinely are, since both are driven by the same time window.
KIND_LOGS = "logs"
KIND_LOG_STATS = "logStats"

#: C11's three e-commerce aggregates. Named after the published field so a human running ``KEYS``
#: against Redis can see which resolver a key belongs to without decoding anything.
KIND_ORDER_STATUS_AGG = "orderStatusDistribution"
KIND_ORDER_FUNNEL_AGG = "orderFunnel"
KIND_PAYMENT_OUTCOME_AGG = "paymentOutcomeBreakdown"

#: Which setting supplies the TTL for each kind — spec §3 Feature Area D asks for a TTL policy
#: defined *per aggregation*, so the policy is a table rather than a constant. Every TTL in the
#: system is visible here, in one place, next to the kind it governs.
#:
#: .. rubric:: THE POLICY, and the property of each aggregate that chose its number
#:
#: The numbers are not a scale from "important" to "unimportant". Each one is the answer to *how
#: wrong can this be after N seconds*, which is a different question per aggregate:
#:
#: ``logs`` — 30s (``CACHE_TTL_SECONDS``)
#:     Rows, not an aggregate. The spec's own §7 value. A new log line is simply absent.
#: ``orderStatusDistribution`` — 20s (``ORDER_STATUS_AGG_TTL_SECONDS``), the shortest
#:     REDISTRIBUTIVE. One new order event does not increment a bucket, it MOVES an order from one
#:     bucket to another, so a stale answer is wrong in two places at once. It is also the panel an
#:     operator stares at during an incident.
#: ``logStats`` / ``paymentOutcomeBreakdown`` — 60s (``AGG_CACHE_TTL_SECONDS``)
#:     ADDITIVE and expensive. A write moves one count by one out of thousands, and the read is a
#:     ``GROUP BY``-class scan over the whole window. The two share a setting because they share
#:     that shape exactly; a second knob with the same value would be two things to keep equal.
#: ``orderFunnel`` — 300s (``FUNNEL_AGG_TTL_SECONDS``), the longest
#:     MONOTONIC. A status once reached is never un-reached, so this can only grow and a stale read
#:     can only *undercount* — never contradict itself. It is also the most expensive of the three
#:     (a ``COUNT(DISTINCT ...)`` per group), so it is where a long TTL buys the most.
#:
#: **There is no invalidation, for any of them.** Spec §3 Feature Area D asks for "an invalidation
#: **or** TTL policy"; this is the TTL half, argued in full in this module's docstring. The short
#: version: keys are opaque hashes of arbitrary filter sets, so invalidating "every key this write
#: affects" needs a reverse index or a keyspace scan per write — machinery that costs more than the
#: query it saves, guarding a window that is already bounded and uniform.
TTL_POLICY: Mapping[str, str] = {
    KIND_LOGS: "cache_ttl_seconds",
    KIND_LOG_STATS: "agg_cache_ttl_seconds",
    KIND_ORDER_STATUS_AGG: "order_status_agg_ttl_seconds",
    KIND_ORDER_FUNNEL_AGG: "funnel_agg_ttl_seconds",
    KIND_PAYMENT_OUTCOME_AGG: "agg_cache_ttl_seconds",
}

#: The TTL an unknown kind gets. The short one, deliberately: a kind that reached
#: :meth:`ResultCache.ttl_for` without a policy row is a caller this table has not been told about,
#: and the conservative answer is the one that goes stale soonest.
DEFAULT_TTL_SETTING = "cache_ttl_seconds"


# =================================================================================================
# Keys
#
# THE FAILURE THIS SECTION EXISTS TO PREVENT, stated plainly: a key that omits something the query
# depends on serves one question's rows as another question's answer. Nothing errors. The response
# is well formed, the row shapes are right, the counts are plausible, and a client filtering on
# `service: "auth-svc"` receives `payments-svc` rows for the next thirty seconds. That is strictly
# worse than no cache and strictly worse than a crash, because it is invisible.
#
# So the payload builders below take a `LogQuery` — the object the repository actually executes —
# rather than the GraphQL input, and enumerate its fields explicitly. A `**dataclasses.asdict()`
# would be shorter and would silently start including any field a later commit adds, which sounds
# like a feature until the added field is one that does NOT change the answer and every key in the
# system moves for nothing.
# =================================================================================================


def _wire_instant(value: Optional[datetime]) -> Optional[str]:
    """Normalise a filter bound to a UTC ISO-8601 string; ``None`` passes through.

    Two normalisations in one line, and both are load-bearing for key equality:

    * :func:`~src.db.repository.as_utc` is the same function the WHERE clause is built through, so
      the key is derived from precisely the instant the query will be run with. A naive value and
      the aware UTC value it is treated as therefore hash the same, because they *are* the same
      query.
    * ``.isoformat()`` rather than ``default=str`` on the dump, so the rendering is pinned here
      rather than inherited from whatever ``str(datetime)`` does today (it emits a space instead of
      the ``T``, which would work fine and would change every key in the system if it were ever
      corrected).
    """
    normalised = as_utc(value)
    return None if normalised is None else to_wire_timestamp(normalised)


def _query_payload(query: LogQuery, *, limit: Optional[int]) -> dict[str, Any]:
    """The filter set as a plain, JSON-stable mapping. ``limit`` is passed in, never read here.

    Every predicate :func:`~src.db.repository.build_predicates` can produce is represented, so two
    payloads are equal exactly when the two WHERE clauses are. ``limit`` is a parameter rather than
    a field because it means something for ``logs`` (the row cap, part of the answer) and nothing at
    all for an aggregate (which ignores it — see :func:`~src.db.repository.build_count_select`), and
    a payload that carried ``limit: null`` for stats would put a field in the hash that can never
    vary.
    """
    return {
        "service": query.service,
        "level": query.level,
        "start_time": _wire_instant(query.start_time),
        "end_time": _wire_instant(query.end_time),
        "search_text": query.search_text,
        "limit": limit,
    }


def logs_key_payload(query: LogQuery, settings: Settings) -> dict[str, Any]:
    """The key payload for ``Query.logs``: every filter, plus the **resolved, clamped** limit.

    The limit is clamped here — through the same :func:`~src.db.repository.clamp_limit` the
    statement builder uses — rather than carried raw, and the difference is observable in both
    directions:

    * ``limit: 5`` and ``limit: 10`` produce *different* keys, because they produce different
      answers. A payload that omitted the limit entirely would serve the 5-row answer to the 10-row
      request, which is the collision this whole section is about.
    * ``limit: 10_000`` and ``limit: 50_000`` produce the *same* key, because both are clamped to
      ``MAX_QUERY_LIMIT`` and therefore both return the same rows. Hashing the raw value would
      write two keys holding one identical result — correct, and a straight waste of the cache.

    Clamping also means the key follows the configuration: an operator who lowers
    ``MAX_QUERY_LIMIT`` gets new keys rather than the old, longer results.
    """
    return _query_payload(query, limit=clamp_limit(query.limit, settings))


def log_stats_key_payload(query: LogQuery) -> dict[str, Any]:
    """The key payload for ``Query.logStats``: the same filters, no limit.

    ``Query.logStats`` publishes only ``startTime``/``endTime`` today, so ``service``, ``level`` and
    ``search_text`` are always ``None`` in this payload. They are included anyway because C11 widens
    the aggregate to the full filter set: including them now means the key for "no service filter"
    is stable across that change, and a service-filtered aggregate lands on a genuinely new key
    instead of on one an unfiltered result is already sitting in.
    """
    return _query_payload(query, limit=None)


def order_event_key_payload(query: OrderEventQuery) -> dict[str, Any]:
    """The key payload for the two order aggregates: every filter, **no limit**.

    Enumerated field by field rather than ``dataclasses.asdict``-ed, for the reason this section's
    header gives: a payload that silently gains whatever field a later commit adds moves every key
    in the system the first time somebody adds one that does not change the answer.

    ``limit`` is absent because an aggregate ignores it — see
    :func:`~src.db.repository.build_order_funnel_select`. Including it would put a field in the hash
    that can never change the value, so two clients asking the same question with different page
    sizes would compute the same numbers twice and store them under two keys.

    The **kind** is what separates ``orderStatusDistribution`` from ``orderFunnel``: the two are
    driven by byte-identical filters and return different numbers, so a payload alone would collide
    them. :func:`make_cache_key` hashes the kind *inside* the document for exactly this case.
    """
    return {
        "service": query.service,
        "level": query.level,
        "start_time": _wire_instant(query.start_time),
        "end_time": _wire_instant(query.end_time),
        "trace_id": query.trace_id,
        "order_id": query.order_id,
        "user_id": query.user_id,
        "status": query.status,
        "search_text": query.search_text,
    }


def payment_event_key_payload(query: PaymentEventQuery) -> dict[str, Any]:
    """The key payload for ``paymentOutcomeBreakdown``: every filter, **no limit**."""
    return {
        "service": query.service,
        "level": query.level,
        "start_time": _wire_instant(query.start_time),
        "end_time": _wire_instant(query.end_time),
        "trace_id": query.trace_id,
        "order_id": query.order_id,
        "method": query.method,
        "outcome": query.outcome,
        "search_text": query.search_text,
    }


def make_cache_key(
    kind: str,
    payload: Mapping[str, Any],
    *,
    namespace: str = DEFAULT_CACHE_NAMESPACE,
    version: int = CACHE_FORMAT_VERSION,
) -> str:
    """Build the deterministic key for ``payload`` under ``kind``.

    ``sha256(json.dumps(..., sort_keys=True))``. ``sort_keys`` is the entire determinism guarantee:
    Python dicts preserve insertion order and two call sites building the same logical filter set in
    different orders would otherwise hash differently, producing two keys, two misses and a cache
    that quietly never hits. ``default=str`` is a backstop for a value that is not JSON-native — the
    payload builders above render datetimes themselves precisely so it never fires on a value whose
    ``str()`` is not stable.

    ``kind`` appears **twice** on purpose: in the readable prefix, so a human running ``KEYS`` can
    see what a key is for, and inside the hashed document, so the digests differ even for identical
    filters. Only the second one is load-bearing — a prefix is a naming convention, and a naming
    convention is not a collision guarantee.

    ``version`` is inside the digest too, so bumping :data:`CACHE_FORMAT_VERSION` invalidates the
    whole keyspace by construction rather than by remembering to change the prefix.

    Returns:
        ``"<namespace>:v<version>:<kind>:<64 hex chars>"``.
    """
    document = json.dumps(
        {"v": version, "kind": kind, "filters": payload},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256(document.encode("utf-8")).hexdigest()
    return f"{namespace}:v{version}:{kind}:{digest}"


# =================================================================================================
# Values
#
# What goes into Redis is JSON, and what comes back out is a fully typed object — spec §2 item 31.
# The two codecs below are the whole of that promise.
#
# `LogEntry` is encoded through `LogEntry.to_wire()` / `.from_wire()`, which is the SAME mapping
# C6's pub/sub bridge uses (it lives in src/graphql/types.py for exactly that reason). There is one
# JSON representation of a published entry in this project, not one per consumer.
#
# `logStats` caches the REPOSITORY's `LogStatsResult`, not the published `LogStats`. That is
# deliberate and it is what C2 anticipated (see that class's docstring): the cache-hit path then
# runs `LogStats.from_result(...)` — the identical projection the cache-miss path runs — so a cached
# summary and a computed one cannot differ in ordering, in the derived `services` list, or in
# anything else that projection decides. Caching the published object would have made the projection
# skippable, and a skipped projection is a second implementation waiting to happen.
# =================================================================================================


@dataclass(frozen=True)
class ValueCodec(Generic[T]):
    """How one cached kind turns into JSON and back.

    A pair of plain callables rather than a class hierarchy: there are two of them, they are pure,
    and the only thing :meth:`ResultCache.get_or_compute` needs to know is that ``decode`` raises
    when it is handed something it does not recognise. The cache treats any such failure as a miss
    and recomputes, which is what makes a format change survivable without a flush.
    """

    kind: str
    encode: Callable[[T], Any]
    decode: Callable[[Any], T]


def encode_log_entries(entries: Sequence[LogEntry]) -> dict[str, Any]:
    """Render a result list for storage. Wrapped in an object, never a bare array.

    A top-level object leaves room for the version tag, and the version tag is what lets a decoder
    reject a blob written by a build with a different idea of the shape instead of half-reading it.
    """
    return {"v": CACHE_FORMAT_VERSION, "entries": [entry.to_wire() for entry in entries]}


def decode_log_entries(payload: Any) -> list[LogEntry]:
    """Rebuild a result list. Raises on anything unexpected; the caller treats that as a miss.

    Raises:
        ValueError: If the blob is not a versioned entries object this build understands.
        KeyError: If an entry inside it is missing a required field — see
            :meth:`src.graphql.types.LogEntry.from_wire`.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"cached logs payload is {type(payload).__name__}, expected an object")
    if payload.get("v") != CACHE_FORMAT_VERSION:
        raise ValueError(f"cached logs payload has format version {payload.get('v')!r}")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("cached logs payload has no `entries` array")
    return [LogEntry.from_wire(entry) for entry in entries]


def encode_log_stats(result: LogStatsResult) -> dict[str, Any]:
    """Render an aggregate result for storage.

    The breakdown is a list of triples rather than a list of objects: it is the one part of this
    payload whose size grows with the data (distinct ``service`` x ``level`` pairs), and three keys
    repeated per bucket is bytes spent on nothing. The pairing is positional and it is decoded two
    functions down, which is the entire distance over which that has to be remembered.
    """
    return {
        "v": CACHE_FORMAT_VERSION,
        "total_logs": result.total_logs,
        "error_count": result.error_count,
        "earliest": None if result.earliest is None else to_wire_timestamp(result.earliest),
        "latest": None if result.latest is None else to_wire_timestamp(result.latest),
        "breakdown": [
            [bucket.service, bucket.level, bucket.entries] for bucket in result.breakdown
        ],
    }


def decode_log_stats(payload: Any) -> LogStatsResult:
    """Rebuild an aggregate result, timestamps aware and the breakdown back in its frozen tuple.

    ``earliest``/``latest`` are ``None`` when nothing matched — a normal answer for a quiet window,
    and distinct from "the value did not survive the round trip", which is why they are read with an
    explicit ``is None`` test rather than a truthiness one.

    Raises:
        ValueError: If the blob is not a versioned stats object this build understands.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"cached stats payload is {type(payload).__name__}, expected an object")
    if payload.get("v") != CACHE_FORMAT_VERSION:
        raise ValueError(f"cached stats payload has format version {payload.get('v')!r}")

    earliest = payload.get("earliest")
    latest = payload.get("latest")
    breakdown = payload.get("breakdown")
    if not isinstance(breakdown, list):
        raise ValueError("cached stats payload has no `breakdown` array")

    return LogStatsResult(
        total_logs=int(payload["total_logs"]),
        error_count=int(payload["error_count"]),
        earliest=None if earliest is None else from_wire_timestamp(earliest),
        latest=None if latest is None else from_wire_timestamp(latest),
        breakdown=tuple(
            ServiceLevelCount(service=service, level=level, entries=int(entries))
            for service, level, entries in breakdown
        ),
    )


def _encode_buckets(rows: Sequence[Any], fields: Sequence[str]) -> dict[str, Any]:
    """Render a tuple of frozen bucket dataclasses as a versioned object of positional rows.

    Positional rows rather than objects for the reason :func:`encode_log_stats` gives: the bucket
    list is the part of an aggregate payload whose size grows with the vocabulary, and repeating
    two to four key names per bucket is bytes spent on nothing. The field order is passed in and
    consumed by :func:`_decode_buckets` four lines away, which is the entire distance over which
    the pairing has to be remembered.
    """
    return {
        "v": CACHE_FORMAT_VERSION,
        "buckets": [[getattr(row, field) for field in fields] for row in rows],
    }


def _decode_buckets(payload: Any, kind: str) -> list[list[Any]]:  # noqa: ANN401 - any JSON value
    """Validate a versioned bucket object and return its raw rows.

    Raises:
        ValueError: If the blob is not a versioned bucket object this build understands. The caller
            treats that as a miss and recomputes, which is what makes a format change survivable
            without a flush.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"cached {kind} payload is {type(payload).__name__}, expected an object")
    if payload.get("v") != CACHE_FORMAT_VERSION:
        raise ValueError(f"cached {kind} payload has format version {payload.get('v')!r}")
    buckets = payload.get("buckets")
    if not isinstance(buckets, list):
        raise ValueError(f"cached {kind} payload has no `buckets` array")
    return buckets


def encode_order_status_distribution(rows: Sequence[OrderStatusBucket]) -> dict[str, Any]:
    """Render the current-status distribution for storage."""
    return _encode_buckets(rows, ("status", "orders"))


def decode_order_status_distribution(payload: Any) -> tuple[OrderStatusBucket, ...]:
    """Rebuild the current-status distribution. Raises on anything unexpected."""
    return tuple(
        OrderStatusBucket(status=status, orders=int(orders))
        for status, orders in _decode_buckets(payload, KIND_ORDER_STATUS_AGG)
    )


def encode_order_funnel(rows: Sequence[FunnelBucket]) -> dict[str, Any]:
    """Render the order funnel for storage."""
    return _encode_buckets(rows, ("status", "orders"))


def decode_order_funnel(payload: Any) -> tuple[FunnelBucket, ...]:
    """Rebuild the order funnel. Raises on anything unexpected."""
    return tuple(
        FunnelBucket(status=status, orders=int(orders))
        for status, orders in _decode_buckets(payload, KIND_ORDER_FUNNEL_AGG)
    )


def encode_payment_outcome_breakdown(rows: Sequence[PaymentOutcomeBucket]) -> dict[str, Any]:
    """Render the payment cross-tabulation for storage."""
    return _encode_buckets(rows, ("method", "outcome", "events", "orders"))


def decode_payment_outcome_breakdown(payload: Any) -> tuple[PaymentOutcomeBucket, ...]:
    """Rebuild the payment cross-tabulation. Raises on anything unexpected."""
    return tuple(
        PaymentOutcomeBucket(
            method=method, outcome=outcome, events=int(events), orders=int(orders)
        )
        for method, outcome, events, orders in _decode_buckets(payload, KIND_PAYMENT_OUTCOME_AGG)
    )


#: The codec for ``Query.logs``.
LOG_ENTRIES_CODEC: ValueCodec[list[LogEntry]] = ValueCodec(
    kind=KIND_LOGS, encode=encode_log_entries, decode=decode_log_entries
)

#: The codec for ``Query.logStats``.
LOG_STATS_CODEC: ValueCodec[LogStatsResult] = ValueCodec(
    kind=KIND_LOG_STATS, encode=encode_log_stats, decode=decode_log_stats
)

#: The codec for ``Query.orderStatusDistribution``.
ORDER_STATUS_AGG_CODEC: ValueCodec[tuple[OrderStatusBucket, ...]] = ValueCodec(
    kind=KIND_ORDER_STATUS_AGG,
    encode=encode_order_status_distribution,
    decode=decode_order_status_distribution,
)

#: The codec for ``Query.orderFunnel``.
ORDER_FUNNEL_AGG_CODEC: ValueCodec[tuple[FunnelBucket, ...]] = ValueCodec(
    kind=KIND_ORDER_FUNNEL_AGG, encode=encode_order_funnel, decode=decode_order_funnel
)

#: The codec for ``Query.paymentOutcomeBreakdown``.
PAYMENT_OUTCOME_AGG_CODEC: ValueCodec[tuple[PaymentOutcomeBucket, ...]] = ValueCodec(
    kind=KIND_PAYMENT_OUTCOME_AGG,
    encode=encode_payment_outcome_breakdown,
    decode=decode_payment_outcome_breakdown,
)


# =================================================================================================
# Counters
# =================================================================================================


@dataclass(frozen=True, slots=True)
class CacheStats:
    """A point-in-time snapshot of the cache's counters.

    Shaped for C9 to lift straight into Prometheus: every field is a monotonic counter except
    :attr:`enabled` and :attr:`inflight`, and the names are the metric names minus their prefix.

    ``hits + misses + bypassed`` is the number of cacheable calls the process has served. That
    invariant is why a Redis failure counts as a **miss** as well as an error: the request was
    answered from the source, which is what a miss means, and ``errors`` is an independent count of
    how badly Redis behaved while that happened. Folding the two would make the hit ratio depend on
    the outage rate and would leave the three-way sum not adding up.

    Attributes:
        enabled: Whether the cache is actually doing anything — ``CACHE_ENABLED`` **and** a client
            that could be constructed. **Gauge** (constant for the life of the process).
        hits: Calls answered from Redis, decoded successfully.
        misses: Calls answered from the source. Includes calls where Redis failed.
        errors: Redis or codec failures. Every one of them was survived: this counter moving is the
            evidence that the never-raises contract was exercised, not that a request failed.
        coalesced: Calls that were answered by another caller's in-flight computation instead of
            running their own. Directly the stampede protection's yield.
        bypassed: Calls made while the cache was disabled. Counted rather than ignored so
            ``CACHE_ENABLED=false`` is visible in the metrics as "off" rather than as "0% hit rate",
            which is what an unnoticed misconfiguration looks like.
        inflight: Keys currently being computed. **Gauge**, and a leak detector: it returns to zero
            whenever the process is idle, so a number that does not is a ``finally`` that stopped
            running.
    """

    enabled: bool
    hits: int
    misses: int
    errors: int
    coalesced: int
    bypassed: int
    inflight: int


# =================================================================================================
# The cache
# =================================================================================================


class ResultCache:
    """Cache-aside over Redis for the read resolvers. **Never raises.**

    One instance per process, built in :func:`src.main.lifespan` and reached by resolvers through
    ``info.context.cache``.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        redis_client: Optional["Redis"] = None,
        namespace: str = DEFAULT_CACHE_NAMESPACE,
        owns_client: bool = False,
    ) -> None:
        """Build a cache.

        Args:
            settings: Supplies ``CACHE_ENABLED``, ``CACHE_TTL_SECONDS`` and
                ``AGG_CACHE_TTL_SECONDS``. Carried rather than read from
                :func:`src.config.get_settings` so a test can run one operation with the cache off
                and the next with it on without touching a process-wide LRU cache.
            redis_client: The store, or ``None`` for a cache that always misses. Duck-typed on
                ``get()`` and ``setex()`` so the unit suite can drive every failure branch with a
                stub that raises rather than asserting "no exception was raised" against a healthy
                server.
            namespace: Key prefix. Overridden per test so two tests cannot answer each other's
                queries — the corpus is deterministic, so a leaked key holds a *plausible* value and
                the resulting failure would be attributed to the wrong test.
            owns_client: Whether :meth:`aclose` should close ``redis_client``. ``False`` for an
                injected one — a client the caller built is a client the caller closes, and a cache
                that closed a shared client would take the pub/sub bridge down with it.
        """
        self._settings = settings
        self._redis = redis_client
        self._namespace = namespace
        self._owns_client = owns_client
        self._configured_enabled = bool(settings.cache_enabled)

        #: In-flight computations, keyed by cache key. See :meth:`_compute_once`.
        self._inflight: dict[str, asyncio.Future[Any]] = {}

        self._hits = 0
        self._misses = 0
        self._errors = 0
        self._coalesced = 0
        self._bypassed = 0

        #: ``None`` until the first observation, so the first transition always logs. Same
        #: once-per-state-change discipline as the broker's bridge: a Redis outage under load must
        #: cost one log line, not one per request.
        self._healthy: Optional[bool] = None

    # -- identity and counters ---------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        """Is this cache doing anything at all?

        ``CACHE_ENABLED`` **and** a client. The two failures are different — an operator switch and
        a broken ``REDIS_URL`` — but the behaviour they produce is identical and there is no branch
        below that wants to tell them apart, so they collapse here rather than at four call sites.
        """
        return self._configured_enabled and self._redis is not None

    @property
    def namespace(self) -> str:
        """The key prefix this cache writes under."""
        return self._namespace

    @property
    def redis_client(self) -> Optional["Redis"]:
        """The client this cache reads through, or ``None``.

        Exposed so "one request-path pool, two consumers" is a claim a test can *check* rather than
        one a comment asserts: C9's persisted query store is handed the same object by
        :func:`src.main.lifespan`, and an integration test compares the two by identity. Read-only,
        and it goes ``None`` at :meth:`aclose` whether or not this cache owned the client.
        """
        return self._redis

    @property
    def stats(self) -> CacheStats:
        """A snapshot of every counter. See :class:`CacheStats`."""
        return CacheStats(
            enabled=self.enabled,
            hits=self._hits,
            misses=self._misses,
            errors=self._errors,
            coalesced=self._coalesced,
            bypassed=self._bypassed,
            inflight=len(self._inflight),
        )

    # -- policy ------------------------------------------------------------------------------

    def ttl_for(self, kind: str) -> int:
        """The TTL, in seconds, that ``kind`` is stored under — spec §3 Feature Area D.

        Looked up in :data:`TTL_POLICY` rather than branched on, so adding a cached aggregate is a
        row in a table instead of an ``elif`` in a method, and so every TTL in the system is
        readable in one place.
        """
        return int(getattr(self._settings, TTL_POLICY.get(kind, DEFAULT_TTL_SETTING)))

    def make_key(self, kind: str, payload: Mapping[str, Any]) -> str:
        """This cache's key for ``payload`` under ``kind``. See :func:`make_cache_key`."""
        return make_cache_key(kind, payload, namespace=self._namespace)

    # -- the cache-aside path ----------------------------------------------------------------

    async def fetch(
        self,
        kind: str,
        payload: Mapping[str, Any],
        compute: Callable[[], Awaitable[T]],
        codec: ValueCodec[T],
    ) -> T:
        """Key, TTL and cache-aside in one call — what a resolver actually wants. **Never raises.**

        The disabled check is *before* the key is built rather than inside
        :meth:`get_or_compute`, so ``CACHE_ENABLED=false`` costs a boolean and not a SHA-256 over a
        filter set nobody will look up.
        """
        if not self.enabled:
            self._bypassed += 1
            return await compute()
        return await self.get_or_compute(
            self.make_key(kind, payload), compute, ttl=self.ttl_for(kind), codec=codec
        )

    async def get_or_compute(
        self,
        key: str,
        compute: Callable[[], Awaitable[T]],
        *,
        ttl: int,
        codec: ValueCodec[T],
    ) -> T:
        """Return ``key``'s cached value, or compute it, store it and return that.

        **This method does not raise for any reason of its own.** Every Redis failure and every
        decode failure is caught, counted, logged and answered by falling through to ``compute()``.
        The only exceptions that escape are the ones ``compute()`` itself raises — which are the
        caller's own, and must propagate: swallowing a database error here would turn a failed query
        into an empty result set, which is the one thing worse than a failed query.

        Property 4 lives in :meth:`_compute_once`: concurrent callers with the same ``key`` share
        one computation, and **only the leader writes it back**. Letting all N store would send N
        identical ``SETEX``es for one value.

        Args:
            key: From :meth:`make_key`. Two different questions must never arrive here with one key
                — see :func:`make_cache_key` for what that costs.
            compute: The source of truth. Called at most once per key per in-flight window.
            ttl: Seconds to store for. **A value of 0 or less stores nothing** and is a supported
                configuration, not an error: ``SETEX`` rejects a non-positive expiry outright, so
                ``CACHE_TTL_SECONDS=0`` would otherwise mean "fail every write" rather than the
                "read-through only, never store" an operator typing it plainly intends.
            codec: How the value becomes JSON and comes back. A ``decode`` failure is a miss.
        """
        client = self._redis
        if not self._configured_enabled or client is None:
            self._bypassed += 1
            return await compute()

        raw: Any = None
        try:
            raw = await client.get(key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - property 3: Redis can never fail a request
            self._errors += 1
            self._note_health(healthy=False, operation="get", reason=exc)
            raw = None

        if raw is not None:
            try:
                value = codec.decode(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a corrupt or stale-format blob is a miss, not a 500
                self._errors += 1
                logger.warning(
                    "discarding an undecodable cache entry (key=%s, kind=%s) and recomputing",
                    key,
                    codec.kind,
                    exc_info=True,
                )
            else:
                self._hits += 1
                self._note_health(healthy=True)
                return value

        self._misses += 1
        value, is_leader = await self._compute_once(key, compute)
        if is_leader:
            await self._store(key, value, ttl=ttl, codec=codec)
        return value

    async def _store(self, key: str, value: T, *, ttl: int, codec: ValueCodec[T]) -> None:
        """Write one computed value back. **Never raises.**

        Attempted even when the preceding ``GET`` failed. That looks wasteful against a Redis that
        is entirely down — it is one more doomed round trip per request — and it is what makes the
        cache repopulate on the *first* request after Redis comes back rather than on the first
        request after that. The round trip is bounded by the client's socket timeout either way; a
        cache that stays cold after recovery is not.
        """
        client = self._redis
        if client is None or ttl <= 0:
            return
        try:
            document = json.dumps(codec.encode(value), separators=(",", ":"), default=str)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - an unserialisable value is a codec bug, not an outage
            self._errors += 1
            logger.warning(
                "could not serialise a computed value for the cache (key=%s, kind=%s) — the "
                "request is unaffected and this key will simply keep missing",
                key,
                codec.kind,
                exc_info=True,
            )
            return

        try:
            await client.setex(key, ttl, document)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - property 3
            self._errors += 1
            self._note_health(healthy=False, operation="setex", reason=exc)
            return
        self._note_health(healthy=True)

    async def _compute_once(
        self, key: str, compute: Callable[[], Awaitable[T]]
    ) -> tuple[T, bool]:
        """Run ``compute`` at most once per in-flight ``key``; report whether we were the one.

        Without this, a hot key expiring under load is a **cache stampede**: every request in flight
        misses at the same instant and every one of them runs the same query. The load harness's 100
        concurrent requests would become 100 simultaneous identical scans, each holding a connection
        from a pool of ten — the cache turning the moment it stops helping into the moment it hurts.

        Returns:
            ``(value, is_leader)``. ``is_leader`` is ``True`` for the caller that actually ran
            ``compute``, and it is what stops N coalesced callers from all writing the same value
            back to Redis.

        Raises:
            Whatever ``compute`` raised — to the leader **and** to every caller waiting on it. See
            the module docstring for why waiters share the leader's fate rather than each retrying.
        """
        inflight = self._inflight.get(key)
        if inflight is not None:
            self._coalesced += 1
            return await inflight, False

        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()
        # `setdefault` rather than a plain assignment: there is no await between the `get` above and
        # here, so under asyncio's cooperative scheduling no other task can have inserted one in the
        # meantime — but that is a property of the current code rather than of the language, and the
        # atomic form costs nothing and survives a refactor that adds an await.
        existing = self._inflight.setdefault(key, future)
        if existing is not future:
            self._coalesced += 1
            return await existing, False

        try:
            value = await compute()
        except asyncio.CancelledError:
            # Cancelling the future rather than storing the CancelledError: a waiter must see "the
            # computation you were waiting on was cancelled", and a cancelled future says that
            # without also being an exception object nobody retrieved.
            future.cancel()
            raise
        except BaseException as exc:  # noqa: BLE001 - propagated to the leader and every waiter
            if not future.done():
                future.set_exception(exc)
                # Retrieve it immediately. An uncontended failure has no waiter, and asyncio logs
                # "Future exception was never retrieved" when such a future is collected — one
                # spurious error line per failed query, describing nothing that went wrong.
                future.exception()
            raise
        else:
            if not future.done():
                future.set_result(value)
            return value, True
        finally:
            # Only if it is still ours. Defensive today (nothing replaces an in-flight entry), and
            # the reason a `compute` that raises cannot wedge a key forever — which is the failure
            # this `finally` exists for: without it, the key keeps a permanently-failed future and
            # every later caller re-raises the original error without ever retrying.
            if self._inflight.get(key) is future:
                del self._inflight[key]

    # -- lifecycle ---------------------------------------------------------------------------

    def _note_health(
        self,
        *,
        healthy: bool,
        operation: str = "",
        reason: Optional[BaseException] = None,
    ) -> None:
        """Log the cache's Redis health **once per transition**, never once per request.

        A Redis outage under the C14 load harness would otherwise print one line per request, which
        turns a degraded optional feature into an operational problem of its own. The first
        observation always logs, because :attr:`_healthy` starts at ``None``.
        """
        if self._healthy is healthy:
            return
        self._healthy = healthy
        if healthy:
            logger.info("result cache connected (namespace=%s)", self._namespace)
        else:
            logger.warning(
                "result cache degraded to read-through (namespace=%s, operation=%s): %s: %s — "
                "queries are still correct and still answered from PostgreSQL; they are simply "
                "not cached until Redis returns",
                self._namespace,
                operation,
                type(reason).__name__ if reason is not None else "unknown",
                reason,
            )

    async def aclose(self) -> None:
        """Release the Redis client **if this cache built it**. Idempotent; never raises.

        An injected client is left alone: :func:`src.main.lifespan` hands the broker a separate one
        and closing somebody else's connection pool from here would take the subscription bridge
        down as a side effect of a cache shutdown.
        """
        client, self._redis = self._redis, None
        if client is None or not self._owns_client:
            return
        try:
            # redis-py renamed the async closer to `aclose()` and kept `close()` as a deprecated
            # alias; tolerate either so a version bump is not a shutdown traceback.
            closer = getattr(client, "aclose", None) or getattr(client, "close", None)
            if closer is None:
                return
            result = closer()
            if asyncio.iscoroutine(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - teardown is best-effort
            logger.debug("failed to close the result cache's Redis client", exc_info=True)


# =================================================================================================
# Construction
# =================================================================================================


#: "This argument was not supplied", as distinct from "``None`` was supplied". Needed by
#: :func:`create_result_cache`, where omitting the client means *build and own one* while passing
#: ``None`` explicitly means *this cache gets no client at all* — two different instructions that a
#: plain ``None`` default could not tell apart.
_UNSET: Any = object()

#: How long a connect to an unreachable Redis may hold a resolver, in seconds.
CACHE_CONNECT_TIMEOUT_SECONDS = 2.0

#: How long a single Redis command may take before the cache gives up and reads through, in
#: seconds. This is a "Redis has stopped answering" detector, not a latency target — a cache lookup
#: that takes anywhere near a second has already failed at its job. The request survives either
#: way; see :meth:`ResultCache.get_or_compute`.
CACHE_SOCKET_TIMEOUT_SECONDS = 1.0


def create_cache_redis_client(settings: Settings) -> Optional["Redis"]:
    """Build the cache's Redis client, or ``None`` if one cannot be constructed. **Never raises.**

    Constructed exactly the way :func:`src.broker.create_redis_client` builds the pub/sub one — lazy
    ``Redis.from_url``, guarded local import, ``None`` on a configuration fault — with **one**
    deliberate difference: a ``socket_timeout``.

    .. rubric:: Why this is a second client and not the broker's

    ``socket_timeout`` bounds how long a *read* can hang. On the request path that is not optional:
    without it, a Redis that accepts connections and then stops answering parks a resolver on a
    socket read forever, and property 3 ("the cache can never fail a request") would hold in every
    sense except the one that matters. Thirty seconds of stale data is the *point* of this module;
    thirty seconds of a hung request is an outage.

    That timeout cannot simply be added to the shared client. The broker's pub/sub reader deliberately
    parks in ``get_message(timeout=1.0)`` waiting for the next event — an idle bridge does nothing
    else — and a connection-level read timeout would turn its steady state into a timeout, a
    reconnect and a warning per poll. So the split is not "two pools because nobody looked": it is
    one client for the **request path**, where a read that does not return is a bug, and one for the
    **long-poll path**, where a read that does not return is the design. C9's persisted-query
    documents belong to the first and reuse this one — see :func:`create_request_redis_client`,
    which is what the lifespan actually calls, and which gates on *either* feature being enabled
    rather than on the cache alone.

    ``decode_responses`` is left off, matching the broker: the payload is UTF-8 JSON either way and
    ``json.loads`` accepts ``bytes`` and ``str`` alike, so neither client's flag can surprise the
    other's consumers.
    """
    try:
        from redis.asyncio import Redis  # noqa: PLC0415 - local so a broken install degrades here

        return Redis.from_url(
            settings.redis_url,
            # Bounds how long a connect to a dead host can hold a resolver. Short, because the
            # fallback is "run the query", not "give up".
            socket_connect_timeout=CACHE_CONNECT_TIMEOUT_SECONDS,
            socket_timeout=CACHE_SOCKET_TIMEOUT_SECONDS,
            socket_keepalive=True,
        )
    except Exception:  # noqa: BLE001 - a bad REDIS_URL must not stop the server from starting
        logger.warning(
            "could not build a Redis client from REDIS_URL=%r — every query will be answered "
            "from PostgreSQL, which is correct and simply slower",
            settings.redis_url,
            exc_info=True,
        )
        return None


def create_request_redis_client(settings: Settings) -> Optional["Redis"]:
    """The **one** request-path Redis client, shared by this cache and C9's persisted query store.

    Both features live on the request path and both want the ``socket_timeout``
    :func:`create_cache_redis_client` sets, so they share a connection pool rather than opening two.
    (The broker's pub/sub client stays separate — see the long note on
    :func:`create_cache_redis_client` for why a read timeout is required here and forbidden there.)

    .. rubric:: THE GATE IS THE **OR** OF THE TWO FEATURES, AND THAT IS THE WHOLE POINT

    ``CACHE_ENABLED`` and ``PERSISTED_QUERIES_ENABLED`` are independent switches, so a client built
    only when the cache is on would silently disable persisted queries whenever an operator turned
    the cache off — which is not hypothetical: the compose ``test`` service runs with exactly that
    combination. Sharing a *pool* must not mean sharing a *gate*.

    Returns ``None`` when neither feature is on, so a stack with both disabled holds no pool, opens
    no socket, and appears nowhere in Redis' ``CLIENT LIST``. Never raises.
    """
    if not (settings.cache_enabled or settings.persisted_queries_enabled):
        return None
    return create_cache_redis_client(settings)


def create_result_cache(
    settings: Settings,
    *,
    namespace: str = DEFAULT_CACHE_NAMESPACE,
    redis_client: Any = _UNSET,  # noqa: ANN401 - a sentinel, or Optional[Redis]
) -> ResultCache:
    """Build the process's :class:`ResultCache`. Used by :func:`src.main.lifespan`.

    **No client is used when ``CACHE_ENABLED`` is false**, on either path below. Not a client that
    is never used — none at all, so a disabled cache holds no connection pool, opens no socket and
    appears nowhere in ``CLIENT LIST``. "Disabled" should be indistinguishable from "not built".

    Args:
        namespace: Key prefix. Overridden per test so two caches cannot answer each other's queries.
        redis_client: An already-built client to **borrow**, or ``None`` for a cache that always
            misses. Omitted (the sentinel) means "build and own one", which is the standalone
            behaviour every caller before C9 relied on. When a client is passed, ``owns_client`` is
            ``False``: the caller built it and the caller closes it, because C9 hands the same
            object to the persisted query store and a cache that closed it on shutdown would take
            persisted queries down as a side effect of its own teardown.
    """
    if redis_client is _UNSET:
        client = create_cache_redis_client(settings) if settings.cache_enabled else None
        return ResultCache(settings, redis_client=client, namespace=namespace, owns_client=True)

    return ResultCache(
        settings,
        redis_client=redis_client if settings.cache_enabled else None,
        namespace=namespace,
        owns_client=False,
    )


# =================================================================================================
# Resolver helpers
#
# The resolvers call these and nothing else from this module. Cache-aside is ONE wrapper at the
# call site, not a branch smeared through a resolver — `Query.logs` reads as "build the query,
# describe how to load it, ask the cache for it", which is the same sentence it read as before C7
# with one clause added.
# =================================================================================================


async def cached_logs(
    cache: Optional[ResultCache],
    query: LogQuery,
    settings: Settings,
    compute: Callable[[], Awaitable[list[LogEntry]]],
) -> list[LogEntry]:
    """``Query.logs`` through the cache, or straight through when there is no cache.

    ``cache`` is ``Optional`` because a :class:`~src.graphql.context.Context` built without one is
    a supported arrangement (the unit suite, and any application assembled without a lifespan), and
    the honest behaviour for "no cache" is "no caching" — not a crash, and not a silently
    uncacheable path that looks like a cache miss in the metrics.
    """
    if cache is None:
        return await compute()
    return await cache.fetch(
        KIND_LOGS, logs_key_payload(query, settings), compute, LOG_ENTRIES_CODEC
    )


async def cached_log_stats(
    cache: Optional[ResultCache],
    query: LogQuery,
    compute: Callable[[], Awaitable[LogStatsResult]],
) -> LogStatsResult:
    """``Query.logStats`` through the cache, or straight through when there is no cache.

    Returns the repository's :class:`~src.db.repository.LogStatsResult`, not the published type: the
    resolver projects it with :meth:`src.graphql.types.LogStats.from_result` on both paths, so the
    projection cannot differ between a hit and a miss. See the Values section above.
    """
    if cache is None:
        return await compute()
    return await cache.fetch(
        KIND_LOG_STATS, log_stats_key_payload(query), compute, LOG_STATS_CODEC
    )


async def cached_order_status_distribution(
    cache: Optional[ResultCache],
    query: OrderEventQuery,
    compute: Callable[[], Awaitable[tuple[OrderStatusBucket, ...]]],
) -> tuple[OrderStatusBucket, ...]:
    """``Query.orderStatusDistribution`` through the cache, under its own 20-second TTL.

    Returns the **repository's** buckets, not the published objects, exactly as
    :func:`cached_log_stats` does — so ``OrderStatusCount.from_buckets`` (which applies the
    lifecycle ordering) runs identically on a hit and on a miss.
    """
    if cache is None:
        return await compute()
    return await cache.fetch(
        KIND_ORDER_STATUS_AGG,
        order_event_key_payload(query),
        compute,
        ORDER_STATUS_AGG_CODEC,
    )


async def cached_order_funnel(
    cache: Optional[ResultCache],
    query: OrderEventQuery,
    compute: Callable[[], Awaitable[tuple[FunnelBucket, ...]]],
) -> tuple[FunnelBucket, ...]:
    """``Query.orderFunnel`` through the cache, under its own 300-second TTL."""
    if cache is None:
        return await compute()
    return await cache.fetch(
        KIND_ORDER_FUNNEL_AGG,
        order_event_key_payload(query),
        compute,
        ORDER_FUNNEL_AGG_CODEC,
    )


async def cached_payment_outcome_breakdown(
    cache: Optional[ResultCache],
    query: PaymentEventQuery,
    compute: Callable[[], Awaitable[tuple[PaymentOutcomeBucket, ...]]],
) -> tuple[PaymentOutcomeBucket, ...]:
    """``Query.paymentOutcomeBreakdown`` through the cache, under the shared aggregate TTL."""
    if cache is None:
        return await compute()
    return await cache.fetch(
        KIND_PAYMENT_OUTCOME_AGG,
        payment_event_key_payload(query),
        compute,
        PAYMENT_OUTCOME_AGG_CODEC,
    )


__all__ = [
    "CACHE_FORMAT_VERSION",
    "DEFAULT_CACHE_NAMESPACE",
    "KIND_LOGS",
    "KIND_LOG_STATS",
    "KIND_ORDER_FUNNEL_AGG",
    "KIND_ORDER_STATUS_AGG",
    "KIND_PAYMENT_OUTCOME_AGG",
    "LOG_ENTRIES_CODEC",
    "LOG_STATS_CODEC",
    "ORDER_FUNNEL_AGG_CODEC",
    "ORDER_STATUS_AGG_CODEC",
    "PAYMENT_OUTCOME_AGG_CODEC",
    "TTL_POLICY",
    "CacheStats",
    "ResultCache",
    "ValueCodec",
    "cached_log_stats",
    "cached_logs",
    "cached_order_funnel",
    "cached_order_status_distribution",
    "cached_payment_outcome_breakdown",
    "create_cache_redis_client",
    "create_request_redis_client",
    "create_result_cache",
    "decode_log_entries",
    "decode_log_stats",
    "decode_order_funnel",
    "decode_order_status_distribution",
    "decode_payment_outcome_breakdown",
    "encode_log_entries",
    "encode_log_stats",
    "encode_order_funnel",
    "encode_order_status_distribution",
    "encode_payment_outcome_breakdown",
    "log_stats_key_payload",
    "logs_key_payload",
    "make_cache_key",
    "order_event_key_payload",
    "payment_event_key_payload",
]
