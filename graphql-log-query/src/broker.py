"""The subscription fan-out: bounded per-subscriber queues, plus a Redis pub/sub bridge.

This module is the whole of ``Subscription.logStream``'s machinery except the resolver itself
(:mod:`src.graphql.subscription`). It is adapted from ``log-query-api-rest/src/store.py``'s SSE
fan-out rather than invented — that code has been in service long enough to have had its edges
found, and the edges are all here: the ``maxsize=0`` trap, the drop-instead-of-block policy, the
terminal sentinel, the idempotent release, the shutdown sweep.

.. rubric:: THE THREE INVARIANTS. Everything below exists to keep one of them.

1. **A subscriber is a bounded queue, never an unbounded one.** ``asyncio.Queue(maxsize=N)``, with
   ``N >= 1`` enforced at construction — see :func:`_check_queue_size` for why ``0`` is the single
   most dangerous number this module can be handed.
2. **Overflow drops the subscriber. It never blocks the writer and never grows the buffer.**
   :meth:`LogBroker.publish` uses ``put_nowait`` and treats ``QueueFull`` as "this consumer is
   gone". Blocking would let one stalled WebSocket reader stall ``createLog`` for everybody;
   buffering more would hand that reader the process's memory.
3. **Fan-out can never break the write path.** Every per-subscriber step runs inside its own
   ``try``, and any failure removes *that* subscriber and nothing else. ``createLog`` returns the
   created entry even if every subscriber in the process is broken, because the row did commit —
   which is what the response claims.

.. rubric:: ``publish`` is ``async def`` and contains NO await points. Both halves are deliberate.

It is ``async`` because the call site reads ``await broker.publish(entry)`` and because C12's order
stream may one day need a real await there. It contains no suspension point *today*: the local
fan-out is a loop of ``put_nowait``, and the Redis hop is handed to a background task rather than
awaited. Awaiting a coroutine that never suspends does not yield to the event loop, so "publish
never blocks" is a property of the code rather than a hope about how fast Redis is — which matters
precisely when Redis is *not* fast, i.e. when it is down and every await would sit on a connect
timeout inside a mutation resolver.

.. rubric:: The event-loop constraint, stated plainly

``asyncio.Queue`` is **not thread-safe**: ``put_nowait`` wakes a parked getter by resolving a
future, and resolving a future from outside its loop's thread is undefined behaviour. That is safe
here because every publisher is on the loop — the ``createLog`` resolver, and the Redis reader task
this module owns. If an ingest thread is ever added, this is the line that changes (to
``loop.call_soon_threadsafe(queue.put_nowait, entry)`` with the loop captured at subscribe time)
and nothing else does. The registry itself is guarded by a :class:`threading.Lock` anyway, so
:meth:`LogBroker.subscriber_count` and the C9 metrics scrape can read it from anywhere.

.. rubric:: Filtering happens HERE, before the enqueue — not in the resolver

Spec §2 item 26 requires server-side filtering "before yielding to the client", which a filter in
the resolver would technically satisfy. Filtering at *enqueue* time is strictly stronger and is the
reason it lives in the broker: a subscriber watching one quiet service must not have its queue
filled — and itself dropped — by a firehose on a different service it never asked for. With the
filter in the resolver, back-pressure would be shared by every subscriber on the process; with the
filter here, each subscriber's queue depth reflects only its own traffic.

.. rubric:: The Redis bridge — spec §4 bonus, cross-worker fan-out

In-process fan-out serves subscribers on *this* worker. Under ``uvicorn --workers N`` a mutation
handled by worker 1 would be invisible to a dashboard whose socket landed on worker 2, and the
symptom is the worst kind: the stream works perfectly in development and drops most events in
production. So every publish also goes out as a ``PUBLISH`` on
:attr:`~src.config.Settings.subscription_channel`, and a background reader ``SUBSCRIBE``s to the
same channel and re-injects what it receives into local fan-out.

Two things make that safe:

* **Own-echo suppression.** Redis delivers a published message to *every* subscriber including the
  publisher, so without a per-process publisher id embedded in the payload every local subscriber
  would see each event twice — once from local fan-out and once off the wire.
* **Degradation, never failure.** A broker with no Redis client, an unreachable Redis, a Redis that
  drops the connection mid-stream: all of them degrade to in-process fan-out. The reader retries
  with exponential backoff, publish errors are counted and swallowed, and the state transition is
  logged **once** rather than once per event — a Redis outage should cost one line, not one line
  per log entry the system ingests.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

import strawberry

from src.config import Settings
from src.graphql.enums import LogLevel
from src.graphql.types import LogEntry

if TYPE_CHECKING:  # pragma: no cover - annotations only
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)

# =================================================================================================
# The wire format
#
# One envelope, one version field, one kind discriminator. C12 adds `orderStatusStream` by adding a
# kind rather than by changing the shape, which is the whole reason the discriminator exists before
# there is a second kind to discriminate.
# =================================================================================================

#: Bumped only for a **breaking** envelope change. A reader that meets a version it does not know
#: drops the message rather than guessing: during a rolling deploy two versions of this process are
#: subscribed to the same channel, and a guess there means one worker fabricating entries.
EVENT_FORMAT_VERSION = 1

#: The only kind C6 publishes. C12's order-status events get their own.
EVENT_KIND_LOG = "log"


@dataclass(frozen=True, slots=True)
class RemoteEvent:
    """A decoded envelope: who published it, and the entry it carried."""

    origin: str
    entry: LogEntry


def _encode_timestamp(value: datetime) -> str:
    """Render a stored ``timestamp`` for the wire, in UTC, with its offset attached.

    ``log_entries.timestamp`` is ``TIMESTAMP WITH TIME ZONE`` and asyncpg hands back aware values,
    so the ``tzinfo is None`` branch cannot fire on the production path. It is here because
    ``createLog`` accepts a client-supplied timestamp and a naive value that reached this far would
    otherwise be encoded without an offset and decoded as naive — a value that compares unequal to
    every aware datetime in the system, including the one the mutation returned.

    Normalising to UTC loses the *original* offset and keeps the *instant*. That is the right
    trade for this system: the column stores instants, ``datetime`` equality compares instants, and
    a subscriber has no use for the wall-clock zone a writer happened to be in.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _decode_timestamp(raw: str) -> datetime:
    """Parse what :func:`_encode_timestamp` wrote back into an aware UTC ``datetime``."""
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:  # pragma: no cover - only reachable via a hand-written payload
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def encode_event(entry: LogEntry, *, origin: str) -> str:
    """Serialise one entry into the JSON envelope published on the pub/sub channel.

    Args:
        entry: The committed entry. Every field of the published type is carried, so a subscriber
            on another worker receives exactly what a local one does — the resolver reconstructs
            a :class:`~src.graphql.types.LogEntry` and never consults the database to fill a gap.
        origin: The publishing process's id. Read back by :meth:`LogBroker._ingest_remote` to
            suppress this process's own echo.

    Returns:
        Compact JSON. ``metadata`` round-trips as ``null`` when it is absent and as an object when
        it is present; the two are distinguishable on the wire even though PostgreSQL's SQL ``NULL``
        and the JSONB scalar ``'null'`` both arrive in Python as ``None`` (see the ``none_as_null``
        note on :class:`src.db.models.LogEntryORM` — by the time an entry reaches here that
        distinction has already collapsed, and collapsing it identically on both sides is what makes
        the round trip lossless *as observed through the schema*, which is the only observation a
        client can make).
    """
    payload = {
        "v": EVENT_FORMAT_VERSION,
        "kind": EVENT_KIND_LOG,
        "origin": origin,
        "entry": {
            "id": str(entry.id),
            "timestamp": _encode_timestamp(entry.timestamp),
            "service": entry.service,
            "level": entry.level.value,
            "message": entry.message,
            "metadata": entry.metadata,
            "trace_id": entry.trace_id,
        },
    }
    # `separators` because this is a hot path in aggregate (one document per ingested log line) and
    # the default separators add a space per key for a machine-only payload.
    return json.dumps(payload, separators=(",", ":"))


def decode_event(raw: str | bytes | bytearray | memoryview | None) -> Optional[RemoteEvent]:
    """Parse a channel message back into a :class:`RemoteEvent`, or ``None`` if it is not one.

    **Never raises.** Everything this function is handed came off a network channel that any
    process with the Redis credentials can write to, and a malformed message must cost one debug
    line rather than the reader task. The rejected cases, each returning ``None``:

    * not valid JSON, or not a JSON object;
    * an envelope version this build does not know (see :data:`EVENT_FORMAT_VERSION`);
    * a kind this build does not handle (a C12 order event reaching a C6 binary during a rolling
      deploy is exactly this case, and dropping it beats mis-decoding it);
    * a missing required field, or a ``level`` outside :class:`~src.graphql.enums.LogLevel`.
    """
    if raw is None:
        return None
    try:
        if isinstance(raw, (bytes, bytearray, memoryview)):
            raw = bytes(raw).decode("utf-8")
        payload = json.loads(raw)
    except Exception:  # noqa: BLE001 - see the docstring: a bad message must not kill the reader
        logger.debug("dropping an undecodable subscription message", exc_info=True)
        return None

    if not isinstance(payload, dict):
        return None
    if payload.get("v") != EVENT_FORMAT_VERSION:
        return None
    if payload.get("kind") != EVENT_KIND_LOG:
        return None

    origin = payload.get("origin")
    body = payload.get("entry")
    if not isinstance(origin, str) or not isinstance(body, dict):
        return None

    try:
        entry = LogEntry(
            id=strawberry.ID(str(body["id"])),
            timestamp=_decode_timestamp(body["timestamp"]),
            service=body["service"],
            level=LogLevel(body["level"]),
            message=body["message"],
            metadata=body.get("metadata"),
            trace_id=body.get("trace_id"),
        )
    except Exception:  # noqa: BLE001 - a missing key, a bad level, an unparseable timestamp
        logger.debug("dropping a malformed subscription message", exc_info=True)
        return None

    return RemoteEvent(origin=origin, entry=entry)


# =================================================================================================
# Filters
# =================================================================================================


@dataclass(frozen=True)
class SubscriptionFilter:
    """What one subscriber is watching. Both fields optional; supplied fields are AND-composed.

    Evaluated in :meth:`LogBroker.publish`, once per live subscriber per published entry, **before
    the enqueue** — see the module docstring for why that is stronger than filtering in the
    resolver.

    ``level`` is coerced from a string in ``__post_init__`` rather than trusted, because the failure
    mode of not coercing is silent: ``LogLevel.ERROR != "ERROR"`` is ``True``, so a filter holding
    a raw string would match nothing at all and present as "the subscription is quiet".
    """

    service: Optional[str] = None
    level: Optional[LogLevel] = None

    def __post_init__(self) -> None:
        if self.level is not None and not isinstance(self.level, LogLevel):
            # frozen dataclass: normalisation goes through object.__setattr__ or nowhere.
            object.__setattr__(self, "level", LogLevel(self.level))

    @property
    def matches_everything(self) -> bool:
        """True when no field is constrained — the unfiltered ``logStream`` subscription."""
        return self.service is None and self.level is None

    def matches(self, entry: LogEntry) -> bool:
        """Does ``entry`` satisfy every constraint this filter carries?"""
        if self.service is not None and entry.service != self.service:
            return False
        if self.level is not None and entry.level != self.level:
            return False
        return True


# =================================================================================================
# Subscribers
# =================================================================================================


class SubscriptionLimitExceeded(RuntimeError):
    """One WebSocket connection is already at ``MAX_SUBSCRIPTIONS_PER_CONNECTION``.

    A plain ``RuntimeError`` and not a ``GraphQLError``, deliberately: this module knows about
    queues and nothing about GraphQL, and :mod:`src.graphql.subscription` translates it into the
    typed :class:`~src.graphql.errors.SubscriptionLimitError` the client sees. That is the same
    split :mod:`src.graphql.cursor` uses for a bad cursor, and it is what keeps the broker
    unit-testable without a schema.
    """

    def __init__(self, held: int, limit: int) -> None:
        self.held = held
        self.limit = limit
        super().__init__(
            f"this connection already holds {held} subscription(s), and the limit is {limit}"
        )


def _check_queue_size(queue_size: int) -> int:
    """Refuse a queue size below 1, because ``asyncio.Queue`` reads ``0`` as *unbounded*.

    This is the one number in the subsystem where the dangerous value looks like the safe one.
    Somebody tightening back-pressure reaches for the smallest number they can type, and
    ``asyncio.Queue(maxsize=0)`` removes the bound entirely — so the "tightest" setting is the only
    one that lets a single stalled WebSocket reader consume the process's memory without limit.

    :class:`~src.config.Settings` refuses it at startup for the same reason; this refuses it at the
    call site, because a test or a future caller can pass a size that never went through Settings.

    Raises:
        ValueError: If ``queue_size`` is less than 1.
    """
    if queue_size < 1:
        raise ValueError(
            f"queue_size must be >= 1 (0 means UNBOUNDED to asyncio.Queue, which removes the "
            f"back-pressure bound instead of tightening it), got {queue_size}"
        )
    return queue_size


class Subscriber:
    """One live subscription: a bounded queue, the filter it watches, and its lifecycle flags.

    Not a dataclass — :attr:`released` and :attr:`dropped` mutate, and the object is used as a dict
    key, which needs identity hashing (an ``eq=True`` dataclass would break it). ``__slots__``
    keeps it cheap enough that ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` is the only thing bounding how
    many exist.

    Attributes:
        queue: Bounded ``asyncio.Queue``. Holds :class:`~src.graphql.types.LogEntry` values plus at
            most one ``None`` **terminal sentinel** meaning "this subscription is over".
        filter: Evaluated by the publisher, on the publisher's task — which is why a filter that
            raises must cost only this subscriber.
        connection: The object identifying the WebSocket connection this subscription is
            multiplexed on (the GraphQL :class:`~src.graphql.context.Context`, which Strawberry
            creates exactly once per socket). ``None`` for a subscriber with no connection scope,
            which is therefore not subject to the per-connection cap.
        released: Set exactly once, by :meth:`LogBroker.unsubscribe`. **This flag is the whole
            idempotency mechanism** — see that method for why idempotence is a correctness property
            here rather than a nicety.
        dropped: True when the subscription ended because the consumer could not keep up, as
            opposed to disconnecting or being closed at shutdown. The resolver turns it into a
            ``SLOW_CONSUMER`` GraphQL error, so a client learns it was cut off instead of silently
            believing it saw everything.
    """

    __slots__ = ("connection", "dropped", "filter", "queue", "released")

    def __init__(
        self,
        *,
        flt: SubscriptionFilter,
        queue_size: int,
        connection: Optional[Any] = None,
    ) -> None:
        self.filter = flt
        self.connection = connection
        self.queue: asyncio.Queue[Optional[LogEntry]] = asyncio.Queue(
            maxsize=_check_queue_size(queue_size)
        )
        self.released = False
        self.dropped = False

    @property
    def queued(self) -> int:
        """How many items are waiting to be read. Diagnostics and back-pressure assertions."""
        return self.queue.qsize()

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"Subscriber(filter={self.filter!r}, queued={self.queue.qsize()}, "
            f"released={self.released}, dropped={self.dropped})"
        )


@dataclass(frozen=True, slots=True)
class BrokerStats:
    """A point-in-time snapshot of the broker's counters.

    Shaped for C9 to lift straight into Prometheus without rework: every field is a monotonic
    counter except :attr:`active_subscribers`, which is a gauge, and the names are the metric names
    minus their prefix.

    Attributes:
        active_subscribers: Live subscriptions in this process. **Gauge.**
        published_total: Entries handed to :meth:`LogBroker.publish` by this process.
        delivered_total: Successful enqueues. One published entry matching three subscribers counts
            as three, so ``delivered / published`` is the average fan-out — on a single instance.
            Entries arriving over the Redis bridge are counted here too (they are real deliveries
            to real local subscribers) but were never published *here*, so with the bridge live
            that ratio reads high; ``remote_received_total`` is the term that accounts for it.
        dropped_total: Subscribers terminated because their queue was full — the number that says
            whether ``SUBSCRIPTION_QUEUE_MAXSIZE`` is right for the deployment.
        remote_published_total: Envelopes successfully ``PUBLISH``ed to Redis.
        remote_received_total: Messages read off the channel, including this process's own echo.
        remote_suppressed_total: Of those, the ones discarded as our own echo. In a single-worker
            deployment this should track ``remote_published_total`` almost exactly; a large gap
            means the publisher id is not doing its job.
        remote_invalid_total: Events that did not cross the bridge because of their **content**
            rather than the transport — a message that could not be decoded (see
            :func:`decode_event`) or an entry that could not be encoded. Kept separate from
            ``redis_errors_total`` because the two point an operator at different systems.
        redis_errors_total: Failed publishes plus reader-loop failures — i.e. **transport**
            problems. This is the one that means "look at Redis".
    """

    active_subscribers: int
    published_total: int
    delivered_total: int
    dropped_total: int
    remote_published_total: int
    remote_received_total: int
    remote_suppressed_total: int
    remote_invalid_total: int
    redis_errors_total: int


# =================================================================================================
# The broker
# =================================================================================================


class LogBroker:
    """In-process fan-out over bounded queues, bridged across workers by Redis pub/sub.

    One instance per process, built in :func:`src.main.lifespan` and reached by resolvers through
    ``info.context.broker``.
    """

    #: First retry delay for the reader loop, in seconds, doubling to :attr:`_BACKOFF_MAX`. Short
    #: enough that a Redis restart is invisible; long enough that an unreachable Redis is not a
    #: connect-storm.
    _BACKOFF_INITIAL = 0.5
    _BACKOFF_MAX = 10.0

    #: How long the reader parks in ``get_message`` before looking at :attr:`_stopping` again.
    #: Bounds shutdown latency *without* relying on task cancellation landing cleanly inside
    #: redis-py's socket read.
    _POLL_TIMEOUT = 1.0

    def __init__(
        self,
        settings: Settings,
        *,
        redis_client: Optional["Redis"] = None,
        publisher_id: Optional[str] = None,
    ) -> None:
        """Build a broker.

        Args:
            settings: Supplies ``SUBSCRIPTION_QUEUE_MAXSIZE``, ``MAX_SUBSCRIPTIONS_PER_CONNECTION``
                and ``SUBSCRIPTION_CHANNEL``. Carried rather than read from
                :func:`src.config.get_settings` so a test can run a broker under deliberately tiny
                limits without touching a process-wide cache.
            redis_client: The pub/sub transport, or ``None`` for a purely in-process broker. Duck-
                typed on ``publish()`` and ``pubsub()`` so the unit suite can drive the bridge with
                a stub and assert on what was published rather than on "no exception was raised".
                **The broker does not own this client's lifetime** — :func:`src.main.lifespan`
                creates it and closes it, after :meth:`stop`.
            publisher_id: Overrides the generated per-process id. Only tests pass it; two brokers
                sharing an id would suppress each other's events as their own echo, which is a
                perfectly good way to write a test for exactly that.
        """
        self._settings = settings
        self._redis = redis_client
        self._channel = settings.subscription_channel
        # pid + uuid4: the uuid alone is sufficient for uniqueness, and the pid is what makes a log
        # line about a suppressed echo attributable to a worker an operator can actually find.
        self._publisher_id = publisher_id or f"{os.getpid()}-{uuid.uuid4().hex[:12]}"

        #: Ordered set of live subscribers (a dict used as one, for deterministic fan-out order).
        self._subscribers: dict[Subscriber, None] = {}
        #: connection object -> how many subscriptions it holds. The entry is DELETED at zero
        #: rather than left there, so the dict cannot grow one dead key per socket the server has
        #: ever served — and so it never keeps a Context (and its session factory) alive.
        self._per_connection: dict[Any, int] = {}
        #: Guards both dicts above. A `threading.Lock` rather than an `asyncio.Lock` so
        #: `subscriber_count()` is callable from a metrics scrape or a debug endpoint without being
        #: a coroutine; it is uncontended in practice because every writer is on the event loop.
        self._lock = threading.Lock()

        self._published = 0
        self._delivered = 0
        self._dropped = 0
        self._remote_published = 0
        self._remote_received = 0
        self._remote_suppressed = 0
        self._remote_invalid = 0
        self._redis_errors = 0

        self._reader: Optional[asyncio.Task[None]] = None
        self._stopping = False
        #: In-flight ``PUBLISH`` tasks. Held in a set with a done-callback that discards them,
        #: which is the documented way to stop the event loop's weak task references from letting
        #: a fire-and-forget task be garbage collected mid-flight.
        self._pending: set[asyncio.Task[None]] = set()
        #: ``None`` until the first observation, so the first transition always logs. See
        #: :meth:`_note_redis_state`.
        self._redis_healthy: Optional[bool] = None

    # -- identity and counters --------------------------------------------------------------

    @property
    def publisher_id(self) -> str:
        """This process's publisher id — the token own-echo suppression matches on."""
        return self._publisher_id

    @property
    def channel(self) -> str:
        """The Redis channel this broker bridges over."""
        return self._channel

    @property
    def redis_healthy(self) -> Optional[bool]:
        """Last observed bridge state: ``True``, ``False``, or ``None`` before any observation."""
        return self._redis_healthy

    def subscriber_count(self) -> int:
        """How many subscriptions are live in this process. C9 publishes it as a gauge."""
        with self._lock:
            return len(self._subscribers)

    def connection_count(self, connection: Any) -> int:
        """How many subscriptions ``connection`` holds. The per-connection cap measures this."""
        with self._lock:
            return self._per_connection.get(connection, 0)

    @property
    def stats(self) -> BrokerStats:
        """A consistent snapshot of every counter. See :class:`BrokerStats`."""
        return BrokerStats(
            active_subscribers=self.subscriber_count(),
            published_total=self._published,
            delivered_total=self._delivered,
            dropped_total=self._dropped,
            remote_published_total=self._remote_published,
            remote_received_total=self._remote_received,
            remote_suppressed_total=self._remote_suppressed,
            remote_invalid_total=self._remote_invalid,
            redis_errors_total=self._redis_errors,
        )

    # -- registration -----------------------------------------------------------------------

    def subscribe(
        self,
        flt: SubscriptionFilter,
        *,
        connection: Optional[Any] = None,
        queue_size: Optional[int] = None,
    ) -> Subscriber:
        """Register a subscriber and return its handle.

        Args:
            flt: What this subscriber watches. Evaluated by the publisher before the enqueue.
            connection: The WebSocket connection this subscription is multiplexed on — in practice
                the :class:`~src.graphql.context.Context`, which Strawberry resolves once per
                socket, so object identity *is* connection identity. Supplying it opts into
                ``MAX_SUBSCRIPTIONS_PER_CONNECTION``; ``None`` means "no connection scope", which
                is what a unit test or a future non-WebSocket caller gets.
            queue_size: Overrides ``SUBSCRIPTION_QUEUE_MAXSIZE``. Tests pass a tiny value to make
                the slow-consumer path reachable deterministically.

        Raises:
            SubscriptionLimitExceeded: ``connection`` is already at the cap. The check and the
                registration happen under **one** acquisition of the lock, so two ``subscribe``
                messages arriving in the same tick cannot both read "one slot left" and both take
                it.
            ValueError: ``queue_size`` is not positive. See :func:`_check_queue_size`.
        """
        resolved_size = (
            self._settings.subscription_queue_maxsize if queue_size is None else queue_size
        )
        # Constructed BEFORE the lock: building a Subscriber allocates an asyncio.Queue and can
        # raise (a bad queue size), and neither belongs inside a critical section that the fan-out
        # loop also takes.
        subscriber = Subscriber(flt=flt, queue_size=resolved_size, connection=connection)

        limit = self._settings.max_subscriptions_per_connection
        with self._lock:
            if connection is not None:
                held = self._per_connection.get(connection, 0)
                if held >= limit:
                    raise SubscriptionLimitExceeded(held, limit)
                self._per_connection[connection] = held + 1
            self._subscribers[subscriber] = None
        return subscriber

    def unsubscribe(self, subscriber: Subscriber) -> bool:
        """Release a subscription. **Idempotent** — returns True only for the call that released.

        Idempotence is the correctness property this method exists for, not a nicety. A streaming
        resolver has several ways to end — the generator's ``finally``, a ``CancelledError``
        when the socket drops, a ``complete`` message from the client, the slow-consumer drop below,
        and the shutdown sweep — and more than one of them routinely runs for the *same*
        subscription (a client that disconnects mid-frame trips both the cancellation and the
        ``finally``). If the per-connection counter decremented per *call* rather than per
        *subscription*, a connection's slot count would drift: downward until the cap stopped
        meaning anything, or upward until that socket could never subscribe again.

        The guard is :attr:`Subscriber.released`, read and written under the registry lock so two
        exit paths racing still produce exactly one release.
        """
        with self._lock:
            if subscriber.released:
                return False
            subscriber.released = True
            self._subscribers.pop(subscriber, None)

            connection = subscriber.connection
            if connection is not None:
                remaining = self._per_connection.get(connection, 1) - 1
                if remaining > 0:
                    self._per_connection[connection] = remaining
                else:
                    self._per_connection.pop(connection, None)
        return True

    def close_all_subscribers(self) -> int:
        """Terminate every live subscription and return how many were closed. Shutdown path.

        **Without this a shutdown hangs.** Every streaming resolver is parked on
        ``await queue.get()`` on a queue nothing will ever write to again, so the ASGI server waits
        on tasks that can never finish and the container has to be killed rather than stopped.

        Unlike the drop path this does **not** drain first: whatever a client has already been sent
        is legitimately theirs, and the sentinel goes in behind it so the generator finishes
        delivering before it returns. Only a completely full queue loses one entry to make room for
        the sentinel — a terminal frame that arrives is worth more than the last entry of a stream
        that is ending regardless.
        """
        with self._lock:
            current = list(self._subscribers)
        for subscriber in current:
            self._terminate(subscriber, drain_first=False)
        return len(current)

    # -- publishing -------------------------------------------------------------------------

    async def publish(self, entry: LogEntry) -> int:
        """Fan ``entry`` out locally and hand it to the Redis bridge. **Never raises, never blocks.**

        Called from ``createLog`` **after the commit** — see :mod:`src.graphql.mutation` for why
        that ordering is not negotiable.

        There is no ``await`` anywhere on this path (see the module docstring): the local fan-out is
        ``put_nowait`` per subscriber, and the ``PUBLISH`` is scheduled as a background task. So a
        stalled subscriber, a slow Redis, or a Redis that is entirely down cannot add latency to,
        or fail, a successful write.

        Returns:
            How many local subscribers the entry was enqueued to. ``0`` is an ordinary answer — it
            means nobody is watching, or nobody's filter matched.
        """
        self._published += 1
        delivered = self._fan_out(entry)
        self._schedule_remote(entry)
        return delivered

    def _fan_out(self, entry: LogEntry) -> int:
        """Enqueue ``entry`` on every matching live subscriber. **Never raises.**

        The method invariants 2 and 3 from the module docstring live in. Each subscriber is handled
        inside its own ``try`` so that a full queue or a misbehaving filter costs *that* subscriber
        and nothing else — not the publish, and certainly not the mutation that triggered it.
        """
        subscribers = self._subscribers
        # Unlocked truthiness check: "nobody is streaming" is the common case by a wide margin, and
        # taking a lock on every ingested log line to discover it is a cost every write pays for a
        # feature nobody is using. A subscriber registering concurrently with this line simply
        # starts one entry later, which is indistinguishable from having connected a moment later.
        if not subscribers:
            return 0

        with self._lock:
            current = list(subscribers)

        delivered = 0
        failed: list[Subscriber] = []
        for subscriber in current:
            if subscriber.released:
                continue
            try:
                if not subscriber.filter.matches(entry):
                    continue
                subscriber.queue.put_nowait(entry)
                delivered += 1
            except asyncio.QueueFull:
                # The entire back-pressure policy, in one branch. This consumer is a full queue
                # behind and still not reading: buffering more would let it grow the process's
                # memory without bound, and blocking would let it stall ingest for every other
                # client. So it loses its subscription — and is told so, via `dropped`.
                failed.append(subscriber)
            except Exception:  # noqa: BLE001 - a broken subscriber must not break the publisher
                logger.exception("dropping a subscriber: its filter raised during fan-out")
                failed.append(subscriber)

        for subscriber in failed:
            self._drop(subscriber)

        # Counted here rather than in `publish` because BOTH fan-out paths reach this line: a
        # locally-written entry and one that arrived over the Redis bridge are equally real
        # deliveries to a local subscriber. The consequence, stated so the metric is not
        # misread: `delivered / published` is the average fan-out only on a single instance —
        # with the bridge live, remote entries lift `delivered` without touching `published`,
        # which is what `remote_received_total` is there to explain.
        self._delivered += delivered
        return delivered

    def _drop(self, subscriber: Subscriber) -> None:
        """End a subscription because its consumer could not keep up."""
        self._dropped += 1
        logger.warning(
            "dropping a slow subscriber (queue full at maxsize=%d, filter=%r)",
            subscriber.queue.maxsize,
            subscriber.filter,
        )
        self._terminate(subscriber, drain_first=True)

    def _terminate(self, subscriber: Subscriber, *, drain_first: bool) -> None:
        """End one subscription: (optionally drain), enqueue the terminal sentinel, unregister.

        ``drain_first`` distinguishes the two reasons a subscription ends without the client going
        away. A **drop** drains, because the queue being full is the entire problem and a sentinel
        that cannot be enqueued is a generator that never learns it was dropped — the exact
        "parked forever" outcome the drop exists to prevent. A **shutdown** does not, because those
        entries were legitimately delivered-in-flight.

        Ordering matters: the sentinel goes in *before* :meth:`unsubscribe`, so a consumer can never
        observe "released, and nothing left in the queue" and be left waiting on a queue that is
        already unregistered.
        """
        subscriber.dropped = subscriber.dropped or drain_first
        queue = subscriber.queue
        if drain_first:
            self._drain(queue)
        try:
            queue.put_nowait(None)
        except asyncio.QueueFull:
            # A shutdown against a completely full queue. Make room by discarding the oldest entry:
            # a terminal frame that arrives beats the last entry of a stream that is ending anyway.
            self._drain(queue, limit=1)
            with suppress(asyncio.QueueFull):
                queue.put_nowait(None)
        except Exception:  # noqa: BLE001 - termination is best-effort by construction
            logger.exception("failed to enqueue the terminal sentinel for a subscriber")
        self.unsubscribe(subscriber)

    @staticmethod
    def _drain(queue: "asyncio.Queue[Optional[LogEntry]]", *, limit: Optional[int] = None) -> int:
        """Discard up to ``limit`` (default: all) queued items; return how many went.

        ``get_nowait`` in a loop rather than replacing the queue object, so the identity a parked
        getter is waiting on is preserved.
        """
        discarded = 0
        while limit is None or discarded < limit:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            discarded += 1
        return discarded

    # -- the Redis bridge -------------------------------------------------------------------

    def _schedule_remote(self, entry: LogEntry) -> None:
        """Hand ``entry`` to a background task that PUBLISHes it. Never raises, never awaits."""
        if self._redis is None or self._stopping:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - publish is always called from the loop
            # No running loop means nothing to schedule on. In-process fan-out already happened,
            # which is the half that has a live subscriber waiting on it.
            return

        task = loop.create_task(self._publish_remote(entry))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _publish_remote(self, entry: LogEntry) -> None:
        """PUBLISH one entry to the channel. Swallows every failure except cancellation."""
        try:
            payload = encode_event(entry, origin=self._publisher_id)
        except Exception:  # noqa: BLE001 - an unserialisable entry is not a transport failure
            # Counted as an envelope problem rather than a Redis one, and deliberately does NOT
            # flip the health flag: reporting a perfectly healthy bridge as degraded because one
            # entry could not be encoded would send an operator looking at the wrong system.
            self._remote_invalid += 1
            logger.exception("could not serialise an entry for the subscription channel")
            return

        try:
            await self._redis.publish(self._channel, payload)  # type: ignore[union-attr]
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - Redis down must never surface to a mutation
            self._redis_errors += 1
            self._note_redis_state(healthy=False, reason=exc)
            return
        self._remote_published += 1
        self._note_redis_state(healthy=True)

    def _ingest_remote(self, raw: Any) -> None:
        """Turn one channel message into local fan-out, or discard it. Never raises."""
        self._remote_received += 1
        decoded = decode_event(raw)
        if decoded is None:
            self._remote_invalid += 1
            return
        if decoded.origin == self._publisher_id:
            # Our own echo. Redis delivers a published message to every subscriber including the
            # publisher, so without this every local subscriber sees each entry twice.
            self._remote_suppressed += 1
            return
        self._fan_out(decoded.entry)

    async def start(self) -> None:
        """Start the background reader. A no-op with no Redis client, and idempotent.

        Deliberately does **not** connect: ``Redis.from_url`` is lazy and the first command is what
        opens a socket, so a Redis that is down at boot means a reader that logs one warning and
        retries — not a container that refuses to start. The spec's core requirements do not depend
        on the bridge; it is a §4 bonus that makes subscriptions survive ``--workers N``.
        """
        if self._redis is None or self._reader is not None:
            return
        self._stopping = False
        self._reader = asyncio.get_running_loop().create_task(
            self._reader_loop(), name="subscription-pubsub-reader"
        )

    async def stop(self) -> None:
        """Stop the reader and settle in-flight publishes. Idempotent; never raises.

        Called **before** :meth:`close_all_subscribers` and before the Redis client is closed — see
        :func:`src.main.lifespan` for the ordering argument.
        """
        self._stopping = True

        reader, self._reader = self._reader, None
        if reader is not None:
            reader.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await reader

        await self.drain_pending_publishes()

    async def drain_pending_publishes(self) -> None:
        """Wait for every in-flight ``PUBLISH`` task to finish. Never raises.

        Production calls it from :meth:`stop` so a shutdown does not cancel a publish that a peer
        worker's subscriber is entitled to. Tests call it directly, because "the envelope reached
        Redis" is otherwise only observable after an arbitrary number of event-loop turns — and
        `await`ing the actual tasks is a bounded wait rather than a sleep with a guessed margin.
        """
        pending = [task for task in self._pending if not task.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _reader_loop(self) -> None:
        """SUBSCRIBE to the channel and re-inject remote events, retrying with backoff forever.

        Structured as connect / consume / (on failure) back off / reconnect, because a pub/sub
        connection is a long-lived socket and every way it can end — Redis restarting, a network
        blip, a ``CLIENT KILL`` — has to be a reconnection rather than the end of the bridge.
        Failures are counted and logged **once per state change** by :meth:`_note_redis_state`;
        logging per attempt would fill a log at two lines a second for as long as Redis is down.

        Consumption is a ``get_message`` poll rather than ``async for … in pubsub.listen()`` so that
        shutdown is bounded by :attr:`_POLL_TIMEOUT` and does not depend on cancellation landing
        cleanly inside redis-py's socket read.
        """
        backoff = self._BACKOFF_INITIAL
        while not self._stopping:
            pubsub = None
            try:
                pubsub = self._redis.pubsub()  # type: ignore[union-attr]
                await pubsub.subscribe(self._channel)
                self._note_redis_state(healthy=True)
                backoff = self._BACKOFF_INITIAL
                while not self._stopping:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=self._POLL_TIMEOUT
                    )
                    if message is None:
                        continue
                    self._ingest_remote(message.get("data"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the bridge degrades, it does not die
                self._redis_errors += 1
                self._note_redis_state(healthy=False, reason=exc)
            finally:
                if pubsub is not None:
                    await self._close_pubsub(pubsub)

            if self._stopping:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._BACKOFF_MAX)

    @staticmethod
    async def _close_pubsub(pubsub: Any) -> None:
        """Release a pub/sub connection, whichever spelling this redis-py exposes.

        redis-py renamed the async closer to ``aclose()`` and kept ``close()`` as a deprecated
        alias; a stub in the unit suite may implement either, or neither. None of those is worth a
        traceback on a teardown path.
        """
        closer = getattr(pubsub, "aclose", None) or getattr(pubsub, "close", None)
        if closer is None:
            return
        try:
            result = closer()
            if asyncio.iscoroutine(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - teardown is best-effort
            logger.debug("failed to close the pub/sub connection", exc_info=True)

    def _note_redis_state(self, *, healthy: bool, reason: Optional[BaseException] = None) -> None:
        """Log the bridge's health **once per transition**, never once per event.

        A Redis outage under load would otherwise print one line per ingested log entry, which
        turns a degraded optional feature into an operational problem of its own. The first
        observation always logs, because :attr:`_redis_healthy` starts at ``None``.
        """
        if self._redis_healthy is healthy:
            return
        self._redis_healthy = healthy
        if healthy:
            logger.info(
                "subscription pub/sub bridge connected (channel=%r, publisher_id=%s)",
                self._channel,
                self._publisher_id,
            )
        else:
            logger.warning(
                "subscription pub/sub bridge degraded to in-process fan-out "
                "(channel=%r): %s: %s — subscriptions still work on THIS worker; events will "
                "not cross workers until Redis returns",
                self._channel,
                type(reason).__name__ if reason is not None else "unknown",
                reason,
            )


def create_redis_client(settings: Settings) -> Optional["Redis"]:
    """Build the pub/sub client, or ``None`` if one cannot be constructed. **Never raises.**

    Construction is not connection: ``Redis.from_url`` parses the URL and returns a client whose
    first command opens the socket. So this returning a client says nothing about Redis being
    reachable, and it should not — an unreachable Redis has to degrade
    (:meth:`LogBroker._note_redis_state`) rather than stop the process from starting.

    ``None`` comes back only when the URL itself is unusable or the library is missing, and both
    are configuration faults rather than outages: they are logged at WARNING and the broker runs
    in-process-only, which is a fully working ``logStream`` for a single-worker deployment.

    ``decode_responses`` is deliberately left off. The channel payload is UTF-8 JSON either way,
    :func:`decode_event` accepts ``bytes`` and ``str`` alike, and turning it on would change what
    every *other* consumer of this client sees when C7 and C9 start using Redis for cached values
    and persisted-query documents.
    """
    try:
        from redis.asyncio import Redis  # noqa: PLC0415 - local so a broken install degrades here

        return Redis.from_url(
            settings.redis_url,
            # Bounds how long a *connect* to a dead host can hold a task. The reader retries with
            # backoff, so a short timeout means "notice quickly", not "give up".
            socket_connect_timeout=2.0,
            socket_keepalive=True,
        )
    except Exception:  # noqa: BLE001 - a bad REDIS_URL must not stop the server from starting
        logger.warning(
            "could not build a Redis client from REDIS_URL=%r — subscriptions will fan out "
            "in-process only, which is correct for a single worker and lossy across workers",
            settings.redis_url,
            exc_info=True,
        )
        return None


__all__ = [
    "EVENT_FORMAT_VERSION",
    "EVENT_KIND_LOG",
    "BrokerStats",
    "LogBroker",
    "RemoteEvent",
    "SubscriptionFilter",
    "SubscriptionLimitExceeded",
    "Subscriber",
    "create_redis_client",
    "decode_event",
    "encode_event",
]
