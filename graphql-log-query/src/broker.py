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

It is ``async`` because the call site reads ``await broker.publish(entry)`` and because a future
kind may one day need a real await there. It contains no suspension point *today*: the local
fan-out is a loop of ``put_nowait``, and the Redis hop is handed to a background task rather than
awaited. Awaiting a coroutine that never suspends does not yield to the event loop, so "publish
never blocks" is a property of the code rather than a hope about how fast Redis is — which matters
precisely when Redis is *not* fast, i.e. when it is down and every await would sit on a connect
timeout inside a mutation resolver.

C12 kept that exactly: :meth:`LogBroker.publish_order_event` is a second ``async`` wrapper over the
same synchronous :meth:`LogBroker._publish` body, so both mutations inherit the property from one
function rather than from two that happen to agree.

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

.. rubric:: TWO STREAMS, ONE BROKER — C12 (spec §3 Feature Area C)

``orderStatusStream`` is the same machinery with a second event kind, not a second broker. One
registry, one lock, one bounded-queue policy, one shutdown sweep, one per-connection cap, one
pub/sub bridge — because every one of those is a property of *the process*, and a second copy of
each would be a second set of edges to find. What distinguishes the two streams is exactly three
things, each of them data rather than a branch:

* the envelope's ``kind`` (:data:`EVENT_KIND_LOG` / :data:`EVENT_KIND_ORDER`) — the discriminator
  C6 put in the payload before there was a second kind to discriminate;
* the row it selects in :data:`_DECODERS`, which is what makes an *unknown* kind a drop rather than
  a guess;
* the ``kind`` tag on the subscriber's filter, which :meth:`LogBroker._fan_out` routes on so an
  order subscriber is never handed a ``LogEntry`` and vice versa.

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
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Optional, Union

from src.config import Settings
from src.graphql.ecommerce import OrderEvent
from src.graphql.enums import LogLevel, OrderStatus
from src.graphql.types import LogEntry

if TYPE_CHECKING:  # pragma: no cover - annotations only
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)

#: Anything this broker can carry. A union rather than a base class: ``LogEntry`` and ``OrderEvent``
#: are Strawberry types whose shared ancestor is the *published* ``LogEvent`` interface, and that
#: interface deliberately carries only the four correlation fields (see
#: :class:`src.graphql.types.LogEvent`) — not ``orderId``, not ``status``, not ``message``. A filter
#: needs the concrete type, so the broker names both rather than narrowing to their common parent.
PublishedEvent = Union[LogEntry, OrderEvent]

# =================================================================================================
# The wire format
#
# One envelope, one version field, one kind discriminator. C12 adds `orderStatusStream` by adding a
# kind rather than by changing the shape, which is the whole reason the discriminator exists before
# there is a second kind to discriminate.
#
# THE ENVELOPE IS THIS MODULE'S; THE ENTRY BODY INSIDE IT IS NOT. `LogEntry.to_wire()` /
# `LogEntry.from_wire()` in `src.graphql.types` are the project's single JSON representation of a
# published entry, shared with C7's result cache. C6 wrote that mapping and C7 lifted it up rather
# than copying it — an entry that crossed the pub/sub bridge and one that came back out of the cache
# are then the same object by construction, instead of by two implementations agreeing today.
# =================================================================================================

#: Bumped only for a **breaking** envelope change. A reader that meets a version it does not know
#: drops the message rather than guessing: during a rolling deploy two versions of this process are
#: subscribed to the same channel, and a guess there means one worker fabricating entries.
EVENT_FORMAT_VERSION = 1

#: The kind C6 publishes: a ``LogEntry`` written by ``createLog``.
EVENT_KIND_LOG = "log"

#: The kind C12 adds: an ``OrderEvent`` written by ``createOrderEvent``. **A new kind rather than a
#: new envelope** — the discriminator was put in the payload at C6 precisely so this commit could
#: be one entry in :data:`_DECODERS` instead of a second wire format, and so a C6 binary meeting a
#: C12 message during a rolling deploy *drops* it (see :func:`decode_event`) rather than trying to
#: read an order event as a log line.
EVENT_KIND_ORDER = "order"


@dataclass(frozen=True, slots=True)
class RemoteEvent:
    """A decoded envelope: who published it, what kind it is, and the event it carried.

    ``kind`` is carried out of the decode rather than re-derived from ``type(entry)`` because it is
    what :meth:`LogBroker._fan_out` routes on, and a router that switched on the Python type would
    silently start guessing the moment two kinds ever shared one class.
    """

    origin: str
    entry: PublishedEvent
    #: Defaulted so every C6-era construction site (and every test that builds one by hand) still
    #: reads as "a log envelope" without being edited.
    kind: str = EVENT_KIND_LOG


def _encode_envelope(kind: str, body: dict[str, Any], *, origin: str) -> str:
    """Wrap one already-rendered event body in the versioned envelope. The only writer of it.

    Split out of :func:`encode_event` when C12 added a second kind, so that ``v``, ``kind`` and
    ``origin`` are assembled in exactly one place: two encoders agreeing today about what an
    envelope looks like is how a version bump lands on half the payloads.
    """
    payload = {"v": EVENT_FORMAT_VERSION, "kind": kind, "origin": origin, "entry": body}
    # `separators` because this is a hot path in aggregate (one document per ingested event) and
    # the default separators add a space per key for a machine-only payload.
    return json.dumps(payload, separators=(",", ":"))


def encode_event(entry: LogEntry, *, origin: str) -> str:
    """Serialise one entry into the JSON envelope published on the pub/sub channel.

    .. rubric:: The ENVELOPE is this module's; the ENTRY BODY is not

    ``origin``, ``v`` and ``kind`` exist for the bridge and mean nothing anywhere else, so they are
    built here. The entry itself is rendered by :meth:`src.graphql.types.LogEntry.to_wire`, which is
    the project's single JSON representation of a published entry — C7's result cache stores lists
    of exactly that dict. Two encoders for one type is how two representations of one row start
    disagreeing about whether ``metadata`` came back, and the disagreement survives a full test
    suite because each test exercises only one of the two paths.

    Args:
        entry: The committed entry. Every field of the published type is carried, so a subscriber
            on another worker receives exactly what a local one does — the resolver reconstructs
            a :class:`~src.graphql.types.LogEntry` and never consults the database to fill a gap.
        origin: The publishing process's id. Read back by :meth:`LogBroker._ingest_remote` to
            suppress this process's own echo.

    Returns:
        Compact JSON. See :meth:`~src.graphql.types.LogEntry.to_wire` for what the body guarantees
        — in particular that ``metadata`` absent and ``metadata`` present stay distinguishable.
    """
    return _encode_envelope(EVENT_KIND_LOG, entry.to_wire(), origin=origin)


def encode_order_event(event: OrderEvent, *, origin: str) -> str:
    """Serialise one order status transition into the same envelope, under the ``order`` kind.

    The twin of :func:`encode_event`, and deliberately nothing more: the envelope is
    :func:`_encode_envelope`'s and the body is
    :meth:`src.graphql.ecommerce.OrderEvent.to_wire`'s, so this function holds no mapping of its own
    and therefore has nothing that can drift from either.

    Every published field travels, for the same reason a log entry's do: ``orderStatusStream``
    issues **zero** database round trips, so a subscriber on another worker has to be able to
    reconstruct the whole event from the payload alone. ``orderId``, ``userId`` and ``status`` in
    particular are what the *receiving* broker's :class:`OrderSubscriptionFilter` matches on — a
    body missing one of them would make a remote event unfilterable rather than merely incomplete,
    and the symptom would be a narrowly-filtered subscription that quietly receives another
    customer's orders.
    """
    return _encode_envelope(EVENT_KIND_ORDER, event.to_wire(), origin=origin)


#: kind -> the constructor that rebuilds that kind's body. **The whole of C12's wire change.**
#:
#: A table rather than an ``if``/``elif`` chain inside :func:`decode_event`, for one reason: an
#: unknown kind must be *dropped*, and a table makes that the structural default (``.get(kind)``
#: is ``None``) instead of an else-branch somebody has to remember to write last. Adding a kind is
#: one row here plus one encoder above; forgetting to add the row fails closed.
_DECODERS: Mapping[str, Any] = MappingProxyType(
    {
        EVENT_KIND_LOG: LogEntry.from_wire,
        EVENT_KIND_ORDER: OrderEvent.from_wire,
    }
)


def decode_event(raw: str | bytes | bytearray | memoryview | None) -> Optional[RemoteEvent]:
    """Parse a channel message back into a :class:`RemoteEvent`, or ``None`` if it is not one.

    **Never raises.** Everything this function is handed came off a network channel that any
    process with the Redis credentials can write to, and a malformed message must cost one debug
    line rather than the reader task. The rejected cases, each returning ``None``:

    * not valid JSON, or not a JSON object;
    * an envelope version this build does not know (see :data:`EVENT_FORMAT_VERSION`);
    * a kind absent from :data:`_DECODERS`. **The kind is DISPATCHED ON, never guessed.** A future
      binary's third kind reaching this one during a rolling deploy is exactly this case, and so is
      a C12 order event reaching a C6 binary — dropping beats decoding an order event's body as a
      log line, which would raise on the missing ``message`` key and, if the two shapes ever
      overlapped, would fabricate an entry instead;
    * a missing required field, a ``level`` outside :class:`~src.graphql.enums.LogLevel`, or a
      ``status`` outside :class:`~src.graphql.enums.OrderStatus` — all of which the ``from_wire``
      constructors raise on, deliberately, so that each caller can decide what a malformed body
      means. Here it means "drop the message".
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

    kind = payload.get("kind")
    from_wire = _DECODERS.get(kind) if isinstance(kind, str) else None
    if from_wire is None:
        # An unknown kind is dropped rather than guessed — see the docstring. This is the branch a
        # rolling deploy runs through in both directions.
        return None

    origin = payload.get("origin")
    body = payload.get("entry")
    if not isinstance(origin, str) or not isinstance(body, dict):
        return None

    try:
        entry = from_wire(body)
    except Exception:  # noqa: BLE001 - a missing key, a bad enum member, an unparseable timestamp
        logger.debug("dropping a malformed subscription message", exc_info=True)
        return None

    return RemoteEvent(origin=origin, entry=entry, kind=kind)


# =================================================================================================
# Filters
#
# TWO filter classes, one per event kind, and **the `kind` class attribute is what keeps them
# apart**. It is not a label: `LogBroker._fan_out` skips every subscriber whose `kind` differs from
# the kind being published, which is the only thing standing between an order subscriber and a
# `LogEntry` its `matches()` would raise an AttributeError on. Without it, publishing a log line
# would evaluate `OrderSubscriptionFilter.matches(LogEntry)`, that would raise, and the fan-out's
# "a broken filter costs only its own subscriber" rule would DROP a perfectly healthy order
# subscription — a bug whose symptom is "orderStatusStream dies whenever anybody writes a log".
#
# It is a plain class attribute rather than an annotated field, deliberately: annotating it would
# make it a dataclass field, so it would join every constructor signature, every `repr`, and every
# equality comparison — and a caller could then pass `SubscriptionFilter(kind="order")` and route a
# log filter onto the order stream.
# =================================================================================================


@dataclass(frozen=True)
class SubscriptionFilter:
    """What one ``logStream`` subscriber is watching. Both fields optional and AND-composed.

    Evaluated in :meth:`LogBroker.publish`, once per live subscriber per published entry, **before
    the enqueue** — see the module docstring for why that is stronger than filtering in the
    resolver.

    ``level`` is coerced from a string in ``__post_init__`` rather than trusted, because the failure
    mode of not coercing is silent: ``LogLevel.ERROR != "ERROR"`` is ``True``, so a filter holding
    a raw string would match nothing at all and present as "the subscription is quiet".
    """

    #: The event kind this filter subscribes to. See the section comment above.
    kind = EVENT_KIND_LOG

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


@dataclass(frozen=True)
class OrderSubscriptionFilter:
    """What one ``orderStatusStream`` subscriber is watching — spec §3 Feature Area C.

    "Filtering by order id, status, and/or user": three optional fields, AND-composed, evaluated
    **at enqueue time** exactly like the log filter and for exactly the same reason. That reason is
    sharper here than it is for logs. A dashboard watching one customer's single order is the
    canonical use of this stream, and an order-status firehose (every order in the system moving
    through seven statuses) is the canonical background traffic — so a filter applied in the
    resolver would let that firehose fill, and then *drop*, a subscription that asked for one order.
    The queue depth a subscriber pays for is its own traffic, not the system's.

    ``status`` is coerced from a string in ``__post_init__`` for the reason ``SubscriptionFilter``
    coerces ``level``: ``OrderStatus.SHIPPED != "SHIPPED"``, so an uncoerced filter matches nothing
    at all and reads as a quiet stream rather than as a bug.

    Note there is deliberately **no** ``service`` or ``level`` field. Every order event is emitted
    by one service and its severity is a function of its status (see
    :data:`src.generators.ORDER_STATUS_LEVELS`), so both would be filters that either match
    everything or nothing — a surface that looks like a choice and is not.
    """

    #: The event kind this filter subscribes to. See the section comment above.
    kind = EVENT_KIND_ORDER

    order_id: Optional[str] = None
    status: Optional[OrderStatus] = None
    user_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.status is not None and not isinstance(self.status, OrderStatus):
            # frozen dataclass: normalisation goes through object.__setattr__ or nowhere.
            object.__setattr__(self, "status", OrderStatus(self.status))

    @property
    def matches_everything(self) -> bool:
        """True when nothing is constrained — the unfiltered ``orderStatusStream`` subscription."""
        return self.order_id is None and self.status is None and self.user_id is None

    def matches(self, event: OrderEvent) -> bool:
        """Does ``event`` satisfy every constraint this filter carries?

        Exact equality on all three, never a prefix or a substring: ``orderId`` and ``userId`` are
        opaque identifiers, and a filter that matched ``ord-6000`` against ``ord-60001`` would leak
        one customer's orders into another's stream — the one failure mode a per-order subscription
        must not have.
        """
        if self.order_id is not None and event.order_id != self.order_id:
            return False
        if self.status is not None and event.status != self.status:
            return False
        if self.user_id is not None and event.user_id != self.user_id:
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
        queue: Bounded ``asyncio.Queue``. Holds events of **this subscriber's kind only** — see
            :attr:`filter` — plus at most one ``None`` **terminal sentinel** meaning "this
            subscription is over".
        filter: What this subscriber watches, and (through its ``kind``) which of the two streams
            it is on. Evaluated by the publisher, on the publisher's task — which is why a filter
            that raises must cost only this subscriber. The ``kind`` check happens *before*
            ``matches`` is called, so a filter is never handed an event of the wrong type.
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
        flt: Union[SubscriptionFilter, OrderSubscriptionFilter],
        queue_size: int,
        connection: Optional[Any] = None,
    ) -> None:
        self.filter = flt
        self.connection = connection
        self.queue: asyncio.Queue[Optional[PublishedEvent]] = asyncio.Queue(
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
        flt: Union[SubscriptionFilter, OrderSubscriptionFilter],
        *,
        connection: Optional[Any] = None,
        queue_size: Optional[int] = None,
    ) -> Subscriber:
        """Register a subscriber and return its handle.

        Args:
            flt: What this subscriber watches, and which stream it is on. Evaluated by the
                publisher before the enqueue. The filter's ``kind`` is what routes it: an
                :class:`OrderSubscriptionFilter` never sees a ``LogEntry`` and vice versa, so one
                registry and one per-connection cap serve both streams without either being able to
                deliver the other's events.
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
        """Fan a **log entry** out locally and hand it to the Redis bridge. Never raises or blocks.

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
        return self._publish(entry, EVENT_KIND_LOG, encode_event)

    async def publish_order_event(self, event: OrderEvent) -> int:
        """Fan an **order status transition** out — C12's half of spec §3 Feature Area C.

        The exact twin of :meth:`publish`, differing only in the kind tag and the encoder, and
        carrying every one of that method's guarantees unchanged: no await point, no way to raise,
        no way for a stalled ``orderStatusStream`` subscriber to add latency to the
        ``createOrderEvent`` mutation that produced the event. It is called from that mutation
        **after the commit**, for the reason :mod:`src.graphql.mutation` argues at length.

        Returns:
            How many local ``orderStatusStream`` subscribers it was enqueued to. Log subscribers
            are not candidates and are not counted — they are on the other kind.
        """
        return self._publish(event, EVENT_KIND_ORDER, encode_order_event)

    def _publish(self, event: PublishedEvent, kind: str, encoder: Any) -> int:
        """The shared body of the two publish methods. **Never raises, never awaits.**

        Not ``async``, and the two public methods are — which looks backwards until you read the
        module docstring's note on ``publish``: the *public* coroutines exist so their call sites
        read ``await broker.publish(...)`` and so a future kind may one day need a real await
        there, while this contains no suspension point at all. Keeping the sync body separate is
        what makes "publish never blocks" checkable by reading one function.
        """
        self._published += 1
        delivered = self._fan_out(event, kind)
        self._schedule_remote(event, encoder)
        return delivered

    def _fan_out(self, event: PublishedEvent, kind: str) -> int:
        """Enqueue ``event`` on every live subscriber **of this kind** whose filter matches.

        The method invariants 2 and 3 from the module docstring live in. Each subscriber is handled
        inside its own ``try`` so that a full queue or a misbehaving filter costs *that* subscriber
        and nothing else — not the publish, and certainly not the mutation that triggered it.

        ``kind`` is checked before ``matches`` is called and is not part of the filter's own logic.
        That ordering is load-bearing: an :class:`OrderSubscriptionFilter` handed a
        :class:`~src.graphql.types.LogEntry` would raise ``AttributeError`` on ``order_id``, the
        ``except Exception`` below would read that as a broken subscriber, and a healthy order
        subscription would be dropped the first time anybody wrote a log line. See the section
        comment above the filters.
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
            if subscriber.filter.kind != kind:
                # A subscriber on the other stream. Skipped before its filter is consulted — see
                # the docstring.
                continue
            try:
                if not subscriber.filter.matches(event):
                    continue
                subscriber.queue.put_nowait(event)
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
    def _drain(
        queue: "asyncio.Queue[Optional[PublishedEvent]]", *, limit: Optional[int] = None
    ) -> int:
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

    def _schedule_remote(self, event: PublishedEvent, encoder: Any) -> None:
        """Hand ``event`` to a background task that PUBLISHes it. Never raises, never awaits.

        ``encoder`` is the kind's envelope writer (:func:`encode_event` or
        :func:`encode_order_event`), passed down from :meth:`_publish` rather than re-derived from
        ``type(event)`` here — one decision about which kind this is, made once, at the call site
        that knows.
        """
        if self._redis is None or self._stopping:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - publish is always called from the loop
            # No running loop means nothing to schedule on. In-process fan-out already happened,
            # which is the half that has a live subscriber waiting on it.
            return

        task = loop.create_task(self._publish_remote(event, encoder))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _publish_remote(self, event: PublishedEvent, encoder: Any) -> None:
        """PUBLISH one event to the channel. Swallows every failure except cancellation."""
        try:
            payload = encoder(event, origin=self._publisher_id)
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
        # The kind travels with the decoded envelope rather than being inferred from the Python
        # object, so a remote order event reaches order subscribers and only those.
        self._fan_out(decoded.entry, decoded.kind)

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
    "EVENT_KIND_ORDER",
    "BrokerStats",
    "LogBroker",
    "OrderSubscriptionFilter",
    "PublishedEvent",
    "RemoteEvent",
    "SubscriptionFilter",
    "SubscriptionLimitExceeded",
    "Subscriber",
    "create_redis_client",
    "decode_event",
    "encode_event",
    "encode_order_event",
]
