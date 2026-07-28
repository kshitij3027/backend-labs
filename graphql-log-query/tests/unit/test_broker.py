"""The broker's in-process half: bounded queues, filtering, lifecycle, and the wire codec.

No Redis and no GraphQL here — :mod:`src.broker`'s local fan-out is deliberately buildable from a
:class:`~src.config.Settings` object and nothing else, so the back-pressure policy can be pinned
without a WebSocket in the way. The bridge gets its own module
(``tests/unit/test_broker_redis.py``) and the transport gets
``tests/integration/test_subscriptions.py``.

.. rubric:: What is asserted here that a "it did not raise" test would miss

Every drop test asserts **three** things, because the policy has three halves and any one of them
can regress alone: the offending subscriber is terminated *and told why*, an unrelated subscriber
keeps receiving, and the publisher is never suspended. A test that only checked "no exception
escaped" would stay green against an implementation that dropped every subscriber on the first full
queue, or one that silently buffered without limit.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest
import strawberry

from src.broker import (
    EVENT_FORMAT_VERSION,
    EVENT_KIND_LOG,
    LogBroker,
    Subscriber,
    SubscriptionFilter,
    SubscriptionLimitExceeded,
    decode_event,
    encode_event,
)
from src.config import Settings
from src.graphql.enums import LogLevel
from src.graphql.types import LogEntry

#: A fixed instant, so nothing in this module depends on when it runs.
ANCHOR = datetime(2026, 7, 27, 9, 30, 0, tzinfo=timezone.utc)


def make_settings(
    *,
    queue_maxsize: int = 500,
    max_per_connection: int = 10,
    channel: str = "test:events",
) -> Settings:
    """Settings for a broker under test, built directly rather than from the environment.

    ``_env_file=None`` so a stray ``.env`` cannot perturb the suite; the three keys that matter are
    passed explicitly so every expected number in a test is visible in the test.
    """
    return Settings(
        _env_file=None,
        seed_entries=0,
        seed_orders=0,
        log_level="WARNING",
        subscription_queue_maxsize=queue_maxsize,
        max_subscriptions_per_connection=max_per_connection,
        subscription_channel=channel,
    )


def make_entry(
    *,
    entry_id: int = 1,
    service: str = "api",
    level: LogLevel = LogLevel.INFO,
    message: str = "hello",
    metadata: Optional[dict[str, Any]] = None,
    trace_id: Optional[str] = None,
    offset_seconds: int = 0,
    timestamp: Optional[datetime] = None,
) -> LogEntry:
    """One published entry, shaped exactly as ``LogEntry.from_orm`` would produce it."""
    return LogEntry(
        id=strawberry.ID(str(entry_id)),
        timestamp=timestamp if timestamp is not None else ANCHOR + timedelta(seconds=offset_seconds),
        service=service,
        level=level,
        message=message,
        metadata=metadata,
        trace_id=trace_id,
    )


def fields_of(entry: LogEntry) -> tuple[Any, ...]:
    """The entry as a comparable tuple.

    Used instead of ``==`` on two distinct :class:`LogEntry` objects so the comparison does not
    depend on whether Strawberry's ``@strawberry.type`` happens to generate ``__eq__`` — that is a
    property of the library, not of the codec under test, and a change to it should not look like a
    round-tripping bug.
    """
    return (
        entry.id,
        entry.timestamp,
        entry.service,
        entry.level,
        entry.message,
        entry.metadata,
        entry.trace_id,
    )


def drain(subscriber: Subscriber) -> list[Optional[LogEntry]]:
    """Everything currently queued for ``subscriber``, without awaiting anything.

    ``get_nowait`` rather than ``await queue.get()`` on purpose: these tests assert on what the
    publisher *already* enqueued, and an await would let the loop run and blur "what was delivered
    synchronously" into "what was delivered eventually".
    """
    items: list[Optional[LogEntry]] = []
    while True:
        try:
            items.append(subscriber.queue.get_nowait())
        except asyncio.QueueEmpty:
            return items


# =================================================================================================
# The queue-size guard
# =================================================================================================


@pytest.mark.parametrize("queue_size", [0, -1, -500])
async def test_a_queue_size_below_one_is_refused_at_the_call_site(queue_size: int) -> None:
    """``asyncio.Queue(maxsize=0)`` is UNBOUNDED, so 0 is the dangerous value that looks safe.

    :class:`~src.config.Settings` already refuses it at startup (``test_config.py``). This is the
    second gate, at the point a queue is actually constructed, and it is not redundant: a test, a
    script or a future caller can pass a size that never went through ``Settings`` at all, and the
    consequence of a zero slipping through is not an error — it is one stalled reader consuming the
    process's memory with nothing to stop it.
    """
    broker = LogBroker(make_settings())

    with pytest.raises(ValueError, match="UNBOUNDED"):
        broker.subscribe(SubscriptionFilter(), queue_size=queue_size)

    # And directly, because `subscribe` is not the only constructor of a Subscriber.
    with pytest.raises(ValueError, match=r"queue_size must be >= 1"):
        Subscriber(flt=SubscriptionFilter(), queue_size=queue_size)

    assert broker.subscriber_count() == 0, "a refused size must not leave a half-registered slot"


async def test_the_configured_maxsize_is_what_the_queue_is_built_with() -> None:
    """``SUBSCRIPTION_QUEUE_MAXSIZE`` reaches the queue rather than being documentation."""
    broker = LogBroker(make_settings(queue_maxsize=7))

    subscriber = broker.subscribe(SubscriptionFilter())

    assert subscriber.queue.maxsize == 7


# =================================================================================================
# Filtering
# =================================================================================================


@pytest.mark.parametrize(
    ("flt", "expected"),
    [
        (SubscriptionFilter(), True),
        (SubscriptionFilter(service="api"), True),
        (SubscriptionFilter(service="worker"), False),
        (SubscriptionFilter(level=LogLevel.ERROR), True),
        (SubscriptionFilter(level=LogLevel.INFO), False),
        # Both supplied: AND, not OR. An OR here would deliver every ERROR from every service to a
        # dashboard that asked for one service's errors, and the response would look plausible.
        (SubscriptionFilter(service="api", level=LogLevel.ERROR), True),
        (SubscriptionFilter(service="api", level=LogLevel.INFO), False),
        (SubscriptionFilter(service="worker", level=LogLevel.ERROR), False),
    ],
)
def test_filters_compose_with_and(flt: SubscriptionFilter, expected: bool) -> None:
    """Supplied fields are AND-composed; omitted fields constrain nothing."""
    entry = make_entry(service="api", level=LogLevel.ERROR)

    assert flt.matches(entry) is expected


def test_a_string_level_is_coerced_to_the_enum() -> None:
    """Because the failure mode of not coercing is a filter that silently matches nothing.

    ``LogLevel.ERROR != "ERROR"`` is ``True``, so a filter holding the raw string would compare
    unequal to every entry ever published and present to the client as "this stream is quiet".
    """
    flt = SubscriptionFilter(service="api", level="ERROR")  # type: ignore[arg-type]

    assert flt.level is LogLevel.ERROR
    assert flt.matches(make_entry(service="api", level=LogLevel.ERROR)) is True
    assert flt.matches(make_entry(service="api", level=LogLevel.WARNING)) is False


def test_an_unknown_string_level_is_refused_rather_than_stored() -> None:
    """A typo becomes a construction error, not a subscription that never yields."""
    with pytest.raises(ValueError):
        SubscriptionFilter(level="EROR")  # type: ignore[arg-type]


def test_matches_everything_reports_the_unfiltered_subscription() -> None:
    assert SubscriptionFilter().matches_everything is True
    assert SubscriptionFilter(service="api").matches_everything is False
    assert SubscriptionFilter(level=LogLevel.ERROR).matches_everything is False


# =================================================================================================
# Fan-out
# =================================================================================================


async def test_publish_enqueues_only_on_matching_subscribers() -> None:
    """One publish, three filters, three answers — decided in the broker, before the queue."""
    broker = LogBroker(make_settings())
    everything = broker.subscribe(SubscriptionFilter())
    api_only = broker.subscribe(SubscriptionFilter(service="api"))
    worker_only = broker.subscribe(SubscriptionFilter(service="worker"))

    entry = make_entry(service="api", level=LogLevel.ERROR)
    delivered = await broker.publish(entry)

    assert delivered == 2, "the unfiltered subscriber and the api one, not the worker one"
    assert drain(everything) == [entry]
    assert drain(api_only) == [entry]
    assert drain(worker_only) == [], "a non-matching entry must never occupy a subscriber's queue"

    stats = broker.stats
    assert stats.published_total == 1
    assert stats.delivered_total == 2
    assert stats.dropped_total == 0
    assert stats.active_subscribers == 3


async def test_publish_with_no_subscribers_is_a_counted_no_op() -> None:
    """The common case by a wide margin, and it must not be an error or a cost."""
    broker = LogBroker(make_settings())

    assert await broker.publish(make_entry()) == 0
    assert broker.stats.published_total == 1
    assert broker.stats.delivered_total == 0


async def test_publish_does_not_suspend() -> None:
    """"Never blocks" is a property of the code, not a hope about how fast the consumers are.

    A callback handed to ``loop.call_soon`` runs the next time the loop regains control. If it has
    not run by the time ``await publish(...)`` returns, ``publish`` never gave the loop control —
    which is the guarantee ``createLog`` relies on to promise that fan-out cannot add latency to a
    committed write.
    """
    broker = LogBroker(make_settings(queue_maxsize=1))
    # A mix that exercises every branch inside the fan-out: a match, a non-match, and an overflow
    # (the second entry into a maxsize=1 queue), so the assertion covers the drop path too.
    broker.subscribe(SubscriptionFilter())
    broker.subscribe(SubscriptionFilter(service="nobody"))

    the_loop_ran: list[bool] = []
    asyncio.get_running_loop().call_soon(lambda: the_loop_ran.append(True))

    await broker.publish(make_entry(entry_id=1))
    await broker.publish(make_entry(entry_id=2))
    await broker.publish(make_entry(entry_id=3))

    assert the_loop_ran == [], (
        "publish suspended: a callback scheduled before it had a chance to run, which means a "
        "slow consumer or a slow Redis can delay a mutation"
    )


async def test_a_filter_that_raises_costs_only_its_own_subscriber() -> None:
    """Invariant 3: fan-out can never break the write path, whatever a subscriber does."""

    class ExplodingFilter(SubscriptionFilter):
        def matches(self, entry: LogEntry) -> bool:
            raise RuntimeError("this filter is broken")

    broker = LogBroker(make_settings())
    healthy = broker.subscribe(SubscriptionFilter())
    broken = broker.subscribe(ExplodingFilter())

    delivered = await broker.publish(make_entry())

    assert delivered == 1, "the healthy subscriber still received the entry"
    assert drain(healthy)[0] is not None
    assert broken.released is True, "the broken subscriber was removed"
    assert broker.subscriber_count() == 1


# =================================================================================================
# Back-pressure: the drop
# =================================================================================================


async def test_a_full_queue_drops_that_subscriber_and_nobody_else() -> None:
    """The whole back-pressure policy, asserted in its three parts.

    1. The slow subscriber is terminated and **told why** (``dropped``, then the sentinel), so its
       resolver can raise ``SLOW_CONSUMER`` instead of the client believing it saw everything.
    2. An unrelated subscriber keeps receiving — a drop is per-subscriber, never a stampede.
    3. The publisher never raises and never blocks: every publish returns and the counters advance.
    """
    broker = LogBroker(make_settings(queue_maxsize=2))
    slow = broker.subscribe(SubscriptionFilter())
    fast = broker.subscribe(SubscriptionFilter())

    # Nobody reads `slow`; `fast` is drained after each publish, so it never fills.
    for index in range(5):
        await broker.publish(make_entry(entry_id=index))
        drain(fast)

    assert slow.released is True
    assert slow.dropped is True, "the drop must be distinguishable from a clean close"
    assert broker.stats.dropped_total == 1

    # The drop DRAINS before enqueuing the sentinel: a sentinel that could not be enqueued (the
    # queue being full is the entire problem) would leave the generator parked forever, which is
    # precisely the outcome dropping exists to prevent.
    assert drain(slow) == [None], "only the terminal sentinel is left, and it did fit"

    assert fast.released is False, "an unrelated subscriber must survive somebody else's overflow"
    assert broker.subscriber_count() == 1
    assert broker.stats.published_total == 5


async def test_a_dropped_subscriber_stops_receiving_further_entries() -> None:
    """Once released, a subscriber is out of the fan-out set — no late writes into a dead queue."""
    broker = LogBroker(make_settings(queue_maxsize=1))
    slow = broker.subscribe(SubscriptionFilter())

    await broker.publish(make_entry(entry_id=1))
    await broker.publish(make_entry(entry_id=2))  # overflows -> drop
    assert slow.released is True

    await broker.publish(make_entry(entry_id=3))

    assert drain(slow) == [None]
    assert broker.stats.delivered_total == 1, "only the entry that fit was ever delivered"


async def test_the_drop_wakes_a_generator_parked_on_the_queue() -> None:
    """The sentinel is what turns a full queue into a terminated stream rather than a hung one.

    A real consumer is parked on ``queue.get()``, which is the state the whole sentinel mechanism
    exists for: the publisher cannot reach it except by putting something in the queue.
    """
    broker = LogBroker(make_settings(queue_maxsize=1))
    subscriber = broker.subscribe(SubscriptionFilter())

    received: list[Optional[LogEntry]] = []

    async def consume() -> None:
        while True:
            item = await subscriber.queue.get()
            received.append(item)
            if item is None:
                return

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)  # let the consumer park on `get()`

    # Three publishes in ONE task with no await between them: the consumer cannot interleave, so
    # the queue provably fills. This is how the overflow is made deterministic rather than raced.
    await broker.publish(make_entry(entry_id=1))
    await broker.publish(make_entry(entry_id=2))
    await broker.publish(make_entry(entry_id=3))

    # Bounded: if the sentinel were missing this would fail in a second rather than hang the suite.
    await asyncio.wait_for(consumer, timeout=5)

    assert received[-1] is None, "the consumer was woken by the terminal sentinel"
    assert subscriber.dropped is True


# =================================================================================================
# Lifecycle
# =================================================================================================


async def test_unsubscribe_is_idempotent() -> None:
    """Only the first call releases. See ``LogBroker.unsubscribe`` for why that is correctness.

    A streaming resolver has several exit paths and more than one routinely runs for the same
    subscription. If the per-connection counter moved per *call*, a socket's slot count would drift
    — down until the cap meant nothing, or up until that socket could never subscribe again.
    """
    broker = LogBroker(make_settings())
    connection = object()
    subscriber = broker.subscribe(SubscriptionFilter(), connection=connection)

    assert broker.connection_count(connection) == 1
    assert broker.unsubscribe(subscriber) is True
    assert broker.unsubscribe(subscriber) is False
    assert broker.unsubscribe(subscriber) is False

    assert broker.subscriber_count() == 0
    assert broker.connection_count(connection) == 0


async def test_a_released_connection_key_is_deleted_rather_than_left_at_zero() -> None:
    """Otherwise the map grows one dead key per socket the server has ever served.

    Worse than the memory: the key is the GraphQL ``Context``, which holds the session factory, so
    a lingering entry keeps a per-connection object graph alive for the life of the process.
    """
    broker = LogBroker(make_settings())
    connection = object()

    first = broker.subscribe(SubscriptionFilter(), connection=connection)
    second = broker.subscribe(SubscriptionFilter(), connection=connection)
    assert broker.connection_count(connection) == 2

    broker.unsubscribe(first)
    assert broker.connection_count(connection) == 1
    broker.unsubscribe(second)

    assert broker.connection_count(connection) == 0
    assert connection not in broker._per_connection, (  # noqa: SLF001 - the point of the test
        "the entry must be deleted at zero, not left behind holding the connection object"
    )


async def test_the_per_connection_cap_rejects_the_one_over() -> None:
    """transport-ws allows unlimited subscribe messages on one socket; this is the bound."""
    broker = LogBroker(make_settings(max_per_connection=2))
    connection = object()

    broker.subscribe(SubscriptionFilter(), connection=connection)
    broker.subscribe(SubscriptionFilter(), connection=connection)

    with pytest.raises(SubscriptionLimitExceeded) as caught:
        broker.subscribe(SubscriptionFilter(), connection=connection)

    assert caught.value.held == 2
    assert caught.value.limit == 2
    assert broker.subscriber_count() == 2, "the rejected subscription left nothing registered"


async def test_the_cap_is_per_connection_not_per_process() -> None:
    """Two sockets each get their own budget; one busy client cannot starve another."""
    broker = LogBroker(make_settings(max_per_connection=1))
    first, second = object(), object()

    broker.subscribe(SubscriptionFilter(), connection=first)
    broker.subscribe(SubscriptionFilter(), connection=second)

    assert broker.subscriber_count() == 2
    with pytest.raises(SubscriptionLimitExceeded):
        broker.subscribe(SubscriptionFilter(), connection=first)


async def test_a_subscriber_with_no_connection_is_not_capped() -> None:
    """No connection means no multiplexing, so there is nothing for a per-connection cap to bound."""
    broker = LogBroker(make_settings(max_per_connection=1))

    for _ in range(5):
        broker.subscribe(SubscriptionFilter())

    assert broker.subscriber_count() == 5


async def test_close_all_subscribers_terminates_everything_cleanly() -> None:
    """The shutdown sweep: without it, every parked generator waits forever and uvicorn will not exit.

    ``dropped`` stays False, which is the whole difference the resolver branches on: a shutdown owes
    the client a clean completion, a slow-consumer drop owes it an error.
    """
    broker = LogBroker(make_settings())
    connection = object()
    unfiltered = broker.subscribe(SubscriptionFilter(), connection=connection)
    api_only = broker.subscribe(SubscriptionFilter(service="api"))
    entry = make_entry(service="api")
    await broker.publish(entry)

    closed = broker.close_all_subscribers()

    assert closed == 2
    assert broker.subscriber_count() == 0
    assert broker.connection_count(connection) == 0
    for subscriber in (unfiltered, api_only):
        assert subscriber.released is True
        assert subscriber.dropped is False, "a shutdown is not a slow-consumer drop"
        # The sentinel goes in BEHIND whatever was already delivered — a shutdown does not drain,
        # because those entries were legitimately in flight and are the client's.
        assert drain(subscriber) == [entry, None]

    assert broker.close_all_subscribers() == 0, "idempotent: nothing left to close"


async def test_close_all_subscribers_makes_room_for_the_sentinel_in_a_full_queue() -> None:
    """A terminal frame that arrives beats the last entry of a stream that is ending anyway."""
    broker = LogBroker(make_settings(queue_maxsize=2))
    subscriber = broker.subscribe(SubscriptionFilter())
    await broker.publish(make_entry(entry_id=1))
    await broker.publish(make_entry(entry_id=2))
    assert subscriber.queued == 2, "the queue is exactly full, with no room for a sentinel"

    broker.close_all_subscribers()

    queued = drain(subscriber)
    assert queued[-1] is None, "the sentinel got in"
    assert len(queued) == 2, "exactly one entry made way for it"
    assert subscriber.dropped is False


# =================================================================================================
# The wire codec
# =================================================================================================


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(make_entry(), id="minimal"),
        pytest.param(
            make_entry(metadata={"host": "web-1", "latency_ms": 12, "ok": True, "tags": ["a"]}),
            id="metadata-object",
        ),
        # SQL NULL and the JSONB scalar `null` both arrive in Python as None (see the `none_as_null`
        # note on LogEntryORM), so by the time an entry reaches the broker the distinction has
        # already collapsed. What matters here is that None survives as None rather than becoming
        # `{}` or vanishing — a subscriber must see the same `metadata: null` a query would return.
        pytest.param(make_entry(metadata=None), id="metadata-null"),
        pytest.param(make_entry(metadata={}), id="metadata-empty-object"),
        pytest.param(make_entry(trace_id=None), id="trace-null"),
        pytest.param(make_entry(trace_id="abcdef0123456789"), id="trace-present"),
        pytest.param(make_entry(level=LogLevel.CRITICAL), id="level-critical"),
        pytest.param(make_entry(message="unicode: ✓ 日本語 \" ' \\ {}"), id="message-escapes"),
        pytest.param(make_entry(entry_id=9_223_372_036_854_775_807), id="bigserial-max"),
    ],
)
def test_an_entry_round_trips_through_the_wire_format(entry: LogEntry) -> None:
    """A subscriber on another worker must receive exactly what a local one does.

    Field-by-field equality rather than ``==`` on the whole object *and* the whole-object check, so
    a failure names the field that drifted instead of printing two dataclasses side by side.
    """
    decoded = decode_event(encode_event(entry, origin="worker-1"))

    assert decoded is not None
    assert decoded.origin == "worker-1"
    restored = decoded.entry

    assert restored.id == entry.id
    assert isinstance(restored.id, str), "GraphQL ID is a string on both sides of the wire"
    assert restored.timestamp == entry.timestamp
    assert restored.timestamp.tzinfo is not None, "a naive value is unequal to every aware one"
    assert restored.service == entry.service
    assert restored.level is entry.level
    assert restored.message == entry.message
    assert restored.metadata == entry.metadata
    assert restored.trace_id == entry.trace_id
    assert fields_of(restored) == fields_of(entry), "nothing else drifted either"


def test_metadata_none_and_metadata_empty_object_stay_distinguishable() -> None:
    """The two collapse into each other under a lazy codec (``metadata or {}``), and must not."""
    absent = decode_event(encode_event(make_entry(metadata=None), origin="w"))
    empty = decode_event(encode_event(make_entry(metadata={}), origin="w"))

    assert absent is not None and empty is not None
    assert absent.entry.metadata is None
    assert empty.entry.metadata == {}


def test_a_non_utc_timestamp_round_trips_to_the_same_instant() -> None:
    """The offset is normalised to UTC; the *instant* — which is what equality compares — is kept.

    ``createLog`` accepts a client-supplied timestamp, so an entry carrying ``+05:30`` really can
    reach the broker. Normalising rather than preserving the zone is the deliberate choice: the
    column stores instants and a subscriber has no use for the wall-clock zone the writer was in.
    """
    india = timezone(timedelta(hours=5, minutes=30))
    entry = make_entry(timestamp=datetime(2026, 7, 27, 15, 0, 0, tzinfo=india))

    decoded = decode_event(encode_event(entry, origin="w"))

    assert decoded is not None
    assert decoded.entry.timestamp == entry.timestamp, "same instant"
    assert decoded.entry.timestamp.utcoffset() == timedelta(0), "expressed in UTC"


def test_microsecond_precision_survives() -> None:
    """A stream ordered by timestamp would otherwise coalesce entries written in the same second."""
    entry = make_entry(timestamp=ANCHOR.replace(microsecond=123456))

    decoded = decode_event(encode_event(entry, origin="w"))

    assert decoded is not None
    assert decoded.entry.timestamp.microsecond == 123456


def test_the_envelope_carries_a_version_and_a_kind() -> None:
    """Both exist so a rolling deploy can drop what it does not understand instead of guessing."""
    payload = json.loads(encode_event(make_entry(), origin="worker-7"))

    assert payload["v"] == EVENT_FORMAT_VERSION
    assert payload["kind"] == EVENT_KIND_LOG
    assert payload["origin"] == "worker-7"


@pytest.mark.parametrize(
    ("raw", "why"),
    [
        (None, "nothing at all"),
        (b"", "an empty message"),
        (b"not json", "not JSON"),
        (b"[1, 2, 3]", "JSON, but not an object"),
        (b'{"v": 99, "kind": "log", "origin": "w", "entry": {}}', "an unknown envelope version"),
        (b'{"v": 1, "kind": "order", "origin": "w", "entry": {}}', "a kind this build cannot decode"),
        (b'{"v": 1, "kind": "log", "entry": {}}', "no origin, so echo suppression is impossible"),
        (b'{"v": 1, "kind": "log", "origin": "w"}', "no entry"),
        (b'{"v": 1, "kind": "log", "origin": "w", "entry": {"id": "1"}}', "a truncated entry"),
    ],
)
def test_decode_rejects_rather_than_raises(raw: bytes | None, why: str) -> None:
    """The channel is writable by anything with the Redis credentials.

    A malformed message must cost one debug line, not the reader task — because the reader task
    dying is the whole cross-worker bridge dying, silently, until somebody notices that
    subscriptions stopped crossing workers.
    """
    assert decode_event(raw) is None, f"expected {why!r} to be rejected"


def test_decode_rejects_a_level_outside_the_enum() -> None:
    """A peer publishing a severity this build does not know is dropped, not smuggled in.

    ``LogLevel`` is the published contract; an entry the schema cannot express must never reach a
    subscriber, because the failure would then happen during serialisation, mid-stream, on a socket
    that has already been told the subscription succeeded.
    """
    payload = encode_event(make_entry(), origin="w").replace('"level":"INFO"', '"level":"TRACE"')

    assert decode_event(payload) is None


def test_decode_accepts_both_bytes_and_str() -> None:
    """redis-py hands back ``bytes`` without ``decode_responses``, and ``str`` with it.

    Whether that flag is set is a decision belonging to whoever builds the client (C7 and C9 will
    also use it), so the codec must not have an opinion about it.
    """
    payload = encode_event(make_entry(entry_id=5), origin="w")

    assert decode_event(payload) is not None
    assert decode_event(payload.encode("utf-8")) is not None
    assert decode_event(bytearray(payload.encode("utf-8"))) is not None
