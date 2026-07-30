"""C12's half of the broker: the order filter, the order envelope, and the kind routing.

``tests/unit/test_broker.py`` owns the machinery both streams share — bounded queues, the drop
policy, the terminal sentinel, idempotent release. None of that is re-tested here. What is new at
C12 is exactly three things, and this module is about those three:

* :class:`~src.broker.OrderSubscriptionFilter` — three optional dimensions, AND-composed;
* :func:`~src.broker.encode_order_event` under the ``order`` kind, and the ``_DECODERS`` table that
  makes an **unknown** kind a drop rather than a guess;
* the ``kind`` tag on a subscriber's filter, which :meth:`~src.broker.LogBroker._fan_out` routes on.

.. rubric:: THE ROUTING TESTS ARE THE POINT OF THIS FILE, AND THEY HAVE TWO FAILURE MODES

It is tempting to read the kind check as belt-and-braces on top of the filters. It is not, and both
directions are worth stating because they break differently:

* **A log entry offered to an order subscriber.** An *unfiltered* ``OrderSubscriptionFilter`` never
  touches an attribute in ``matches()`` — all three constraints are ``None``, so it returns ``True``
  — and would therefore **accept a ``LogEntry``**, which would then be serialised against
  ``OrderEvent!`` on a live socket. A *filtered* one reads ``event.order_id``, raises
  ``AttributeError``, and the fan-out's "a broken filter costs only its own subscriber" rule would
  **drop a healthy order subscription** the first time anybody wrote a log line.
* **An order event offered to a log subscriber.** ``OrderEvent`` genuinely has ``service`` and
  ``level``, so ``SubscriptionFilter`` matches it happily — filtered or not. Nothing raises and
  nothing is dropped; ``logStream`` simply starts emitting order events.

So the kind check is not redundant with the filters in either direction, and no assertion about
filter *logic* would notice its removal. :func:`test_a_log_publish_neither_reaches_nor_breaks_an_order_subscriber`
and :func:`test_an_order_publish_never_reaches_a_log_subscriber` are what would.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest
import strawberry

from src.broker import (
    EVENT_FORMAT_VERSION,
    EVENT_KIND_LOG,
    EVENT_KIND_ORDER,
    LogBroker,
    OrderSubscriptionFilter,
    RemoteEvent,
    SubscriptionFilter,
    _DECODERS,
    decode_event,
    encode_event,
    encode_order_event,
)
from src.graphql.ecommerce import OrderEvent
from src.graphql.enums import LogLevel, OrderStatus
from tests.unit.test_broker import ANCHOR, drain, make_entry, make_settings


def make_order_event(
    *,
    event_id: int = 1,
    order_id: str = "ord-60000",
    user_id: str = "usr-900",
    status: OrderStatus = OrderStatus.CREATED,
    service: str = "order-service",
    level: LogLevel = LogLevel.INFO,
    metadata: Optional[dict[str, Any]] = None,
    trace_id: Optional[str] = None,
    offset_seconds: int = 0,
    timestamp: Optional[datetime] = None,
) -> OrderEvent:
    """One published transition, shaped exactly as ``OrderEvent.from_orm`` would produce it."""
    return OrderEvent(
        id=strawberry.ID(str(event_id)),
        timestamp=timestamp if timestamp is not None else ANCHOR + timedelta(seconds=offset_seconds),
        service=service,
        level=level,
        trace_id=trace_id,
        order_id=order_id,
        user_id=user_id,
        status=status,
        metadata=metadata,
    )


def fields_of(event: OrderEvent) -> tuple[Any, ...]:
    """The event as a comparable tuple.

    Used instead of ``==`` on two distinct :class:`OrderEvent` objects for the reason
    ``tests/unit/test_broker.py`` gives: whether ``@strawberry.type`` generates ``__eq__`` is a
    property of the library, and a change to it should not look like a round-tripping bug.
    """
    return (
        event.id,
        event.timestamp,
        event.service,
        event.level,
        event.trace_id,
        event.order_id,
        event.user_id,
        event.status,
        event.metadata,
    )


# =================================================================================================
# OrderSubscriptionFilter — "filtering by order id, status, and/or user" (spec §3 Feature Area C)
# =================================================================================================

#: The event every filter below is evaluated against. Every decoy differs from it in exactly one
#: dimension, which is what makes each parametrisation isolate one constraint.
SUBJECT = dict(order_id="ord-60000", user_id="usr-900", status=OrderStatus.SHIPPED)


@pytest.mark.parametrize(
    ("flt", "expected", "why"),
    [
        # --- nothing constrained ---------------------------------------------------------------
        (OrderSubscriptionFilter(), True, "the unfiltered firehose takes everything"),
        # --- one dimension at a time -----------------------------------------------------------
        (OrderSubscriptionFilter(order_id="ord-60000"), True, "order id matches"),
        (OrderSubscriptionFilter(order_id="ord-60001"), False, "a different order"),
        (OrderSubscriptionFilter(status=OrderStatus.SHIPPED), True, "status matches"),
        (OrderSubscriptionFilter(status=OrderStatus.DELIVERED), False, "the next status along"),
        (OrderSubscriptionFilter(user_id="usr-900"), True, "user matches"),
        (OrderSubscriptionFilter(user_id="usr-901"), False, "a different customer"),
        # --- two dimensions: AND, so one miss is a miss ------------------------------------------
        (
            OrderSubscriptionFilter(order_id="ord-60000", status=OrderStatus.SHIPPED),
            True,
            "both halves hold",
        ),
        (
            OrderSubscriptionFilter(order_id="ord-60000", status=OrderStatus.PACKED),
            False,
            "right order, wrong status — an OR would deliver this",
        ),
        (
            OrderSubscriptionFilter(order_id="ord-60001", status=OrderStatus.SHIPPED),
            False,
            "wrong order, right status — an OR would deliver this too",
        ),
        (
            OrderSubscriptionFilter(order_id="ord-60000", user_id="usr-900"),
            True,
            "order and user",
        ),
        (
            OrderSubscriptionFilter(order_id="ord-60000", user_id="usr-901"),
            False,
            "right order, wrong user",
        ),
        (
            OrderSubscriptionFilter(status=OrderStatus.SHIPPED, user_id="usr-900"),
            True,
            "status and user",
        ),
        (
            OrderSubscriptionFilter(status=OrderStatus.CANCELLED, user_id="usr-900"),
            False,
            "right user, wrong status",
        ),
        # --- all three ----------------------------------------------------------------------------
        (
            OrderSubscriptionFilter(
                order_id="ord-60000", status=OrderStatus.SHIPPED, user_id="usr-900"
            ),
            True,
            "every dimension holds",
        ),
        (
            OrderSubscriptionFilter(
                order_id="ord-60001", status=OrderStatus.SHIPPED, user_id="usr-900"
            ),
            False,
            "two of three is not a match",
        ),
        (
            OrderSubscriptionFilter(
                order_id="ord-60000", status=OrderStatus.PACKED, user_id="usr-900"
            ),
            False,
            "two of three, the other way",
        ),
        (
            OrderSubscriptionFilter(
                order_id="ord-60000", status=OrderStatus.SHIPPED, user_id="usr-901"
            ),
            False,
            "two of three, the third way",
        ),
    ],
)
def test_the_three_dimensions_are_and_composed(
    flt: OrderSubscriptionFilter, expected: bool, why: str
) -> None:
    """Every combination, against one event, with each decoy off by exactly one dimension.

    An OR implementation is the plausible wrong answer here — events would arrive, and they would
    each match *something* the client asked for — so the two-dimension rows deliberately include
    both "right order, wrong status" and "wrong order, right status".
    """
    assert flt.matches(make_order_event(**SUBJECT)) is expected, why


@pytest.mark.parametrize(
    ("filter_id", "event_id"),
    [
        ("ord-6000", "ord-60001"),
        ("ord-60001", "ord-6000"),
        ("usr-9", "usr-90"),
    ],
)
def test_identifier_matching_is_exact_and_never_a_prefix(filter_id: str, event_id: str) -> None:
    """A prefix match here would leak one customer's orders into another's stream.

    Both directions, because ``startswith`` in either operand is a plausible mistake and each
    produces the leak from a different side. ``ord-60000`` and ``ord-60001`` are adjacent ids from
    the generated corpus, so this is not a contrived pair.
    """
    assert not OrderSubscriptionFilter(order_id=filter_id).matches(
        make_order_event(order_id=event_id)
    )
    assert not OrderSubscriptionFilter(user_id=filter_id).matches(
        make_order_event(user_id=event_id)
    )


def test_matches_everything_reports_the_unfiltered_subscription() -> None:
    """``matches_everything`` has to consider all three fields, not the first one it was written for."""
    assert OrderSubscriptionFilter().matches_everything is True
    assert OrderSubscriptionFilter(order_id="ord-1").matches_everything is False
    assert OrderSubscriptionFilter(status=OrderStatus.PAID).matches_everything is False
    assert OrderSubscriptionFilter(user_id="usr-1").matches_everything is False


def test_a_string_status_is_coerced_to_the_enum() -> None:
    """``OrderStatus.SHIPPED != "SHIPPED"``, so an uncoerced filter matches nothing at all.

    That failure is silent: the subscription opens, registers a queue, and stays quiet forever —
    indistinguishable from an order nothing is happening to. Coercion in ``__post_init__`` is what
    turns it into either a match or a loud ``ValueError``.
    """
    coerced = OrderSubscriptionFilter(status="SHIPPED")  # type: ignore[arg-type]

    assert coerced.status is OrderStatus.SHIPPED
    assert coerced.matches(make_order_event(status=OrderStatus.SHIPPED))
    assert not coerced.matches(make_order_event(status=OrderStatus.DELIVERED))


def test_an_unknown_string_status_is_refused_rather_than_stored() -> None:
    """A typo must raise where it was written, not become a stream that is quiet forever."""
    with pytest.raises(ValueError):
        OrderSubscriptionFilter(status="SHIPPPED")  # type: ignore[arg-type]


def test_the_filter_is_frozen() -> None:
    """A subscriber's filter is read by the publisher on the publisher's task; it must not move."""
    flt = OrderSubscriptionFilter(order_id="ord-60000")

    with pytest.raises(dataclasses.FrozenInstanceError):
        flt.order_id = "ord-60001"  # type: ignore[misc]


# =================================================================================================
# The kind tag — a class attribute, deliberately not a field
# =================================================================================================


def test_the_two_filters_carry_different_kinds() -> None:
    """The whole of the routing discriminator, and the one line that keeps the streams apart."""
    assert SubscriptionFilter.kind == EVENT_KIND_LOG
    assert OrderSubscriptionFilter.kind == EVENT_KIND_ORDER
    assert SubscriptionFilter.kind != OrderSubscriptionFilter.kind
    assert OrderSubscriptionFilter().kind == EVENT_KIND_ORDER, "readable off an instance too"


@pytest.mark.parametrize("filter_class", [SubscriptionFilter, OrderSubscriptionFilter])
def test_the_kind_is_not_a_constructor_argument(filter_class: type) -> None:
    """A caller must not be able to route a log filter onto the order stream.

    ``kind`` is a plain class attribute rather than an annotated field precisely so it stays out of
    the generated ``__init__``. Annotating it would make ``SubscriptionFilter(kind="order")`` legal,
    and that call would hand a filter reading ``entry.service`` a stream of ``OrderEvent`` — which
    it would happily match, because an order event has a service.
    """
    field_names = {field.name for field in dataclasses.fields(filter_class)}
    assert "kind" not in field_names

    with pytest.raises(TypeError):
        filter_class(kind="something-else")


def test_the_kind_is_absent_from_equality_and_repr() -> None:
    """The other consequence of not being a field, asserted so the reason is not lost."""
    assert OrderSubscriptionFilter(order_id="a") == OrderSubscriptionFilter(order_id="a")
    assert "kind" not in repr(OrderSubscriptionFilter(order_id="a"))


# =================================================================================================
# The wire format under the order kind
# =================================================================================================


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(make_order_event(), id="minimal"),
        pytest.param(
            make_order_event(
                event_id=987_654_321,
                order_id="ord-60123",
                user_id="usr-42",
                status=OrderStatus.REFUNDED,
                level=LogLevel.WARNING,
                service="partner-feed",
                trace_id="abcdef0123456789",
                metadata={"carrier": "dhl", "attempt": 2, "ok": True, "tags": ["x"]},
            ),
            id="every-field-populated",
        ),
        pytest.param(make_order_event(metadata=None), id="metadata-null"),
        pytest.param(make_order_event(metadata={}), id="metadata-empty-object"),
        pytest.param(make_order_event(trace_id=None), id="trace-null"),
        pytest.param(make_order_event(status=OrderStatus.CANCELLED), id="terminal-status"),
        pytest.param(make_order_event(order_id="ord-✓-日本語"), id="unicode-identifier"),
        pytest.param(make_order_event(event_id=9_223_372_036_854_775_807), id="bigserial-max"),
    ],
)
def test_an_order_event_round_trips_through_the_wire_format(event: OrderEvent) -> None:
    """A subscriber on another worker must receive exactly what a local one does.

    Field by field *and* as a whole, so a failure names the field that drifted. ``orderId``,
    ``userId`` and ``status`` matter more than the rest: they are what the **receiving** broker's
    filter matches on, so losing one would not merely truncate an event — it would make it
    unfilterable, and a subscription scoped to one order would start receiving somebody else's.
    """
    decoded = decode_event(encode_order_event(event, origin="worker-1"))

    assert decoded is not None
    assert decoded.origin == "worker-1"
    assert decoded.kind == EVENT_KIND_ORDER

    restored = decoded.entry
    assert isinstance(restored, OrderEvent)
    assert restored.id == event.id
    assert isinstance(restored.id, str), "GraphQL ID is a string on both sides of the wire"
    assert restored.timestamp == event.timestamp
    assert restored.timestamp.tzinfo is not None, "a naive value is unequal to every aware one"
    assert restored.service == event.service
    assert restored.level is event.level
    assert restored.trace_id == event.trace_id
    assert restored.order_id == event.order_id
    assert restored.user_id == event.user_id
    assert restored.status is event.status
    assert restored.metadata == event.metadata
    assert fields_of(restored) == fields_of(event), "nothing else drifted either"


def test_metadata_none_and_metadata_empty_object_stay_distinguishable() -> None:
    """A lazy codec (``metadata or {}``) collapses these two, and a subscriber must see the same
    ``metadata: null`` a query would return."""
    absent = decode_event(encode_order_event(make_order_event(metadata=None), origin="w"))
    empty = decode_event(encode_order_event(make_order_event(metadata={}), origin="w"))

    assert absent is not None and empty is not None
    assert absent.entry.metadata is None
    assert empty.entry.metadata == {}


def test_a_non_utc_timestamp_round_trips_to_the_same_instant() -> None:
    """``createOrderEvent`` accepts a client-supplied timestamp, so this really can reach the wire.

    The offset is normalised and the *instant* — which is what equality compares — is kept.
    """
    india = timezone(timedelta(hours=5, minutes=30))
    event = make_order_event(timestamp=datetime(2026, 7, 27, 15, 0, 0, tzinfo=india))

    decoded = decode_event(encode_order_event(event, origin="w"))

    assert decoded is not None
    assert decoded.entry.timestamp == event.timestamp, "same instant"
    assert decoded.entry.timestamp.utcoffset() == timedelta(0), "expressed in UTC"
    assert decoded.entry.timestamp.tzinfo is not None


def test_microsecond_precision_survives() -> None:
    """Order transitions written in the same second would otherwise coalesce in a timeline."""
    event = make_order_event(timestamp=ANCHOR.replace(microsecond=123456))

    decoded = decode_event(encode_order_event(event, origin="w"))

    assert decoded is not None
    assert decoded.entry.timestamp.microsecond == 123456


def test_the_order_envelope_carries_the_order_kind_and_the_shared_version() -> None:
    """One envelope, two kinds — C12 added a kind rather than a second wire format."""
    payload = json.loads(encode_order_event(make_order_event(), origin="worker-7"))

    assert payload["v"] == EVENT_FORMAT_VERSION
    assert payload["kind"] == EVENT_KIND_ORDER
    assert payload["origin"] == "worker-7"
    assert isinstance(payload["entry"], dict)
    assert set(payload) == {"v", "kind", "origin", "entry"}, "no field crept into the envelope"


def test_both_kinds_share_one_envelope_shape() -> None:
    """The discriminator is the only difference, which is what makes one reader serve both."""
    log_envelope = json.loads(encode_event(make_entry(), origin="w"))
    order_envelope = json.loads(encode_order_event(make_order_event(), origin="w"))

    assert set(log_envelope) == set(order_envelope)
    assert log_envelope["kind"] == EVENT_KIND_LOG
    assert order_envelope["kind"] == EVENT_KIND_ORDER
    assert log_envelope["v"] == order_envelope["v"]


def test_the_body_carries_every_published_scalar() -> None:
    """``orderStatusStream`` issues zero database reads, so the payload has to be the whole event.

    A body missing a field would be a subscriber on another worker that either renders a null the
    local one does not, or reaches for the database to fill the gap — and reaching for the database
    is the thing this stream is built not to do.
    """
    body = json.loads(encode_order_event(make_order_event(), origin="w"))["entry"]

    assert set(body) == {
        "id",
        "timestamp",
        "service",
        "level",
        "trace_id",
        "order_id",
        "user_id",
        "status",
        "metadata",
    }
    assert "payments" not in body and "userActivity" not in body and "relatedLogs" not in body, (
        "the traversals are resolvers, not state — serialising them would put a point-in-time "
        "snapshot of another table into an event payload"
    )


def test_two_encodings_of_one_event_are_byte_identical() -> None:
    """Key order is fixed, so a payload can be compared or deduplicated as a string."""
    event = make_order_event()

    assert encode_order_event(event, origin="w") == encode_order_event(event, origin="w")


def test_a_channel_message_arrives_as_bytes_and_decodes_the_same() -> None:
    """``create_redis_client`` leaves ``decode_responses`` off, so the reader is handed bytes."""
    payload = encode_order_event(make_order_event(order_id="ord-77"), origin="w")

    from_str = decode_event(payload)
    from_bytes = decode_event(payload.encode("utf-8"))

    assert from_str is not None and from_bytes is not None
    assert fields_of(from_bytes.entry) == fields_of(from_str.entry)
    assert from_bytes.kind == EVENT_KIND_ORDER


# =================================================================================================
# An unknown kind is DROPPED, never guessed — the _DECODERS claim
# =================================================================================================


def envelope(kind: Any, body: dict[str, Any], *, version: int = EVENT_FORMAT_VERSION) -> str:
    """A hand-built envelope, so a kind this build does not publish can be put on the wire."""
    return json.dumps({"v": version, "kind": kind, "origin": "peer", "entry": body})


ORDER_BODY = json.loads(encode_order_event(make_order_event(), origin="w"))["entry"]
LOG_BODY = json.loads(encode_event(make_entry(), origin="w"))["entry"]


def test_the_decoder_table_holds_exactly_the_published_kinds() -> None:
    """Adding a kind is one row here plus one encoder. Forgetting the row fails closed."""
    assert set(_DECODERS) == {EVENT_KIND_LOG, EVENT_KIND_ORDER}

    with pytest.raises(TypeError):
        _DECODERS["shipment"] = object  # type: ignore[index]


@pytest.mark.parametrize(
    ("kind", "why"),
    [
        ("shipment", "a future binary's third kind, met during a rolling deploy"),
        ("payment", "a plausible-looking kind that this build does not publish"),
        ("Order", "the right word in the wrong case — kinds are matched exactly"),
        ("", "the empty string is not a kind"),
        (None, "a kind that is absent rather than unknown"),
        (7, "a kind that is not even a string"),
        ({"kind": "order"}, "a kind that is a nested object"),
    ],
)
def test_an_unknown_kind_is_dropped_rather_than_guessed(kind: Any, why: str) -> None:
    """**The whole reason ``_DECODERS`` is a table and not an if/elif chain.**

    The body handed in is a perfectly valid *order* body, so a decoder that fell through to "well,
    it parses as an order event" would return one. That is precisely the guess this must not make:
    during a rolling deploy two versions of this process share the channel, and a guess there means
    one worker fabricating events for a stream a client is watching.
    """
    assert decode_event(envelope(kind, ORDER_BODY)) is None, why


def test_a_valid_log_body_under_an_unknown_kind_is_not_read_as_a_log() -> None:
    """The same claim from the other side: the *body* being decodable is not permission to decode.

    Dispatch is on the kind alone. A reader that tried each decoder in turn would accept this and
    deliver a ``LogEntry`` to whichever stream the unknown kind was meant for.
    """
    assert decode_event(envelope("telemetry", LOG_BODY)) is None


def test_a_kind_key_that_is_missing_entirely_is_dropped() -> None:
    """``payload.get("kind")`` is ``None``, which is not in the table — the structural default."""
    without_kind = json.dumps({"v": EVENT_FORMAT_VERSION, "origin": "p", "entry": ORDER_BODY})

    assert decode_event(without_kind) is None


def test_an_order_envelope_from_a_future_format_version_is_dropped() -> None:
    """Version and kind are independent gates; a known kind at an unknown version is still a drop."""
    newer = envelope(EVENT_KIND_ORDER, ORDER_BODY, version=EVENT_FORMAT_VERSION + 1)

    assert decode_event(newer) is None
    assert decode_event(envelope(EVENT_KIND_ORDER, ORDER_BODY, version=0)) is None


@pytest.mark.parametrize(
    ("mutate", "why"),
    [
        pytest.param({"status": "SHIPPPED"}, "a status outside OrderStatus", id="bad-status"),
        pytest.param({"level": "TRACE"}, "a level outside LogLevel", id="bad-level"),
        pytest.param({"timestamp": "not-a-time"}, "an unparseable instant", id="bad-timestamp"),
    ],
)
def test_a_body_the_published_schema_cannot_express_is_dropped(
    mutate: dict[str, Any], why: str
) -> None:
    """Dropping beats delivering: the failure would otherwise happen during serialisation,
    mid-stream, on a socket that has already been told the subscription succeeded."""
    body = dict(ORDER_BODY)
    body.update(mutate)

    assert decode_event(envelope(EVENT_KIND_ORDER, body)) is None, why


@pytest.mark.parametrize("field", ["order_id", "user_id", "id", "service", "status", "timestamp"])
def test_a_body_missing_a_required_field_is_dropped(field: str) -> None:
    """``order_id`` and ``user_id`` in particular: a body missing one is not slightly incomplete,
    it is **unfilterable**, and delivering it would mean a subscription scoped to a single order
    receiving somebody else's."""
    body = {key: value for key, value in ORDER_BODY.items() if key != field}

    assert decode_event(envelope(EVENT_KIND_ORDER, body)) is None


@pytest.mark.parametrize("nullable", ["trace_id", "metadata"])
def test_a_body_missing_a_nullable_field_still_decodes(nullable: str) -> None:
    """The counterpart: the two ``.get`` fields are genuinely optional, so their absence is data.

    Without this the test above would also pass against a codec that required all nine keys — and
    that codec would drop every untraced event on the floor.
    """
    body = {key: value for key, value in ORDER_BODY.items() if key != nullable}

    decoded = decode_event(envelope(EVENT_KIND_ORDER, body))

    assert decoded is not None
    assert getattr(decoded.entry, nullable) is None


def test_the_remote_envelope_defaults_to_the_log_kind() -> None:
    """C6-era construction sites still read as "a log envelope" without being edited."""
    assert RemoteEvent(origin="w", entry=make_entry()).kind == EVENT_KIND_LOG
    assert RemoteEvent(origin="w", entry=make_order_event(), kind=EVENT_KIND_ORDER).kind == (
        EVENT_KIND_ORDER
    )


# =================================================================================================
# Kind routing in the fan-out — the two failure modes the module docstring names
# =================================================================================================


async def test_a_log_publish_neither_reaches_nor_breaks_an_order_subscriber() -> None:
    """Both order-side failure modes at once, because they need different filters to surface.

    * The **unfiltered** order subscriber would *receive* the ``LogEntry``: with all three
      constraints ``None``, ``OrderSubscriptionFilter.matches`` returns ``True`` without ever
      touching an attribute, so nothing raises and a log line lands on ``orderStatusStream``.
    * The **filtered** one would be *dropped*: ``event.order_id`` raises ``AttributeError``, the
      fan-out reads that as a broken subscriber, and a healthy subscription dies the first time
      anybody writes a log.

    Neither is visible in any assertion about filter logic, and only the kind check prevents them.
    """
    broker = LogBroker(make_settings())
    firehose = broker.subscribe(OrderSubscriptionFilter())
    scoped = broker.subscribe(OrderSubscriptionFilter(order_id="ord-60000"))
    watcher = broker.subscribe(SubscriptionFilter())

    for index in range(5):
        delivered = await broker.publish(make_entry(entry_id=index, service="api"))
        assert delivered == 1, "only the log subscriber is a candidate"

    assert drain(firehose) == [], "an unfiltered order subscriber received a LogEntry"
    assert drain(scoped) == []
    assert len(drain(watcher)) == 5

    assert scoped.released is False and scoped.dropped is False, (
        "the order subscription was dropped by log traffic — the filter was consulted with a "
        "LogEntry and raised"
    )
    assert firehose.released is False and firehose.dropped is False
    assert broker.stats.dropped_total == 0
    assert broker.subscriber_count() == 3


async def test_an_order_publish_never_reaches_a_log_subscriber() -> None:
    """The other direction, where **nothing raises** — which is what makes it easy to miss.

    ``OrderEvent`` really has ``service`` and ``level``, so ``SubscriptionFilter`` matches it
    happily. A log subscriber filtered on ``order-service`` is the realistic case: it is watching
    the service that emits these events, and without the kind check its ``logStream`` would start
    emitting ``OrderEvent`` objects against a ``LogEntry!`` selection.
    """
    broker = LogBroker(make_settings())
    unfiltered = broker.subscribe(SubscriptionFilter())
    by_service = broker.subscribe(SubscriptionFilter(service="order-service"))
    order_watcher = broker.subscribe(OrderSubscriptionFilter())

    delivered = await broker.publish_order_event(make_order_event(service="order-service"))

    assert delivered == 1, "only the order subscriber is a candidate"
    assert drain(unfiltered) == []
    assert drain(by_service) == [], (
        "a logStream subscriber watching order-service received an OrderEvent"
    )
    assert len(drain(order_watcher)) == 1
    assert broker.stats.dropped_total == 0


async def test_the_order_filter_is_applied_at_enqueue_time_across_subscribers() -> None:
    """One publish stream, four order subscribers, four different subsets — evaluated per
    subscriber before the enqueue, so a narrow subscription's queue depth reflects only its own
    traffic."""
    broker = LogBroker(make_settings())
    everything = broker.subscribe(OrderSubscriptionFilter())
    one_order = broker.subscribe(OrderSubscriptionFilter(order_id="ord-A"))
    one_user = broker.subscribe(OrderSubscriptionFilter(user_id="usr-2"))
    shipped_only = broker.subscribe(OrderSubscriptionFilter(status=OrderStatus.SHIPPED))

    events = [
        make_order_event(event_id=1, order_id="ord-A", user_id="usr-1", status=OrderStatus.CREATED),
        make_order_event(event_id=2, order_id="ord-B", user_id="usr-2", status=OrderStatus.SHIPPED),
        make_order_event(event_id=3, order_id="ord-A", user_id="usr-2", status=OrderStatus.SHIPPED),
    ]
    for event in events:
        await broker.publish_order_event(event)

    assert [event.id for event in drain(everything)] == ["1", "2", "3"]
    assert [event.id for event in drain(one_order)] == ["1", "3"]
    assert [event.id for event in drain(one_user)] == ["2", "3"]
    assert [event.id for event in drain(shipped_only)] == ["2", "3"]


async def test_publish_order_event_counts_only_order_subscribers() -> None:
    """The return value is the fan-out, and log subscribers are not part of this one."""
    broker = LogBroker(make_settings())
    for _ in range(3):
        broker.subscribe(SubscriptionFilter())
    for _ in range(2):
        broker.subscribe(OrderSubscriptionFilter())

    assert await broker.publish_order_event(make_order_event()) == 2
    assert await broker.publish(make_entry()) == 3


async def test_publishing_an_order_event_with_nobody_watching_is_a_counted_no_op() -> None:
    """``createOrderEvent`` must succeed with no subscribers, and the counters must still move."""
    broker = LogBroker(make_settings())
    before = broker.stats.published_total

    assert await broker.publish_order_event(make_order_event()) == 0
    assert broker.stats.published_total == before + 1
    assert broker.stats.delivered_total == 0


async def test_publish_order_event_does_not_suspend() -> None:
    """The property ``createOrderEvent`` relies on: a stalled subscriber cannot delay a write.

    ``publish_order_event`` is a second ``async`` wrapper over the same synchronous body as
    ``publish``, so it inherits the no-suspension-point guarantee from one function rather than
    from two that happen to agree — but "inherits" is a claim about the code, and this is what
    checks it. Same instrument C6 used: a callback handed to ``loop.call_soon`` runs the next time
    the loop regains control, so if it has not run by the time the awaits return, nothing yielded.

    The subscriber mix exercises every branch of the fan-out: a match, a non-match, and an overflow
    into a ``maxsize=1`` queue, so the drop path is covered by the assertion too.
    """
    broker = LogBroker(make_settings(queue_maxsize=1))
    broker.subscribe(OrderSubscriptionFilter())
    broker.subscribe(OrderSubscriptionFilter(order_id="nobody"))

    the_loop_ran: list[bool] = []
    asyncio.get_running_loop().call_soon(lambda: the_loop_ran.append(True))

    await broker.publish_order_event(make_order_event(event_id=1))
    await broker.publish_order_event(make_order_event(event_id=2))
    await broker.publish_order_event(make_order_event(event_id=3))

    assert the_loop_ran == [], (
        "publish_order_event suspended: a callback scheduled before it had a chance to run, which "
        "means a slow subscriber or a slow Redis can delay a committed order transition"
    )


async def test_a_remote_order_envelope_reaches_only_order_subscribers() -> None:
    """The bridge carries the kind, so cross-worker delivery routes exactly as local delivery does.

    Inferring the kind from ``type(entry)`` on the receiving side would work today and start
    guessing the moment two kinds shared a class; carrying it is what makes this a lookup.
    """
    broker = LogBroker(make_settings(), publisher_id="worker-a")
    order_watcher = broker.subscribe(OrderSubscriptionFilter(order_id="ord-99"))
    log_watcher = broker.subscribe(SubscriptionFilter())

    payload = encode_order_event(make_order_event(order_id="ord-99"), origin="worker-b")
    broker._ingest_remote(payload)  # noqa: SLF001 - the reader loop's one line, without a socket

    received = drain(order_watcher)
    assert len(received) == 1
    assert isinstance(received[0], OrderEvent)
    assert received[0].order_id == "ord-99"
    assert drain(log_watcher) == []
    assert broker.stats.remote_received_total == 1
    assert broker.stats.remote_invalid_total == 0


async def test_a_remote_order_envelope_from_this_worker_is_suppressed_as_its_own_echo() -> None:
    """Redis delivers a publish back to the publisher, so without this every local subscriber
    would see each transition twice — once locally and once off the wire."""
    broker = LogBroker(make_settings(), publisher_id="worker-a")
    watcher = broker.subscribe(OrderSubscriptionFilter())

    broker._ingest_remote(  # noqa: SLF001
        encode_order_event(make_order_event(), origin="worker-a")
    )

    assert drain(watcher) == []
    assert broker.stats.remote_suppressed_total == 1


async def test_a_remote_message_of_an_unknown_kind_is_counted_invalid_and_delivered_to_nobody() -> None:
    """The drop is observable as a counter rather than only as silence, so an operator can tell a
    quiet channel from one carrying something this build cannot read."""
    broker = LogBroker(make_settings(), publisher_id="worker-a")
    order_watcher = broker.subscribe(OrderSubscriptionFilter())
    log_watcher = broker.subscribe(SubscriptionFilter())

    broker._ingest_remote(envelope("shipment", ORDER_BODY))  # noqa: SLF001

    assert drain(order_watcher) == []
    assert drain(log_watcher) == []
    assert broker.stats.remote_received_total == 1
    assert broker.stats.remote_invalid_total == 1
