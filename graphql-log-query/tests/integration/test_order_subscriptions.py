"""``Subscription.orderStatusStream`` over a real ``graphql-transport-ws`` socket — spec §3 Area C.

Every test here drives the **actual WebSocket transport**: a real upgrade, a real
``connection_init``/``connection_ack`` handshake, real ``subscribe``/``next``/``complete`` frames,
and real ``createOrderEvent`` mutations over HTTP. Nothing calls ``schema.subscribe`` directly — the
schema is async-only (its extension hooks are async generators, so ``execute_sync`` does not exist
on it), and calling into it would skip the half of the feature that can actually break: the router
mount, the protocol negotiation, the per-connection context, and the cancellation a dropped socket
triggers.

.. rubric:: The transport driver is C6's, deliberately

``connection_init``, ``receive_message``, ``read_until_terminal``, ``wait_for_subscribers`` and
``run_on_app_loop`` are imported from ``tests/integration/test_subscriptions.py`` rather than
rewritten. They encode decisions that took a while to get right — the daemon-threaded read with a
deadline, the ping/pong handling, the "a server-initiated close is recorded rather than raised"
rule — and two copies of them would be two protocol drivers that can disagree about what "the next
frame" means. What this module adds is the **order** vocabulary on top: its document, its mutation
helper, and its own fixtures.

**THE TESTCLIENT MUST BE ENTERED AS A CONTEXT MANAGER.** Un-entered, its WebSocket sessions and its
HTTP requests run on *different* event loops, and every ``asyncio.Queue`` in the broker belongs to
whichever loop created it — so the fan-out would be a data race rather than a feature. C6's module
docstring gives the whole argument.

.. rubric:: WHAT IS NEW HERE, AND WHAT IS DELIBERATELY NOT

C6 already proved the machinery both streams share: bounded queues, drop-on-overflow with a
``SLOW_CONSUMER`` code, the terminal sentinel, the shutdown sweep. ``orderStatusStream`` reuses all
of it, so none of it is re-tested. What C12 introduces, and what is tested here:

* delivery and the three new filter dimensions (order id, status, user), individually and combined;
* **the two streams not crossing** — an ``orderStatusStream`` subscriber must never be handed a
  ``LogEntry`` and a ``logStream`` subscriber must never be handed an ``OrderEvent``. This is the
  kind-routing claim, it has two *different* failure modes, and it is the most valuable thing in
  this file. See :func:`test_the_two_streams_do_not_cross` and
  :func:`test_log_traffic_neither_reaches_nor_kills_a_filtered_order_subscription`;
* the per-connection cap counting across **both** streams, which C6 could not have tested;
* the zero-SQL claim, on the order path.

.. rubric:: Absence is asserted with a tracer, never with a sleep

Every "this must not arrive" test writes the events that must *not* arrive first, then a **tracer**
that must, and asserts the tracer is the very next frame. The queue is FIFO, so anything wrongly
enqueued would be read first — which makes the assertion exact and bounded instead of a guess about
how long is long enough.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from uuid import uuid4

import pytest
import strawberry
from fastapi import FastAPI
from sqlalchemy import text
from starlette.testclient import TestClient

from src.broker import LogBroker
from src.config import Settings
from src.db.session import Database
from src.graphql.ecommerce import OrderEvent
from src.graphql.enums import LogLevel, OrderStatus
from src.main import create_app
from tests.integration.corpus import count_statements, run_sync
from tests.integration.test_subscriptions import (
    DEADLINE_SECONDS,
    connection_init,
    create_log,
    next_entry,
    open_socket,
    post_graphql,
    read_until_terminal,
    receive_message,
    run_on_app_loop,
    subscribe as subscribe_logs,
    wait_for_subscribers,
)

ORDER_STREAM = """
subscription Orders($orderId: String, $status: OrderStatus, $userId: String) {
  orderStatusStream(orderId: $orderId, status: $status, userId: $userId) {
    id
    timestamp
    service
    level
    traceId
    orderId
    userId
    status
    metadata
  }
}
"""

CREATE_ORDER_EVENT = """
mutation CreateOrder($orderData: CreateOrderEventInput!) {
  createOrderEvent(orderData: $orderData) {
    id
    timestamp
    service
    level
    traceId
    orderId
    userId
    status
    metadata
  }
}
"""


# =================================================================================================
# Fixtures
# =================================================================================================


@pytest.fixture()
def clean_event_store(_schema: None, db_settings: Settings) -> None:
    """Truncate ``order_events`` **and** ``log_entries`` before the test, synchronously.

    Both, because the cross-stream tests write to both tables and because a leftover row in either
    would make :meth:`~src.db.session.Database.seed_if_empty`'s "does this table already hold rows"
    check see a non-empty table — turning a later test's seeding into a silent no-op.

    A **sync** fixture because every test in this module is sync (the WebSocket session API is), and
    a sync test cannot consume the async ``database`` fixture the rest of the integration suite
    uses. :func:`~tests.integration.corpus.run_sync` runs the truncation on a private loop that is
    created, used and closed here, so it never touches the loop pytest-asyncio manages or the one
    the ``TestClient`` portal will later start.
    """

    async def _truncate() -> None:
        database = Database.create(db_settings)
        try:
            async with database.engine.begin() as connection:
                await connection.execute(
                    text("TRUNCATE TABLE order_events, log_entries RESTART IDENTITY")
                )
        finally:
            await database.dispose()

    run_sync(_truncate())


@pytest.fixture()
def make_stream_app() -> Callable[..., FastAPI]:
    """Build an application whose subscription limits are chosen by the test.

    ``DATABASE_URL`` and ``REDIS_URL`` still come from the environment compose injects, so this is
    the real stack; only the three subscription knobs are pinned. ``subscription_channel`` is unique
    per app because Redis ``PUBLISH``/``SUBSCRIBE`` is instance-wide and ignores the selected
    logical database — two applications alive in one session would otherwise cross-talk, and one
    test's events would arrive on another's socket.
    """

    def _make(*, queue_maxsize: int = 500, max_per_connection: int = 10) -> FastAPI:
        settings = Settings(
            _env_file=None,
            seed_entries=0,
            seed_orders=0,
            log_level="WARNING",
            subscription_queue_maxsize=queue_maxsize,
            max_subscriptions_per_connection=max_per_connection,
            subscription_channel=f"test:order-subscriptions:{uuid4().hex}",
        )
        return create_app(settings=settings)

    return _make


# =================================================================================================
# The order vocabulary on top of C6's protocol driver
# =================================================================================================


def subscribe_orders(
    session: Any,
    operation_id: str,
    *,
    order_id: Optional[str] = None,
    status: Optional[str] = None,
    user_id: Optional[str] = None,
) -> None:
    """Send a ``subscribe`` for :data:`ORDER_STREAM`. Does not wait for anything."""
    session.send_json(
        {
            "id": operation_id,
            "type": "subscribe",
            "payload": {
                "query": ORDER_STREAM,
                "variables": {"orderId": order_id, "status": status, "userId": user_id},
            },
        }
    )


def next_order_event(session: Any, operation_id: str) -> dict[str, Any]:
    """The next ``next`` frame for ``operation_id``, unwrapped to the ``orderStatusStream`` object.

    The ``data`` key is asserted rather than assumed: a ``LogEntry`` that reached this subscriber
    would come back under a different key (or, more likely, as a field error on ``orderId``), and
    the assertion below is what turns that into a legible failure instead of a ``KeyError``.
    """
    message = receive_message(session, timeout=DEADLINE_SECONDS)
    assert message["type"] == "next", f"expected a next frame, got {message!r}"
    assert message["id"] == operation_id
    payload = message["payload"]
    assert not payload.get("errors"), f"the stream yielded errors: {payload['errors']!r}"
    data = payload["data"]
    assert set(data) == {"orderStatusStream"}, f"unexpected stream payload: {data!r}"
    return data["orderStatusStream"]


def create_order_event(
    client: TestClient,
    *,
    order_id: str,
    user_id: str,
    status: str,
    service: Optional[str] = None,
    level: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    trace_id: Optional[str] = None,
) -> dict[str, Any]:
    """Create one transition over HTTP and return it. Fails loudly on a GraphQL error."""
    order_data: dict[str, Any] = {"orderId": order_id, "userId": user_id, "status": status}
    if service is not None:
        order_data["service"] = service
    if level is not None:
        order_data["level"] = level
    if metadata is not None:
        order_data["metadata"] = metadata
    if trace_id is not None:
        order_data["traceId"] = trace_id

    body = post_graphql(client, CREATE_ORDER_EVENT, {"orderData": order_data})
    assert "errors" not in body, f"createOrderEvent failed: {body['errors']!r}"
    return body["data"]["createOrderEvent"]


def identity(event: dict[str, Any]) -> tuple[str, str, str]:
    """``(id, orderId, status)`` — what a delivered frame is compared on.

    Never the id alone. ``order_events`` and ``log_entries`` have independent ``BIGSERIAL``
    sequences, so a leaked log entry could carry the same numeric id as the order event a test
    expected — and an id-only assertion would pass on it.
    """
    return (event["id"], event["orderId"], event["status"])


ANCHOR = datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc)


def burst_order_event(
    index: int,
    *,
    order_id: str = "ord-burst",
    user_id: str = "usr-burst",
    status: OrderStatus = OrderStatus.SHIPPED,
) -> OrderEvent:
    """One event for a direct-to-broker burst.

    Built in Python rather than created through ``createOrderEvent`` because a burst has to happen
    inside a single event-loop slice, and an HTTP round trip per event cannot. The ids are far above
    anything the ``BIGSERIAL`` sequence will reach in a test, so a burst event is never confusable
    with a persisted one.
    """
    return OrderEvent(
        id=strawberry.ID(str(8_000_000 + index)),
        timestamp=ANCHOR + timedelta(milliseconds=index),
        service="order-service",
        level=LogLevel.INFO,
        trace_id=None,
        order_id=order_id,
        user_id=user_id,
        status=status,
        metadata=None,
    )


def publish_all_orders(broker: LogBroker, events: list[OrderEvent]) -> Callable[[], Any]:
    """A zero-argument coroutine function publishing ``events`` with no await between them."""

    async def _burst() -> int:
        published = 0
        for event in events:
            await broker.publish_order_event(event)
            published += 1
        return published

    return _burst


# =================================================================================================
# Delivery
# =================================================================================================


def test_a_transition_created_over_http_arrives_on_a_subscribed_socket(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The whole feature, end to end: HTTP write in, WebSocket frame out.

    Every published field is compared against what ``createOrderEvent`` returned, not merely checked
    for presence — a stream that delivered the right *number* of events with a null ``metadata``
    would otherwise pass.

    The ``id`` comparison is also the proof of the **publish-after-commit** ordering: an id exists
    only because PostgreSQL assigned it during the INSERT, so a subscriber holding the same id as
    the mutation's response is holding a row that really was written. Publishing before the commit
    could not produce this.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1")
            wait_for_subscribers(broker, 1)

            created = create_order_event(
                client,
                order_id="ord-60000",
                user_id="usr-900",
                status="SHIPPED",
                metadata={"carrier": "dhl", "attempt": 2},
                trace_id="trace-order-1",
            )

            streamed = next_order_event(session, "s1")

    assert streamed["id"] == created["id"]
    assert streamed["timestamp"] == created["timestamp"]
    assert streamed["orderId"] == "ord-60000"
    assert streamed["userId"] == "usr-900"
    assert streamed["status"] == "SHIPPED"
    assert streamed["metadata"] == {"carrier": "dhl", "attempt": 2}
    assert streamed["traceId"] == "trace-order-1"
    assert streamed == created, "the stream and the mutation must serialise the event identically"


def test_the_defaulted_service_and_level_reach_the_subscriber(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """``service`` and ``level`` are resolved during validation, so a subscriber sees the resolved
    values rather than the nulls the client sent.

    A stream that carried the *input* rather than the committed row would show ``null`` here — and
    the difference is invisible on the mutation's own response, which is built from the same row.
    ``CANCELLED`` is chosen because its derived severity is ``WARNING`` rather than the ``INFO``
    every other status gets, so a defaulting bug that hard-coded one level would fail here.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1")
            wait_for_subscribers(broker, 1)

            created = create_order_event(
                client, order_id="ord-1", user_id="usr-1", status="CANCELLED"
            )
            streamed = next_order_event(session, "s1")

    assert streamed["service"] == "order-service"
    assert streamed["level"] == "WARNING", "severity is a property of the status, not of the caller"
    assert streamed["service"] == created["service"]
    assert streamed["level"] == created["level"]
    assert streamed["traceId"] is None
    assert streamed["metadata"] is None


# =================================================================================================
# Server-side filtering — "by order id, status, and/or user" (spec §3 Feature Area C)
# =================================================================================================


def test_an_order_id_filter_never_delivers_another_orders_transitions(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """Absence asserted with a tracer, not a sleep.

    The decoys are a **different order with the same user and the same status**, so this isolates
    the order-id dimension exactly: a server that implemented only the user or the status filter
    would deliver them and fail on the first frame.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-A")
            wait_for_subscribers(broker, 1)

            create_order_event(client, order_id="ord-B", user_id="usr-1", status="PAID")
            create_order_event(client, order_id="ord-C", user_id="usr-1", status="PAID")
            tracer = create_order_event(client, order_id="ord-A", user_id="usr-1", status="PAID")

            streamed = next_order_event(session, "s1")

    assert identity(streamed) == identity(tracer)
    assert streamed["orderId"] == "ord-A"


def test_an_order_id_filter_is_exact_and_not_a_prefix(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """``ord-6000`` must not match ``ord-60001``.

    The one failure mode a per-order subscription must not have: a prefix match here leaks one
    customer's orders into another's stream, and the ids in the generated corpus are adjacent
    enough that it would happen immediately rather than theoretically.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-6000")
            wait_for_subscribers(broker, 1)

            create_order_event(client, order_id="ord-60001", user_id="usr-1", status="PAID")
            create_order_event(client, order_id="ord-600", user_id="usr-1", status="PAID")
            tracer = create_order_event(client, order_id="ord-6000", user_id="usr-1", status="PAID")

            streamed = next_order_event(session, "s1")

    assert identity(streamed) == identity(tracer)
    assert streamed["orderId"] == "ord-6000"


def test_a_status_filter_never_delivers_another_transition(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The decoys are the **same order and user** at every other status, so only the status
    dimension is under test — and ``PACKED``/``SHIPPED``/``CANCELLED`` bracket ``DELIVERED`` in the
    lifecycle, which is where an off-by-one comparison would land."""
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", status="DELIVERED")
            wait_for_subscribers(broker, 1)

            for decoy in ("PACKED", "SHIPPED", "CANCELLED"):
                create_order_event(client, order_id="ord-A", user_id="usr-1", status=decoy)
            tracer = create_order_event(
                client, order_id="ord-A", user_id="usr-1", status="DELIVERED"
            )

            streamed = next_order_event(session, "s1")

    assert identity(streamed) == identity(tracer)
    assert streamed["status"] == "DELIVERED"


def test_a_user_filter_never_delivers_another_customers_orders(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The support-agent view: one customer, every order they have. The decoys are a different
    user at the same status, so the user dimension is isolated."""
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", user_id="usr-900")
            wait_for_subscribers(broker, 1)

            create_order_event(client, order_id="ord-A", user_id="usr-901", status="REFUNDED")
            create_order_event(client, order_id="ord-B", user_id="usr-9000", status="REFUNDED")
            tracer = create_order_event(
                client, order_id="ord-C", user_id="usr-900", status="REFUNDED"
            )

            streamed = next_order_event(session, "s1")

    assert identity(streamed) == identity(tracer)
    assert streamed["userId"] == "usr-900"


def test_two_filters_together_are_and_composed(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """An OR here would look plausible: events arrive, and each matches *something* asked for.

    The two decoys satisfy exactly one half of the filter each, so an OR implementation would
    deliver the first of them and this would fail on the identity comparison.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-A", status="SHIPPED")
            wait_for_subscribers(broker, 1)

            create_order_event(client, order_id="ord-A", user_id="usr-1", status="PACKED")
            create_order_event(client, order_id="ord-B", user_id="usr-1", status="SHIPPED")
            tracer = create_order_event(client, order_id="ord-A", user_id="usr-1", status="SHIPPED")

            streamed = next_order_event(session, "s1")

    assert identity(streamed) == identity(tracer)


def test_all_three_filters_together_are_and_composed(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """Three decoys, each differing in exactly **one** dimension.

    A decoy that differed in more than one would pass even against a server that implemented only
    one of the three filters, which is the mistake this arrangement exists to avoid.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-A", status="SHIPPED", user_id="usr-1")
            wait_for_subscribers(broker, 1)

            # wrong order, right status and user
            create_order_event(client, order_id="ord-B", user_id="usr-1", status="SHIPPED")
            # right order and user, wrong status
            create_order_event(client, order_id="ord-A", user_id="usr-1", status="PACKED")
            # right order and status, wrong user
            create_order_event(client, order_id="ord-A", user_id="usr-2", status="SHIPPED")
            tracer = create_order_event(client, order_id="ord-A", user_id="usr-1", status="SHIPPED")

            streamed = next_order_event(session, "s1")

    assert identity(streamed) == identity(tracer)
    assert (streamed["orderId"], streamed["status"], streamed["userId"]) == (
        "ord-A",
        "SHIPPED",
        "usr-1",
    )


def test_two_subscribers_with_different_filters_get_their_own_subsets(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """Each subscriber has an independent queue.

    One write stream, two sockets, two filters that **overlap** — the third event matches both, so a
    shared queue would give it to one of them and not the other. Each socket then reads exactly its
    own expected sequence, followed by a tracer that matches both: the tracer arriving as the very
    next frame is what proves no non-matching event is sitting behind it.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as order_socket, open_socket(client) as user_socket:
            connection_init(order_socket)
            connection_init(user_socket)
            subscribe_orders(order_socket, "by-order", order_id="ord-A")
            subscribe_orders(user_socket, "by-user", user_id="usr-2")
            wait_for_subscribers(broker, 2)

            first = create_order_event(client, order_id="ord-A", user_id="usr-1", status="CREATED")
            second = create_order_event(client, order_id="ord-B", user_id="usr-2", status="PAID")
            both = create_order_event(client, order_id="ord-A", user_id="usr-2", status="PACKED")
            create_order_event(client, order_id="ord-C", user_id="usr-3", status="PAID")
            tracer = create_order_event(client, order_id="ord-A", user_id="usr-2", status="SHIPPED")

            by_order = [next_order_event(order_socket, "by-order") for _ in range(3)]
            by_user = [next_order_event(user_socket, "by-user") for _ in range(3)]

    assert [identity(event) for event in by_order] == [
        identity(first),
        identity(both),
        identity(tracer),
    ]
    assert [identity(event) for event in by_user] == [
        identity(second),
        identity(both),
        identity(tracer),
    ]


# =================================================================================================
# THE TWO STREAMS DO NOT CROSS — the kind-routing claim
#
# This is the section that would notice `LogBroker._fan_out`'s `subscriber.filter.kind != kind`
# check being removed, and NOTHING ELSE IN THE SUITE WOULD. The two directions fail differently and
# each needs its own arrangement:
#
#  * A log entry offered to an UNFILTERED order subscriber is ACCEPTED — with all three constraints
#    None, `OrderSubscriptionFilter.matches` returns True without touching an attribute — so a log
#    line lands on `orderStatusStream` and is serialised against `OrderEvent!`.
#  * A log entry offered to a FILTERED order subscriber raises `AttributeError` on `order_id`, the
#    fan-out reads that as a broken subscriber, and a healthy subscription is DROPPED the first
#    time anybody writes a log.
#  * An order event offered to a log subscriber is accepted either way, because `OrderEvent` really
#    does have `service` and `level`. Nothing raises; `logStream` just starts emitting order events.
# =================================================================================================


def test_the_two_streams_do_not_cross(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """One socket on each stream, interleaved writes, each reading exactly its own events.

    Both subscriptions are **unfiltered**, which is the arrangement in which every filter would
    match everything — so the only thing separating the two streams here is the kind tag.

    Compared on ``(id, orderId, status)`` and on ``message`` rather than on ids alone: the two
    tables have independent ``BIGSERIAL`` sequences, so a leaked row can carry the id its neighbour
    expected. (In practice a leak fails earlier, as a field error — ``LogEntry`` has no ``orderId``
    and ``OrderEvent`` has no ``message`` — and the helpers assert on that too.)
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as order_socket, open_socket(client) as log_socket:
            connection_init(order_socket)
            connection_init(log_socket)
            subscribe_orders(order_socket, "orders")
            subscribe_logs(log_socket, "logs")
            wait_for_subscribers(broker, 2)

            first_log = create_log(client, service="api", message="before any order")
            event_a = create_order_event(
                client, order_id="ord-A", user_id="usr-1", status="CREATED"
            )
            second_log = create_log(client, service="order-service", message="between transitions")
            event_b = create_order_event(client, order_id="ord-B", user_id="usr-2", status="PAID")
            third_log = create_log(client, service="api", message="after the orders")

            streamed_orders = [next_order_event(order_socket, "orders") for _ in range(2)]
            streamed_logs = [next_entry(log_socket, "logs") for _ in range(3)]

            assert broker.stats.dropped_total == 0, (
                "a subscriber was dropped during ordinary cross-stream traffic — the wrong "
                "filter was consulted with the wrong event type and raised"
            )
            assert broker.subscriber_count() == 2

    assert [identity(event) for event in streamed_orders] == [
        identity(event_a),
        identity(event_b),
    ], "the order stream delivered something other than exactly its own two events"
    assert [entry["message"] for entry in streamed_logs] == [
        first_log["message"],
        second_log["message"],
        third_log["message"],
    ], "the log stream delivered something other than exactly its own three entries"


def test_log_traffic_neither_reaches_nor_kills_a_filtered_order_subscription(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The second failure mode, which is a **drop** rather than a mis-delivery.

    A *filtered* order subscriber's ``matches`` reads ``event.order_id``. Handed a ``LogEntry`` that
    raises ``AttributeError``, and the fan-out's "a broken filter costs only its own subscriber"
    rule would then terminate a perfectly healthy subscription — presenting as "orderStatusStream
    dies whenever anybody writes a log", which is both catastrophic and easy to misattribute.

    So: twenty log entries first, and then the subscription has to still be alive and still
    delivering. Twenty rather than one because a single entry could be lost to a race; twenty
    cannot.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-watched")
            wait_for_subscribers(broker, 1)

            for index in range(20):
                create_log(client, service="api", message=f"noise {index}")

            assert broker.subscriber_count() == 1, "the order subscription survived the log traffic"
            assert broker.stats.dropped_total == 0

            tracer = create_order_event(
                client, order_id="ord-watched", user_id="usr-1", status="SHIPPED"
            )
            streamed = next_order_event(session, "s1")

    assert identity(streamed) == identity(tracer), (
        "the frame delivered after twenty log entries was not the order transition — a LogEntry "
        "reached the order stream"
    )


def test_order_traffic_never_reaches_a_log_subscriber_watching_the_order_service(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The realistic version of the other direction, where **nothing raises**.

    ``createOrderEvent`` defaults ``service`` to ``order-service``, and a ``logStream`` subscriber
    filtered on that service is a completely ordinary thing for an operator to open. ``OrderEvent``
    genuinely has ``service`` and ``level``, so ``SubscriptionFilter.matches`` accepts it happily —
    the kind check is the only thing between that subscriber and a stream of order events
    serialised against ``LogEntry!``.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_logs(session, "s1", service="order-service")
            wait_for_subscribers(broker, 1)

            for status in ("CREATED", "PAID", "PACKED"):
                create_order_event(client, order_id="ord-A", user_id="usr-1", status=status)
            tracer = create_log(client, service="order-service", message="the only log line")

            streamed = next_entry(session, "s1")

    assert streamed["id"] == tracer["id"]
    assert streamed["message"] == "the only log line"


# =================================================================================================
# Deregistration
# =================================================================================================


def test_completing_an_order_subscription_returns_its_slot_to_the_connection(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The graceful path: a ``complete`` from the client, on a socket that stays open.

    Starting a second subscription afterwards is the part that matters. Under a cap of 1 it only
    succeeds if the first subscription's slot came back — if the per-connection counter leaked, a
    long-lived socket that cycled subscriptions would eventually be refused while holding nothing.
    """
    app = make_stream_app(max_per_connection=1)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        baseline = broker.subscriber_count()

        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-A")
            wait_for_subscribers(broker, baseline + 1)

            session.send_json({"id": "s1", "type": "complete"})
            wait_for_subscribers(broker, baseline)

            subscribe_orders(session, "s2", order_id="ord-B")
            wait_for_subscribers(broker, baseline + 1)

            created = create_order_event(
                client, order_id="ord-B", user_id="usr-1", status="DELIVERED"
            )
            assert identity(next_order_event(session, "s2")) == identity(created)

    assert broker.subscriber_count() == baseline


def test_a_dropped_socket_deregisters_through_the_cancellation_path(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The path a real client actually takes: the browser tab closes mid-stream.

    No ``complete`` is sent. Strawberry cancels the operation task, which raises ``CancelledError``
    inside the generator wherever it is parked, and only the resolver's ``finally`` can deregister
    the queue from there. Without it the broker would keep a queue, a filter and a connection slot
    for every socket the server had ever served, and the count would never return to baseline.

    One event is delivered first so the generator is provably mid-stream — parked on ``queue.get()``
    inside the ``try`` — rather than still starting up when the socket goes away.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        baseline = broker.subscriber_count()

        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1")
            wait_for_subscribers(broker, baseline + 1)

            create_order_event(client, order_id="ord-A", user_id="usr-1", status="PAID")
            next_order_event(session, "s1")

        # The socket is closed here, abruptly, with the subscription still live.
        wait_for_subscribers(broker, baseline)


def test_cycling_order_subscriptions_does_not_leak_slots(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """Five subscribe/complete cycles on one socket under a cap of 1.

    A counter that drifted *upwards* by even one per cycle would refuse the second iteration; one
    that drifted downwards would let the cap stop meaning anything. Both are invisible in a single
    subscribe-then-release test, which is why this repeats.
    """
    app = make_stream_app(max_per_connection=1)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            for cycle in range(5):
                operation = f"cycle-{cycle}"
                subscribe_orders(session, operation, order_id=f"ord-{cycle}")
                wait_for_subscribers(broker, 1)

                created = create_order_event(
                    client, order_id=f"ord-{cycle}", user_id="usr-1", status="PAID"
                )
                assert identity(next_order_event(session, operation)) == identity(created)

                session.send_json({"id": operation, "type": "complete"})
                wait_for_subscribers(broker, 0)

    assert broker.subscriber_count() == 0


# =================================================================================================
# The stream costs nothing at the database (C5's instrument)
# =================================================================================================


def test_streaming_order_events_issues_no_sql_at_all(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The broker payload is the whole event, so the resolver never opens a session.

    Counted with the same ``before_cursor_execute`` listener C5 used for the N+1 proof, so this is a
    statement about what PostgreSQL was actually asked — not about which methods were called.
    Events are published straight through the broker here, which isolates the *stream* from the
    INSERT that would otherwise be in the window.

    This is a structural requirement rather than an optimisation: ``context_getter`` resolves once
    per WebSocket **connection**, so any session opened for the stream would be a session held for
    as long as the socket is open — a pinned pool connection and a permanently frozen read snapshot.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-burst")
            wait_for_subscribers(broker, 1)

            events = [burst_order_event(index) for index in range(25)]
            with count_statements(app.state.db.engine) as counter:
                run_on_app_loop(client, publish_all_orders(broker, events))
                streamed = [next_order_event(session, "s1")["id"] for _ in range(25)]

    assert streamed == [event.id for event in events], "all twenty-five, in order"
    assert len(counter) == 0, f"streaming touched the database:\n{counter.report()}"


def test_a_subscriber_adds_no_database_work_to_create_order_event(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The same claim on the real path: the same mutations cost the same SQL, subscriber or not.

    A baseline is measured with nobody listening and then compared against the identical workload
    with a live subscriber consuming every event. Equality is the assertion — a resolver that
    re-read each event, or a fan-out that touched the store, would show up as extra statements even
    though every response stayed correct.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        engine = app.state.db.engine
        broker: LogBroker = app.state.broker

        # Warm the connection pool BEFORE either measurement. A checkout that has to open a new
        # asyncpg connection can carry dialect-level work with it, and a block that happened to pay
        # for one while the other did not would fail this comparison for a reason that has nothing
        # to do with subscriptions.
        create_order_event(client, order_id="ord-warmup", user_id="usr-0", status="CREATED")

        with count_statements(engine) as baseline:
            for _ in range(5):
                create_order_event(client, order_id="ord-alone", user_id="usr-0", status="PAID")

        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1", order_id="ord-watched")
            wait_for_subscribers(broker, 1)

            with count_statements(engine) as watched:
                created = [
                    create_order_event(
                        client, order_id="ord-watched", user_id="usr-0", status="PACKED"
                    )
                    for _ in range(5)
                ]
                streamed = [next_order_event(session, "s1")["id"] for _ in range(5)]

    assert streamed == [event["id"] for event in created]
    assert len(baseline) > 0, "the baseline must actually have measured something"
    assert len(watched) == len(baseline), (
        "a subscriber changed how much SQL createOrderEvent costs.\n"
        f"baseline:\n{baseline.report()}\nwith a subscriber:\n{watched.report()}"
    )
    assert watched.count("select", "order_events") == 0, (
        f"the stream issued a read against order_events:\n{watched.report()}"
    )


# =================================================================================================
# The per-connection cap (§7 MAX_SUBSCRIPTIONS_PER_CONNECTION)
# =================================================================================================


def test_one_socket_cannot_exceed_the_cap_with_order_subscriptions(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """The rejection has to be a **typed GraphQL error**, not a silently extra queue.

    ``graphql-transport-ws`` allows unlimited ``subscribe`` messages on one socket, so without a cap
    a single connection can allocate unbounded queues — the same denial of service the bounded queue
    closes, reached one level up. The client's correct response is to complete something it already
    holds, and it can only know that if it is told, with the numbers.
    """
    app = make_stream_app(max_per_connection=2)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "a", order_id="ord-A")
            wait_for_subscribers(broker, 1)
            subscribe_orders(session, "b", order_id="ord-B")
            wait_for_subscribers(broker, 2)

            subscribe_orders(session, "c", order_id="ord-C")
            frames = read_until_terminal(session)
            exchange = json.dumps(frames)

            assert broker.subscriber_count() == 2, "the refused subscription allocated nothing"

    assert "SUBSCRIPTION_LIMIT_EXCEEDED" in exchange, (
        f"the cap must be reported with its code so a client can act on it. Frames: {exchange}"
    )
    assert '"limit": 2' in exchange or '"limit":2' in exchange, (
        f"the numbers belong in extensions, not only in the prose. Frames: {exchange}"
    )
    assert all(frame.get("id") == "c" for frame in frames), "only the refused operation was affected"


def test_the_cap_counts_subscriptions_across_both_streams(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """What the cap bounds is **queues per socket**, not subscriptions per field.

    So one ``logStream`` plus one ``orderStatusStream`` fills a cap of two, and the third
    ``subscribe`` on that connection is refused whichever stream it asks for. A cap that counted per
    field would let a client double its allocation for free by alternating — which is exactly the
    resource the cap exists to bound.
    """
    app = make_stream_app(max_per_connection=2)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_logs(session, "logs")
            wait_for_subscribers(broker, 1)
            subscribe_orders(session, "orders")
            wait_for_subscribers(broker, 2)

            subscribe_orders(session, "third")
            frames = read_until_terminal(session)
            exchange = json.dumps(frames)

            assert broker.subscriber_count() == 2

    assert "SUBSCRIPTION_LIMIT_EXCEEDED" in exchange, f"frames: {exchange}"
    assert all(frame.get("id") == "third" for frame in frames)


def test_the_cap_is_per_connection_so_a_second_socket_is_unaffected(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """One busy client must not be able to lock every other client out of the order stream."""
    app = make_stream_app(max_per_connection=1)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as first, open_socket(client) as second:
            connection_init(first)
            connection_init(second)
            subscribe_orders(first, "a")
            wait_for_subscribers(broker, 1)
            subscribe_orders(second, "b")
            wait_for_subscribers(broker, 2)

            created = create_order_event(
                client, order_id="ord-A", user_id="usr-1", status="SHIPPED"
            )
            assert identity(next_order_event(first, "a")) == identity(created)
            assert identity(next_order_event(second, "b")) == identity(created)


# =================================================================================================
# Validation and the publish-after-commit ordering
# =================================================================================================


def test_a_rejected_order_mutation_publishes_nothing(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """Validation runs before the insert, and the publish runs after the commit — so a rejected
    payload can reach neither the table nor a subscriber.

    Both halves are asserted: the broker's ``published_total`` does not move, and the very next
    frame on the live socket is the *valid* event created afterwards. The second is the one that
    would catch a publish that happened before validation, because the counter alone could be
    satisfied by an implementation that published and then failed to enqueue.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "s1")
            wait_for_subscribers(broker, 1)

            before = broker.stats.published_total
            rejected = post_graphql(
                client,
                CREATE_ORDER_EVENT,
                {"orderData": {"orderId": "   ", "userId": "usr-1", "status": "CREATED"}},
            )
            assert rejected["errors"][0]["extensions"]["code"] == "VALIDATION_ERROR"
            assert broker.stats.published_total == before, "a rejected write published nothing"

            accepted = create_order_event(
                client, order_id="ord-A", user_id="usr-1", status="CREATED"
            )
            assert identity(next_order_event(session, "s1")) == identity(accepted)


def test_an_invalid_order_subscription_filter_is_rejected_before_a_queue_is_allocated(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """A blank ``orderId`` is refused rather than treated as "no filter".

    An empty id cannot equal any stored one, so a subscription carrying it would open successfully,
    register a queue, and stay silent forever — indistinguishable, to the client, from an order
    nothing is happening to.

    Asserting the subscriber count is what makes this more than an error-message test: a filter
    rejected *after* registration would still have consumed one of the connection's slots, and the
    leak would only show up as a client that eventually cannot subscribe at all.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "bad", order_id="   ")

            frames = read_until_terminal(session)
            exchange = json.dumps(frames)
            assert broker.subscriber_count() == 0, "the rejected filter registered nothing"

    assert "VALIDATION_ERROR" in exchange, f"frames: {exchange}"
    assert "orderId" in exchange, f"the error must name the offending field. Frames: {exchange}"


def test_a_blank_user_filter_is_rejected_too(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """Both identifiers go through the same three rules, so both are checked.

    Without this, ``validate_order_subscription_filter`` could check only its first argument and
    every ``orderId`` test above would still pass.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe_orders(session, "bad", user_id="  \t ")

            frames = read_until_terminal(session)
            exchange = json.dumps(frames)
            assert broker.subscriber_count() == 0

    assert "VALIDATION_ERROR" in exchange, f"frames: {exchange}"
    assert "userId" in exchange, f"the error must name the offending field. Frames: {exchange}"


def test_an_unknown_status_is_rejected_during_validation(
    make_stream_app: Callable[..., FastAPI], clean_event_store: None
) -> None:
    """``status`` is an enum, so a typo names the legal values instead of opening a silent stream.

    This is the whole argument for the enum, applied to the subscription path: with ``status`` typed
    as ``String`` the socket would subscribe successfully, match nothing ever, and be
    indistinguishable from an order that is not moving.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            session.send_json(
                {
                    "id": "bad",
                    "type": "subscribe",
                    "payload": {"query": ORDER_STREAM, "variables": {"status": "SHIPPPED"}},
                }
            )

            frames = read_until_terminal(session)
            exchange = json.dumps(frames)
            assert broker.subscriber_count() == 0

    assert "SHIPPPED" in exchange and "OrderStatus" in exchange, (
        f"the error must name the offending value and the enum. Frames: {exchange}"
    )
