"""``Subscription.logStream`` over a real ``graphql-transport-ws`` socket — spec §2 items 25-27.

Every test here drives the **actual WebSocket transport**: a real upgrade, a real
``connection_init``/``connection_ack`` handshake, real ``subscribe``/``next``/``complete`` frames.
Nothing calls ``schema.subscribe`` directly, because doing so would skip the half of the feature
that can actually break — the router mount, the protocol negotiation, the per-connection context,
and the cancellation that a dropped socket triggers.

.. rubric:: Why ``TestClient`` and not ``httpx``

``httpx``'s ASGI transport speaks ``http.*`` scopes only; it cannot open a WebSocket at all, which
is why the rest of the integration suite's ``http_client`` fixture is no use here.
:class:`starlette.testclient.TestClient` (which is the same class FastAPI re-exports) runs the app
on a blocking portal in a background thread and gives a synchronous WebSocket session over it.

That also settles the one question that decides whether any of this works:

**THE TESTCLIENT MUST BE ENTERED AS A CONTEXT MANAGER, AND NOT ONLY FOR THE LIFESPAN.**
``TestClient._portal_factory`` yields ``self.portal`` when the client has been entered, and starts a
*fresh* portal — a second event loop, in a second thread — when it has not. Every
``asyncio.Queue`` in the broker is created by whichever loop ran the subscription, and
``put_nowait`` wakes a parked getter by resolving a future on that loop; resolving it from another
thread is undefined behaviour. So an un-entered client would put the WebSocket sessions and the
HTTP mutations on **different loops**, and the fan-out would be a data race rather than a feature.
Entered, they share one loop and one thread, and every publish reaches its queue on the loop that
owns it.

.. rubric:: Bounded waits, and why absence is asserted with a tracer rather than a sleep

``WebSocketTestSession.receive_json`` has no timeout, so a broken implementation would hang the
suite rather than fail it. :func:`receive_message` therefore reads on a **daemon** thread with a
deadline — daemon so that a receive that never returns cannot keep the interpreter alive at exit.

Non-delivery is never asserted with "sleep, then check nothing arrived". Every such test publishes
the entries that must *not* arrive and then a **tracer** that must, and asserts the tracer is the
very next frame. The queue is FIFO, so anything wrongly enqueued would be read first — which makes
the assertion exact and bounded instead of a guess about how long is long enough.

.. rubric:: A unique pub/sub channel per app

Redis ``PUBLISH``/``SUBSCRIBE`` is instance-wide and ignores the selected logical database, so two
applications alive in one test session would cross-talk on a shared channel and one test's entries
would arrive on another's socket. :func:`make_stream_app` gives every app its own channel.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from uuid import uuid4

import pytest
import strawberry
from fastapi import FastAPI
from sqlalchemy import text
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from src.broker import LogBroker, Subscriber, SubscriptionFilter
from src.config import Settings
from src.db.session import Database
from src.graphql.enums import LogLevel
from src.graphql.types import LogEntry
from src.main import create_app
from tests.integration.corpus import count_statements, run_sync

#: The subprotocol the C13 Apollo client speaks (``graphql-ws@5``) and the one the E2E verifier
#: will use. The legacy ``graphql-ws`` protocol is also offered by the mount; it is not exercised
#: here because nothing in this project speaks it.
GRAPHQL_TRANSPORT_WS = "graphql-transport-ws"

#: Deadline for any single frame, and for any "wait until the server has caught up" poll. Generous
#: on purpose: it is a *failure* deadline, not a delay. A healthy run satisfies every one of these
#: in microseconds and never waits; a broken one fails in ten seconds instead of hanging CI.
DEADLINE_SECONDS = 10.0

LOG_STREAM = """
subscription Stream($service: String, $level: LogLevel) {
  logStream(service: $service, level: $level) {
    id
    timestamp
    service
    level
    message
    metadata
    traceId
  }
}
"""

CREATE_LOG = """
mutation Create($logData: CreateLogInput!) {
  createLog(logData: $logData) {
    id
    timestamp
    service
    level
    message
    metadata
    traceId
  }
}
"""


# =================================================================================================
# Fixtures
# =================================================================================================


@pytest.fixture()
def clean_store(_schema: None, db_settings: Settings) -> None:
    """Truncate ``log_entries`` before the test, synchronously.

    A **sync** fixture because every test in this module is sync (the WebSocket session API is),
    and a sync test cannot consume the async ``database`` fixture the rest of the integration suite
    uses. :func:`~tests.integration.corpus.run_sync` runs the truncation on a private loop that is
    created, used and closed here, so it never touches the loop pytest-asyncio manages or the one
    the ``TestClient`` portal will later start.
    """

    async def _truncate() -> None:
        database = Database.create(db_settings)
        try:
            async with database.engine.begin() as connection:
                await connection.execute(text("TRUNCATE TABLE log_entries RESTART IDENTITY"))
        finally:
            await database.dispose()

    run_sync(_truncate())


@pytest.fixture()
def make_stream_app() -> Callable[..., FastAPI]:
    """Build an application whose subscription limits are chosen by the test.

    ``DATABASE_URL`` and ``REDIS_URL`` still come from the environment compose injects (the test
    database, Redis logical DB 1), so this is the real stack; only the three subscription knobs are
    pinned. ``subscription_channel`` is unique per app — see the module docstring.
    """

    def _make(*, queue_maxsize: int = 500, max_per_connection: int = 10) -> FastAPI:
        settings = Settings(
            _env_file=None,
            seed_entries=0,
            seed_orders=0,
            log_level="WARNING",
            subscription_queue_maxsize=queue_maxsize,
            max_subscriptions_per_connection=max_per_connection,
            subscription_channel=f"test:subscriptions:{uuid4().hex}",
        )
        return create_app(settings=settings)

    return _make


# =================================================================================================
# The graphql-transport-ws protocol, driven by hand
# =================================================================================================


def open_socket(client: TestClient) -> Any:
    """A WebSocket session on ``/graphql``, negotiated as ``graphql-transport-ws``."""
    return client.websocket_connect("/graphql", subprotocols=[GRAPHQL_TRANSPORT_WS])


def receive_message(session: Any, *, timeout: float = DEADLINE_SECONDS) -> dict[str, Any]:
    """One protocol message, or an assertion failure after ``timeout``.

    The read happens on a **daemon** thread because ``WebSocketTestSession.receive_json`` blocks
    without a deadline: a regression that never sends the expected frame would otherwise stop the
    suite rather than fail it, and a non-daemon reader still parked at interpreter exit would keep
    the process alive after pytest had finished. anyio's blocking portal is safe to call from
    several threads, and the app's own loop keeps running underneath either way.

    ``ping``/``pong`` are handled here rather than in every caller: the protocol allows either side
    to ping at any time, and a test asserting on "the next message" means the next *interesting*
    one.
    """
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"no WebSocket frame arrived within {timeout}s")

        box: dict[str, Any] = {}

        def _pull() -> None:
            try:
                box["message"] = session.receive_json()
            except BaseException as exc:  # noqa: BLE001 - reported on the calling thread instead
                box["error"] = exc

        reader = threading.Thread(target=_pull, daemon=True, name="ws-receive")
        reader.start()
        reader.join(remaining)
        if reader.is_alive():
            raise AssertionError(f"no WebSocket frame arrived within {timeout}s")
        if "error" in box:
            # Re-raised with its original type rather than wrapped, so a caller can distinguish a
            # server-initiated close (`WebSocketDisconnect`) from a genuine read failure.
            raise box["error"]

        message = box["message"]
        if message.get("type") == "ping":
            session.send_json({"type": "pong"})
            continue
        if message.get("type") == "pong":
            continue
        return message


def connection_init(session: Any) -> None:
    """Complete the handshake. Nothing may be sent on the socket before the ack arrives."""
    session.send_json({"type": "connection_init", "payload": {}})
    ack = receive_message(session)
    assert ack["type"] == "connection_ack", f"expected connection_ack, got {ack!r}"


def subscribe(
    session: Any,
    operation_id: str,
    *,
    service: Optional[str] = None,
    level: Optional[str] = None,
) -> None:
    """Send a ``subscribe`` message for :data:`LOG_STREAM`. Does not wait for anything."""
    session.send_json(
        {
            "id": operation_id,
            "type": "subscribe",
            "payload": {"query": LOG_STREAM, "variables": {"service": service, "level": level}},
        }
    )


def next_entry(session: Any, operation_id: str) -> dict[str, Any]:
    """The next ``next`` frame for ``operation_id``, unwrapped to the ``logStream`` object."""
    message = receive_message(session)
    assert message["type"] == "next", f"expected a next frame, got {message!r}"
    assert message["id"] == operation_id
    payload = message["payload"]
    assert not payload.get("errors"), f"the stream yielded errors: {payload['errors']!r}"
    return payload["data"]["logStream"]


def read_until_terminal(session: Any, *, limit: int = 50) -> list[dict[str, Any]]:
    """Every frame up to and including the first ``error`` or ``complete``.

    Returned as a list rather than a single frame because the two ways a subscription can end
    unhappily — an ``error`` message, or a ``next`` carrying ``errors`` followed by ``complete`` —
    are both legal under ``graphql-transport-ws``, and which one a given Strawberry release emits
    for an exception raised *mid-stream* is an implementation detail this project has no business
    pinning. What it does pin is that the error **reaches the client and names its code**, which is
    asserted against the whole exchange.
    """
    frames: list[dict[str, Any]] = []
    for _ in range(limit):
        try:
            message = receive_message(session)
        except WebSocketDisconnect as disconnect:
            # A third, less good ending: the server closed the whole connection instead of
            # terminating the one operation. Recorded rather than raised so the caller's assertion
            # about *what the client was told* produces the useful failure message ("the code never
            # reached the client") rather than an opaque disconnect traceback.
            frames.append({"type": "__connection_closed__", "detail": repr(disconnect)})
            return frames
        frames.append(message)
        if message.get("type") in {"error", "complete"}:
            return frames
    raise AssertionError(f"the subscription never terminated within {limit} frames: {frames!r}")


# =================================================================================================
# HTTP helpers
# =================================================================================================


def post_graphql(
    client: TestClient, query: str, variables: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    """POST one operation and return the parsed GraphQL envelope."""
    response = client.post("/graphql", json={"query": query, "variables": variables or {}})
    assert response.status_code == 200, response.text
    return response.json()


def create_log(
    client: TestClient,
    *,
    service: str = "api",
    level: str = "INFO",
    message: str = "created",
    metadata: Optional[dict[str, Any]] = None,
    trace_id: Optional[str] = None,
) -> dict[str, Any]:
    """Create one entry over HTTP and return it. Fails loudly on a GraphQL error."""
    log_data: dict[str, Any] = {"service": service, "level": level, "message": message}
    if metadata is not None:
        log_data["metadata"] = metadata
    if trace_id is not None:
        log_data["traceId"] = trace_id

    body = post_graphql(client, CREATE_LOG, {"logData": log_data})
    assert "errors" not in body, f"createLog failed: {body['errors']!r}"
    return body["data"]["createLog"]


# =================================================================================================
# Server-side helpers
# =================================================================================================


def wait_for_subscribers(
    broker: LogBroker, expected: int, *, timeout: float = DEADLINE_SECONDS
) -> None:
    """Block until the broker holds exactly ``expected`` subscriptions.

    A subscription registers when its generator first runs, which is some number of event-loop
    turns after the ``subscribe`` frame is written — so every test that cares about *when* a
    subscriber exists (the ordering of a publish against it, the per-connection cap) waits on this
    rather than on a sleep. It is a bounded poll of a plain integer, guarded by the broker's own
    lock, and safe to call from the test thread.
    """
    deadline = time.monotonic() + timeout
    while True:
        actual = broker.subscriber_count()
        if actual == expected:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"the broker held {actual} subscription(s), expected {expected}, after {timeout}s"
            )
        time.sleep(0.005)


def run_on_app_loop(client: TestClient, coroutine_function: Callable[[], Any]) -> Any:
    """Run ``coroutine_function`` on the application's own event loop and return its result.

    The only correct way to touch a server-side ``asyncio`` object from a test thread. It is used
    for exactly one thing: publishing a **burst** of entries straight through the broker, in one
    task, with no await between them — which is what makes the slow-consumer overflow deterministic
    rather than a race against how fast a consumer drains. ``LogBroker.publish`` has no suspension
    points, so a burst issued this way cannot be interleaved by any subscriber's consumer task, and
    a queue of size N provably fills on the N+1'th entry.

    The returned :class:`concurrent.futures.Future` is waited on with a deadline, so a ``publish``
    that ever *did* block would fail this call rather than hang the suite — which is itself one of
    the assertions the slow-consumer test relies on.
    """
    portal = client.portal
    assert portal is not None, (
        "TestClient must be entered as a context manager (`with TestClient(app) as client:`) — "
        "see this module's docstring: without it the WebSocket sessions and the HTTP requests run "
        "on different event loops and the broker's queues are shared across threads."
    )
    return portal.start_task_soon(coroutine_function).result(timeout=DEADLINE_SECONDS)


ANCHOR = datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc)


def burst_entry(index: int, *, service: str, level: LogLevel = LogLevel.INFO) -> LogEntry:
    """One entry for a direct-to-broker burst.

    Built in Python rather than created through ``createLog`` because a burst has to happen inside
    a single event-loop slice, and an HTTP round trip per entry cannot. The ids are far above
    anything the ``BIGSERIAL`` sequence will reach in a test, so a burst entry is never confusable
    with a persisted one.
    """
    return LogEntry(
        id=strawberry.ID(str(9_000_000 + index)),
        timestamp=ANCHOR + timedelta(milliseconds=index),
        service=service,
        level=level,
        message=f"burst {index}",
        metadata=None,
        trace_id=None,
    )


def publish_all(broker: LogBroker, entries: list[LogEntry]) -> Callable[[], Any]:
    """A zero-argument coroutine function that publishes ``entries`` with no await between them."""

    async def _burst() -> int:
        published = 0
        for entry in entries:
            await broker.publish(entry)
            published += 1
        return published

    return _burst


def register_subscriber(broker: LogBroker) -> Callable[[], Any]:
    """A zero-argument coroutine function that registers one unfiltered subscriber.

    Run through :func:`run_on_app_loop` because the queue it allocates must belong to the
    application's loop — the same loop that will later be asked to put a sentinel in it.
    """

    async def _register() -> Subscriber:
        return broker.subscribe(SubscriptionFilter())

    return _register


# =================================================================================================
# Delivery
# =================================================================================================


def test_an_entry_created_over_http_arrives_on_a_subscribed_socket(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The whole feature, end to end: HTTP write in, WebSocket frame out.

    Every published field is compared against what ``createLog`` returned, not merely checked for
    presence — a stream that delivered the right *number* of entries with a null ``metadata`` would
    otherwise pass.

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
            subscribe(session, "s1")
            wait_for_subscribers(broker, 1)

            created = create_log(
                client,
                service="api",
                level="ERROR",
                message="checkout failed",
                metadata={"host": "web-1", "attempt": 3},
                trace_id="trace-abc",
            )

            streamed = next_entry(session, "s1")

    assert streamed["id"] == created["id"]
    assert streamed["timestamp"] == created["timestamp"]
    assert streamed["service"] == "api"
    assert streamed["level"] == "ERROR"
    assert streamed["message"] == "checkout failed"
    assert streamed["metadata"] == {"host": "web-1", "attempt": 3}
    assert streamed["traceId"] == "trace-abc"
    assert streamed == created, "the stream and the mutation must serialise the entry identically"


def test_only_the_fields_the_client_selected_come_back(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """Spec §2 item 28 applies to the stream too: no over-fetching of full log objects."""
    app = make_stream_app()
    narrow = "subscription { logStream { id service } }"
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            session.send_json({"id": "s1", "type": "subscribe", "payload": {"query": narrow}})
            wait_for_subscribers(broker, 1)

            create_log(client, service="api", message="narrow")
            streamed = next_entry(session, "s1")

    assert set(streamed) == {"id", "service"}


# =================================================================================================
# Server-side filtering (spec §2 item 26)
# =================================================================================================


def test_a_service_filter_never_delivers_another_services_entries(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """Absence asserted with a tracer, not a sleep — see the module docstring.

    The two non-matching entries are created **first**. The queue is FIFO, so if either had been
    enqueued it would be the frame read below; getting the tracer proves neither ever entered the
    queue at all.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1", service="api")
            wait_for_subscribers(broker, 1)

            create_log(client, service="worker", message="not for you")
            create_log(client, service="billing", message="nor this")
            tracer = create_log(client, service="api", message="tracer")

            streamed = next_entry(session, "s1")

    assert streamed["id"] == tracer["id"]
    assert streamed["service"] == "api"


def test_a_level_filter_never_delivers_another_severity(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """Same tracer technique for the second filter dimension."""
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1", level="ERROR")
            wait_for_subscribers(broker, 1)

            create_log(client, level="INFO", message="chatter")
            create_log(client, level="WARNING", message="more chatter")
            create_log(client, level="CRITICAL", message="close, but not ERROR")
            tracer = create_log(client, level="ERROR", message="tracer")

            streamed = next_entry(session, "s1")

    assert streamed["id"] == tracer["id"]
    assert streamed["level"] == "ERROR"


def test_both_filters_together_are_and_composed(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """An OR here would look plausible: entries arrive, they match *something* the client asked for.

    The three decoys each satisfy exactly one half of the filter, so an OR implementation would
    deliver the first of them and this would fail on the id.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1", service="api", level="ERROR")
            wait_for_subscribers(broker, 1)

            create_log(client, service="api", level="INFO", message="right service, wrong level")
            create_log(client, service="worker", level="ERROR", message="wrong service, right level")
            create_log(client, service="worker", level="INFO", message="neither")
            tracer = create_log(client, service="api", level="ERROR", message="both")

            streamed = next_entry(session, "s1")

    assert streamed["id"] == tracer["id"]


def test_two_subscribers_with_different_filters_get_their_own_subsets(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """Spec §2 item 27: each subscriber has an independent queue.

    One publish stream, two sockets, two filters that **overlap** — the third entry matches both, so
    a shared queue would give it to one of them and not the other. Each socket then reads exactly
    its own expected sequence, followed by a tracer that matches both: the tracer arriving as the
    very next frame is what proves no fourth, non-matching entry is sitting behind it.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as api_socket, open_socket(client) as error_socket:
            connection_init(api_socket)
            connection_init(error_socket)
            subscribe(api_socket, "api", service="api")
            subscribe(error_socket, "errors", level="ERROR")
            wait_for_subscribers(broker, 2)

            first = create_log(client, service="api", level="INFO", message="1")
            second = create_log(client, service="worker", level="ERROR", message="2")
            both = create_log(client, service="api", level="ERROR", message="3")
            create_log(client, service="billing", level="DEBUG", message="4 - nobody wants this")
            tracer = create_log(client, service="api", level="ERROR", message="tracer")

            api_ids = [next_entry(api_socket, "api")["id"] for _ in range(3)]
            error_ids = [next_entry(error_socket, "errors")["id"] for _ in range(3)]

    assert api_ids == [first["id"], both["id"], tracer["id"]]
    assert error_ids == [second["id"], both["id"], tracer["id"]]


# =================================================================================================
# Deregistration (spec §2 item 27)
# =================================================================================================


def test_completing_one_operation_releases_its_queue_and_its_slot(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The graceful path: a ``complete`` from the client, on a socket that stays open.

    Starting a second subscription afterwards is the part that matters. It proves the release
    returned the connection's slot rather than merely removing the queue — if the per-connection
    counter leaked, a long-lived socket that cycled subscriptions would eventually be refused.
    """
    app = make_stream_app(max_per_connection=1)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1")
            wait_for_subscribers(broker, 1)

            session.send_json({"id": "s1", "type": "complete"})
            wait_for_subscribers(broker, 0)

            # The cap is 1, so this only succeeds if the first subscription's slot came back.
            subscribe(session, "s2")
            wait_for_subscribers(broker, 1)

            created = create_log(client, message="after the restart")
            assert next_entry(session, "s2")["id"] == created["id"]


def test_a_dropped_socket_deregisters_through_the_cancellation_path(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The path a real client actually takes: the browser tab closes mid-stream.

    No ``complete`` is sent. Strawberry cancels the operation task, which raises
    ``CancelledError`` inside the generator wherever it is parked, and only the resolver's
    ``finally`` can deregister the queue from there. Without it the broker would keep a queue, a
    filter and a connection slot for every socket the server had ever served.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1")
            wait_for_subscribers(broker, 1)
            # Deliver one entry so the generator is provably mid-stream — parked on `queue.get()`
            # inside the try block — rather than still starting up when the socket goes away.
            create_log(client, message="mid-stream")
            next_entry(session, "s1")

        # The socket is closed here, abruptly, with the subscription still live.
        wait_for_subscribers(broker, 0)


def test_the_lifespan_sweeps_live_subscriptions_and_stops_the_reader(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The teardown wiring: ``broker.stop()`` then ``close_all_subscribers()`` then the client.

    The live subscription is registered **directly on the broker** rather than through a socket, and
    that is deliberate rather than a shortcut. ``WebSocketTestSession`` cancels its ASGI task in its
    own ``__exit__``, so a socket left open past the end of a test would still be running when the
    ``TestClient``'s portal shuts down — and the portal waits for its tasks. The bug this test is
    about (a shutdown that hangs because generators are parked on a queue nothing will write to)
    would be masked by a *different* hang belonging to the test harness.

    Registering on the broker isolates exactly the claim being made: after the lifespan's shutdown
    has run, every subscriber has been released, has been given its terminal sentinel, and the
    pub/sub reader task is gone.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        subscriber = run_on_app_loop(client, register_subscriber(broker))
        assert broker.subscriber_count() == 1

    assert broker.subscriber_count() == 0, "the shutdown sweep released every subscription"
    assert subscriber.released is True
    assert subscriber.dropped is False, "a shutdown is a clean completion, not a slow-consumer drop"
    assert subscriber.queue.get_nowait() is None, "the terminal sentinel was enqueued"
    assert broker._reader is None, "the pub/sub reader task was stopped"  # noqa: SLF001


# =================================================================================================
# Back-pressure (spec §4 bonus: WebSocket backpressure)
# =================================================================================================


def test_a_slow_consumer_is_dropped_while_everyone_else_keeps_receiving(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The invariant that matters most under load, asserted in all three of its parts.

    ``SUBSCRIPTION_QUEUE_MAXSIZE`` is set to 2 and a burst of 20 matching entries is published
    **inside one event-loop slice** (see :func:`run_on_app_loop`), so the slow subscriber's consumer
    task provably cannot drain between publishes and the queue provably overflows. Then:

    1. the slow socket is terminated and told **why** — the ``SLOW_CONSUMER`` code reaches the
       client, so it knows it has a gap rather than believing it saw everything;
    2. a second socket, subscribed to a service the flood never touches, is still delivering
       afterwards — a drop is per-subscriber, never a stampede;
    3. the publisher was never blocked, which is what ``run_on_app_loop``'s deadline enforces: a
       ``publish`` that waited on a full queue would fail this call instead of returning.
    """
    app = make_stream_app(queue_maxsize=2)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as slow_socket, open_socket(client) as calm_socket:
            connection_init(slow_socket)
            connection_init(calm_socket)
            subscribe(slow_socket, "flood", service="flood")
            subscribe(calm_socket, "calm", service="calm")
            wait_for_subscribers(broker, 2)

            entries = [burst_entry(index, service="flood") for index in range(20)]
            published = run_on_app_loop(client, publish_all(broker, entries))
            assert published == 20, "every publish returned; none blocked on the full queue"

            frames = read_until_terminal(slow_socket)
            exchange = json.dumps(frames)

            # The calm socket is untouched by somebody else's overflow and still streaming.
            wait_for_subscribers(broker, 1)
            created = create_log(client, service="calm", message="still here")
            assert next_entry(calm_socket, "calm")["id"] == created["id"]

    assert "SLOW_CONSUMER" in exchange, (
        "the dropped subscriber must be told why the stream ended, or a client silently believes "
        f"it saw every entry. Frames were: {exchange}"
    )
    assert broker.stats.dropped_total == 1, "exactly one subscriber was dropped, not both"
    assert broker.stats.published_total >= 20


def test_the_queue_bound_comes_from_configuration(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """A larger ``SUBSCRIPTION_QUEUE_MAXSIZE`` absorbs the same burst that overflowed a size of 2.

    Without this, the test above would also pass against an implementation that dropped every
    subscriber on every burst regardless of depth.
    """
    app = make_stream_app(queue_maxsize=200)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "flood", service="flood")
            wait_for_subscribers(broker, 1)

            entries = [burst_entry(index, service="flood") for index in range(20)]
            run_on_app_loop(client, publish_all(broker, entries))

            ids = [next_entry(session, "flood")["id"] for _ in range(20)]

    assert ids == [entry.id for entry in entries], "all twenty, in order, none dropped"
    assert broker.stats.dropped_total == 0


# =================================================================================================
# The stream costs nothing at the database (spec §2 item 30, C5's instrument)
# =================================================================================================


def test_streaming_issues_no_sql_at_all(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The broker payload is the whole entry, so the resolver never opens a session.

    Counted with the same ``before_cursor_execute`` listener C5 used for the N+1 proof, so this is
    a statement about what PostgreSQL was actually asked — not about which methods were called.
    Entries are published straight through the broker here, which isolates the *stream* from the
    INSERT that would otherwise be in the window.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1", service="stream")
            wait_for_subscribers(broker, 1)

            entries = [burst_entry(index, service="stream") for index in range(25)]
            with count_statements(app.state.db.engine) as counter:
                run_on_app_loop(client, publish_all(broker, entries))
                ids = [next_entry(session, "s1")["id"] for _ in range(25)]

    assert ids == [entry.id for entry in entries]
    assert len(counter) == 0, f"streaming touched the database:\n{counter.report()}"


def test_a_subscriber_adds_no_database_work_to_create_log(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The same claim on the real path: the same mutations cost the same SQL, subscriber or not.

    A baseline is measured with nobody listening and then compared against the identical workload
    with a live subscriber consuming every entry. Equality is the assertion — a resolver that
    re-read each entry, or a fan-out that touched the store, would show up as extra statements even
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
        create_log(client, service="warmup", message="not measured")

        with count_statements(engine) as baseline:
            for index in range(5):
                create_log(client, service="alone", message=f"baseline {index}")

        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1", service="watched")
            wait_for_subscribers(broker, 1)

            with count_statements(engine) as watched:
                created = [
                    create_log(client, service="watched", message=f"watched {index}")
                    for index in range(5)
                ]
                streamed = [next_entry(session, "s1")["id"] for _ in range(5)]

    assert streamed == [entry["id"] for entry in created]
    assert len(baseline) > 0, "the baseline must actually have measured something"
    assert len(watched) == len(baseline), (
        "a subscriber changed how much SQL createLog costs.\n"
        f"baseline:\n{baseline.report()}\nwith a subscriber:\n{watched.report()}"
    )
    assert watched.count("select", "log_entries") == 0, (
        f"the stream issued a read against log_entries:\n{watched.report()}"
    )


# =================================================================================================
# The per-connection cap (§7 MAX_SUBSCRIPTIONS_PER_CONNECTION)
# =================================================================================================


def test_one_socket_cannot_exceed_the_configured_subscription_cap(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """``graphql-transport-ws`` allows unlimited ``subscribe`` messages on one socket.

    Without a cap, a single connection can allocate unbounded queues — the same denial of service
    the bounded queue closes, reached one level up. The rejection has to be a **typed GraphQL
    error**, not a silently extra queue and not a dropped connection: the client's correct response
    is to complete something it already holds, and it can only know that if it is told.
    """
    app = make_stream_app(max_per_connection=2)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "a")
            wait_for_subscribers(broker, 1)
            subscribe(session, "b")
            wait_for_subscribers(broker, 2)

            subscribe(session, "c")
            frames = read_until_terminal(session)
            exchange = json.dumps(frames)

            assert broker.subscriber_count() == 2, "the refused subscription allocated nothing"

    assert "SUBSCRIPTION_LIMIT_EXCEEDED" in exchange, (
        f"the cap must be reported with its code so a client can act on it. Frames: {exchange}"
    )
    assert all(frame.get("id") == "c" for frame in frames), "only the refused operation was affected"


def test_the_cap_is_per_connection_so_a_second_socket_is_unaffected(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """One busy client must not be able to lock every other client out of subscribing."""
    app = make_stream_app(max_per_connection=1)
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as first, open_socket(client) as second:
            connection_init(first)
            connection_init(second)
            subscribe(first, "a")
            wait_for_subscribers(broker, 1)
            subscribe(second, "b")
            wait_for_subscribers(broker, 2)

            created = create_log(client, message="both are live")
            assert next_entry(first, "a")["id"] == created["id"]
            assert next_entry(second, "b")["id"] == created["id"]


# =================================================================================================
# Validation and the publish-after-commit ordering
# =================================================================================================


def test_a_rejected_mutation_publishes_nothing(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """Validation runs before the insert, and the publish runs after the commit — so a rejected
    payload can reach neither the table nor a subscriber.

    Both halves are asserted: the broker's ``published_total`` does not move, and the very next
    frame on the live socket is the *valid* entry created afterwards. The second is the one that
    would catch a publish that happened before validation, because the counter alone could be
    satisfied by an implementation that published and then failed to enqueue.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "s1")
            wait_for_subscribers(broker, 1)

            before = broker.stats.published_total
            rejected = post_graphql(
                client,
                CREATE_LOG,
                {"logData": {"service": "   ", "level": "INFO", "message": "blank service"}},
            )
            assert rejected["errors"][0]["extensions"]["code"] == "VALIDATION_ERROR"
            assert broker.stats.published_total == before, "a rejected write published nothing"

            accepted = create_log(client, message="the first thing that should stream")
            assert next_entry(session, "s1")["id"] == accepted["id"]


def test_an_invalid_subscription_filter_is_rejected_before_a_queue_is_allocated(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """The same input rules the query path applies (spec §2 item 34), on the WebSocket path.

    Asserting the subscriber count is what makes this more than an error-message test: a filter
    rejected *after* registration would still have consumed one of the connection's slots, and the
    leak would only show up as a client that eventually cannot subscribe at all.
    """
    app = make_stream_app()
    with TestClient(app) as client:
        broker: LogBroker = app.state.broker
        with open_socket(client) as session:
            connection_init(session)
            subscribe(session, "bad", service="   ")

            frames = read_until_terminal(session)
            exchange = json.dumps(frames)
            assert broker.subscriber_count() == 0, "the rejected filter registered nothing"

    assert "VALIDATION_ERROR" in exchange, f"frames: {exchange}"


def test_an_unknown_level_is_rejected_during_validation(
    make_stream_app: Callable[..., FastAPI], clean_store: None
) -> None:
    """``level`` is an enum, so a typo names the legal values instead of opening a silent stream.

    This is the whole argument for the enum, applied to the subscription path: with ``level`` typed
    as ``String`` the socket would subscribe successfully, match nothing ever, and be
    indistinguishable from a quiet server.
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
                    "payload": {"query": LOG_STREAM, "variables": {"level": "EROR"}},
                }
            )

            frames = read_until_terminal(session)
            exchange = json.dumps(frames)
            assert broker.subscriber_count() == 0

    assert "EROR" in exchange and "LogLevel" in exchange, (
        f"the error must name the offending value and the enum. Frames: {exchange}"
    )
