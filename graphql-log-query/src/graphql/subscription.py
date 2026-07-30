"""The ``Subscription`` root type: ``logStream`` (spec §2 items 25-27) and ``orderStatusStream``
(spec §3 Feature Area C).

Both resolvers are deliberately thin. Everything that is hard about streaming — bounded queues,
drop-on-overflow, the terminal sentinel, idempotent release, cross-worker fan-out, and which
subscriber is even a candidate for a given event — lives in :mod:`src.broker`, and this module is
the handful of lines that turn a queue into an ``AsyncGenerator`` the GraphQL layer can iterate.
That split is what lets the back-pressure policy be unit-tested without a WebSocket and the
transport be integration-tested without re-testing the policy.

.. rubric:: C12 added a stream, not a second streaming mechanism

``orderStatusStream`` reuses the C6 broker whole: same bounded per-subscriber queue, same
drop-on-full with a ``SLOW_CONSUMER`` error, same idempotent deregistration on disconnect *and* on
cancellation, same ``close_all_subscribers()`` sweep at shutdown, same Redis pub/sub bridge so it
works under ``--workers N``, same own-echo suppression, and the same zero database round trips
while streaming. What is new is one event kind on the wire and one filter class — see the "TWO
STREAMS, ONE BROKER" section of :mod:`src.broker`. Everything below that both resolvers share is
factored into ``_subscribe`` / ``_next_event`` rather than copied, because the policy those two
encode is the part that must not come to mean two different things.

.. rubric:: THIS RESOLVER TOUCHES THE DATABASE ZERO TIMES, AND THAT IS THE DESIGN

Not an optimisation — a structural requirement. :mod:`src.graphql.context` explains at length why a
subscription must not hold a session: ``context_getter`` resolves once per WebSocket **connection**,
and Strawberry wraps a subscription's entire yield loop in one ``on_operation``, so any session
opened for the stream is a session held for as long as the socket is open. That is a pinned pool
connection, a permanently frozen read snapshot, and a ``PendingRollbackError`` waiting for its first
failed statement.

The way out is that the broker's payload is already the whole entry. ``createLog`` commits, projects
the row through :meth:`~src.graphql.types.LogEntry.from_orm`, and publishes **that object**; a
remote event arrives as JSON carrying every published field and is reconstructed by
:func:`src.broker.decode_event`. Either way the value this generator yields is complete before it
reaches the queue, so there is nothing left to fetch. ``tests/integration/test_subscriptions.py``
pins it with the same SQLAlchemy statement counter C5 used for the N+1 proof: streaming N entries
adds **zero** statements.

(The one field that could still reach for the database is ``LogEntry.relatedLogs``, if a client
selects it inside ``logStream``. That resolves through the per-operation DataLoader like any other
field, using the short-lived session ``OperationResources`` hands a subscription — one statement per
batch, released immediately, never held across a yield. The "zero round trips" claim above is about
the *stream*, not about a client that asks the stream to do a correlated lookup.)

.. rubric:: ``try``/``finally`` around the yield loop, and the cancellation case in particular

A streaming resolver ends in more ways than it returns:

* the client sends ``complete`` for this operation;
* the socket drops, and Strawberry cancels the operation task;
* the broker drops this subscriber for being slow;
* the process shuts down and :meth:`~src.broker.LogBroker.close_all_subscribers` sweeps.

Every one of them must deregister the queue, and only the third and fourth arrive *through* the
queue. The other two arrive as ``asyncio.CancelledError`` (or ``GeneratorExit``) raised at whichever
``await`` or ``yield`` the generator is parked on — so the deregistration cannot live at the bottom
of the loop, it has to be a ``finally``. Without it the broker keeps a queue, a filter and a
per-connection slot for every socket the server has ever served, and
``MAX_SUBSCRIPTIONS_PER_CONNECTION`` starts rejecting a reconnecting client that holds nothing.

:meth:`~src.broker.LogBroker.unsubscribe` is idempotent precisely because more than one of these
paths routinely runs for the same subscription.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Optional, Union

import strawberry

from src.broker import (
    LogBroker,
    OrderSubscriptionFilter,
    PublishedEvent,
    Subscriber,
    SubscriptionFilter,
    SubscriptionLimitExceeded,
)
from src.graphql.context import Context
from src.graphql.ecommerce import OrderEvent
from src.graphql.enums import LogLevel, OrderStatus
from src.graphql.errors import SlowConsumerError, SubscriptionLimitError
from src.graphql.types import LogEntry
from src.graphql.validation import (
    validate_order_subscription_filter,
    validate_subscription_filter,
)

logger = logging.getLogger(__name__)

LOG_STREAM_DESCRIPTION = (
    "Live log entries, pushed over WebSocket as `createLog` commits them. `service` and `level` "
    "are applied SERVER-SIDE, before an entry is queued for this subscriber — so a narrow "
    "subscription costs neither bandwidth nor queue depth for traffic it did not ask for. Each "
    "subscriber gets its own bounded queue: a client that stops reading long enough to fill it is "
    "disconnected with a SLOW_CONSUMER error rather than being buffered without limit, so a gap is "
    "always reported and never silent."
)

ORDER_STATUS_STREAM_DESCRIPTION = (
    "Live order status transitions, pushed over WebSocket as `createOrderEvent` commits them. "
    "`orderId`, `status` and `userId` are optional and AND-composed, and all three are applied "
    "SERVER-SIDE, before an event is queued for this subscriber — so a board watching one "
    "customer's order pays neither bandwidth nor queue depth for every other order in the system. "
    "Each subscriber gets its own bounded queue: a client that stops reading long enough to fill "
    "it is disconnected with a SLOW_CONSUMER error rather than being buffered without limit, so a "
    "gap is always reported and never silent."
)


# =================================================================================================
# The two things both resolvers do, factored out so they cannot diverge
#
# The stream policy — what the terminal sentinel means, which ending owes the client an error, how
# the broker's transport-agnostic limit error becomes the client's typed one — is the part of this
# module that is genuinely hard, and it is identical for both streams. Two copies of it would be
# two places for "was this a drop or a shutdown?" to be answered, and the second copy is the one
# that gets it wrong six months later.
#
# Deliberately helper FUNCTIONS rather than a shared async generator the resolvers delegate to. A
# generator wrapping a generator adds a finalisation ordering question — which one's `finally` runs
# when the socket drops mid-yield — to the exact code path whose whole job is to deregister
# reliably. `_next_event` has no yield at all, so both resolvers keep their own flat
# try/except/finally and the cancellation semantics C6 established are visible in each.
# =================================================================================================


def _subscribe(
    broker: LogBroker,
    flt: Union[SubscriptionFilter, OrderSubscriptionFilter],
    connection: Context,
) -> Subscriber:
    """Register with the broker, translating its limit error into the client's typed one.

    Raises:
        src.graphql.errors.SubscriptionLimitError: This connection already holds
            ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` subscriptions **across both streams** — the cap
            counts subscriptions on a socket, not subscriptions per field, because what it bounds
            is the number of queues one connection can allocate. The numbers go into ``extensions``
            rather than only into the message, so a client can decide what to do (close an idle
            stream and retry) without parsing prose.
    """
    try:
        return broker.subscribe(flt, connection=connection)
    except SubscriptionLimitExceeded as exc:
        raise SubscriptionLimitError(
            f"this connection already holds {exc.held} subscription(s) and the limit is "
            f"{exc.limit}. Complete an existing subscription before starting another, or open "
            "a second WebSocket connection.",
            extensions={"held": exc.held, "limit": exc.limit},
        ) from exc


async def _next_event(subscriber: Subscriber) -> Optional[PublishedEvent]:
    """Await the next event for ``subscriber``, or ``None`` when the stream has ended cleanly.

    The single place the **terminal sentinel** is interpreted. It is enqueued for two reasons and
    they end the stream differently: a **drop** owes the client an error (entries were discarded,
    and pretending otherwise would be a lie about completeness), while a **shutdown sweep** owes it
    a clean completion (nothing was lost — the server is going away).

    Raises:
        src.graphql.errors.SlowConsumerError: This subscriber's queue filled and the broker dropped
            it. Terminal: the stream ends with this rather than resuming.
    """
    event = await subscriber.queue.get()
    if event is not None:
        return event

    if subscriber.dropped:
        raise SlowConsumerError(
            "this subscription was dropped because its queue filled: the client was not reading "
            "fast enough and events were discarded rather than buffered without limit. "
            "Resubscribe — ideally with a narrower filter — and treat the stream as having a gap.",
            extensions={"queueMaxsize": subscriber.queue.maxsize},
        )
    return None


def _require_broker(context: Context) -> LogBroker:
    """The process broker, or a loud failure.

    Reached only if the application was assembled without a lifespan (the broker is created in
    :func:`src.main.lifespan`). Raised rather than degraded to an empty stream: a subscription that
    connects, yields nothing and never errors is indistinguishable from a quiet server, and would
    hide a broken deployment for as long as traffic was low.
    """
    broker = context.broker
    if broker is None:
        raise RuntimeError(
            "no subscription broker is available on the GraphQL context. src.main.lifespan "
            "creates it on startup and attaches it to app.state.broker, so this means the "
            "lifespan never ran — a TestClient must be entered as a context manager for that "
            "to happen."
        )
    return broker


@strawberry.type
class Subscription:
    """The streaming surface: ``logStream`` (C6) and ``orderStatusStream`` (C12)."""

    @strawberry.subscription(description=LOG_STREAM_DESCRIPTION)
    async def log_stream(
        self,
        info: strawberry.Info[Context, None],
        service: Optional[str] = None,
        level: Optional[LogLevel] = None,
    ) -> AsyncGenerator[LogEntry, None]:
        """Stream newly created log entries matching ``service`` and ``level``.

        Args:
            info: Carries the :class:`~src.graphql.context.Context`. Two things are taken off it:
                the broker, and **the context object itself** as the connection identity for the
                per-connection cap — Strawberry builds exactly one context per WebSocket connection
                (see :mod:`src.graphql.context`), so its identity *is* the socket's identity, and
                the property this module would otherwise have to invent is already guaranteed.
            service: Exact-match service filter. Omitted means every service.
            level: Severity filter, as the ``LogLevel`` enum. Omitted means every severity.

        Yields:
            :class:`~src.graphql.types.LogEntry` values, reconstructed from the broker payload.
            No database round trip happens per entry — see this module's docstring.

        Raises:
            src.graphql.errors.ValidationError: ``service`` is blank, over-long or contains a NUL
                byte. Raised before a queue is allocated, so a bad filter cannot consume a slot.
            src.graphql.errors.SubscriptionLimitError: This connection already holds
                ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` subscriptions. ``graphql-transport-ws`` allows
                unlimited ``subscribe`` messages on one socket; without the cap a single connection
                could allocate unbounded queues, which is the same denial-of-service the bounded
                queue closes, reached one level up.
            src.graphql.errors.SlowConsumerError: This subscriber's queue filled and the broker
                dropped it. Terminal: the stream ends with this error rather than resuming, because
                entries were discarded and pretending otherwise would be a lie about completeness.
        """
        context = info.context
        broker = _require_broker(context)

        # Validated FIRST, before anything is allocated or registered. A rejected filter must not
        # occupy one of this connection's subscription slots even momentarily.
        validate_subscription_filter(service)

        subscriber = _subscribe(broker, SubscriptionFilter(service=service, level=level), context)

        try:
            while True:
                entry = await _next_event(subscriber)
                if entry is None:
                    # A clean end — the shutdown sweep. `_next_event` has already raised for the
                    # other terminal case (a slow-consumer drop, which owes the client an error).
                    return
                yield entry
        except asyncio.CancelledError:
            # Re-raised, never swallowed. `CancelledError` derives from `BaseException` in 3.8+, so
            # no `except Exception` above could have caught it — this clause is here to make the
            # decision explicit and to keep it correct if a broader handler is ever added: a
            # subscription that absorbs its own cancellation is a task the event loop cannot stop,
            # and the symptom is a container that will not shut down.
            raise
        finally:
            # Runs on EVERY exit path — clean return, client `complete`, socket drop, cancellation,
            # slow-consumer drop, shutdown sweep. Idempotent, so the paths that also unsubscribe
            # from the broker side (the drop and the sweep) do not double-decrement this
            # connection's slot count. See src/broker.py's `unsubscribe`.
            broker.unsubscribe(subscriber)

    @strawberry.subscription(description=ORDER_STATUS_STREAM_DESCRIPTION)
    async def order_status_stream(
        self,
        info: strawberry.Info[Context, None],
        order_id: Optional[str] = None,
        status: Optional[OrderStatus] = None,
        user_id: Optional[str] = None,
    ) -> AsyncGenerator[OrderEvent, None]:
        """Stream order status transitions as they occur — spec §3 Feature Area C, all three items.

        .. rubric:: Item by item, and where each one actually lives

        * *"streams order status transitions as they occur"* — the events come from
          :meth:`src.graphql.mutation.Mutation.create_order_event`, published **after** its commit,
          so "as they occur" means "as they durably occur" rather than "as they are attempted".
        * *"supports filtering by order id, status, and/or user"* — the three arguments below, in
          any combination, AND-composed and evaluated **at enqueue time** by
          :class:`~src.broker.OrderSubscriptionFilter`. "And/or" is why all three are optional and
          why none of them is a required scope: a board watching one order, an ops view watching
          every CANCELLED, and a support agent watching one customer are all legitimate, and so is
          the unfiltered firehose C13's dashboard opens.
        * *"end-to-end delivery latency stays under 100 ms"* — a property of the path, not of a
          knob, and the path is: commit -> ``put_nowait`` -> ``queue.get()`` -> serialise -> frame.
          There is no database read, no poll interval and no timer anywhere in it, which is what
          makes the number achievable rather than tuned. ``scripts/verify_e2e.py`` measures it
          against a live container and gates on ``E2E_SUB_LATENCY_MS``.

        .. rubric:: This resolver touches the database ZERO times, exactly like ``logStream``

        For the reason this module's docstring gives at length: the broker's payload already
        carries the whole event (:meth:`src.graphql.ecommerce.OrderEvent.to_wire` carries every
        published scalar), whether it arrived by local fan-out or across the Redis bridge. So there
        is nothing left to fetch, and a subscription never holds a session open for the life of a
        socket. ``tests/integration/test_order_subscriptions.py`` pins it with the same statement
        counter C5 used for the N+1 proof.

        (The three traversal fields — ``payments``, ``userActivity``, ``relatedLogs`` — do reach the
        database if a client selects them, through the per-operation DataLoader on a short-lived
        session. The "zero round trips" claim is about the *stream*, not about a client that asks
        each delivered event to do a correlated lookup.)

        Args:
            info: Carries the :class:`~src.graphql.context.Context`. Two things are taken off it:
                the broker, and **the context object itself** as the connection identity for the
                per-connection cap — Strawberry builds exactly one context per WebSocket connection,
                so its identity *is* the socket's identity.
            order_id: Exact-match order id. Omitted means every order. Exact, never a prefix — see
                :meth:`src.broker.OrderSubscriptionFilter.matches`.
            status: Transition filter, as the ``OrderStatus`` enum. Omitted means every status.
            user_id: Exact-match acting user. Omitted means every user.

        Yields:
            :class:`~src.graphql.ecommerce.OrderEvent` values, reconstructed from the broker
            payload.

        Raises:
            src.graphql.errors.ValidationError: ``orderId`` or ``userId`` is blank, over-long or
                contains a NUL byte. Raised before a queue is allocated, so a bad filter cannot
                consume one of this connection's slots even momentarily.
            src.graphql.errors.SubscriptionLimitError: This connection is at
                ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` — counted across **both** streams, because
                what the cap bounds is queues per socket.
            src.graphql.errors.SlowConsumerError: This subscriber's queue filled and the broker
                dropped it. Terminal: transitions were discarded, and an order board that silently
                missed a SHIPPED would show a stale state forever rather than for a moment.
        """
        context = info.context
        broker = _require_broker(context)

        # Validated FIRST, before anything is allocated or registered — same rule as `logStream`.
        validate_order_subscription_filter(order_id, user_id)

        subscriber = _subscribe(
            broker,
            OrderSubscriptionFilter(order_id=order_id, status=status, user_id=user_id),
            context,
        )

        try:
            while True:
                event = await _next_event(subscriber)
                if event is None:
                    return
                yield event
        except asyncio.CancelledError:
            # Re-raised, never swallowed — see `log_stream` for the whole argument.
            raise
        finally:
            # Every exit path, idempotently. See `log_stream`.
            broker.unsubscribe(subscriber)


__all__ = [
    "LOG_STREAM_DESCRIPTION",
    "ORDER_STATUS_STREAM_DESCRIPTION",
    "Subscription",
]
