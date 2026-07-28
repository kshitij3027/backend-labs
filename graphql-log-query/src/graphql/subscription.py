"""The ``Subscription`` root type: ``logStream`` — spec §2 items 25, 26 and 27.

The resolver is deliberately thin. Everything that is hard about streaming — bounded queues,
drop-on-overflow, the terminal sentinel, idempotent release, cross-worker fan-out — lives in
:mod:`src.broker`, and this module is the ~30 lines that turn a queue into an ``AsyncGenerator``
the GraphQL layer can iterate. That split is what lets the back-pressure policy be unit-tested
without a WebSocket and the transport be integration-tested without re-testing the policy.

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
from typing import Optional

import strawberry

from src.broker import SubscriptionFilter, SubscriptionLimitExceeded
from src.graphql.context import Context
from src.graphql.enums import LogLevel
from src.graphql.errors import SlowConsumerError, SubscriptionLimitError
from src.graphql.types import LogEntry
from src.graphql.validation import validate_subscription_filter

logger = logging.getLogger(__name__)

LOG_STREAM_DESCRIPTION = (
    "Live log entries, pushed over WebSocket as `createLog` commits them. `service` and `level` "
    "are applied SERVER-SIDE, before an entry is queued for this subscriber — so a narrow "
    "subscription costs neither bandwidth nor queue depth for traffic it did not ask for. Each "
    "subscriber gets its own bounded queue: a client that stops reading long enough to fill it is "
    "disconnected with a SLOW_CONSUMER error rather than being buffered without limit, so a gap is "
    "always reported and never silent."
)


@strawberry.type
class Subscription:
    """The streaming surface. C6 gives it ``logStream``; C12 adds ``orderStatusStream``."""

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
        broker = context.broker
        if broker is None:
            # Reached only if the application was assembled without a lifespan (the broker is
            # created in `src.main.lifespan`). Raised rather than degraded to an empty stream: a
            # subscription that connects, yields nothing and never errors is indistinguishable from
            # a quiet server, and would hide a broken deployment for as long as traffic was low.
            raise RuntimeError(
                "no subscription broker is available on the GraphQL context. src.main.lifespan "
                "creates it on startup and attaches it to app.state.broker, so this means the "
                "lifespan never ran — a TestClient must be entered as a context manager for that "
                "to happen."
            )

        # Validated FIRST, before anything is allocated or registered. A rejected filter must not
        # occupy one of this connection's subscription slots even momentarily.
        validate_subscription_filter(service)

        try:
            subscriber = broker.subscribe(
                SubscriptionFilter(service=service, level=level),
                connection=context,
            )
        except SubscriptionLimitExceeded as exc:
            # The broker's transport-agnostic error becomes the client's typed one. The numbers go
            # into `extensions` rather than only into the message so a client can decide what to do
            # (close an idle stream and retry) without parsing prose.
            raise SubscriptionLimitError(
                f"this connection already holds {exc.held} subscription(s) and the limit is "
                f"{exc.limit}. Complete an existing subscription before starting another, or open "
                "a second WebSocket connection.",
                extensions={"held": exc.held, "limit": exc.limit},
            ) from exc

        try:
            while True:
                entry = await subscriber.queue.get()
                if entry is None:
                    # The terminal sentinel. Two reasons it is there, and they end the stream
                    # differently: a DROP owes the client an error (it lost entries), while a
                    # shutdown sweep owes it a clean completion (it did not).
                    if subscriber.dropped:
                        raise SlowConsumerError(
                            "this subscription was dropped because its queue filled: the client "
                            "was not reading fast enough and entries were discarded rather than "
                            "buffered without limit. Resubscribe — ideally with a narrower "
                            "`service`/`level` filter — and treat the stream as having a gap.",
                            extensions={"queueMaxsize": subscriber.queue.maxsize},
                        )
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


__all__ = ["LOG_STREAM_DESCRIPTION", "Subscription"]
