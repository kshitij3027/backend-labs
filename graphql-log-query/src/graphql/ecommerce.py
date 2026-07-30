"""The three e-commerce event types — spec §3 Feature Area A.

``OrderEvent``, ``PaymentEvent`` and ``UserEvent``, each implementing
:class:`src.graphql.types.LogEvent` alongside ``LogEntry``. Together with the interface itself
(which lives beside ``LogEntry``, because it is a generalisation *of* it rather than a new thing
next to it) these four types are the whole of Feature Area A's published surface.

.. rubric:: Why these live in their own module

Not because they are a different kind of object — they are all log events, which is exactly what
the interface says. Because they are a different *domain*: ``types.py`` is the log-store schema the
spec's §2 core requirements describe, and this is the e-commerce schema §3 layers on top. C11 adds
the nested traversals (``OrderEvent.user``, ``OrderEvent.payments``) and C12 the status
subscription, both of which land here, and keeping them out of ``types.py`` is what stops the core
type module from slowly becoming the module where everything is.

.. rubric:: C11 filled the seams: nested traversal, and the aggregates the dashboard renders

Spec §3 **Feature Area B** ("nested resolution: an order query can traverse to its user and payment
events in the same request") is the three field resolvers on ``OrderEvent`` plus their counterparts
on the other two. **Feature Area D** ("Redis caching applied to aggregations") is the three
aggregate types at the bottom of this module.

**EVERY NESTED FIELD HERE GOES THROUGH A DATALOADER, WITHOUT EXCEPTION.** That is not a performance
preference, it is the reason these fields were held back until C11: a naive ``OrderEvent.payments``
is one ``SELECT ... WHERE order_id = :id`` per order, so a page of a hundred orders is a hundred
statements — and it returns byte-identical JSON, so no test of the response can tell the two apart.
The resolvers below therefore contain no SQL, no session and no repository call; each is a single
``await info.context.loaders.<edge>.load(key)``, and the batching is proved by counting the
statements PostgreSQL actually received at two different page sizes.

.. rubric:: THE ONE RULE A RESOLVER HERE MUST NOT BREAK: never await a loader inside a session

A DataLoader dispatches its batch in **its own asyncio task**, and that task queues for the
operation's shared session. A resolver that held the session and then awaited a loader would be
waiting on a task waiting on it — a deadlock that presents as a test suite which stops rather than
fails. See :class:`src.graphql.context.OperationResources`. Nothing in this module opens a session
at all, which is the structural way to keep that true.

.. rubric:: ``from_orm`` is the only construction path, exactly as it is for ``LogEntry``

Each type's classmethod is the single place its row is projected onto the published shape, so the
``metadata_`` / ``metadata`` rename and the ``int`` -> :class:`strawberry.ID` coercion are written
once per type and cannot drift between a query path and a subscription path. See
:meth:`src.graphql.types.LogEntry.from_orm` for the full argument; it applies here unchanged.

A stored value outside the published enum raises :class:`ValueError` from the enum constructor, and
that is deliberate and loud for the same reason it is on ``LogLevel``: the column is a plain
``String``, the enum is the contract, and a row the schema cannot express is a data-integrity
problem rather than something to paper over with a default.

.. rubric:: Annotation style

``Optional[X]`` rather than ``X | None``, matching :mod:`src.graphql.types` — these annotations are
evaluated by Strawberry at schema-construction time, so the spelling is executable code and one
spelling everywhere beats two that differ per field type.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional

import strawberry
from strawberry.scalars import JSON

from src.db.models import OrderEventORM, PaymentEventORM, UserEventORM
from src.db.repository import FunnelBucket, OrderStatusBucket, PaymentOutcomeBucket
from src.graphql.enums import (
    LogLevel,
    OrderStatus,
    PaymentMethod,
    PaymentOutcome,
    UserActivity,
)
from src.graphql.types import (
    LogEntry,
    LogEvent,
    from_wire_timestamp,
    to_wire_timestamp,
)


def _log_level(stored: str) -> LogLevel:
    """Coerce a stored ``level`` string into the published enum.

    A one-line helper rather than three inline ``LogLevel(row.level)`` calls, so the three
    ``from_orm`` methods below stay symmetric and so there is exactly one place to look when a
    stored severity cannot be represented.

    Raises:
        ValueError: If ``stored`` is outside :class:`~src.graphql.enums.LogLevel`. Deliberate and
            loud — the ``level`` column is shared with ``log_entries``, the enum is the published
            contract, and a row that cannot be serialised out is a data-integrity problem. C4's
            masking turns it into an ``INTERNAL_ERROR`` for the client and a full stack trace in the
            server log, which is the right pair for a server fault.
    """
    return LogLevel(stored)


@strawberry.type(
    description=(
        "One transition in an order's lifecycle. Orders are stored as an append-only event "
        "stream, so an order's current state is the status of its newest event rather than a "
        "mutable column — which is what makes the whole history queryable and what "
        "orderStatusStream replays."
    )
)
class OrderEvent(LogEvent):
    """An order status transition.

    Carries **two** identifiers: ``orderId`` is what the event is about, and ``userId`` is the
    modeled order -> user relationship. Both are denormalised onto the row rather than joined,
    because this system stores events and not entities — there is no ``users`` table to join to.
    See the section comment in :mod:`src.db.models` for the full argument.
    """

    id: strawberry.ID
    order_id: str
    user_id: str
    status: OrderStatus
    metadata: Optional[JSON]

    # =============================================================================================
    # C11 — the three traversals that make `orderEvents` a one-round-trip dossier (Feature Area B).
    #
    # THE `strawberry.Info` ANNOTATIONS BELOW ARE DELIBERATELY BARE — no type parameters. That is
    # the same decision `LogEntry.related_logs` documents at length and for the same reason:
    # `strawberry.Info[Context, None]` would require `Context` to be importable *here*, at
    # schema-construction time, and this module sits UNDER `src.graphql.loaders`, which
    # `src.graphql.context` imports. Parameterising these would close the cycle
    # (ecommerce -> context -> loaders -> ecommerce) and the application would fail to import.
    # Strawberry resolves the parameter annotation with or without arguments, so dropping them
    # costs nothing at run time.
    # =============================================================================================

    @strawberry.field(
        description=(
            "Every payment event filed under this order, newest first — the order -> payments "
            "relationship. Batched across the whole selection set by a per-operation DataLoader, "
            "so a page of N orders costs one query rather than N. Empty when the order has no "
            "payment events."
        )
    )
    async def payments(self, info: strawberry.Info) -> list[PaymentEvent]:
        """This order's payment stream — spec §3 Feature Area B, batched on ``order_id``.

        One REST API would answer this with ``GET /orders/{id}/payments``, once per order on the
        page. Here it is one ``WHERE order_id IN (...)`` for the entire page, and the client did not
        have to know that.

        Note this returns the whole stream rather than "the payment": a payment is authorized, then
        captured, then possibly refunded, so an order has several payment *events* and collapsing
        them to one would throw away the history that makes ``outcome`` meaningful.
        """
        return await info.context.loaders.payment_events_by_order_id.load(self.order_id)

    @strawberry.field(
        description=(
            "Everything the user who placed this order did, newest first — the order -> user "
            "relationship, traversed to that user's activity stream. There is no `users` table to "
            "join to (this system stores events, not entities), so `userId` IS the edge and this "
            "field is the batched read across it."
        )
    )
    async def user_activity(self, info: strawberry.Info) -> list[UserEvent]:
        """The acting user's activity — spec §3 Feature Area B, batched on ``user_id``.

        Published as ``userActivity`` rather than ``user`` on purpose. ``user`` would promise a
        *User entity*, and there is none: ``user_events`` is the user's history and nothing else in
        this system knows a user's name, tier or email. Naming the field after what it returns is
        the difference between a schema a client can reason about and one that invites a
        ``user { email }`` selection that can never exist.

        Note the scope: this is every activity that user has ever produced, **not** only the
        activity around this order. That is the useful question ("who is this customer") and it is
        why the key is ``user_id`` rather than ``trace_id`` — the session-scoped view is
        ``relatedLogs``' neighbour, ``Query.correlatedEvents(traceId:)``.
        """
        return await info.context.loaders.user_events_by_user_id.load(self.user_id)

    @strawberry.field(
        description=(
            "The log lines emitted under this event's traceId, newest first. Empty when traceId "
            "is null. This is the fourth leg of the correlation: order status, payments, user "
            "activity and the raw log output of the same unit of work, in one request."
        )
    )
    async def related_logs(self, info: strawberry.Info) -> list[LogEntry]:
        """Log lines sharing this event's trace — batched through **C5's own** loader.

        Deliberately the same ``logs_by_trace_id`` loader ``LogEntry.relatedLogs`` uses, not a new
        one: "sharing a trace" has to mean one thing across the schema, and two loaders over one
        key would also mean two statements for a document that selected both.

        Nothing is excluded here, unlike :meth:`src.graphql.types.LogEntry.related_logs`. That
        field drops the entry it was resolved from because a log line correlated only with itself
        is not "related"; an order event is not in ``log_entries`` at all, so there is nothing to
        drop and every member of the group is a genuine answer.

        The ``None`` guard is above every ``await``, so an untraced event costs **zero** round
        trips rather than one that returns nothing — the same property C5 asserts with a statement
        counter.
        """
        if self.trace_id is None:
            return []
        return await info.context.loaders.logs_by_trace_id.load(self.trace_id)

    @classmethod
    def from_orm(cls, row: OrderEventORM) -> OrderEvent:
        """Project a stored row onto the published type. The only place that mapping exists."""
        return cls(
            id=strawberry.ID(str(row.id)),
            timestamp=row.timestamp,
            service=row.service,
            level=_log_level(row.level),
            trace_id=row.trace_id,
            order_id=row.order_id,
            user_id=row.user_id,
            status=OrderStatus(row.status),
            metadata=row.metadata_,
        )

    # =============================================================================================
    # C12 — the JSON representation, for the pub/sub bridge behind `orderStatusStream`.
    #
    # The exact counterpart of `LogEntry.to_wire()` / `from_wire()`, written the same way and for
    # the same reason: `src.broker` needs ONE representation of a published order event, and a
    # codec living in the broker would be a second mapping of this type sitting a module away from
    # the `from_orm` it has to agree with. `metadata_` -> `metadata` and `int` -> `ID` are already
    # spelled out above; putting them here too, in the module that owns the type, is what stops the
    # two spellings from drifting.
    #
    # WHY THIS TYPE AND NOT `PaymentEvent` / `UserEvent`: only order events are published. Spec §3
    # Feature Area C asks for a stream of *order status transitions*, and a `to_wire` on a type
    # nothing publishes would be untested surface that looks maintained.
    # =============================================================================================

    def to_wire(self) -> dict[str, Any]:
        """This event as a JSON-serialisable dict. Every published scalar field, no envelope.

        All nine, rather than the ones a caller happens to need: a subscriber on another worker
        reconstructs a complete :class:`OrderEvent` and never consults the database to fill a gap,
        which is what makes ``orderStatusStream`` cost zero SQL statements on *every* worker rather
        than only on the one that wrote the row.

        The three traversal fields (``payments``, ``userActivity``, ``relatedLogs``) are absent and
        must stay absent — they are resolvers, not state. A client that selects them inside a
        subscription resolves them per delivered event through the per-operation DataLoader, on a
        short-lived session, exactly as it would under a query. Serialising them here would put a
        point-in-time snapshot of another table into an event payload.

        Key order matches the declaration order of the type, for the same reason
        :meth:`src.graphql.types.LogEntry.to_wire` fixes its own: nothing depends on it
        semantically, and it keeps two encodings of one event byte-identical.
        """
        return {
            "id": str(self.id),
            "timestamp": to_wire_timestamp(self.timestamp),
            "service": self.service,
            "level": self.level.value,
            "trace_id": self.trace_id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "status": self.status.value,
            "metadata": self.metadata,
        }

    @classmethod
    def from_wire(cls, body: Mapping[str, Any]) -> OrderEvent:
        """Rebuild an event from what :meth:`to_wire` produced. The inverse, and nothing else.

        **Raises rather than tolerating**, exactly as ``LogEntry.from_wire`` does. The seven
        required fields are read with ``[]`` and the two nullable ones with ``.get``, so a
        truncated payload is a ``KeyError`` and a ``level``/``status`` outside its enum is a
        ``ValueError``. :func:`src.broker.decode_event` turns any of them into "drop this message",
        which is the only safe answer for a channel any process with the Redis credentials can
        write to — and an event the published schema cannot express must never reach a subscriber,
        because the failure would otherwise happen during serialisation, mid-stream, on a socket
        that has already been told the subscription succeeded.

        Note ``order_id`` and ``user_id`` are read as **required**. They are what the receiving
        broker's :class:`~src.broker.OrderSubscriptionFilter` matches on, so a body missing one is
        not a slightly-incomplete event — it is an unfilterable one, and delivering it would mean a
        subscription scoped to a single order receiving somebody else's.
        """
        return cls(
            id=strawberry.ID(str(body["id"])),
            timestamp=from_wire_timestamp(body["timestamp"]),
            service=body["service"],
            level=LogLevel(body["level"]),
            trace_id=body.get("trace_id"),
            order_id=body["order_id"],
            user_id=body["user_id"],
            status=OrderStatus(body["status"]),
            metadata=body.get("metadata"),
        )


@strawberry.type(
    description=(
        "One event in a payment's life: authorized, captured, declined or refunded. Several "
        "events share one orderId — a payment is a stream, not a record with a status column — so "
        "`method` is a property of the attempt and `outcome` is the event's own verb."
    )
)
class PaymentEvent(LogEvent):
    """A payment attempt event. ``orderId`` is the modeled order -> payments relationship."""

    id: strawberry.ID
    order_id: str
    method: PaymentMethod
    outcome: PaymentOutcome
    metadata: Optional[JSON]

    @strawberry.field(
        description=(
            "The current state of the order this payment belongs to — the newest OrderEvent "
            "carrying the same orderId, or null when no order event exists for it. Batched by "
            "orderId through a per-operation DataLoader."
        )
    )
    async def order(self, info: strawberry.Info) -> Optional[OrderEvent]:
        """This payment's order, as of its newest status transition — batched on ``order_id``.

        .. rubric:: Why this reads the head of the history loader instead of having its own

        "The newest order event for this id" could be a loader of its own with a ``DISTINCT ON``
        behind it. It is not, because ``order_events_by_order_id`` already returns that order's
        whole history **newest first**, so the answer is its first element — and a second loader
        over the same key would issue a second statement for a row the first one already fetched.
        A document selecting both ``order`` and (in a future commit) the full history would pay
        twice for one read.

        Returns ``None`` rather than raising when the order has no events. That is an ordinary
        answer: a payment event ingested for an order this store has not seen (a partner feed
        arriving out of order) is data, not a failure, and ``NOT_FOUND`` would turn a nullable
        field into an errors envelope for something no client can act on.
        """
        history = await info.context.loaders.order_events_by_order_id.load(self.order_id)
        return history[0] if history else None

    @strawberry.field(
        description=(
            "The log lines emitted under this event's traceId, newest first. Empty when traceId "
            "is null."
        )
    )
    async def related_logs(self, info: strawberry.Info) -> list[LogEntry]:
        """Log lines sharing this payment's trace — through C5's ``logs_by_trace_id`` loader."""
        if self.trace_id is None:
            return []
        return await info.context.loaders.logs_by_trace_id.load(self.trace_id)

    @classmethod
    def from_orm(cls, row: PaymentEventORM) -> PaymentEvent:
        """Project a stored row onto the published type. The only place that mapping exists."""
        return cls(
            id=strawberry.ID(str(row.id)),
            timestamp=row.timestamp,
            service=row.service,
            level=_log_level(row.level),
            trace_id=row.trace_id,
            order_id=row.order_id,
            method=PaymentMethod(row.method),
            outcome=PaymentOutcome(row.outcome),
            metadata=row.metadata_,
        )


@strawberry.type(
    description=(
        "One thing a user did — signing up, logging in, browsing, adding to cart, checking out, "
        "reviewing, logging out. Shares a traceId with the order and payment events from the same "
        "session, which is what makes correlatedEvents return a story rather than a list."
    )
)
class UserEvent(LogEvent):
    """A user activity event."""

    id: strawberry.ID
    user_id: str
    activity_type: UserActivity
    metadata: Optional[JSON]

    @strawberry.field(
        description=(
            "Every order event this user produced, newest first — the order -> user relationship "
            "traversed from the user's side. Batched by userId through a per-operation DataLoader."
        )
    )
    async def orders(self, info: strawberry.Info) -> list[OrderEvent]:
        """This user's order events — spec §3 Feature Area B, batched on ``user_id``.

        The reverse of :meth:`OrderEvent.user_activity`, over the same denormalised ``user_id``
        column and the same index (``ix_order_events_user_ts``). Order *events*, not orders: this
        system has no ``orders`` table, so "this user's orders" is spelled as the transitions they
        caused, and a client that wants one row per order groups by ``orderId`` (or asks
        ``Query.orderStatusDistribution``, which does exactly that in SQL).
        """
        return await info.context.loaders.order_events_by_user_id.load(self.user_id)

    @strawberry.field(
        description=(
            "The log lines emitted under this event's traceId, newest first. Empty when traceId "
            "is null."
        )
    )
    async def related_logs(self, info: strawberry.Info) -> list[LogEntry]:
        """Log lines sharing this activity's trace — through C5's ``logs_by_trace_id`` loader."""
        if self.trace_id is None:
            return []
        return await info.context.loaders.logs_by_trace_id.load(self.trace_id)

    @classmethod
    def from_orm(cls, row: UserEventORM) -> UserEvent:
        """Project a stored row onto the published type. The only place that mapping exists."""
        return cls(
            id=strawberry.ID(str(row.id)),
            timestamp=row.timestamp,
            service=row.service,
            level=_log_level(row.level),
            trace_id=row.trace_id,
            user_id=row.user_id,
            activity_type=UserActivity(row.activity_type),
            metadata=row.metadata_,
        )


# =================================================================================================
# C11 — the published aggregates (spec §3 Feature Area D, and what Feature Area E's dashboard draws)
#
# Three types, one per aggregate, because the three answer genuinely different questions and a
# shared `{ status, count }` shape would invite a client to plot two of them on one axis. See the
# section comment above `src.db.repository.OrderStatusBucket` for the SQL and for why the funnel and
# the distribution are not the same number.
#
# EACH `from_buckets` IS WHERE THE PUBLISHED ORDERING IS APPLIED, and it runs on the cache-HIT path
# and the cache-MISS path alike — because what is cached is the repository's bucket tuple, not the
# published objects. That is the arrangement `logStats` established (see `src.cache`'s Values
# section): caching the published object would make this projection skippable, and a skippable
# projection is a second implementation of the ordering waiting to be written.
#
# The SQL orders by the status STRING, for determinism only. The order a client sees is the ORDER
# LIFECYCLE — CREATED, PAID, PACKED, SHIPPED, DELIVERED, CANCELLED, REFUNDED — which is exactly
# `OrderStatus`'s declaration order, so enumerating the enum IS the ordering and a status added to
# the enum lands in the right place with no second list to update. Same trick, same reason, as
# `LogStats.from_result` applying ascending severity to `levelBreakdown`.
# =================================================================================================


def _by_lifecycle(status: str) -> int:
    """Position of ``status`` in the order lifecycle, for sorting. Unknown values sort last.

    Unknown cannot happen through the schema (the column is validated into
    :class:`~src.graphql.enums.OrderStatus` before it is published) — but this function runs over
    strings that came out of a ``GROUP BY``, and a sort key that raised on an unexpected value would
    turn one unrecognised row into a failed dashboard rather than a row at the end of the list.
    """
    order = {member.value: index for index, member in enumerate(OrderStatus)}
    return order.get(status, len(order))


@strawberry.type(
    description=(
        "How many orders are sitting at one status RIGHT NOW — i.e. how many orders have this as "
        "the status of their newest event. Every order appears in exactly one bucket, so these "
        "counts sum to the number of orders in the window."
    )
)
class OrderStatusCount:
    """One bucket of the current-status distribution."""

    status: OrderStatus
    orders: int

    @classmethod
    def from_buckets(cls, buckets: Sequence[OrderStatusBucket]) -> list[OrderStatusCount]:
        """Project the repository's buckets onto the published shape, in lifecycle order."""
        return [
            cls(status=OrderStatus(bucket.status), orders=bucket.orders)
            for bucket in sorted(buckets, key=lambda bucket: _by_lifecycle(bucket.status))
        ]


@strawberry.type(
    description=(
        "How many distinct orders have EVER reached one status. Cumulative rather than current, so "
        "an order that was delivered is counted at every stage it passed through — which is what "
        "makes this a funnel and makes `share` a conversion rate."
    )
)
class FunnelStage:
    """One stage of the order funnel."""

    status: OrderStatus
    orders_reached: int
    #: ``ordersReached`` as a fraction of the widest stage, rounded to four places. Derived here
    #: rather than in SQL because it is a ratio between rows of one result — a window function
    #: could compute it, at the cost of a second pass over a result that is at most seven rows
    #: long. Four places because a conversion rate is read as a percentage with two decimals, and
    #: because an unrounded float would make the cached blob and the response differ in their last
    #: bit depending on the platform.
    share: float

    @classmethod
    def from_buckets(cls, buckets: Sequence[FunnelBucket]) -> list[FunnelStage]:
        """Project the repository's buckets onto the published shape, in lifecycle order.

        ``share`` is computed against the **widest** stage rather than against CREATED. They are
        the same number for any corpus this project generates (every lifecycle starts at CREATED,
        so CREATED is always the widest), and they stop being the same the moment a time-window
        filter clips the beginning of an order's history off the result — at which point dividing
        by a CREATED bucket that is missing or small would produce shares above 1.0. Dividing by
        the maximum cannot.
        """
        widest = max((bucket.orders for bucket in buckets), default=0)
        return [
            cls(
                status=OrderStatus(bucket.status),
                orders_reached=bucket.orders,
                share=round(bucket.orders / widest, 4) if widest else 0.0,
            )
            for bucket in sorted(buckets, key=lambda bucket: _by_lifecycle(bucket.status))
        ]


@strawberry.type(
    description=(
        "One (method, outcome) cell of the payment cross-tabulation. `events` counts payment "
        "attempts and `orders` counts the distinct orders behind them, so `events > orders` in a "
        "DECLINED cell is retries — which a single count could not express."
    )
)
class PaymentOutcomeCount:
    """One cell of the payment method x outcome breakdown."""

    method: PaymentMethod
    outcome: PaymentOutcome
    events: int
    orders: int

    @classmethod
    def from_buckets(cls, buckets: Sequence[PaymentOutcomeBucket]) -> list[PaymentOutcomeCount]:
        """Project the repository's cells onto the published shape.

        Ordered busiest cell first, ties broken by the enum declaration order of the method and
        then the outcome — busiest first because that is what a stacked bar chart leads with, and
        the tiebreak because two cells with equal counts must not swap places between two identical
        requests.
        """
        methods = {member.value: index for index, member in enumerate(PaymentMethod)}
        outcomes = {member.value: index for index, member in enumerate(PaymentOutcome)}
        ordered = sorted(
            buckets,
            key=lambda bucket: (
                -bucket.events,
                methods.get(bucket.method, len(methods)),
                outcomes.get(bucket.outcome, len(outcomes)),
            ),
        )
        return [
            cls(
                method=PaymentMethod(bucket.method),
                outcome=PaymentOutcome(bucket.outcome),
                events=bucket.events,
                orders=bucket.orders,
            )
            for bucket in ordered
        ]


__all__ = [
    "FunnelStage",
    "OrderEvent",
    "OrderStatusCount",
    "PaymentEvent",
    "PaymentOutcomeCount",
    "UserEvent",
]
