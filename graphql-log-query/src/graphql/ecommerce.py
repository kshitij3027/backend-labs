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

.. rubric:: What is deliberately NOT here yet

**No nested field resolvers, no cross-entity DataLoaders, no aggregations.** Those are C11 (spec
§3 Feature Areas B and D), and the seams are marked below. Building them now would mean building
them without the batching that makes them correct — a naive ``OrderEvent.payments`` resolver is one
SELECT per order, i.e. precisely the N+1 this project exists to demonstrate the absence of.

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

from typing import Optional

import strawberry
from strawberry.scalars import JSON

from src.db.models import OrderEventORM, PaymentEventORM, UserEventORM
from src.graphql.enums import (
    LogLevel,
    OrderStatus,
    PaymentMethod,
    PaymentOutcome,
    UserActivity,
)
from src.graphql.types import LogEvent


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

    # C11 SEAM: `user: UserEvent`-style traversal and `payments: [PaymentEvent!]!` land here, both
    # routed through cross-entity DataLoaders keyed on `user_id` / `order_id`. They are not written
    # now because a resolver added without its loader is one SELECT per parent, and the cost gate
    # would have to price a field whose weight nobody has calibrated yet.

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

    # C11 SEAM: `order: OrderEvent` (the newest status of the order this payment belongs to),
    # batched by `order_id`.

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

    # C11 SEAM: `orders: [OrderEvent!]!` for this user, batched by `user_id`.

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


__all__ = ["OrderEvent", "PaymentEvent", "UserEvent"]
