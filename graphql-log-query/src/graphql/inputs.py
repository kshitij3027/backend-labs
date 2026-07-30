"""``LogFilterInput`` and the one function that turns it into C2's :class:`LogQuery`.

.. rubric:: Every field is optional, and that is a requirement rather than a convenience

Spec §2 item 19: *"omitted filters are ignored"*. Two things have to be true for that to hold, and
they are easy to satisfy separately and get wrong together:

1. **Omitted means "no filter".** Every field defaults to ``None`` and
   :func:`src.db.repository.build_predicates` contributes a WHERE clause only for values that are
   ``is not None``. Nothing is defaulted to a value that would narrow the result.
2. **An explicitly-supplied ``null`` means the same thing.** A GraphQL client that builds a filter
   object from a form will send ``{"service": null}`` for an empty box rather than omitting the
   key, and a variables payload built with ``json.dumps`` on a partially-filled dict does the same.
   With ``= None`` defaults both cases arrive at the resolver as the identical Python ``None``, so
   they *cannot* diverge — there is no third state to mishandle. (Strawberry's ``UNSET`` sentinel
   would distinguish them, which is useful for a partial-update mutation where "set this to null"
   and "leave it alone" are different intentions. For a filter they are the same intention, and
   introducing a distinction the domain does not have only creates a branch to get wrong.)

The integration suite pins this: omitted, ``filters: null``, and a filter object whose every field
is explicitly ``null`` all return the identical result set.

.. rubric:: The limit is resolved here and clamped somewhere else, on purpose

:meth:`LogFilterInput.to_log_query` resolves an omitted ``limit`` to ``DEFAULT_QUERY_LIMIT`` so the
:class:`~src.db.repository.LogQuery` this produces is fully explicit about what it is asking for.
It does **not** clamp to ``MAX_QUERY_LIMIT``. That clamp lives in
:func:`src.db.repository.clamp_limit`, inside the statement builder, and it stays there because the
spec (§2 item 22) requires the cap on *every* query path — the resolver, the connection resolver,
C5's DataLoader, C7's cache warm path and the C12 E2E script. A clamp applied at the GraphQL edge
protects only the callers that come through the GraphQL edge, which is a property that quietly
stops being true the first time something else calls the repository. Applying it in the one
function that constructs the statement makes "every path is capped" structurally true.

(The two are not redundant with each other: ``clamp_limit(None, settings)`` would also resolve to
``DEFAULT_QUERY_LIMIT``, so resolving here changes no behaviour. It is done anyway because a
``LogQuery`` that leaves ``limit`` as ``None`` carries less information than one that says what it
wants, and both read the same ``settings.default_query_limit`` so the two cannot disagree.)

.. rubric:: Validation happens in the conversion, and that placement is the requirement

Spec §2 item 34 asks for validation on **all** filter and mutation inputs.
:meth:`LogFilterInput.to_log_query` is the single conversion every read path performs — ``logs``,
``logsConnection``, and whatever C7's cache warm path turns out to be — so calling
:func:`src.graphql.validation.validate_log_filter` from inside it makes "the filters were checked"
a structural property rather than a line each resolver has to remember. A resolver added later
cannot forget it, because a resolver that skipped it would have no ``LogQuery`` to run.

It does mean a *conversion* function raises, which is worth stating out loud rather than
discovering. The alternative — validating in each resolver — puts the guarantee back in the hands
of whoever writes the next one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import strawberry
from strawberry.scalars import JSON

from src.config import Settings
from src.db.repository import (
    LogQuery,
    OrderEventQuery,
    PaymentEventQuery,
    UserEventQuery,
)
from src.graphql.enums import (
    LogLevel,
    OrderStatus,
    PaymentMethod,
    PaymentOutcome,
    UserActivity,
)
from src.graphql.validation import (
    validate_log_filter,
    validate_order_event_filter,
    validate_payment_event_filter,
    validate_user_event_filter,
)


@strawberry.input
class LogFilterInput:
    """The spec's §2 item 18 filter set. All six fields, all optional.

    Field names are published camel-cased (``startTime``, ``endTime``, ``searchText``) — see the
    naming note in :mod:`src.graphql.types`.
    """

    service: Optional[str] = None
    level: Optional[LogLevel] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    search_text: Optional[str] = None
    limit: Optional[int] = None

    def to_log_query(self, settings: Settings) -> LogQuery:
        """Validate this input, then map it onto the request object the repository understands.

        The only interesting conversion is ``level``: Strawberry hands the resolver a
        :class:`~src.graphql.enums.LogLevel` **member**, and the ``level`` column holds the
        member's ``value`` (the two are identical strings, pinned by
        :func:`src.graphql.enums._assert_levels_match_the_corpus`). Passing the member straight
        through would compare an ``Enum`` against a ``VARCHAR`` — asyncpg would reject it, and a
        driver rejection surfaces to the client as an opaque internal error rather than as the
        clean answer this conversion produces.

        Raises:
            src.graphql.errors.ValidationError: If any supplied filter breaks a rule in
                :mod:`src.graphql.validation` — an over-long or blank ``service``, an over-long
                ``searchText``, a NUL byte, or a ``startTime`` after its ``endTime``. Carries
                ``extensions.code = "VALIDATION_ERROR"`` and reaches the client as a normal errors
                envelope.
        """
        # Before the mapping, not after: a value that fails here must never reach a statement
        # builder, and validating the LogQuery instead would lose which GraphQL field to name.
        validate_log_filter(self)

        return LogQuery(
            service=self.service,
            level=self.level.value if self.level is not None else None,
            start_time=self.start_time,
            end_time=self.end_time,
            search_text=self.search_text,
            # Resolved, not clamped. See the module docstring.
            limit=self.limit if self.limit is not None else settings.default_query_limit,
        )


@strawberry.input
class CreateLogInput:
    """The ``createLog`` payload — spec §2 item 24, published as ``logData``.

    Three required fields and three optional ones, and the split is the domain's rather than a
    convenience: a log line without a source, a severity or a message is not a log line, while a
    timestamp, a metadata object and a correlation id are all things a real emitter legitimately
    does not have.

    ``level`` is the :class:`~src.graphql.enums.LogLevel` **enum**, so ``level: "EROR"`` is
    rejected during validation with a message naming the five legal values — before a resolver
    runs, before a session is opened. That is the same guarantee ``LogFilterInput`` gets, applied
    to the write path, and it is why nothing in :mod:`src.graphql.validation` checks ``level``.

    ``timestamp`` omitted means **now, server-side**. Not "now, client-side": a client's clock is
    not something this server can vouch for, and the C6 subscription stream orders by this column.
    The default is applied in :meth:`src.db.repository.LogRepository.insert_log`, which is the one
    place in the project allowed to read the wall clock for a stored row.

    ``metadata`` is a ``JSON`` scalar — untyped on the wire — so
    :func:`src.graphql.validation.validate_metadata` is what enforces that it is an *object* of
    bounded depth and size. Omitted, it is stored as SQL ``NULL`` rather than the JSONB scalar
    ``'null'``; see the ``none_as_null`` note on :class:`src.db.models.LogEntryORM`.
    """

    service: str
    level: LogLevel
    message: str
    timestamp: Optional[datetime] = None
    metadata: Optional[JSON] = None
    trace_id: Optional[str] = None


@strawberry.input(
    description=(
        "The createOrderEvent payload — one transition in an order's lifecycle. `orderId`, "
        "`userId` and `status` are the event; `service` defaults to the order service and `level` "
        "is derived from the status (CANCELLED and REFUNDED are WARNING, the rest INFO) so a "
        "caller states only what it actually knows."
    )
)
class CreateOrderEventInput:
    """The ``createOrderEvent`` payload — C12, published as ``orderData``.

    Three required fields and five optional ones. The split is the domain's: an order status
    transition without an order, an acting user or a status is not an event, while the emitting
    service, the severity, the instant, a metadata object and a correlation id are all things a
    real emitter legitimately leaves to the server.

    ``status`` is the :class:`~src.graphql.enums.OrderStatus` **enum**, so ``status: "SHIPED"`` is
    rejected during validation with a message naming the seven legal values — before a resolver
    runs, before a session is opened, and before anything is published to a subscriber. That is the
    same guarantee ``level`` gives ``CreateLogInput``, and it is why nothing in
    :mod:`src.graphql.validation` checks ``status``.

    ``service`` and ``level`` are ``Optional`` **with server-side defaults** rather than required —
    see :func:`src.graphql.validation.validate_create_order_event` for the argument, which is that
    an order event's emitter is known and its severity is a function of its status.

    ``timestamp`` omitted means **now, server-side**, applied in
    :meth:`src.db.repository.LogRepository.insert_order_event` — the one place in the project
    allowed to read the wall clock for a stored row. Not "now, client-side": ``orderStatusStream``
    orders by this column and a client's clock is not something this server can vouch for.

    .. rubric:: There is deliberately no ``createPaymentEvent`` or ``createUserEvent`` beside it

    Spec §3 Feature Area C asks for a stream of **order status transitions**, and this mutation
    exists to be that stream's event source (the same role ``createLog`` plays for ``logStream``).
    Two more write paths for two streams nothing subscribes to would be surface with no reader —
    and payment and user events are already reachable, seeded and queryable, which is what the spec
    asks of them.
    """

    order_id: str
    user_id: str
    status: OrderStatus
    service: Optional[str] = None
    level: Optional[LogLevel] = None
    timestamp: Optional[datetime] = None
    metadata: Optional[JSON] = None
    trace_id: Optional[str] = None


# =================================================================================================
# C10 — the e-commerce event filters (spec §3 Feature Area A)
#
# Three inputs, written out FLAT rather than derived from a shared base. Strawberry builds each
# input type from the dataclass fields it can see, and a shared `@strawberry.input` base would be a
# fourth input type in the schema that nothing references — or, if left undecorated, a base whose
# fields Strawberry's collection is not contractually obliged to pick up. The published SDL is flat
# either way, so the only thing inheritance would save here is nine lines of source, at the cost of
# making the published contract depend on an inheritance detail. The five shared fields ARE shared
# where it matters: `validation._validate_common_event_filter` checks them once, and
# `repository.build_common_event_predicates` turns them into WHERE clauses once.
#
# Every field is optional, and an omitted field is IGNORED rather than matched against NULL — the
# same rule `LogFilterInput` documents at the top of this module, and the same reason a supplied
# `null` and an omitted key cannot diverge.
#
# The `limit` discipline is also identical: resolved here to `DEFAULT_QUERY_LIMIT`, CLAMPED in the
# statement builder, so "the cap applies on every query path" stays structurally true.
# =================================================================================================


@strawberry.input(description="Filters for Query.orderEvents. Every field is optional and ANDed.")
class OrderEventFilterInput:
    """The order-event filter set: the shared five, plus the three order dimensions."""

    service: Optional[str] = None
    level: Optional[LogLevel] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    trace_id: Optional[str] = None
    order_id: Optional[str] = None
    user_id: Optional[str] = None
    status: Optional[OrderStatus] = None
    #: Substring match on ``orderId``. Not on a message — an order event has no free text.
    search_text: Optional[str] = None
    limit: Optional[int] = None

    def to_query(self, settings: Settings) -> OrderEventQuery:
        """Validate, then map onto the request object the repository understands.

        Enum members are reduced to their ``value`` for the same reason
        :meth:`LogFilterInput.to_log_query` reduces ``level``: the column holds the string, and
        passing an ``Enum`` through would have asyncpg compare it against a ``VARCHAR`` — a driver
        rejection, which reaches the client as an opaque internal error rather than an answer.

        Raises:
            src.graphql.errors.ValidationError: If any supplied filter breaks a rule in
                :mod:`src.graphql.validation`.
        """
        validate_order_event_filter(self)
        return OrderEventQuery(
            service=self.service,
            level=self.level.value if self.level is not None else None,
            start_time=self.start_time,
            end_time=self.end_time,
            trace_id=self.trace_id,
            order_id=self.order_id,
            user_id=self.user_id,
            status=self.status.value if self.status is not None else None,
            search_text=self.search_text,
            limit=self.limit if self.limit is not None else settings.default_query_limit,
        )


@strawberry.input(description="Filters for Query.paymentEvents. Every field is optional and ANDed.")
class PaymentEventFilterInput:
    """The payment-event filter set: the shared five, plus order id, method and outcome."""

    service: Optional[str] = None
    level: Optional[LogLevel] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    trace_id: Optional[str] = None
    order_id: Optional[str] = None
    method: Optional[PaymentMethod] = None
    outcome: Optional[PaymentOutcome] = None
    #: Substring match on ``orderId`` — the identifier a payment event is filed under.
    search_text: Optional[str] = None
    limit: Optional[int] = None

    def to_query(self, settings: Settings) -> PaymentEventQuery:
        """Validate, then map onto :class:`~src.db.repository.PaymentEventQuery`."""
        validate_payment_event_filter(self)
        return PaymentEventQuery(
            service=self.service,
            level=self.level.value if self.level is not None else None,
            start_time=self.start_time,
            end_time=self.end_time,
            trace_id=self.trace_id,
            order_id=self.order_id,
            method=self.method.value if self.method is not None else None,
            outcome=self.outcome.value if self.outcome is not None else None,
            search_text=self.search_text,
            limit=self.limit if self.limit is not None else settings.default_query_limit,
        )


@strawberry.input(description="Filters for Query.userEvents. Every field is optional and ANDed.")
class UserEventFilterInput:
    """The user-event filter set: the shared five, plus user id and activity type."""

    service: Optional[str] = None
    level: Optional[LogLevel] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    trace_id: Optional[str] = None
    user_id: Optional[str] = None
    activity_type: Optional[UserActivity] = None
    #: Substring match on ``userId``.
    search_text: Optional[str] = None
    limit: Optional[int] = None

    def to_query(self, settings: Settings) -> UserEventQuery:
        """Validate, then map onto :class:`~src.db.repository.UserEventQuery`."""
        validate_user_event_filter(self)
        return UserEventQuery(
            service=self.service,
            level=self.level.value if self.level is not None else None,
            start_time=self.start_time,
            end_time=self.end_time,
            trace_id=self.trace_id,
            user_id=self.user_id,
            activity_type=(
                self.activity_type.value if self.activity_type is not None else None
            ),
            search_text=self.search_text,
            limit=self.limit if self.limit is not None else settings.default_query_limit,
        )


def to_log_query(filters: Optional[LogFilterInput], settings: Settings) -> LogQuery:
    """``LogFilterInput | None`` -> :class:`LogQuery`, with ``None`` meaning "no filters at all".

    Exists so that every resolver spells the "the client sent no ``filters`` argument" case the
    same way. ``filters: null`` and an omitted ``filters`` argument both arrive here as ``None``
    and both produce an unfiltered query capped at ``DEFAULT_QUERY_LIMIT`` — which is precisely
    the spec's "omitted filters are ignored", applied one level up from the individual fields.

    Nothing to validate in the ``None`` branch: "no filters" cannot break a rule.
    """
    if filters is None:
        return LogQuery(limit=settings.default_query_limit)
    return filters.to_log_query(settings)


def to_order_event_query(
    filters: Optional[OrderEventFilterInput], settings: Settings
) -> OrderEventQuery:
    """``OrderEventFilterInput | None`` -> query object, ``None`` meaning "no filters at all"."""
    if filters is None:
        return OrderEventQuery(limit=settings.default_query_limit)
    return filters.to_query(settings)


def to_payment_event_query(
    filters: Optional[PaymentEventFilterInput], settings: Settings
) -> PaymentEventQuery:
    """``PaymentEventFilterInput | None`` -> query object, ``None`` meaning "no filters at all"."""
    if filters is None:
        return PaymentEventQuery(limit=settings.default_query_limit)
    return filters.to_query(settings)


def to_user_event_query(
    filters: Optional[UserEventFilterInput], settings: Settings
) -> UserEventQuery:
    """``UserEventFilterInput | None`` -> query object, ``None`` meaning "no filters at all"."""
    if filters is None:
        return UserEventQuery(limit=settings.default_query_limit)
    return filters.to_query(settings)
