"""The ``Query`` root type: ``logs``, ``log`` and ``logsConnection``.

.. rubric:: WHY ``logs`` RETURNS A BARE LIST AND NOT A CONNECTION — read before "improving" it

Spec §5 lists this as a literal acceptance command::

    curl -X POST /graphql -d '{"query": "{ logs { id service level message } }"}'

That document only validates if ``logs`` is ``[LogEntry!]!``. Against a Relay connection the same
string is a **validation error** — ``id``, ``service``, ``level`` and ``message`` are not fields of
a connection — so "upgrading ``logs`` to return ``LogConnection``" would break the spec's own
verification command while every unit test that asserts on shapes kept passing. Cursor pagination
is a §4 *bonus*; the core requirement is a filtered list capped by ``limit``.

So the two live side by side on **separate fields**:

* ``logs(filters)`` -> ``[LogEntry!]!`` — the core surface. Capped by ``limit``.
* ``logsConnection(filters, first, after)`` -> ``LogConnection!`` — the bonus. Keyset cursors.

They share :class:`~src.graphql.inputs.LogFilterInput` and the same predicate builder, so a filter
means precisely the same thing on both; only the windowing differs. Adding a field is cheap.
Changing the type of an existing one is a contract break, and this one is a contract break against
the acceptance criteria.

.. rubric:: Every resolver is ``async def``

Not stylistic. One uvicorn worker serves the load harness's 100 concurrent requests, and a
synchronous resolver would hold the event loop for the whole of its database round trip — turning
the spec's "100 concurrent requests, sub-100ms average" into a measurement of how long 100 queries
take end to end. The driver is asyncpg for the same reason (see ``database_url`` in
:mod:`src.config`).

.. rubric:: Only what the client asked for is resolved (spec §2 item 28)

Strawberry invokes a field resolver only when the field appears in the selection set, and
serialises only the selected fields. Nothing here defeats that: :meth:`Query.logs` builds
:class:`~src.graphql.types.LogEntry` objects out of rows the database returned anyway, so a
``{ logs { id } }`` response carries exactly one key per entry. Where this becomes load-bearing is
``LogEntry.relatedLogs`` (C5), which is a *field resolver* precisely so that it costs nothing until
a client selects it — computed eagerly in ``from_orm``, every query would pay for correlated
lookups it never asked for while the response shape stayed identical, so no test of the payload
could notice.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from typing import Optional

import strawberry

from src.cache import (
    cached_log_stats,
    cached_logs,
    cached_order_funnel,
    cached_order_status_distribution,
    cached_payment_outcome_breakdown,
)
from src.db.repository import (
    FunnelBucket,
    LogQuery,
    LogStatsResult,
    OrderStatusBucket,
    PaymentOutcomeBucket,
    clamp_limit,
)
from src.graphql import errors
from src.graphql.context import Context

# Two exception types spell "bad cursor" in this project and they are not interchangeable. The one
# imported here is the codec's plain `ValueError`, raised by `decode_cursor`; the one this module
# raises in its place is `errors.InvalidCursorError`, which carries `extensions.code`. Keeping the
# `errors.` qualifier on the second is what makes the translation visible at the point it happens
# rather than hidden behind two identical-looking names. See src/graphql/cursor.py.
from src.graphql.cursor import InvalidCursorError, decode_cursor, encode_cursor
from src.graphql.ecommerce import (
    FunnelStage,
    OrderEvent,
    OrderStatusCount,
    PaymentEvent,
    PaymentOutcomeCount,
    UserEvent,
)
from src.graphql.inputs import (
    LogFilterInput,
    OrderEventFilterInput,
    PaymentEventFilterInput,
    UserEventFilterInput,
    to_log_query,
    to_order_event_query,
    to_payment_event_query,
    to_user_event_query,
)
from src.graphql.types import (
    LogConnection,
    LogEdge,
    LogEntry,
    LogEvent,
    LogStats,
    PageInfo,
)
from src.graphql.validation import validate_time_range, validate_trace_id


def _parse_entity_id(raw: str, type_name: str) -> int:
    """Turn a GraphQL ``ID`` into the ``BIGSERIAL`` primary key it names.

    ``ID`` is a string on the wire, and the ids this server issues are decimal integers. Anything
    else is a malformed request rather than a miss, so it is rejected with a readable error instead
    of being folded into "not found": returning ``null`` for ``log(id: "abc")`` would tell a client
    with a broken id-building bug that the row simply does not exist, and it would go on believing
    that forever.

    ``type_name`` is in the message rather than inferred, because the four by-id fields read four
    **different** BIGSERIAL sequences: ``LogEntry`` 42 and ``OrderEvent`` 42 are two rows in two
    tables that happen to have reached the same number, and an error that did not say which type it
    was talking about would be actively misleading to a client juggling both.

    Raises:
        src.graphql.errors.ValidationError: If ``raw`` is not a run of digits. C4 gave this the
            ``VALIDATION_ERROR`` code it had been missing — it was a bare ``GraphQLError``, which
            reached the client with a good message and no way to branch on it. Raising a
            :class:`~src.graphql.errors.DomainError` also keeps it out of the masking path and out
            of the stack-trace log, both of which treat an uncoded resolver exception as a fault.
    """
    if not raw.isascii() or not raw.isdigit():
        raise errors.ValidationError(
            f"invalid {type_name} id {raw!r}: ids issued by this server are decimal integers"
        )
    return int(raw)


def _parse_log_id(raw: str) -> int:
    """``_parse_entity_id`` for ``LogEntry`` — the spelling C3's ``Query.log`` was written against."""
    return _parse_entity_id(raw, "LogEntry")


@strawberry.type
class Query:
    """The read surface: ``logs``, ``log``, ``logsConnection`` and ``logStats``.

    C10/C11 add the e-commerce entry points alongside these.
    """

    @strawberry.field
    async def logs(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[LogFilterInput] = None,
    ) -> list[LogEntry]:
        """Entries matching every supplied filter, newest first, capped by ``limit``.

        The argument is named ``filters`` because the spec writes ``Query.logs(filters)``; renaming
        it would break every published example.

        All of the actual behaviour — AND-composition, ignoring omitted filters, ILIKE escaping,
        UTC normalisation, ``(timestamp DESC, id DESC)`` ordering and the ``MAX_QUERY_LIMIT``
        clamp — lives in :mod:`src.db.repository` and is exercised identically by every other
        caller. This resolver's whole job is the translation at the edges: GraphQL input in,
        published type out.

        .. rubric:: Cached, cache-aside, keyed on the filters — and STALE FOR UP TO THE TTL

        C7 wraps the load in :func:`src.cache.cached_logs`. A hit returns fully reconstructed
        :class:`~src.graphql.types.LogEntry` objects and issues **zero** SQL (spec §2 item 31,
        proven by a statement counter in the integration suite).

        **This is not write-through.** A ``createLog`` does not invalidate anything, so a result
        already in the cache keeps answering without the new row for up to ``CACHE_TTL_SECONDS``
        (30). That is the spec's own choice of TTL-over-invalidation and it is argued in full in
        :mod:`src.cache`; the short version is that invalidating "every key whose filters this row
        matches" needs either a reverse index or a keyspace scan per write, and both are elaborate
        machinery guarding a window that is thirty seconds wide anyway. Clients that need the live
        view subscribe to ``logStream``, which is never cached.

        The key is the **filter set**, not the selection set — so ``{ logs { id } }`` populates the
        same entry ``{ logs { id message metadata } }`` reads. That is safe precisely because
        :meth:`~src.graphql.types.LogEntry.from_orm` always projects the whole row: what is cached
        is the complete entry, and the selection set is applied by Strawberry afterwards, on the
        way out, exactly as it is on a miss.
        """
        context = info.context
        query = to_log_query(filters, context.settings)

        async def load() -> list[LogEntry]:
            async with context.repository() as repository:
                rows = await repository.list_logs(query)
                # Projected INSIDE the block. `expire_on_commit=False` means detached instances stay
                # usable, so doing it outside would also work today — but it would work by relying
                # on a session-configuration detail rather than on the object still being attached,
                # and that is the kind of dependency that breaks silently when somebody adds a
                # commit.
                return [LogEntry.from_orm(row) for row in rows]

        return await cached_logs(context.cache, query, context.settings, load)

    @strawberry.field
    async def log(
        self,
        info: strawberry.Info[Context, None],
        id: strawberry.ID,  # noqa: A002 - the spec names this argument `id`
    ) -> Optional[LogEntry]:
        """One entry by id, or ``null`` when there is no such row.

        ``null`` with **no** ``errors`` entry for a miss: absence is an ordinary answer to
        "is there a row with this id", not a failure. (C4 gives genuinely exceptional lookups a
        ``NOT_FOUND`` code; a single-entity fetch is not one of them.)

        .. rubric:: Routed through the by-id DataLoader (C5)

        Not because one lookup needs batching — because a *document* is not one lookup. A client
        hydrating a list of ids writes ``{ a: log(id:"1"){…} b: log(id:"2"){…} … }``, and every
        alias is a separate resolver call; without the loader that is one statement per alias.
        Through it, the whole selection set collapses into a single
        ``WHERE id IN (…)``. The loader also memoises within the operation, so an entry reached
        twice — once here, once through another entry's ``relatedLogs`` — is fetched once.

        The loader is per-operation (see :class:`src.graphql.context.PerOperationResources`), so
        that memoisation can never serve a stale row to a later request.
        """
        log_id = _parse_log_id(id)

        # Already a published `LogEntry` (or None): the projection happens inside the batch, while
        # the rows are still attached to the session that loaded them.
        return await info.context.loaders.log_by_id.load(log_id)

    @strawberry.field
    async def logs_connection(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[LogFilterInput] = None,
        first: Optional[int] = None,
        after: Optional[str] = None,
    ) -> LogConnection:
        """A keyset-paginated window over the same filtered result — published ``logsConnection``.

        The §4 bonus, on its own field so ``logs`` can keep the shape the spec's acceptance command
        requires. See this module's docstring for why that separation is not negotiable, and
        :func:`src.db.repository.build_keyset_log_select` for why the cursor is a key rather than
        an offset.

        Args:
            filters: Exactly the same input ``logs`` takes, meaning exactly the same thing.
            first: Page size. Overrides ``filters.limit`` when both are supplied — ``first`` is the
                connection spelling of the same idea, and letting the more specific argument win is
                less surprising than either erroring or silently taking the smaller. Clamped to
                ``MAX_QUERY_LIMIT`` by the builder like every other path.
            after: An opaque cursor from a previous page's ``endCursor``. A malformed value is a
                clean GraphQL error naming the problem — never a traceback and never a 500.
        """
        context = info.context
        settings = context.settings

        filter_query = to_log_query(filters, settings)
        page_query = replace(filter_query, limit=first if first is not None else filter_query.limit)

        after_key = None
        if after is not None:
            try:
                after_key = decode_cursor(after)
            except InvalidCursorError as exc:
                # Translated at the boundary: `src.graphql.cursor` stays free of the GraphQL layer
                # so it can be unit tested as pure logic, and this is the one place that knows how
                # to turn its failure into a response. Letting the ValueError escape would still
                # produce an `errors` envelope, but C4 installs `MaskErrors`, after which an
                # unrecognised exception becomes "an unexpected error occurred" — hiding a problem
                # the client could have fixed itself.
                #
                # C4 also unified the code: this used to be a bare `GraphQLError` with a
                # hand-written `extensions={"code": "INVALID_CURSOR"}` dict, i.e. a second
                # convention for spelling something the taxonomy now owns. The wire format is
                # identical; what changed is that the string lives in one enum.
                raise errors.InvalidCursorError(str(exc)) from exc

        async with context.repository() as repository:
            rows, has_next_page = await repository.list_logs_page(page_query, after=after_key)
            # Counted over `filter_query`, which carries the filters and NOT the cursor: the number
            # answers "how big is this result set", so it must not shrink as the client pages
            # through it. `count_logs` ignores the limit for the same reason.
            total_count = await repository.count_logs(filter_query)
            edges = [
                LogEdge(
                    cursor=encode_cursor(row.timestamp, row.id),
                    node=LogEntry.from_orm(row),
                )
                for row in rows
            ]

        return LogConnection(
            edges=edges,
            page_info=PageInfo(
                has_next_page=has_next_page,
                # "The client asked to resume from somewhere", which is the only honest answer a
                # forward-only connection can give. See PageInfo's docstring.
                has_previous_page=after is not None,
                start_cursor=edges[0].cursor if edges else None,
                end_cursor=edges[-1].cursor if edges else None,
            ),
            total_count=total_count,
        )

    @strawberry.field
    async def log_stats(
        self,
        info: strawberry.Info[Context, None],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> LogStats:
        """Aggregate summary of the entries in a time window — spec §2 item 23.

        Published as ``logStats``, with ``startTime`` / ``endTime`` arguments. **Both optional**,
        because the spec's own acceptance command supplies neither::

            { logStats { totalLogs errorCount services } }

        Omitting a bound means that end is unbounded, exactly as it does on ``LogFilterInput`` —
        the two share :func:`src.db.repository.build_predicates`, so "the last hour" means the same
        instant range to ``logs`` and to ``logStats``, and a dashboard's summary can never describe
        a different window from the table beneath it.

        The arguments are the spec's two and no more. It would be easy to accept the whole
        ``LogFilterInput`` here — the repository already takes a full :class:`LogQuery` and would
        apply every predicate — but ``limit`` is meaningless for an aggregate and ``searchText``
        would turn a summary into a trigram scan. C11 widens this deliberately, for the
        multi-dimensional aggregations that need it.

        Every number is computed by PostgreSQL. See the section comment above
        :class:`~src.db.repository.LogStatsResult` for what pulling rows and counting in Python
        would cost, and why the silently-capped total it produces is the failure that would survive
        a casual test.

        .. rubric:: Cached under its OWN TTL, and the cached thing is the repository's result

        ``AGG_CACHE_TTL_SECONDS`` (60), not ``CACHE_TTL_SECONDS`` (30) — spec §3 Feature Area D
        asks for a TTL policy defined per aggregation, and an aggregate earns a longer one on both
        counts: it is the more expensive query (two ``GROUP BY``-class scans over the whole window
        rather than a limited index read) and the less sensitive answer (one new row moves a count
        of thousands by one). The policy table lives in :data:`src.cache.TTL_POLICY`.

        What is cached is :class:`~src.db.repository.LogStatsResult`, the *repository's* value —
        and :meth:`~src.graphql.types.LogStats.from_result` then runs on the hit path and the miss
        path alike. Caching the published object instead would have made that projection skippable,
        and a skippable projection is a second implementation of the ordering and the derived
        ``services`` list waiting to be written.

        The staleness note on :meth:`logs` applies here too, with the longer bound: a ``createLog``
        is invisible to an already-cached summary for up to a minute. See :mod:`src.cache`.

        Raises:
            src.graphql.errors.ValidationError: If ``startTime`` is after ``endTime``. That range
                cannot match a row, so without the check it would return a confident set of zeros
                indistinguishable from a genuinely quiet window.
        """
        # Returns the bounds normalised to UTC. Passing those on rather than the raw arguments
        # keeps the value that was *validated* and the value that reaches the WHERE clause the
        # same object — `build_predicates` would normalise again, identically, but a check made
        # against one instant and a query run against another is a bug waiting for a mixed-offset
        # client to find it. It is also what the cache key is derived from, so two clients asking
        # for the same instant in two different offsets land on one key rather than two.
        start, end = validate_time_range(start_time, end_time)
        query = LogQuery(start_time=start, end_time=end)
        context = info.context

        async def load() -> LogStatsResult:
            async with context.repository() as repository:
                return await repository.log_stats(query)

        return LogStats.from_result(await cached_log_stats(context.cache, query, load))

    # =============================================================================================
    # C10 — the e-commerce entry points (spec §3 Feature Area A)
    #
    # FOUR fields, and the fourth is the one that matters. The three list fields are deliberately
    # FLAT — one filtered index read each, the same shape as `logs`, no nesting and no aggregation —
    # because C11 owns multi-dimensional composition, cross-entity traversal, the DataLoaders that
    # batch it and the cached aggregates on top. Shipping a nested `OrderEvent.payments` here would
    # mean shipping it without its loader, i.e. one SELECT per order.
    #
    # `correlatedEvents` is what makes `LogEvent` a real interface rather than a documentation
    # device: it is the only field whose type is the interface, so it is the only place a client
    # must write inline fragments, and it is what proves the four implementors are actually
    # substitutable. It is also the single query the correlation id exists for.
    #
    # NOT CACHED, and that is a C11 decision rather than an oversight: spec §3 Feature Area D asks
    # for a TTL policy defined PER AGGREGATION, and these are not aggregations — they are filtered
    # row reads whose cache key would be a fourth entry in `src.cache.TTL_POLICY` written before
    # anybody has measured what it should be. `Query.logs` is cached because C7 measured it.
    # =============================================================================================

    @strawberry.field(
        description=(
            "Order lifecycle events matching every supplied filter, newest first, capped by "
            "`limit`. Orders are an append-only stream, so an order's current status is the "
            "status of its newest event."
        )
    )
    async def order_events(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[OrderEventFilterInput] = None,
    ) -> list[OrderEvent]:
        """Order events matching the supplied filters — spec §3 Feature Area A.

        Structurally identical to :meth:`logs`: the filtering, AND-composition, ILIKE escaping, UTC
        normalisation, ``(timestamp DESC, id DESC)`` ordering and ``MAX_QUERY_LIMIT`` clamp all live
        in :mod:`src.db.repository` and are shared with every other read path. This resolver is the
        translation at the edges and nothing else.
        """
        context = info.context
        query = to_order_event_query(filters, context.settings)

        async with context.repository() as repository:
            rows = await repository.list_order_events(query)
            # Projected inside the block, while the rows are still attached to the session that
            # loaded them — the same rule every other read path follows.
            return [OrderEvent.from_orm(row) for row in rows]

    @strawberry.field(
        description=(
            "User activity events matching every supplied filter, newest first, capped by `limit`."
        )
    )
    async def user_events(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[UserEventFilterInput] = None,
    ) -> list[UserEvent]:
        """User activity events matching the supplied filters — spec §3 Feature Area A."""
        context = info.context
        query = to_user_event_query(filters, context.settings)

        async with context.repository() as repository:
            rows = await repository.list_user_events(query)
            return [UserEvent.from_orm(row) for row in rows]

    @strawberry.field(
        description=(
            "Payment events matching every supplied filter, newest first, capped by `limit`. A "
            "payment is a stream of outcomes (authorized, captured, declined, refunded) filed "
            "under one orderId, not a single mutable record."
        )
    )
    async def payment_events(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[PaymentEventFilterInput] = None,
    ) -> list[PaymentEvent]:
        """Payment events matching the supplied filters — spec §3 Feature Area A."""
        context = info.context
        query = to_payment_event_query(filters, context.settings)

        async with context.repository() as repository:
            rows = await repository.list_payment_events(query)
            return [PaymentEvent.from_orm(row) for row in rows]

    @strawberry.field(
        description=(
            "Every event of every kind carrying this trace id, newest first — log lines, order "
            "status transitions, payment outcomes and user actions in one heterogeneous list. "
            "Returns the LogEvent interface, so a client selects per-type fields with inline "
            "fragments (`... on OrderEvent { orderId status }`). This is the question a "
            "correlation id exists to answer, and answering it in one round trip is what would "
            "otherwise take four REST calls."
        )
    )
    async def correlated_events(
        self,
        info: strawberry.Info[Context, None],
        trace_id: str,
        limit: Optional[int] = None,
    ) -> list[LogEvent]:
        """Everything correlated with one trace id, across all four event types.

        .. rubric:: Why this field is the proof that ``LogEvent`` is an interface and not a comment

        Every other field in the schema returns a concrete type, so an interface implemented by
        four of them would be unobservable: a client could never write a selection that needed it.
        This one returns ``[LogEvent!]!``, so ``__typename`` and inline fragments are the *only* way
        to read it — which means the interface is exercised by every caller rather than by a
        docstring.

        .. rubric:: Four statements, one per table, and the cap is per-table

        Deliberately four flat SELECTs rather than a ``UNION ALL``: the four tables have different
        columns, so a union would have to project them onto a common column list and then re-split
        the rows in Python — more code, one statement, and a plan the planner cannot use each
        table's own ``trace_id`` index for as cleanly. Four indexed equality reads on four small
        result sets is the cheaper and far more legible shape.

        ``limit`` is applied **per table**, so the worst case is ``4 x MAX_QUERY_LIMIT`` rows. That
        is stated rather than hidden: a single per-trace cap would need the merge to happen in SQL,
        which is the union this deliberately is not. A trace holding more than ``MAX_QUERY_LIMIT``
        events of one kind is a retry storm, and truncating the newest of each kind is the right
        answer for it.

        .. rubric:: C11 closed the seam: the four reads go through the four trace-id DataLoaders

        Before C11 this issued four flat SELECTs directly on the operation's session. It still
        issues four statements — but they are now **batched by trace id**, so a document naming
        several traces under aliases (``a: correlatedEvents(traceId: "x") b: correlatedEvents(…)``)
        costs four statements in total rather than four *per alias*. Nothing about the answer moved:
        the loaders return the same rows in the same ``(timestamp DESC, id DESC)`` order and the
        per-table cap is applied here instead of in the statement's ``LIMIT``, which is the same
        truncation of the same newest-first list.

        The four loads are gathered rather than awaited in sequence. They are four *different*
        loaders, so they cannot merge into one batch whatever happens; gathering them just means
        the four batches are dispatched in one tick instead of four, and each one takes the
        operation's session in turn (one session is one connection, so they queue either way).

        **No session is held across these awaits, and that is a hard requirement rather than a
        style choice** — a loader dispatches its batch in its own task, and a resolver holding the
        session while awaiting one would deadlock against itself. See
        :class:`src.graphql.context.OperationResources`.

        Raises:
            src.graphql.errors.ValidationError: If ``traceId`` is blank, over-long or contains a
                NUL byte.
        """
        context = info.context
        # Required, so it is validated here rather than inside a filter conversion. See
        # `validate_trace_id` for why the argument is required at all.
        trace = validate_trace_id(trace_id)
        loaders = context.loaders
        # Resolved once and applied to all four lists, so "per table" means one number rather than
        # four clamps that could drift apart.
        cap = clamp_limit(limit, context.settings)

        log_entries, order_events, payment_events, user_events = await asyncio.gather(
            # The SAME loader `LogEntry.relatedLogs` uses, deliberately: the correlation read here
            # and the one behind that field cannot disagree about what "sharing a trace" means,
            # because there is one statement builder and one loader between them.
            loaders.logs_by_trace_id.load(trace),
            loaders.order_events_by_trace_id.load(trace),
            loaders.payment_events_by_trace_id.load(trace),
            loaders.user_events_by_trace_id.load(trace),
        )

        events: list[LogEvent] = []
        # Sliced, never mutated: a loader hands the *same list object* to every caller of a key, so
        # truncating in place would shorten it for the next reader of that trace in this operation.
        events.extend(log_entries[:cap])
        events.extend(order_events[:cap])
        events.extend(payment_events[:cap])
        events.extend(user_events[:cap])

        # Merged newest-first in Python, because the four statements each ordered their own table
        # and nothing has interleaved them yet. The `id` is NOT part of the sort key: it is unique
        # only within its own table, so using it as a tiebreak across four BIGSERIAL sequences would
        # order by an accident of which table happened to be written first. `__typename` is the
        # tiebreak instead — arbitrary, but stable and total, which is what a diff-stable response
        # needs.
        events.sort(key=lambda event: (event.timestamp, type(event).__name__), reverse=True)
        return events

    # =============================================================================================
    # C11 — the by-id entry points (spec §3 Feature Area D: "DataLoader batching extended across all
    # entity types, not just logs").
    #
    # Three fields that mirror `Query.log(id:)` exactly, including the reason it goes through a
    # loader at all: not because ONE lookup needs batching, but because a DOCUMENT is not one
    # lookup. A client hydrating a list of ids writes `{ a: orderEvent(id:"1"){…} b: orderEvent(
    # id:"2"){…} … }` and every alias is a separate resolver call, which without the loader is one
    # statement per alias. Through it the whole selection set collapses into a single
    # `WHERE id IN (…)`.
    #
    # `null` for a miss, with NO `errors` entry, on all three: absence is an ordinary answer to "is
    # there a row with this id". C4 reserves `NOT_FOUND` for lookups where absence really is
    # exceptional, and a single-entity fetch is not one of them.
    # =============================================================================================

    @strawberry.field(
        description=(
            "One order event by id, or null when there is no such row. Batched with its siblings "
            "by a per-operation DataLoader, so a document naming several under aliases costs one "
            "query."
        )
    )
    async def order_event(
        self,
        info: strawberry.Info[Context, None],
        id: strawberry.ID,  # noqa: A002 - matches `Query.log(id:)`, which the spec names this way
    ) -> Optional[OrderEvent]:
        """One order event by id — batched through ``loaders.order_event_by_id``."""
        return await info.context.loaders.order_event_by_id.load(
            _parse_entity_id(id, "OrderEvent")
        )

    @strawberry.field(
        description="One payment event by id, or null when there is no such row. Batched by id."
    )
    async def payment_event(
        self,
        info: strawberry.Info[Context, None],
        id: strawberry.ID,  # noqa: A002
    ) -> Optional[PaymentEvent]:
        """One payment event by id — batched through ``loaders.payment_event_by_id``."""
        return await info.context.loaders.payment_event_by_id.load(
            _parse_entity_id(id, "PaymentEvent")
        )

    @strawberry.field(
        description="One user activity event by id, or null when there is no such row. Batched by id."
    )
    async def user_event(
        self,
        info: strawberry.Info[Context, None],
        id: strawberry.ID,  # noqa: A002
    ) -> Optional[UserEvent]:
        """One user event by id — batched through ``loaders.user_event_by_id``."""
        return await info.context.loaders.user_event_by_id.load(
            _parse_entity_id(id, "UserEvent")
        )

    # =============================================================================================
    # C11 — the cached aggregates (spec §3 Feature Area D: "Redis caching applied to aggregations,
    # with an invalidation or TTL policy defined per aggregation").
    #
    # THREE root fields rather than one `ecommerceStats` object, and the choice is the requirement's
    # rather than a preference. "A TTL policy defined PER AGGREGATION" needs the aggregations to be
    # separately cacheable, and a single object field is a single cache entry with a single TTL —
    # the policy would have exactly one row and would be a constant wearing a table's clothes. As
    # three fields they get three keys, three TTLs (20s / 300s / 60s — see `src.cache.TTL_POLICY`
    # for what property of each aggregate chose its number) and three independent expiries.
    #
    # Feature Area E's "dashboard renders multi-series analytics from a SINGLE query result" is
    # unaffected, and this is worth being precise about because it looks like a tension: a GraphQL
    # document selects all three in one request and receives one `data` object. Three root FIELDS is
    # not three round trips — that conflation is the REST habit this project exists to demonstrate
    # against.
    #
    # ALL THREE ARE COMPUTED IN SQL. There is no `SELECT *` behind any of them; see the section
    # comment above `src.db.repository.OrderStatusBucket` for the three statements and for what
    # counting rows in Python would cost (a silently capped total, the whole table on the wire, and
    # all of it in memory in a process serving 100 concurrent requests).
    #
    # THEY REUSE THE EXISTING FILTER INPUTS AND THE EXISTING PREDICATE BUILDER. `filters` here is
    # the same `OrderEventFilterInput` `Query.orderEvents` takes, converted by the same
    # `to_order_event_query`, so an aggregate and the rows behind it can never disagree about what a
    # filter means — a dashboard's summary panel and the table under it describe one set. The one
    # field that means nothing here is `limit`, which every aggregate ignores (and which is
    # therefore absent from the cache key, so two page sizes do not compute one answer twice).
    # =============================================================================================

    @strawberry.field(
        description=(
            "How many orders are sitting at each status right now — one bucket per status, in "
            "lifecycle order, counted from each order's NEWEST event. Computed in SQL "
            "(DISTINCT ON + GROUP BY) and cached in Redis under its own short TTL, because one new "
            "order event moves an order between buckets rather than merely incrementing one. "
            "`filters.limit` is ignored: an aggregate describes the whole matching set."
        )
    )
    async def order_status_distribution(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[OrderEventFilterInput] = None,
    ) -> list[OrderStatusCount]:
        """Current-status distribution — spec §3 Feature Area D, cached at 20s.

        The projection (:meth:`OrderStatusCount.from_buckets`, which applies the lifecycle
        ordering) runs on the cache-hit path and the cache-miss path alike, because what is cached
        is the repository's bucket tuple rather than the published objects. See the Values section
        in :mod:`src.cache` for why that separation is not an accident.
        """
        context = info.context
        query = to_order_event_query(filters, context.settings)

        async def load() -> tuple[OrderStatusBucket, ...]:
            async with context.repository() as repository:
                return await repository.order_status_distribution(query)

        return OrderStatusCount.from_buckets(
            await cached_order_status_distribution(context.cache, query, load)
        )

    @strawberry.field(
        description=(
            "How many distinct orders have EVER reached each status, in lifecycle order, with each "
            "stage's share of the widest one. Cumulative rather than current, so an order counts at "
            "every stage it passed through — which is what makes this a conversion funnel. "
            "Computed in SQL (COUNT DISTINCT + GROUP BY) and cached under the longest TTL in the "
            "system, because the numbers are monotonic and a stale read can only undercount."
        )
    )
    async def order_funnel(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[OrderEventFilterInput] = None,
    ) -> list[FunnelStage]:
        """The order conversion funnel — spec §3 Feature Area D, cached at 300s."""
        context = info.context
        query = to_order_event_query(filters, context.settings)

        async def load() -> tuple[FunnelBucket, ...]:
            async with context.repository() as repository:
                return await repository.order_funnel(query)

        return FunnelStage.from_buckets(await cached_order_funnel(context.cache, query, load))

    @strawberry.field(
        description=(
            "The payment method x outcome cross-tabulation, busiest cell first. Each cell carries "
            "both the number of payment ATTEMPTS and the number of distinct ORDERS behind them, so "
            "retries are visible rather than folded into one number. Computed in SQL (GROUP BY + "
            "COUNT DISTINCT) and cached under the shared aggregate TTL."
        )
    )
    async def payment_outcome_breakdown(
        self,
        info: strawberry.Info[Context, None],
        filters: Optional[PaymentEventFilterInput] = None,
    ) -> list[PaymentOutcomeCount]:
        """The payment cross-tabulation — spec §3 Feature Area D, cached at 60s."""
        context = info.context
        query = to_payment_event_query(filters, context.settings)

        async def load() -> tuple[PaymentOutcomeBucket, ...]:
            async with context.repository() as repository:
                return await repository.payment_outcome_breakdown(query)

        return PaymentOutcomeCount.from_buckets(
            await cached_payment_outcome_breakdown(context.cache, query, load)
        )
