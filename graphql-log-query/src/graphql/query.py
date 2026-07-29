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

from dataclasses import replace
from datetime import datetime
from typing import Optional

import strawberry

from src.cache import cached_log_stats, cached_logs
from src.db.repository import LogQuery, LogStatsResult, clamp_limit
from src.graphql import errors
from src.graphql.context import Context

# Two exception types spell "bad cursor" in this project and they are not interchangeable. The one
# imported here is the codec's plain `ValueError`, raised by `decode_cursor`; the one this module
# raises in its place is `errors.InvalidCursorError`, which carries `extensions.code`. Keeping the
# `errors.` qualifier on the second is what makes the translation visible at the point it happens
# rather than hidden behind two identical-looking names. See src/graphql/cursor.py.
from src.graphql.cursor import InvalidCursorError, decode_cursor, encode_cursor
from src.graphql.ecommerce import OrderEvent, PaymentEvent, UserEvent
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


def _parse_log_id(raw: str) -> int:
    """Turn a GraphQL ``ID`` into the ``BIGSERIAL`` primary key it names.

    ``ID`` is a string on the wire, and the ids this server issues are decimal integers. Anything
    else is a malformed request rather than a miss, so it is rejected with a readable error instead
    of being folded into "not found": returning ``null`` for ``log(id: "abc")`` would tell a client
    with a broken id-building bug that the row simply does not exist, and it would go on believing
    that forever.

    Raises:
        src.graphql.errors.ValidationError: If ``raw`` is not a run of digits. C4 gave this the
            ``VALIDATION_ERROR`` code it had been missing — it was a bare ``GraphQLError``, which
            reached the client with a good message and no way to branch on it. Raising a
            :class:`~src.graphql.errors.DomainError` also keeps it out of the masking path and out
            of the stack-trace log, both of which treat an uncoded resolver exception as a fault.
    """
    if not raw.isascii() or not raw.isdigit():
        raise errors.ValidationError(
            f"invalid LogEntry id {raw!r}: ids issued by this server are decimal integers"
        )
    return int(raw)


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

        ``limit`` is applied **per table** by the shared clamp, so the worst case is
        ``4 x MAX_QUERY_LIMIT`` rows. That is stated rather than hidden: a single per-trace cap
        would need the merge to happen in SQL, which is the union this deliberately is not. A trace
        holding more than ``MAX_QUERY_LIMIT`` events of one kind is a retry storm, and truncating
        the newest of each kind is the right answer for it.

        C11 SEAM: the four loads are issued sequentially on one session, which is correct here (one
        session is one connection, so concurrency would only queue) but is exactly where the
        cross-entity DataLoaders go — at which point a batch of trace ids costs the same four
        statements as one.

        Raises:
            src.graphql.errors.ValidationError: If ``traceId`` is blank, over-long or contains a
                NUL byte.
        """
        context = info.context
        # Required, so it is validated here rather than inside a filter conversion. See
        # `validate_trace_id` for why the argument is required at all.
        trace = validate_trace_id(trace_id)
        settings = context.settings

        order_query = to_order_event_query(
            OrderEventFilterInput(trace_id=trace, limit=limit), settings
        )
        payment_query = to_payment_event_query(
            PaymentEventFilterInput(trace_id=trace, limit=limit), settings
        )
        user_query = to_user_event_query(
            UserEventFilterInput(trace_id=trace, limit=limit), settings
        )

        events: list[LogEvent] = []
        async with context.repository() as repository:
            # `list_logs_by_trace_ids` rather than a `LogQuery(trace_id=…)`: `LogQuery` has no
            # trace filter (C2 built it for the spec's six filter fields), and the batch builder is
            # the statement that already exists for exactly this lookup — it is what C5's
            # DataLoader runs, so the correlation read here and the one behind `relatedLogs` cannot
            # disagree about what "sharing a trace" means.
            log_rows = await repository.list_logs_by_trace_ids([trace])
            events.extend(LogEntry.from_orm(row) for row in log_rows[: clamp_limit(limit, settings)])
            events.extend(
                OrderEvent.from_orm(row)
                for row in await repository.list_order_events(order_query)
            )
            events.extend(
                PaymentEvent.from_orm(row)
                for row in await repository.list_payment_events(payment_query)
            )
            events.extend(
                UserEvent.from_orm(row)
                for row in await repository.list_user_events(user_query)
            )

        # Merged newest-first in Python, because the four statements each ordered their own table
        # and nothing has interleaved them yet. The `id` is NOT part of the sort key: it is unique
        # only within its own table, so using it as a tiebreak across four BIGSERIAL sequences would
        # order by an accident of which table happened to be written first. `__typename` is the
        # tiebreak instead — arbitrary, but stable and total, which is what a diff-stable response
        # needs.
        events.sort(key=lambda event: (event.timestamp, type(event).__name__), reverse=True)
        return events
