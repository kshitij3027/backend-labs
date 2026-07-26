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
``{ logs { id } }`` response carries exactly one key per entry. The place this becomes load-bearing
is C5's ``related_logs``, which must stay a *field resolver* — the moment it is computed eagerly in
``from_orm``, every query pays for correlated lookups it never asked for, and the response shape is
identical, so no test of the payload can notice. The seam is marked in
:mod:`src.graphql.types`.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Optional

import strawberry
from graphql import GraphQLError

from src.graphql.context import Context
from src.graphql.cursor import InvalidCursorError, decode_cursor, encode_cursor
from src.graphql.inputs import LogFilterInput, to_log_query
from src.graphql.types import LogConnection, LogEdge, LogEntry, PageInfo


def _parse_log_id(raw: str) -> int:
    """Turn a GraphQL ``ID`` into the ``BIGSERIAL`` primary key it names.

    ``ID`` is a string on the wire, and the ids this server issues are decimal integers. Anything
    else is a malformed request rather than a miss, so it is rejected with a readable error instead
    of being folded into "not found": returning ``null`` for ``log(id: "abc")`` would tell a client
    with a broken id-building bug that the row simply does not exist, and it would go on believing
    that forever.

    Raises:
        GraphQLError: If ``raw`` is not a run of digits. Raising the GraphQL error type (rather
            than a ``ValueError`` the executor would wrap) keeps the message intact and the
            response a normal ``errors`` envelope — never a 500.
    """
    if not raw.isascii() or not raw.isdigit():
        raise GraphQLError(
            f"invalid LogEntry id {raw!r}: ids issued by this server are decimal integers"
        )
    return int(raw)


@strawberry.type
class Query:
    """The read surface. C4 adds ``logStats``; C10/C11 add the e-commerce entry points."""

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
        """
        context = info.context
        query = to_log_query(filters, context.settings)

        async with context.repository() as repository:
            rows = await repository.list_logs(query)
            # Projected INSIDE the block. `expire_on_commit=False` means detached instances stay
            # usable, so doing it outside would also work today — but it would work by relying on
            # a session-configuration detail rather than on the object still being attached, and
            # that is the kind of dependency that breaks silently when somebody adds a commit.
            return [LogEntry.from_orm(row) for row in rows]

    @strawberry.field
    async def log(
        self,
        info: strawberry.Info[Context, None],
        id: strawberry.ID,  # noqa: A002 - the spec names this argument `id`
    ) -> Optional[LogEntry]:
        """One entry by id, or ``null`` when there is no such row.

        ``null`` with **no** ``errors`` entry for a miss: absence is an ordinary answer to
        "is there a row with this id", not a failure. (C4 gives genuinely exceptional lookups a
        ``NOT_FOUND`` code; a single-entity fetch is not one of them.) Routed through
        :meth:`~src.db.repository.LogRepository.get_by_id` so it goes through the session identity
        map — within one operation the same entry is often reached twice, once through ``logs`` and
        once through another entry's ``related_logs``.
        """
        log_id = _parse_log_id(id)

        async with info.context.repository() as repository:
            row = await repository.get_by_id(log_id)
            return None if row is None else LogEntry.from_orm(row)

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
                raise GraphQLError(str(exc), extensions={"code": "INVALID_CURSOR"}) from exc

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
