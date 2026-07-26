"""The published object types: ``LogEntry`` and the cursor-connection view of it.

.. rubric:: Naming: auto camel-casing stays on, and the spec is the reason

Strawberry camel-cases field and argument names by default (``StrawberryConfig(auto_camel_case=
True)``), so the Python ``trace_id`` is published as ``traceId``, ``start_time`` as ``startTime``,
``search_text`` as ``searchText``, ``total_logs`` as ``totalLogs`` and ``logs_connection`` as
``logsConnection``. **Do not turn this off.** The spec's own sample operations are written in that
casing — ``{ logStats { totalLogs errorCount services } }`` and ``createLog(logData: …)`` — so
disabling it would not be a cosmetic preference, it would make the spec's verification commands
fail to validate. The C13 React client is generated against the same names.

The one field where this matters in the other direction is ``metadata``: it has no underscore, so
camel-casing leaves it alone, and it is published as ``metadata`` exactly as the spec asks. Note
that the *storage* attribute is ``metadata_`` (``metadata`` is reserved on a SQLAlchemy declarative
class — see :class:`src.db.models.LogEntryORM`), and :meth:`LogEntry.from_orm` is the single place
that translation happens.

.. rubric:: Why the connection types live in this module

``LogEdge``, ``PageInfo`` and ``LogConnection`` are not a separate concern — every one of them
exists solely to wrap ``LogEntry``, none of them is meaningful without it, and a
``src/graphql/connection.py`` holding three field-only dataclasses would be a module whose entire
content is a forward reference to this one. They are kept together, and the pagination *policy*
(what a cursor is, how a page is fetched) is what got its own modules: :mod:`src.graphql.cursor`
and the keyset builder in :mod:`src.db.repository`.

.. rubric:: A note on annotation style

Nullable fields are spelled ``Optional[X]`` rather than ``X | None``. These annotations are not
decoration — Strawberry **evaluates** them at schema-construction time to build the GraphQL type,
so the spelling is executable code rather than a hint, and ``Optional[...]`` is the form with the
widest support across the scalar wrappers Strawberry ships (``strawberry.scalars.JSON`` is a
wrapper object, not a class, so ``JSON | None`` depends on that wrapper implementing ``__or__``).
Consistency is the point: one spelling everywhere beats two that differ per field type.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import strawberry
from strawberry.scalars import JSON

from src.db.models import LogEntryORM
from src.graphql.enums import LogLevel


@strawberry.type
class LogEntry:
    """One log line, as published. Field-for-field the spec's §2 item 15 shape."""

    id: strawberry.ID
    timestamp: datetime
    service: str
    level: LogLevel
    message: str
    metadata: Optional[JSON]
    trace_id: Optional[str]

    # === C5 ===  `related_logs` lands here:
    #
    #     @strawberry.field
    #     async def related_logs(self, info: strawberry.Info[Context, None]) -> list[LogEntry]:
    #         """Every entry sharing this one's trace_id; [] when trace_id is null."""
    #         if self.trace_id is None:
    #             return []
    #         rows = await info.context.loaders.logs_by_trace_id.load(self.trace_id)
    #         return [LogEntry.from_orm(row) for row in rows]
    #
    # It is a FIELD RESOLVER rather than a value computed in `from_orm`, and that is the whole
    # requirement (spec §2 items 17 and 28): a client that asks for `{ logs { id } }` must not pay
    # for a second query per row. Strawberry only invokes a field resolver when the field appears
    # in the selection set, so the cost is opt-in — which is exactly what the DataLoader in C5 is
    # then batching. Computing related entries eagerly here would defeat both requirements at once
    # and would be invisible in any test that only checks the response shape.

    @classmethod
    def from_orm(cls, row: LogEntryORM) -> LogEntry:
        """Project a stored row onto the published type. **The only place that mapping exists.**

        Two translations happen here and nowhere else:

        * ``metadata_`` -> ``metadata``. The trailing underscore is a SQLAlchemy constraint
          (``metadata`` is the declarative table registry), not something the API should ever see.
        * ``int`` id -> :class:`strawberry.ID`. GraphQL's ``ID`` serialises as a string; a bare
          ``int`` would publish ``id: Int`` and change the contract.

        C4's ``createLog``, C5's ``related_logs``, C6's ``logStream`` and C7's cache-hit
        reconstruction all funnel through this classmethod. A second copy of the mapping is how
        two representations of one row drift — one path returning ``metadata`` and another
        returning ``null`` for it is the kind of bug that survives a full test suite because every
        test happens to exercise only one of the two paths.

        ``LogLevel(row.level)`` raises :class:`ValueError` for a stored level outside the enum.
        That is deliberate and loud: the enum is the published contract, the column is a plain
        ``String``, and a row that cannot be represented is a data-integrity problem rather than
        something to paper over with a default severity. C4's error taxonomy gives it a typed
        code; until then it surfaces as a GraphQL error rather than as a wrong answer.
        """
        return cls(
            id=strawberry.ID(str(row.id)),
            timestamp=row.timestamp,
            service=row.service,
            level=LogLevel(row.level),
            message=row.message,
            metadata=row.metadata_,
            trace_id=row.trace_id,
        )


@strawberry.type
class PageInfo:
    """Relay-shaped page metadata for :class:`LogConnection`.

    ``hasPreviousPage`` is reported as "this request supplied an ``after`` cursor". The Relay
    specification only obliges a server to compute it when paginating backwards (``last``/
    ``before``), which this connection does not support, and returning a constant ``false`` would
    be a worse lie than the approximation: a client on page 4 would be told there is nothing
    behind it.
    """

    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str]
    end_cursor: Optional[str]


@strawberry.type
class LogEdge:
    """One entry plus the cursor that resumes iteration immediately after it."""

    cursor: str
    node: LogEntry


@strawberry.type
class LogConnection:
    """A cursor-paginated window over the same filtered result ``Query.logs`` returns.

    ``totalCount`` is how many rows match the **filters**, ignoring both the page size and the
    ``after`` cursor — it answers "how big is this result set", which is what a client renders as
    "1–37 of 1200". It deliberately does not answer "how many are left", which a client can
    compute and which would make the number change on every page.
    """

    edges: list[LogEdge]
    page_info: PageInfo
    total_count: int
