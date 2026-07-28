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
from src.db.repository import LogStatsResult
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

    # `related_logs` is a FIELD RESOLVER rather than a value computed in `from_orm`, and that is
    # the whole requirement (spec §2 items 17 and 28): a client that asks for `{ logs { id } }`
    # must not pay for a correlated lookup per row. Strawberry only invokes a field resolver when
    # the field appears in the selection set, so the cost is opt-in — which is exactly what C5's
    # DataLoader then batches. Computing related entries eagerly here would defeat both
    # requirements at once and would be invisible in any test that only checks the response shape.
    #
    # NOTE THE BARE `strawberry.Info` ANNOTATION BELOW. It is not laziness and it is not a lost
    # type parameter: `strawberry.Info[Context, None]` would require `Context` to be importable
    # *here*, at schema-construction time, and this module is imported BY the loaders that
    # `Context` imports (types -> loaders -> context -> types). Strawberry resolves the parameter
    # annotation whether or not it carries type arguments, so dropping them breaks the cycle at no
    # runtime cost. Every other resolver in the project keeps the parameterised form, because none
    # of them sits underneath `Context` in the import graph.
    @strawberry.field(
        description=(
            "Every OTHER entry sharing this entry's traceId, newest first. The entry itself is "
            "deliberately excluded: a uniquely-traced entry would otherwise answer with itself, "
            "which is not what any caller means by 'related'. Empty when traceId is null. "
            "Batched across the whole selection set by a per-operation DataLoader, so N entries "
            "cost one query rather than N."
        )
    )
    async def related_logs(self, info: strawberry.Info) -> list[LogEntry]:
        """Every other entry carrying this entry's ``trace_id`` — spec §2 item 17.

        .. rubric:: The entry itself is excluded, and that is a decision

        Read literally, "all logs sharing the same trace_id" includes the row being resolved from.
        That reading makes the field almost useless: a client rendering "related entries" under a
        log line would get that same line back at the top of its own list, and an entry whose trace
        has no other members would answer ``[itself]`` rather than ``[]`` — indistinguishable, to a
        client counting results, from a correlation that actually found something. So *related*
        here means *other*, the exclusion is stated in the field description so it reaches the SDL
        and GraphiQL, and a test pins it.

        The exclusion happens **here rather than in the loader** because the loader's result is
        shared by every entry in the group and each of them excludes a different row. Filtering
        inside the batch would mean one query per parent, which is the thing this field exists to
        demonstrate the absence of.

        .. rubric:: A null trace_id costs ZERO round trips

        The early return is above every ``await``: an entry with no correlation id never reaches
        the loader, so a selection over a hundred untraced entries issues no batch at all — not one
        batch that returns nothing. That is observable (the integration suite counts statements)
        and it is the difference between "cheap" and "free" on the ~40% of the corpus that carries
        no trace id.
        """
        if self.trace_id is None:
            return []

        group = await info.context.loaders.logs_by_trace_id.load(self.trace_id)
        # `id` is a GraphQL ID, i.e. a string on both sides — `LogEntry.from_orm` is the only
        # constructor of these objects and it always renders the primary key the same way, so the
        # comparison cannot be an int/str mismatch that silently excludes nothing.
        return [entry for entry in group if entry.id != self.id]

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
        something to paper over with a default severity.

        C4 settled what the client sees when that happens, and it is **not** a member of the
        :class:`~src.graphql.errors.ErrorCode` taxonomy: a stored level the schema cannot express
        is a *server* fault, not something a client can act on, so it is masked as
        ``INTERNAL_ERROR`` with the real ``ValueError`` and its trace going to the server log. The
        codes exist to tell a client what to change about its request; there is nothing to change
        here.
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


# =================================================================================================
# logStats — spec §2 item 23
#
# ############################################################################################
# ##  READ THIS BEFORE "IMPROVING" `services` INTO A LIST OF OBJECTS.                        ##
# ############################################################################################
#
# Spec §5 lists this as a literal acceptance command:
#
#     { logStats { totalLogs errorCount services } }
#
# `services` is selected there as a LEAF — no sub-selection. GraphQL requires a sub-selection on
# every field of an object type and FORBIDS one on every field of a scalar type, so the moment
# `services` becomes `[ServiceCount!]!` that document stops validating: "Field 'services' of type
# '[ServiceCount!]!' must have a selection of subfields." The acceptance command would break while
# every test asserting on the richer shape stayed green — the failure would surface as a support
# ticket, not as a red build.
#
# It is still true that a bare list of names is a thin answer for a dashboard. Both things are
# satisfied by publishing BOTH, from ONE query:
#
#     services:         [String!]!         <- the spec's field. Leaf. Selectable exactly as written.
#     serviceBreakdown: [ServiceCount!]!   <- the useful one. `{ service count }`.
#
# `services` is DERIVED from `serviceBreakdown` in `from_result` (it is literally the `service`
# column of the same list, in the same order), so the two cannot disagree about which services
# exist, and there is no second aggregation to keep in sync. `tests/unit/test_graphql_schema.py`
# pins `services: [String!]!`, and the integration suite executes the spec document verbatim.
# =================================================================================================


@strawberry.type
class ServiceCount:
    """How many entries one service contributed to a window."""

    service: str
    count: int


@strawberry.type
class LevelCount:
    """How many entries one severity contributed to a window."""

    level: LogLevel
    count: int


@strawberry.type
class LogStats:
    """The aggregate summary ``Query.logStats`` returns. Every number computed in SQL.

    The spec (§2 item 23) requires ``totalLogs``, ``errorCount`` and ``services`` *at minimum*. The
    other four exist because a stats endpoint that answers only "how many" forces a dashboard to
    issue a second, third and fourth query to draw anything — which is the exact multiplication of
    round trips this project is a demonstration against. All seven come out of the same two
    statements, so the extras are free.

    Attributes:
        total_logs: Rows matching the window. Exact: no limit is applied to an aggregate.
        error_count: How many of them are ``ERROR``. **ERROR only** — see
            :data:`src.db.repository.ERROR_LEVEL` for why CRITICAL is not folded in, and use
            ``levelBreakdown`` to sum severities yourself.
        services: Distinct service names, busiest first. The spec's field, kept a leaf.
        service_breakdown: The same services with their counts, busiest first, ties broken by
            name so the ordering is total and a response is diff-stable.
        level_breakdown: Counts by severity, in ascending severity order (the ``LogLevel``
            declaration order). Only severities that actually occurred appear; an absent one had
            zero entries.
        earliest: Oldest matching ``timestamp``, ``null`` when nothing matched. This is the span
            the data **actually** covers, not the window that was asked for — "the newest entry is
            40 minutes old" is a different and more useful fact than "I asked for 24 hours".
        latest: Newest matching ``timestamp``, ``null`` when nothing matched.
    """

    total_logs: int
    error_count: int
    services: list[str]
    service_breakdown: list[ServiceCount]
    level_breakdown: list[LevelCount]
    earliest: Optional[datetime]
    latest: Optional[datetime]

    @classmethod
    def from_result(cls, result: LogStatsResult) -> LogStats:
        """Project the repository's ``(service, level)`` cross-tabulation onto the published shape.

        The two breakdowns are two *marginals* of one cross-tabulation, folded here rather than
        asked of the database twice. That is what makes them consistent by construction: both sum
        to the same number because both are sums over the same buckets.

        Ordering is applied here rather than in SQL because the two views want different orders
        from the same rows — services by descending volume (what a dashboard leads with), levels
        by ascending severity (what a legend reads in). Sorting at most ``services x levels``
        entries in Python is nothing; asking PostgreSQL for the same rows twice in two orders is a
        second scan.
        """
        per_service: dict[str, int] = {}
        per_level: dict[str, int] = {}
        for bucket in result.breakdown:
            per_service[bucket.service] = per_service.get(bucket.service, 0) + bucket.entries
            per_level[bucket.level] = per_level.get(bucket.level, 0) + bucket.entries

        # Busiest first, ties broken by name: without the name tiebreak two services with equal
        # counts could swap places between two identical requests, which makes a response
        # needlessly unstable and a test needlessly flaky.
        service_breakdown = [
            ServiceCount(service=service, count=count)
            for service, count in sorted(per_service.items(), key=lambda item: (-item[1], item[0]))
        ]

        # Ascending severity — `LogLevel` is declared in that order, so enumerating it IS the
        # ordering, and a severity added to the enum lands in the right place automatically.
        severity = {member.value: index for index, member in enumerate(LogLevel)}
        level_breakdown = [
            LevelCount(level=LogLevel(level), count=count)
            for level, count in sorted(
                per_level.items(), key=lambda item: severity.get(item[0], len(severity))
            )
        ]

        return cls(
            total_logs=result.total_logs,
            error_count=result.error_count,
            # Derived, never computed separately. See the block comment above.
            services=[entry.service for entry in service_breakdown],
            service_breakdown=service_breakdown,
            level_breakdown=level_breakdown,
            earliest=result.earliest,
            latest=result.latest,
        )
