"""The fixed corpus every integration test grades itself against, plus the oracle helpers.

Not a test module (pytest collects ``test_*.py`` only) and not a conftest — it holds no fixtures.
It exists so the *values* and the *projections* that define "what the database should contain" are
written once, and so the fixtures in ``conftest.py`` and the assertions in each test module are
looking at the same corpus rather than at two that happen to agree today.

.. rubric:: What makes the integration suite worth having

Almost every assertion computes its expected answer **in Python**, by running the same filter over
the objects :func:`~src.generators.generate_log_records` returned, and then asserts the database
(or the GraphQL layer above it) returned exactly that set. That is a comparison between two
independent computations. The tempting alternative — asserting that a ``service`` filter returns
rows whose service is the one asked for — is a tautology: it passes against an implementation that
returns one arbitrary matching row and silently drops the other forty.

That only works because the generator is pure: fixed seed, fixed anchor instant, no wall clock.
See its module docstring for the contract.

.. rubric:: And the pairing of the two corpora, because they are only an oracle together

:class:`CorrelatedCorpus` carries the seeded log corpus, the seeded event corpus **and** the trace
ids that deliberately join them (:data:`~src.generators.ORDER_TRACE_LOG_RATIO`). It is here rather
than in a test module because the log half has to be regenerated with the event half's trace ids or
it is not the corpus the seeder wrote — a mistake that would fail every assertion graded against it,
loudly but a long way from the cause.

.. rubric:: It also holds the probes that ask PostgreSQL something Python cannot answer

:func:`metadata_storage` is here rather than in one test module because two of them need it
(``test_db_store.py`` for the seeding and repository write paths, ``test_graphql_mutation.py`` for
the ``createLog`` path) and a second copy would be a second chance to write the check wrong — in a
way that, by construction, passes.

.. rubric:: And the statement counter, for the same reason

:func:`count_statements` records every SQL statement PostgreSQL is actually sent while a block
runs. It is what turns "DataLoader prevents N+1 queries" (spec §5) from a comment into an
assertion, and C7 needs the identical instrument to prove a cache hit issues **zero** statements.
One implementation, so the two commits cannot end up measuring subtly different things.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeVar

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from src.db.models import LogEntryORM, LogRecord
from src.generators import EventCorpus, order_traces_with_logs

#: Rows in the fixed corpus.
#:
#: 1200 rather than a couple of hundred because the level mix has a 1% CRITICAL tail: at 300 rows
#: the expected count is three, and "does the CRITICAL filter return the right rows" would be one
#: unlucky seed away from asserting that an empty set equals an empty set. At 1200 the thinnest
#: bucket is a dozen rows and every filter test grades a real subset.
CORPUS_SIZE = 1200

#: RNG seed. Same seed, same corpus, in any process — which is what lets a test regenerate the
#: expected answer instead of asking the database twice.
SEED = 20260725

#: The newest instant in the generated corpus. A CONSTANT, not ``now()``: the oracle a test
#: computes and the rows the database holds must describe the same corpus no matter when the suite
#: runs.
ANCHOR = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)

#: Orders in the fixed e-commerce corpus (C10).
#:
#: 120 rather than a handful for the same reason ``CORPUS_SIZE`` is 1200: the rosters have thin
#: buckets. Seven lifecycle paths are drawn uniformly, so 120 orders expects ~17 of each — enough
#: that "every declared status appears" is a statement about the generator rather than about luck,
#: and enough that a status filter grades a real subset. It also stays well under
#: ``db_settings.max_query_limit`` (``CORPUS_SIZE * 5``), so a test can pull a whole stream in one
#: query and compare it against the oracle as a set rather than as a prefix.
EVENT_CORPUS_ORDERS = 120

_T = TypeVar("_T")

#: One of the three C10 event record types. Generic rather than a union so
#: :func:`matching_events` returns the *same* record type it was handed, which is what lets a test
#: compare its result against a stream without a cast.
_R = TypeVar("_R")


@dataclass(frozen=True, slots=True)
class CorrelatedCorpus:
    """Both seeded corpora **and** the trace ids that deliberately join them.

    One object rather than two fixtures, because the two halves are only an oracle *together*: the
    log corpus is generated from the event corpus's trace ids (see
    :data:`~src.generators.ORDER_TRACE_LOG_RATIO`), so a test that regenerated the log corpus
    without them would be grading the database against a different corpus and would fail for a
    reason that has nothing to do with the code under test.

    It exists at all because ``Query.correlatedEvents(traceId:)`` returning all **four**
    ``__typename``s is the flagship claim of spec §3 Feature Area A, and proving it needs a store
    holding both corpora at once — which no other fixture does.
    """

    logs: list[LogRecord]
    events: EventCorpus

    @property
    def shared_traces(self) -> tuple[str, ...]:
        """The order traces the log corpus files log lines under. **Declared, not discovered.**

        Read from :func:`~src.generators.order_traces_with_logs` rather than by intersecting the two
        corpora, which is the whole point: a test that searched for a trace carrying both kinds of
        row would silently pass on an accidental collision — the thing this correlation replaced —
        and would silently *skip* on a seed that produced none.
        """
        return order_traces_with_logs(self.events.trace_ids())

    def log_only_trace(self) -> str:
        """A trace carried by log rows and by no order — the independent population C5 needs.

        Sorted before picking so the choice is stable across runs rather than dependent on set
        iteration order, and asserted non-empty here so a corpus that correlated *everything* fails
        as a missing precondition rather than as a confusing assertion three lines later.
        """
        order_traces = set(self.events.trace_ids())
        candidates = sorted(
            {
                record.trace_id
                for record in self.logs
                if record.trace_id is not None and record.trace_id not in order_traces
            }
        )
        assert candidates, "the corpus must keep log-only traces; relatedLogs depends on them"
        return candidates[0]


def run_sync(coro: Awaitable[_T]) -> _T:
    """Run ``coro`` on a private event loop, leaving the ambient loop policy untouched.

    Used by the synchronous session-scoped schema fixture. :func:`asyncio.run` would also work, but
    it *sets* and then clears the current event loop, and pytest-asyncio manages that same global
    around every test — so this creates a loop, uses it, and closes it without ever touching
    :func:`asyncio.set_event_loop`. Anything opened inside the coroutine (an engine, its
    connections) must also be closed inside it, since the loop does not outlive the call.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)  # type: ignore[arg-type]
    finally:
        loop.close()


def newest_first(records: list[LogRecord]) -> list[LogRecord]:
    """Put a generated corpus into the order the database returns it in.

    The generator emits oldest-first with strictly increasing timestamps, and seeding inserts in
    that order — so ``BIGSERIAL`` ids ascend with time and ``ORDER BY timestamp DESC, id DESC`` is
    exactly the reverse of generation order. Reversing (rather than re-sorting) states that
    relationship instead of re-deriving it, so a test would notice if it ever stopped holding.
    """
    return list(reversed(records))


def matching(
    records: list[LogRecord], predicate: Callable[[LogRecord], bool]
) -> list[LogRecord]:
    """The subset of the corpus a filter should select, newest first."""
    return newest_first([record for record in records if predicate(record)])


def as_records(rows: list[LogEntryORM]) -> list[LogRecord]:
    """Project database rows onto the identity-free value objects the oracle is made of."""
    return [LogRecord.from_orm_row(row) for row in rows]


# =================================================================================================
# Counting the statements the database actually received
#
# WHY AN EVENT LISTENER AND NOT A SPY ON THE REPOSITORY: a spy proves the repository was called
# once, which is a statement about the code the test's author already read. `before_cursor_execute`
# fires inside SQLAlchemy's execution path, once per statement handed to the DBAPI cursor, so what
# it counts is what PostgreSQL was actually asked — including the statements a lazy load, an
# identity-map miss or a helpful `.refresh()` would emit behind the resolver's back. Those are
# exactly the N+1s that survive a mocked test.
#
# WHAT IT DOES NOT SEE, stated so a count is not over-read: transaction control (BEGIN / COMMIT /
# ROLLBACK go through the dialect on the raw connection, not through a cursor) and `pool_pre_ping`'s
# liveness check (which asyncpg answers at the driver level). Both absences are what make an exact
# count like "two statements" stable rather than a hostage to connection-pool timing.
# =================================================================================================


@dataclass
class StatementCounter:
    """Every SQL statement executed while a :func:`count_statements` block was open."""

    statements: list[str] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.statements)

    def matching(self, *fragments: str) -> list[str]:
        """Statements containing **all** of ``fragments``, compared case-insensitively.

        Case-insensitive because the two halves of a statement do not agree on case: SQLAlchemy
        renders keywords upper (``IN``, ``ORDER BY``) and identifiers lower (``log_entries``,
        ``trace_id``), so a test that hardcoded either would be pinning a rendering detail rather
        than the query it means.
        """
        needles = [fragment.lower() for fragment in fragments]
        return [
            statement
            for statement in self.statements
            if all(needle in statement.lower() for needle in needles)
        ]

    def count(self, *fragments: str) -> int:
        """How many statements match every fragment. ``count()`` with no fragments is the total."""
        return len(self.matching(*fragments))

    def report(self) -> str:
        """The recorded statements, one per line, for an assertion message.

        A failing count is unreadable without this: "expected 2, got 27" says nothing about which
        query multiplied, and the whole diagnosis is in the shape of the 27.
        """
        if not self.statements:
            return "(no statements)"
        return "\n".join(
            f"  {index}. {' '.join(statement.split())[:160]}"
            for index, statement in enumerate(self.statements, start=1)
        )


@contextmanager
def count_statements(engine: AsyncEngine) -> Iterator[StatementCounter]:
    """Record every statement executed on ``engine`` for the duration of the block.

    Attached to ``engine.sync_engine``: SQLAlchemy's event system is defined over the synchronous
    core, and the async engine is a façade over it, so this is where the async engine's statements
    actually surface. Removed in a ``finally``, because a listener left attached would keep
    appending to a dead counter for the rest of the session — and the engine outlives the block.
    """
    counter = StatementCounter()

    def _record(
        conn: Any,  # noqa: ANN401 - SQLAlchemy's event signature, positional and untyped
        cursor: Any,  # noqa: ANN401
        statement: str,
        parameters: Any,  # noqa: ANN401
        context: Any,  # noqa: ANN401
        executemany: bool,
    ) -> None:
        counter.statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", _record)
    try:
        yield counter
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", _record)


def matching_events(
    records: list[_R], predicate: Callable[[_R], bool]
) -> list[_R]:
    """The subset of one event stream a filter should select, **newest first**.

    The event-stream twin of :func:`matching`, and it relies on the same property: every generator
    in this project emits oldest-first with ``(timestamp, id)`` strictly increasing down the list
    (see :meth:`src.generators.EventCorpus`), and seeding inserts in that order — so the database's
    ``ORDER BY timestamp DESC, id DESC`` is exactly ``reversed(...)``. Reversing rather than
    re-sorting states that relationship instead of re-deriving it, so a test notices if it ever
    stops holding.
    """
    return list(reversed([record for record in records if predicate(record)]))


async def metadata_storage(session: AsyncSession, log_id: int) -> tuple[bool, str | None]:
    """Ask **PostgreSQL** how one row's ``metadata`` is stored: ``(is_sql_null, json_type)``.

    Python cannot answer this question. A JSONB column can hold the JSON scalar ``null``, which is
    not SQL ``NULL`` — and asyncpg deserialises *both* of them to the Python ``None``. So
    ``row.metadata_ is None`` is true in either case and an assertion built on it cannot fail.

    These two expressions can:

    * ``metadata IS NULL`` is true only for SQL ``NULL``. For the JSONB value ``'null'`` the column
      is not null at all, so it is false.
    * ``jsonb_typeof(metadata)`` returns SQL ``NULL`` (``None`` here) for SQL ``NULL``, and the
      **string** ``'null'`` for the JSON scalar — the one place the two are told apart by name.
    """
    row = (
        await session.execute(
            text(
                "SELECT metadata IS NULL AS is_sql_null, jsonb_typeof(metadata) AS json_type "
                "FROM log_entries WHERE id = :id"
            ),
            {"id": log_id},
        )
    ).one()
    return bool(row.is_sql_null), row.json_type


async def event_metadata_counts(session: AsyncSession, table: str) -> tuple[int, int, int]:
    """``(sql_nulls, json_nulls, objects)`` for one event table's ``metadata`` column.

    The same question :func:`metadata_storage` asks about one log row, asked about a whole event
    table — because the C10 seeding path writes those tables through the identical Core multi-row
    INSERT, and the identical ``none_as_null=True`` flag is what keeps a Python ``None`` a SQL NULL
    rather than the JSONB scalar ``'null'``. Python cannot tell the two apart (asyncpg deserialises
    both to ``None``), so the assertion has to be made in SQL or it cannot fail.

    ``table`` is interpolated rather than bound because a table name cannot be a bind parameter in
    PostgreSQL. It is safe here and only here: every caller passes a literal from this test suite,
    never anything a client can influence.
    """
    if table not in {"order_events", "payment_events", "user_events"}:
        raise ValueError(f"unexpected table {table!r}")
    row = (
        await session.execute(
            text(
                "SELECT count(*) FILTER (WHERE metadata IS NULL) AS sql_nulls, "
                "count(*) FILTER (WHERE jsonb_typeof(metadata) = 'null') AS json_nulls, "
                "count(*) FILTER (WHERE jsonb_typeof(metadata) = 'object') AS objects "
                f"FROM {table}"
            )
        )
    ).one()
    return int(row.sql_nulls), int(row.json_nulls), int(row.objects)
